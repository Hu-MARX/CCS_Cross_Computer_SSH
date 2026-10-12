import tempfile
import unittest
from pathlib import Path

from controller.domain import DomainError, ExperimentSpec, RunStatus, utc_now
from controller.configuration import spec_digest
from controller.orchestrator import Orchestrator
from controller.remote import FakeRemoteRunner, RemoteRunnerError, RemoteSnapshot
from controller.repository import RunRepository
from controller.workflow import WorkflowEngine


CHECKPOINT = {"path": "/synthetic/model.pt", "sha256": "a" * 64}


def experiment(kind, *, checkpoint=None):
    task = {"kind": kind, "inputs": []}
    if checkpoint is not None:
        task["checkpoint"] = checkpoint
    return {
        "schema_version": 1,
        "name": "synthetic " + kind,
        "project": {},
        "runtime": {},
        "command": {"argv": ["synthetic-only"]},
        "task": task,
    }


def study(*, checkpoint=CHECKPOINT, max_runs=2):
    return {
        "schema_version": 1,
        "name": "synthetic study",
        "research_question": "Does the synthetic candidate pass its held-out test?",
        "planning": {
            "hypotheses": ["Synthetic loss decreases"],
            "statistical_plan": {"primary_metric": "test/score"},
        },
        "budget": {"max_runs": max_runs},
        "stages": [
            {
                "id": "train",
                "name": "Train candidate",
                "kind": "train",
                "spec": experiment("train"),
                "depends_on": [],
                "requires_approval": False,
            },
            {
                "id": "test",
                "name": "Held-out final test",
                "kind": "test",
                "spec": experiment("test", checkpoint=checkpoint),
                "depends_on": ["train"],
                "requires_approval": True,
            },
        ],
    }


def bound_study(*, artifact_path="/synthetic/remote_0001/model.pt"):
    raw = study(checkpoint=None)
    raw["stages"][1]["bindings"] = [{
        "role": "checkpoint",
        "from_stage": "train",
        "selector": {"path": artifact_path},
    }]
    return raw


def recommendation(plan_sha256, run_id, *, decision="PROCEED", observation_state="fresh",
                   observed_at=None, skills_used=None, key="rec-v1", artifact_bindings_sha256=None):
    result = {
        "schema_version": 1,
        "idempotency_key": key,
        "stage_id": "test",
        "plan_sha256": plan_sha256,
        "proposer": {"id": "codex-local", "kind": "agent"},
        "decision": decision,
        "summary": "The completed synthetic training supports held-out evaluation.",
        "rationale": "Training completed and its result snapshot passed the planned checks.",
        "confidence": 0.9,
        "skills_used": ["nature-statistics"] if skills_used is None else skills_used,
        "evidence": [{
            "run_id": run_id,
            "result_sha256": "b" * 64,
            "observed_at": observed_at or utc_now(),
            "observation_state": observation_state,
        }],
        "limitations": ["Synthetic evidence only"],
    }
    if artifact_bindings_sha256 is not None:
        result["artifact_bindings_sha256"] = artifact_bindings_sha256
    return result


class OfflineRemote(FakeRemoteRunner):
    offline = False

    def snapshot(self, run_id):
        if self.offline:
            raise RemoteRunnerError("SSH_UNREACHABLE", "synthetic disconnect", retryable=True)
        return super().snapshot(run_id)

    def events(self, run_id, after_seq):
        if self.offline:
            raise RemoteRunnerError("SSH_UNREACHABLE", "synthetic disconnect", retryable=True)
        return super().events(run_id, after_seq)


class ArtifactOfflineRemote(FakeRemoteRunner):
    def artifacts(self, run_id, path=None):
        raise RemoteRunnerError("SSH_UNREACHABLE", "synthetic artifact disconnect", retryable=True)


class DecisionPackageRemote(FakeRemoteRunner):
    def __init__(self):
        super().__init__()
        self.checkpoint_calls = 0

    def snapshot(self, run_id):
        base = super().snapshot(run_id)
        if base.status != RunStatus.SUCCEEDED:
            return base
        return RemoteSnapshot(
            run_id, base.status, base.exit_code, utc_now(),
            observation={
                "observed_at": "2026-09-05T12:00:00Z",
                "observation_state": "fresh",
                "checkpoint": {
                    "state": "committed",
                    "latest": {
                        "id": "ckpt_" + "1" * 32,
                        "created_at": "2026-09-05T11:59:00Z",
                        "sha256": "c" * 64,
                        "size": 1234,
                        "state": {"global_step": 5000, "epoch": 49, "safe_to_resume": True},
                    },
                },
            },
            progress={
                "updated_at": "2026-09-05T12:00:00Z", "phase": "validation_finished", "step": 5000,
                "metrics": {"val/mAP_50_95": 0.42, "val/loss": 1.25},
            },
            evidence_gate={"verified": True, "state": "passed", "manifest_sha256": "d" * 64},
            evidence_runtime={
                "schema_version": 2, "state": "closed", "updated_at": "2026-09-05T12:00:00Z",
                "execution_integrity": {"state": "no_violation_observed", "issue_count": 0},
                "deployment": {"state": "changed", "issue_count": 9},
                "observer": {"state": "closed", "health": "healthy",
                             "coverage": "selected_scope_checked", "warning_count": 0},
                "drift_detected": False, "issue_count": 0,
            },
            source_snapshot={"files": 17, "root": "/synthetic/source"},
        )

    def artifacts(self, run_id, path=None):
        self.checkpoint_calls += 1
        return [{"path": path, "size": 1234, "sha256": "c" * 64}]


class WorkflowTests(unittest.TestCase):
    def make_engine(self, directory, remote=None):
        path = Path(directory) / "state.db"
        remote = remote or FakeRemoteRunner()
        orchestrator = Orchestrator(RunRepository(path), remote, worker="synthetic")
        return WorkflowEngine(path, orchestrator), remote

    def create(self, engine, raw=None, key="study-v1"):
        return engine.execute({"action": "create", "idempotency_key": key, "study": raw or study()})

    def test_successful_dependency_chain_submits_each_stage_once(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            study_id = created["study"]["id"]
            digest = created["study"]["plan_sha256"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            train_run = running["stages"][0]["run_id"]
            self.assertEqual(running["stages"][0]["status"], "RUNNING")
            self.assertIsNone(running["stages"][1]["run_id"])

            remote.advance(train_run, RunStatus.SUCCEEDED, exit_code=0)
            engine.execute({"action": "reconcile", "study_id": study_id})
            recommended = engine.execute({"action": "recommend", "study_id": study_id,
                                          "recommendation": recommendation(digest, train_run)})
            recommendation_id = recommended["recommendations"][0]["id"]
            engine.execute({"action": "approve", "study_id": study_id, "stage_id": "test",
                            "plan_sha256": digest, "recommendation_id": recommendation_id,
                            "approved_by": "test-user"})
            testing = engine.execute({"action": "reconcile", "study_id": study_id})
            test_run = testing["stages"][1]["run_id"]
            self.assertIsNotNone(test_run)
            self.assertEqual(len(remote.runs), 2)

            engine.execute({"action": "reconcile", "study_id": study_id})
            self.assertEqual(len(remote.runs), 2)
            remote.advance(test_run, RunStatus.SUCCEEDED, exit_code=0)
            finished = engine.execute({"action": "reconcile", "study_id": study_id})
            self.assertEqual(finished["study"]["status"], "SUCCEEDED")

    def test_existing_run_can_be_adopted_without_submitting_or_restarting_it(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            existing = engine.orchestrator.submit(
                ExperimentSpec.from_dict(experiment('train')),
                idempotency_key='preexisting-run',
            )
            remote.advance(existing.id, RunStatus.RUNNING)
            request = {
                "action": "adopt_run",
                "study_id": created["study"]["id"],
                "stage_id": "train",
                "run_id": existing.id,
                "run_spec_sha256": spec_digest(experiment("train")),
                "idempotency_key": "adopt-existing-train-v1",
                "adopted_by": "test-user",
            }

            first = engine.execute(request)
            second = engine.execute(request)

            self.assertEqual(first["stages"][0]["status"], "RUNNING")
            self.assertEqual(first["stages"][0]["run_id"], existing.id)
            self.assertEqual(first["stages"][0]["run_link"]["kind"], "adopted")
            self.assertEqual(second["stages"][0]["run_id"], existing.id)
            self.assertEqual(len(remote.runs), 1)
            self.assertEqual(len([event for event in second["events"]
                                  if event["kind"] == "stage_run_adopted"]), 1)
            conflict = dict(request)
            conflict["adopted_by"] = "different-user"
            with self.assertRaises(DomainError):
                engine.execute(conflict)

    def test_running_stage_decision_package_waits_without_scanning_checkpoints(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory, DecisionPackageRemote())
            created = self.create(engine)
            running = engine.execute({
                "action": "reconcile", "study_id": created["study"]["id"],
            })
            remote.advance(running["stages"][0]["run_id"], RunStatus.RUNNING)

            package = engine.execute({
                "action": "prepare_decision_package",
                "study_id": created["study"]["id"],
                "stage_id": "train",
                "checkpoint_directory": "/synthetic/checkpoints",
            })

            self.assertEqual(package, {
                "schema_version": 1,
                "study_id": created["study"]["id"],
                "stage_id": "train",
                "run_id": running["stages"][0]["run_id"],
                "readiness": "waiting_for_terminal",
                "run_status": "RUNNING",
                "reason": "run_not_terminal",
            })
            self.assertEqual(len(remote.runs), 1)
            self.assertEqual(remote.checkpoint_calls, 0)

    def test_succeeded_stage_produces_one_immutable_verified_decision_package(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory, DecisionPackageRemote())
            created = self.create(engine)
            running = engine.execute({
                "action": "reconcile", "study_id": created["study"]["id"],
            })
            run_id = running["stages"][0]["run_id"]
            remote.advance(run_id, RunStatus.SUCCEEDED, exit_code=0)
            request = {
                "action": "prepare_decision_package",
                "study_id": created["study"]["id"],
                "stage_id": "train",
                "checkpoint_directory": "/synthetic/orchestrator_runs/" + run_id + "/checkpoints",
            }

            first = engine.execute(request)
            second = engine.execute(request)

            self.assertEqual(first, second)
            self.assertEqual(first["readiness"], "ready_for_scientific_review")
            self.assertEqual(first["execution"], {"status": "SUCCEEDED", "exit_code": 0})
            self.assertEqual(first["progress"]["metrics"], {
                "val/mAP_50_95": 0.42, "val/loss": 1.25,
            })
            self.assertEqual(first["evidence"]["runtime"], {
                "schema_version": 2,
                "state": "closed",
                "updated_at": "2026-09-05T12:00:00Z",
                "execution_integrity": {"state": "no_violation_observed", "issue_count": 0},
                "deployment": {"state": "changed", "issue_count": 9},
                "observer": {"state": "closed", "health": "healthy",
                             "coverage": "selected_scope_checked", "warning_count": 0},
                "drift_detected": False,
                "issue_count": 0,
            })
            self.assertEqual(first["checkpoint_candidates"], [{
                "id": "ckpt_" + "1" * 32,
                "path": "/synthetic/orchestrator_runs/" + run_id +
                        "/checkpoints/ckpt_" + "1" * 32 + ".ckpt",
                "created_at": "2026-09-05T11:59:00Z",
                "sha256": "c" * 64,
                "size": 1234,
                "state": {"global_step": 5000, "epoch": 49, "safe_to_resume": True},
                "verification": "exact_path_sha256",
            }])
            self.assertIsNone(first["checkpoint_selection"])
            self.assertRegex(first["package_sha256"], r"^[a-f0-9]{64}$")
            self.assertEqual(remote.checkpoint_calls, 1)
            shown = engine.execute({"action": "show", "study_id": created["study"]["id"]})
            self.assertEqual(shown["stages"][0]["decision_package"], {
                "readiness": "ready_for_scientific_review",
                "package_sha256": first["package_sha256"],
                "prepared_at": first["prepared_at"],
                "checkpoint_count": 1,
            })

    def test_failed_stage_produces_a_failure_package_without_checkpoint_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory, DecisionPackageRemote())
            created = self.create(engine)
            running = engine.execute({
                "action": "reconcile", "study_id": created["study"]["id"],
            })
            run_id = running["stages"][0]["run_id"]
            remote.advance(run_id, RunStatus.FAILED, exit_code=7)

            package = engine.execute({
                "action": "prepare_decision_package",
                "study_id": created["study"]["id"],
                "stage_id": "train",
                "checkpoint_directory": "/synthetic/orchestrator_runs/" + run_id + "/checkpoints",
            })

            self.assertEqual(package["readiness"], "ready_for_failure_review")
            self.assertEqual(package["execution"], {"status": "FAILED", "exit_code": 7})
            self.assertEqual(package["checkpoint_candidates"], [])
            self.assertIsNone(package["checkpoint_selection"])
            self.assertRegex(package["package_sha256"], r"^[a-f0-9]{64}$")
            self.assertEqual(remote.checkpoint_calls, 0)

    def test_legacy_untyped_run_can_be_adopted_with_an_explicit_metadata_upgrade(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            legacy = experiment("train")
            del legacy["task"]
            existing = engine.orchestrator.submit(
                ExperimentSpec.from_dict(legacy), idempotency_key="legacy-preexisting-run")
            remote.advance(existing.id, RunStatus.RUNNING)

            adopted = engine.execute({
                "action": "adopt_run", "study_id": created["study"]["id"], "stage_id": "train",
                "run_id": existing.id, "run_spec_sha256": spec_digest(legacy),
                "idempotency_key": "adopt-legacy-train-v1", "adopted_by": "test-user",
            })

            self.assertEqual(adopted["stages"][0]["run_id"], existing.id)
            self.assertEqual(adopted["stages"][0]["run_link"]["metadata_upgrade"],
                             "legacy_kind_and_empty_inputs")

    def test_run_adoption_rejects_a_different_experiment_definition(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, _ = self.make_engine(directory)
            created = self.create(engine)
            different = experiment("train")
            different["name"] = "different training condition"
            existing = engine.orchestrator.submit(
                ExperimentSpec.from_dict(different), idempotency_key="different-run")

            with self.assertRaises(DomainError):
                engine.execute({
                    "action": "adopt_run", "study_id": created["study"]["id"], "stage_id": "train",
                    "run_id": existing.id, "run_spec_sha256": spec_digest(different),
                    "idempotency_key": "adopt-different-v1", "adopted_by": "test-user",
                })
            shown = engine.execute({"action": "show", "study_id": created["study"]["id"]})
            self.assertIsNone(shown["stages"][0]["run_id"])

    def test_run_adoption_cannot_retroactively_authorize_a_test(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, _ = self.make_engine(directory)
            raw = study()
            raw["stages"] = [raw["stages"][1]]
            raw["stages"][0]["depends_on"] = []
            created = self.create(engine, raw)
            existing = engine.orchestrator.submit(
                ExperimentSpec.from_dict(experiment("test", checkpoint=CHECKPOINT)),
                idempotency_key="external-test-run")

            with self.assertRaises(DomainError):
                engine.execute({
                    "action": "adopt_run", "study_id": created["study"]["id"], "stage_id": "test",
                    "run_id": existing.id,
                    "run_spec_sha256": spec_digest(experiment("test", checkpoint=CHECKPOINT)),
                    "idempotency_key": "adopt-test-v1", "adopted_by": "test-user",
                })

    def test_dynamic_checkpoint_waits_for_a_verified_source_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine, bound_study())
            study_id = created["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            remote.advance(running["stages"][0]["run_id"], RunStatus.SUCCEEDED, exit_code=0)

            blocked = engine.execute({"action": "reconcile", "study_id": study_id})

            self.assertEqual(blocked["stages"][1]["status"], "BLOCKED")
            self.assertEqual(blocked["stages"][1]["block_reason"], "artifact_binding_required")
            self.assertEqual(blocked["stages"][1]["artifact_bindings"], [])
            self.assertEqual(len(remote.runs), 1)

    def test_verified_checkpoint_binding_is_injected_into_the_test_run(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine, bound_study())
            study_id = created["study"]["id"]
            digest = created["study"]["plan_sha256"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            train_run = running["stages"][0]["run_id"]
            remote.set_artifacts(train_run, [{
                "path": "/synthetic/remote_0001/model.pt",
                "size": 1234,
                "sha256": "c" * 64,
            }])
            remote.advance(train_run, RunStatus.SUCCEEDED, exit_code=0)

            bound = engine.execute({"action": "reconcile", "study_id": study_id})
            self.assertEqual(bound["stages"][1]["block_reason"], "recommendation_required")
            self.assertEqual(bound["stages"][1]["artifact_bindings"][0]["source_run_id"], train_run)
            bindings_sha256 = bound["stages"][1]["artifact_bindings_sha256"]
            with self.assertRaises(DomainError):
                engine.execute({"action": "recommend", "study_id": study_id,
                                "recommendation": recommendation(digest, train_run)})
            recommended = engine.execute({"action": "recommend", "study_id": study_id,
                                          "recommendation": recommendation(
                                              digest, train_run,
                                              artifact_bindings_sha256=bindings_sha256)})
            recommendation_id = recommended["recommendations"][0]["id"]
            engine.execute({"action": "approve", "study_id": study_id, "stage_id": "test",
                            "plan_sha256": digest, "recommendation_id": recommendation_id,
                            "approved_by": "test-user"})

            testing = engine.execute({"action": "reconcile", "study_id": study_id})

            test_run = testing["stages"][1]["run_id"]
            task = remote.runs[test_run]["spec"]["task"]
            self.assertEqual(task["source_run_id"], train_run)
            self.assertEqual(task["checkpoint"], {
                "path": "/synthetic/remote_0001/model.pt", "sha256": "c" * 64,
            })
            self.assertEqual(len(remote.runs), 2)

    def test_changed_checkpoint_invalidates_the_bound_test_input(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine, bound_study())
            study_id = created["study"]["id"]
            plan_sha256 = created["study"]["plan_sha256"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            train_run = running["stages"][0]["run_id"]
            artifact = {"path": "/synthetic/remote_0001/model.pt", "size": 1234, "sha256": "c" * 64}
            remote.set_artifacts(train_run, [artifact])
            remote.advance(train_run, RunStatus.SUCCEEDED, exit_code=0)
            bound = engine.execute({"action": "reconcile", "study_id": study_id})
            recommendation_request = recommendation(
                plan_sha256, train_run,
                artifact_bindings_sha256=bound["stages"][1]["artifact_bindings_sha256"],
            )
            recommended = engine.execute({"action": "recommend", "study_id": study_id,
                                          "recommendation": recommendation_request})
            engine.execute({"action": "approve", "study_id": study_id, "stage_id": "test",
                            "plan_sha256": plan_sha256,
                            "recommendation_id": recommended["recommendations"][0]["id"],
                            "approved_by": "test-user"})
            remote.set_artifacts(train_run, [{**artifact, "sha256": "d" * 64}])

            blocked = engine.execute({"action": "reconcile", "study_id": study_id})

            self.assertEqual(blocked["stages"][1]["block_reason"], "artifact_binding_changed")
            self.assertEqual(len(remote.runs), 1)

    def test_artifact_disconnect_is_retryable_and_does_not_fail_the_study(self):
        with tempfile.TemporaryDirectory() as directory:
            remote = ArtifactOfflineRemote()
            engine, _ = self.make_engine(directory, remote)
            created = self.create(engine, bound_study())
            study_id = created["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            remote.advance(running["stages"][0]["run_id"], RunStatus.SUCCEEDED, exit_code=0)

            blocked = engine.execute({"action": "reconcile", "study_id": study_id})

            self.assertEqual(blocked["study"]["status"], "BLOCKED")
            self.assertEqual(blocked["stages"][1]["block_reason"], "artifact_binding_unavailable")
            self.assertEqual(len(remote.runs), 1)

    def test_artifact_without_a_digest_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine, bound_study())
            study_id = created["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            train_run = running["stages"][0]["run_id"]
            remote.set_artifacts(train_run, [{"path": "/synthetic/remote_0001/model.pt", "size": 12}])
            remote.advance(train_run, RunStatus.SUCCEEDED, exit_code=0)

            blocked = engine.execute({"action": "reconcile", "study_id": study_id})

            self.assertEqual(blocked["stages"][1]["block_reason"], "artifact_binding_invalid")
            self.assertEqual(len(remote.runs), 1)

    def test_restart_preserves_binding_and_submits_the_test_once(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine, bound_study())
            study_id = created["study"]["id"]
            plan_sha256 = created["study"]["plan_sha256"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            train_run = running["stages"][0]["run_id"]
            remote.set_artifacts(train_run, [{
                "path": "/synthetic/remote_0001/model.pt", "size": 1234, "sha256": "c" * 64,
            }])
            remote.advance(train_run, RunStatus.SUCCEEDED, exit_code=0)
            bound = engine.execute({"action": "reconcile", "study_id": study_id})
            recommended = engine.execute({"action": "recommend", "study_id": study_id,
                                          "recommendation": recommendation(
                                              plan_sha256, train_run,
                                              artifact_bindings_sha256=bound["stages"][1]["artifact_bindings_sha256"])})
            engine.execute({"action": "approve", "study_id": study_id, "stage_id": "test",
                            "plan_sha256": plan_sha256,
                            "recommendation_id": recommended["recommendations"][0]["id"],
                            "approved_by": "test-user"})

            restarted, _ = self.make_engine(directory, remote)
            first = restarted.execute({"action": "reconcile", "study_id": study_id})
            second = restarted.execute({"action": "reconcile", "study_id": study_id})

            self.assertIsNotNone(first["stages"][1]["run_id"])
            self.assertEqual(second["stages"][1]["run_id"], first["stages"][1]["run_id"])
            self.assertEqual(len(remote.runs), 2)

    def test_training_failure_blocks_test_without_submitting_it(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            study_id = created["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            remote.advance(running["stages"][0]["run_id"], RunStatus.FAILED, exit_code=1)
            failed = engine.execute({"action": "reconcile", "study_id": study_id})
            self.assertEqual(failed["study"]["status"], "FAILED")
            self.assertEqual(failed["stages"][1]["status"], "BLOCKED")
            self.assertEqual(failed["stages"][1]["block_reason"], "dependency_failed")
            self.assertEqual(len(remote.runs), 1)

    def test_disconnect_does_not_turn_running_stage_into_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            remote = OfflineRemote()
            engine, _ = self.make_engine(directory, remote)
            created = self.create(engine)
            study_id = created["study"]["id"]
            engine.execute({"action": "reconcile", "study_id": study_id})
            remote.offline = True
            disconnected = engine.execute({"action": "reconcile", "study_id": study_id})
            self.assertEqual(disconnected["stages"][0]["status"], "RUNNING")
            self.assertEqual(disconnected["study"]["status"], "RUNNING")
            self.assertEqual(disconnected["sync_errors"][0]["code"], "SSH_UNREACHABLE")

    def test_restart_recovers_without_duplicate_submission(self):
        with tempfile.TemporaryDirectory() as directory:
            remote = FakeRemoteRunner()
            engine, _ = self.make_engine(directory, remote)
            study_id = self.create(engine)["study"]["id"]
            engine.execute({"action": "reconcile", "study_id": study_id})
            restarted, _ = self.make_engine(directory, remote)
            recovered = restarted.execute({"action": "reconcile", "study_id": study_id})
            self.assertEqual(len(remote.runs), 1)
            self.assertEqual(recovered["stages"][0]["status"], "RUNNING")

    def test_final_test_requires_checkpoint_digest_and_approval(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, _ = self.make_engine(directory)
            with self.assertRaises(DomainError):
                self.create(engine, study(checkpoint={"path": "/synthetic/model.pt"}))
            raw = study()
            raw["stages"][1]["requires_approval"] = False
            with self.assertRaises(DomainError):
                self.create(engine, raw, key="no-approval")

    def test_approval_is_bound_to_exact_plan_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, _ = self.make_engine(directory)
            created = self.create(engine)
            with self.assertRaises(DomainError):
                engine.execute({"action": "approve", "study_id": created["study"]["id"],
                                "stage_id": "test", "plan_sha256": "0" * 64,
                                "approved_by": "test-user"})

    def test_successful_training_waits_at_unapproved_test_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            study_id = created["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            remote.advance(running["stages"][0]["run_id"], RunStatus.SUCCEEDED, exit_code=0)
            trained = engine.execute({"action": "reconcile", "study_id": study_id})
            recommended = engine.execute({"action": "recommend", "study_id": study_id,
                                          "recommendation": recommendation(
                                              created["study"]["plan_sha256"], trained["stages"][0]["run_id"])})
            blocked = engine.execute({"action": "reconcile", "study_id": study_id})
            self.assertEqual(blocked["study"]["status"], "BLOCKED")
            self.assertEqual(blocked["stages"][1]["block_reason"], "approval_required")
            self.assertFalse(recommended["stages"][1]["approved"])
            self.assertEqual(len(remote.runs), 1)

    def test_successful_training_cannot_be_approved_or_tested_without_recommendation(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            study_id = created["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            remote.advance(running["stages"][0]["run_id"], RunStatus.SUCCEEDED, exit_code=0)
            blocked = engine.execute({"action": "reconcile", "study_id": study_id})
            self.assertEqual(blocked["stages"][1]["block_reason"], "recommendation_required")
            with self.assertRaises(DomainError):
                engine.execute({"action": "approve", "study_id": study_id, "stage_id": "test",
                                "plan_sha256": created["study"]["plan_sha256"],
                                "recommendation_id": "rec_" + "a" * 32,
                                "approved_by": "test-user"})
            self.assertEqual(len(remote.runs), 1)

    def test_agent_recommendation_is_persistent_and_idempotent_through_public_interface(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            study_id = created["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            train_run = running["stages"][0]["run_id"]
            remote.advance(train_run, RunStatus.SUCCEEDED, exit_code=0)
            engine.execute({"action": "reconcile", "study_id": study_id})
            request = {"action": "recommend", "study_id": study_id,
                       "recommendation": recommendation(created["study"]["plan_sha256"], train_run)}
            first = engine.execute(request)
            second = engine.execute(request)
            self.assertEqual(len(first["recommendations"]), 1)
            self.assertEqual(first["recommendations"][0]["id"], second["recommendations"][0]["id"])
            self.assertEqual(first["recommendations"][0]["decision"], "PROCEED")
            changed = recommendation(created["study"]["plan_sha256"], train_run)
            changed["summary"] = "Conflicting recommendation"
            with self.assertRaises(DomainError):
                engine.execute({"action": "recommend", "study_id": study_id, "recommendation": changed})

    def test_stale_evidence_cannot_support_proceed_recommendation(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            study_id = created["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            train_run = running["stages"][0]["run_id"]
            remote.advance(train_run, RunStatus.SUCCEEDED, exit_code=0)
            engine.execute({"action": "reconcile", "study_id": study_id})
            stale = recommendation(created["study"]["plan_sha256"], train_run, observation_state="stale")
            with self.assertRaises(DomainError):
                engine.execute({"action": "recommend", "study_id": study_id, "recommendation": stale})
            shown = engine.execute({"action": "show", "study_id": study_id})
            self.assertEqual(shown["recommendations"], [])

    def test_old_evidence_cannot_be_disguised_as_fresh(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            study_id = created["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            train_run = running["stages"][0]["run_id"]
            remote.advance(train_run, RunStatus.SUCCEEDED, exit_code=0)
            engine.execute({"action": "reconcile", "study_id": study_id})
            old = recommendation(created["study"]["plan_sha256"], train_run,
                                 observed_at="2000-01-01T00:00:00Z")
            with self.assertRaises(DomainError):
                engine.execute({"action": "recommend", "study_id": study_id, "recommendation": old})

    def test_proceed_recommendation_requires_declared_review_method(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            study_id = created["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            train_run = running["stages"][0]["run_id"]
            remote.advance(train_run, RunStatus.SUCCEEDED, exit_code=0)
            engine.execute({"action": "reconcile", "study_id": study_id})
            unsupported = recommendation(created["study"]["plan_sha256"], train_run, skills_used=[])
            with self.assertRaises(DomainError):
                engine.execute({"action": "recommend", "study_id": study_id,
                                "recommendation": unsupported})

    def test_non_test_stage_can_keep_plain_human_approval(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            raw = study(max_runs=1)
            raw["stages"] = [raw["stages"][0]]
            raw["stages"][0]["requires_approval"] = True
            created = self.create(engine, raw)
            study_id = created["study"]["id"]
            engine.execute({"action": "approve", "study_id": study_id, "stage_id": "train",
                            "plan_sha256": created["study"]["plan_sha256"],
                            "approved_by": "test-user"})
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            self.assertEqual(running["stages"][0]["status"], "RUNNING")
            self.assertEqual(len(remote.runs), 1)

    def test_proceed_recommendation_rejects_wrong_plan_or_unfinished_dependency(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            study_id = created["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            train_run = running["stages"][0]["run_id"]
            wrong_plan = recommendation("0" * 64, train_run, key="wrong-plan")
            with self.assertRaises(DomainError):
                engine.execute({"action": "recommend", "study_id": study_id,
                                "recommendation": wrong_plan})
            unfinished = recommendation(created["study"]["plan_sha256"], train_run, key="unfinished")
            with self.assertRaises(DomainError):
                engine.execute({"action": "recommend", "study_id": study_id,
                                "recommendation": unfinished})

    def test_proceed_recommendation_rejects_unlinked_evidence_run(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            study_id = created["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            train_run = running["stages"][0]["run_id"]
            remote.advance(train_run, RunStatus.SUCCEEDED, exit_code=0)
            engine.execute({"action": "reconcile", "study_id": study_id})
            unlinked = recommendation(created["study"]["plan_sha256"], train_run)
            unlinked["evidence"].append({
                "run_id": "remote_unlinked",
                "result_sha256": "c" * 64,
                "observed_at": utc_now(),
                "observation_state": "fresh",
            })
            with self.assertRaises(DomainError):
                engine.execute({"action": "recommend", "study_id": study_id,
                                "recommendation": unlinked})

    def test_new_wait_recommendation_supersedes_prior_proceed_and_approval(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            created = self.create(engine)
            study_id = created["study"]["id"]
            digest = created["study"]["plan_sha256"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            train_run = running["stages"][0]["run_id"]
            remote.advance(train_run, RunStatus.SUCCEEDED, exit_code=0)
            engine.execute({"action": "reconcile", "study_id": study_id})
            proceed = engine.execute({"action": "recommend", "study_id": study_id,
                                      "recommendation": recommendation(digest, train_run)})
            proceed_id = proceed["recommendations"][0]["id"]
            engine.execute({"action": "approve", "study_id": study_id, "stage_id": "test",
                            "plan_sha256": digest, "recommendation_id": proceed_id,
                            "approved_by": "test-user"})
            waiting = recommendation(digest, train_run, decision="WAIT", key="rec-v2")
            engine.execute({"action": "recommend", "study_id": study_id, "recommendation": waiting})
            blocked = engine.execute({"action": "reconcile", "study_id": study_id})
            self.assertEqual(blocked["stages"][1]["block_reason"], "recommendation_wait")
            self.assertFalse(blocked["stages"][1]["approved"])
            self.assertEqual(len(blocked["recommendations"]), 2)
            self.assertEqual(len(remote.runs), 1)

    def test_stop_cancels_active_and_never_starts_downstream(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            study_id = self.create(engine)["study"]["id"]
            running = engine.execute({"action": "reconcile", "study_id": study_id})
            stopped = engine.execute({"action": "stop", "study_id": study_id, "reason": "test stop"})
            self.assertEqual(stopped["study"]["status"], "CANCELLED")
            self.assertEqual(stopped["stages"][0]["status"], "CANCELLED")
            self.assertEqual(stopped["stages"][1]["status"], "CANCELLED")
            self.assertEqual(remote.runs[running["stages"][0]["run_id"]]["status"], RunStatus.CANCELLED)

    def test_create_is_idempotent_and_rejects_conflicting_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, _ = self.make_engine(directory)
            first = self.create(engine)
            second = self.create(engine)
            self.assertEqual(first["study"]["id"], second["study"]["id"])
            changed = study()
            changed["research_question"] = "A different question"
            with self.assertRaises(DomainError):
                self.create(engine, changed)

    def test_cycle_is_rejected_before_any_run_exists(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, remote = self.make_engine(directory)
            raw = study()
            raw["stages"][0]["depends_on"] = ["test"]
            with self.assertRaises(DomainError):
                self.create(engine, raw)
            self.assertEqual(remote.runs, {})


if __name__ == "__main__":
    unittest.main()
