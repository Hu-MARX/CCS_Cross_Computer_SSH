"""Persistent Study/Stage workflow orchestration.

The public interface is intentionally small: callers submit versioned request
objects to :meth:`WorkflowEngine.execute`.  SQLite layout, dependency gates,
approval matching and Run reconciliation remain implementation details.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from .domain import (
    DomainError,
    ExperimentSpec,
    Recommendation,
    RecommendationDecision,
    RunStatus,
    StageSpec,
    StageStatus,
    StudySpec,
    StudyStatus,
    utc_now,
)
from .orchestrator import Orchestrator
from .remote import RemoteRunnerError
from .configuration import spec_digest
from .results import SAFE_ID, evidence_status, metric_values


_SAFE_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_SAFE_STUDY_ID = re.compile(r"study_[a-f0-9]{32}")
_TERMINAL_STAGE = {StageStatus.SUCCEEDED, StageStatus.FAILED, StageStatus.CANCELLED}
_MAX_RECOMMENDATION_EVIDENCE_AGE_SECONDS = 600


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


class WorkflowEngine:
    """Deep workflow module hiding persistence, DAG state and Run submission."""

    def __init__(self, database: str | Path, orchestrator: Orchestrator):
        self.database = str(database)
        self.orchestrator = orchestrator
        if self.database == ":memory:":
            raise ValueError("WorkflowEngine requires a persistent SQLite path")
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def execute(self, request: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(request, dict):
            raise DomainError("workflow request must be an object")
        action = request.get("action")
        if action == "create":
            self._require_keys(request, {"action", "idempotency_key", "study"})
            return self._create(request["study"], request["idempotency_key"])
        if action == "show":
            self._require_keys(request, {"action", "study_id"})
            return self._view(self._study_id(request["study_id"]))
        if action == "list":
            self._require_keys(request, {"action", "limit"}, optional={"limit"})
            limit = request.get("limit", 20)
            if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
                raise DomainError("workflow list limit must be between 1 and 100")
            return {"schema_version": 1, "studies": self._list(limit)}
        if action == "reconcile":
            self._require_keys(request, {"action", "study_id"})
            return self._reconcile(self._study_id(request["study_id"]))
        if action == "adopt_run":
            self._require_keys(request, {
                "action", "study_id", "stage_id", "run_id", "run_spec_sha256",
                "idempotency_key", "adopted_by",
            })
            return self._adopt_run(
                self._study_id(request["study_id"]), request["stage_id"], request["run_id"],
                request["run_spec_sha256"], request["idempotency_key"], request["adopted_by"],
            )
        if action == "prepare_decision_package":
            self._require_keys(request, {
                "action", "study_id", "stage_id", "checkpoint_directory",
            })
            return self._prepare_decision_package(
                self._study_id(request["study_id"]), request["stage_id"],
                request["checkpoint_directory"],
            )
        if action == "recommend":
            self._require_keys(request, {"action", "study_id", "recommendation"})
            return self._recommend(self._study_id(request["study_id"]), request["recommendation"])
        if action == "approve":
            self._require_keys(request, {
                "action", "study_id", "stage_id", "plan_sha256", "recommendation_id", "approved_by"
            }, optional={"recommendation_id"})
            return self._approve(
                self._study_id(request["study_id"]), request["stage_id"],
                request["plan_sha256"], request.get("recommendation_id"), request["approved_by"],
            )
        if action == "stop":
            self._require_keys(request, {"action", "study_id", "reason"})
            return self._stop(self._study_id(request["study_id"]), request["reason"])
        raise DomainError(
            "workflow action must be create, show, list, reconcile, adopt_run, "
            "prepare_decision_package, recommend, approve, or stop"
        )

    @staticmethod
    def _require_keys(request: dict[str, Any], allowed: set[str], *, optional: set[str] = frozenset()) -> None:
        required = allowed - set(optional)
        missing = required - set(request)
        extra = set(request) - allowed
        if missing or extra:
            raise DomainError("invalid workflow request fields")

    @staticmethod
    def _study_id(value: Any) -> str:
        if not isinstance(value, str) or not _SAFE_STUDY_ID.fullmatch(value):
            raise DomainError("invalid study_id")
        return value

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.database, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _initialize(self) -> None:
        with self._db() as db:
            db.executescript("""
                PRAGMA foreign_keys = ON;
                CREATE TABLE IF NOT EXISTS studies (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    research_question TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    spec_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL,
                    stop_requested INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS study_stages (
                    study_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    requires_approval INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    block_reason TEXT,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    run_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (study_id, id),
                    UNIQUE (run_id),
                    FOREIGN KEY (study_id) REFERENCES studies(id)
                );
                CREATE TABLE IF NOT EXISTS stage_dependencies (
                    study_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    depends_on TEXT NOT NULL,
                    PRIMARY KEY (study_id, stage_id, depends_on),
                    FOREIGN KEY (study_id, stage_id) REFERENCES study_stages(study_id, id),
                    FOREIGN KEY (study_id, depends_on) REFERENCES study_stages(study_id, id)
                );
                CREATE TABLE IF NOT EXISTS stage_runs (
                    study_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    run_id TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (study_id, stage_id, attempt),
                    FOREIGN KEY (study_id, stage_id) REFERENCES study_stages(study_id, id)
                );
                CREATE TABLE IF NOT EXISTS stage_run_adoptions (
                    study_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    run_id TEXT NOT NULL UNIQUE,
                    idempotency_key TEXT NOT NULL,
                    run_spec_sha256 TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL,
                    adopted_by TEXT NOT NULL,
                    metadata_upgrade TEXT,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (study_id, stage_id),
                    UNIQUE (study_id, idempotency_key),
                    FOREIGN KEY (study_id, stage_id) REFERENCES study_stages(study_id, id)
                );
                CREATE TABLE IF NOT EXISTS stage_decision_packages (
                    study_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    run_id TEXT NOT NULL UNIQUE,
                    checkpoint_directory TEXT NOT NULL,
                    package_json TEXT NOT NULL,
                    package_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (study_id, stage_id),
                    FOREIGN KEY (study_id, stage_id) REFERENCES study_stages(study_id, id)
                );
                CREATE TABLE IF NOT EXISTS stage_artifact_bindings (
                    study_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    source_stage_id TEXT NOT NULL,
                    source_run_id TEXT NOT NULL,
                    selector_json TEXT NOT NULL,
                    path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    binding_sha256 TEXT NOT NULL,
                    verified_at TEXT NOT NULL,
                    PRIMARY KEY (study_id, stage_id, role),
                    FOREIGN KEY (study_id, stage_id) REFERENCES study_stages(study_id, id),
                    FOREIGN KEY (study_id, source_stage_id) REFERENCES study_stages(study_id, id)
                );
                CREATE TABLE IF NOT EXISTS stage_approvals (
                    study_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    plan_sha256 TEXT NOT NULL,
                    approved_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (study_id, stage_id, plan_sha256),
                    FOREIGN KEY (study_id, stage_id) REFERENCES study_stages(study_id, id)
                );
                CREATE TABLE IF NOT EXISTS agent_recommendations (
                    id TEXT PRIMARY KEY,
                    study_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    plan_sha256 TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    recommendation_json TEXT NOT NULL,
                    recommendation_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (study_id, idempotency_key),
                    FOREIGN KEY (study_id, stage_id) REFERENCES study_stages(study_id, id)
                );
                CREATE TABLE IF NOT EXISTS stage_authorizations (
                    study_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    recommendation_id TEXT NOT NULL,
                    plan_sha256 TEXT NOT NULL,
                    approved_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (study_id, stage_id, recommendation_id),
                    FOREIGN KEY (recommendation_id) REFERENCES agent_recommendations(id),
                    FOREIGN KEY (study_id, stage_id) REFERENCES study_stages(study_id, id)
                );
                CREATE TABLE IF NOT EXISTS workflow_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    study_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (study_id) REFERENCES studies(id)
                );
                CREATE TABLE IF NOT EXISTS workflow_events (
                    study_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    stage_id TEXT,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (study_id, seq),
                    FOREIGN KEY (study_id) REFERENCES studies(id)
                );
            """)
            adoption_columns = {row["name"] for row in db.execute("PRAGMA table_info(stage_run_adoptions)")}
            if "metadata_upgrade" not in adoption_columns:
                db.execute("ALTER TABLE stage_run_adoptions ADD COLUMN metadata_upgrade TEXT")

    def _create(self, raw_study: Any, idempotency_key: Any) -> dict[str, Any]:
        if not isinstance(idempotency_key, str) or not _SAFE_KEY.fullmatch(idempotency_key):
            raise DomainError("workflow idempotency_key contains unsupported characters")
        study = StudySpec.from_dict(raw_study)
        normalized = study.to_dict()
        spec_json = _canonical_json(normalized)
        spec_sha256 = _digest(normalized)
        now = utc_now()
        with self._db() as db:
            existing = db.execute(
                "SELECT id, spec_sha256 FROM studies WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if existing:
                if existing["spec_sha256"] != spec_sha256:
                    raise DomainError("workflow idempotency key conflicts with a different StudySpec")
                study_id = existing["id"]
            else:
                study_id = f"study_{uuid4().hex}"
                db.execute(
                    "INSERT INTO studies VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
                    (study_id, idempotency_key, study.name, study.research_question, spec_json,
                     spec_sha256, StudyStatus.PENDING.value, now, now),
                )
                for position, stage in enumerate(study.stages):
                    db.execute(
                        """INSERT INTO study_stages
                           (study_id,id,position,name,kind,spec_json,requires_approval,status,
                            block_reason,attempt,run_id,created_at,updated_at)
                           VALUES (?,?,?,?,?,?,?,?,NULL,0,NULL,?,?)""",
                        (study_id, stage.id, position, stage.name, stage.kind,
                         _canonical_json(stage.experiment.to_dict()), int(stage.requires_approval),
                         StageStatus.PENDING.value, now, now),
                    )
                for stage in study.stages:
                    for dependency in stage.depends_on:
                        db.execute(
                            "INSERT INTO stage_dependencies VALUES (?, ?, ?)",
                            (study_id, stage.id, dependency),
                        )
                self._event(db, study_id, None, "study_created", {"spec_sha256": spec_sha256})
                self._action(db, study_id, "create", {"idempotency_key": idempotency_key})
        return self._view(study_id)

    def _adopt_run(self, study_id: str, stage_id: Any, run_id: Any, run_spec_sha256: Any,
                   idempotency_key: Any, adopted_by: Any) -> dict[str, Any]:
        if not isinstance(stage_id, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", stage_id):
            raise DomainError("invalid stage_id")
        if not isinstance(run_id, str) or not SAFE_ID.fullmatch(run_id):
            raise DomainError("invalid run_id")
        if not isinstance(run_spec_sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", run_spec_sha256):
            raise DomainError("invalid run_spec_sha256")
        if not isinstance(idempotency_key, str) or not _SAFE_KEY.fullmatch(idempotency_key):
            raise DomainError("adoption idempotency_key contains unsupported characters")
        if not isinstance(adopted_by, str) or not adopted_by.strip() or len(adopted_by) > 200:
            raise DomainError("adopted_by must contain 1-200 characters")
        request_value = {
            "stage_id": stage_id, "run_id": run_id, "run_spec_sha256": run_spec_sha256,
            "idempotency_key": idempotency_key, "adopted_by": adopted_by.strip(),
        }
        request_sha256 = _digest(request_value)
        with self._db() as db:
            self._study_row(db, study_id)
            existing = db.execute(
                """SELECT request_sha256 FROM stage_run_adoptions
                   WHERE study_id=? AND idempotency_key=?""",
                (study_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                if existing["request_sha256"] != request_sha256:
                    raise DomainError("adoption idempotency key conflicts with different content")
                return self._view(study_id)

        self.orchestrator.reconcile()
        if any(item.get("run_id") == run_id for item in self.orchestrator.last_errors):
            raise DomainError("cannot adopt a Run whose current remote state is unavailable")
        try:
            run = self.orchestrator.repository.get_run(run_id)
        except KeyError as exc:
            raise DomainError("adopted Run is not registered in the local repository") from exc
        if run.worker != self.orchestrator.worker:
            raise DomainError("adopted Run belongs to a different worker")
        actual_digest = spec_digest(run.spec.to_dict())
        if actual_digest != run_spec_sha256:
            raise DomainError("run_spec_sha256 does not match the registered Run")

        with self._db() as db:
            stage = db.execute(
                """SELECT kind,spec_json,requires_approval,status,run_id,attempt
                   FROM study_stages WHERE study_id=? AND id=?""",
                (study_id, stage_id),
            ).fetchone()
            if stage is None:
                raise KeyError(stage_id)
            if stage["run_id"] is not None:
                raise DomainError("stage already has a linked Run")
            if stage["kind"] == "test" or stage["requires_approval"]:
                raise DomainError("Run Adoption cannot target test or approval-gated stages")
            if db.execute(
                    "SELECT 1 FROM stage_dependencies WHERE study_id=? AND stage_id=?",
                    (study_id, stage_id)).fetchone() is not None:
                raise DomainError("Run Adoption v1 only supports root stages")
            if db.execute(
                    "SELECT 1 FROM stage_artifact_bindings WHERE study_id=? AND stage_id=?",
                    (study_id, stage_id)).fetchone() is not None:
                raise DomainError("Run Adoption cannot replace a resolved artifact-bound stage")
            planned = json.loads(stage["spec_json"])
            planned_digest = spec_digest(planned)
            metadata_upgrade = None
            if planned_digest != actual_digest:
                declared_task = planned.get("task")
                legacy_equivalent = dict(planned)
                legacy_equivalent.pop("task", None)
                if (run.spec.task is None and declared_task == {"kind": stage["kind"], "inputs": []} and
                        spec_digest(legacy_equivalent) == actual_digest):
                    metadata_upgrade = "legacy_kind_and_empty_inputs"
                else:
                    raise DomainError("adopted Run does not match the Stage ExperimentSpec")
            if db.execute("SELECT 1 FROM stage_runs WHERE run_id=?", (run_id,)).fetchone() is not None:
                raise DomainError("Run is already linked to a Stage")
            states = {
                RunStatus.SUCCEEDED: StageStatus.SUCCEEDED,
                RunStatus.FAILED: StageStatus.FAILED,
                RunStatus.CANCELLED: StageStatus.CANCELLED,
            }
            target = states.get(run.status, StageStatus.RUNNING)
            attempt = stage["attempt"] + 1
            now = utc_now()
            db.execute(
                """INSERT INTO stage_runs(study_id,stage_id,attempt,run_id,created_at)
                   VALUES (?,?,?,?,?)""",
                (study_id, stage_id, attempt, run_id, now),
            )
            db.execute(
                """INSERT INTO stage_run_adoptions
                   (study_id,stage_id,run_id,idempotency_key,run_spec_sha256,request_sha256,adopted_by,
                    metadata_upgrade,created_at) VALUES (?,?,?,?,?,?,?,?,?)""",
                (study_id, stage_id, run_id, idempotency_key, actual_digest,
                 request_sha256, adopted_by.strip(), metadata_upgrade, now),
            )
            db.execute(
                """UPDATE study_stages SET status=?,block_reason=NULL,attempt=?,run_id=?,updated_at=?
                   WHERE study_id=? AND id=?""",
                (target.value, attempt, run_id, now, study_id, stage_id),
            )
            self._event(db, study_id, stage_id, "stage_run_adopted", {
                "run_id": run_id, "run_spec_sha256": actual_digest,
                "adopted_by": adopted_by.strip(), "run_status": run.status.value,
                "metadata_upgrade": metadata_upgrade,
            })
            self._action(db, study_id, "adopt_run", request_value)
        self._refresh_study_status(study_id)
        return self._view(study_id)

    def _prepare_decision_package(self, study_id: str, stage_id: Any,
                                  checkpoint_directory: Any) -> dict[str, Any]:
        if not isinstance(stage_id, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", stage_id):
            raise DomainError("invalid stage_id")
        if (not isinstance(checkpoint_directory, str) or not checkpoint_directory.startswith("/") or
                "\x00" in checkpoint_directory or len(checkpoint_directory) > 4096 or
                ".." in PurePosixPath(checkpoint_directory).parts):
            raise DomainError("checkpoint_directory must be an absolute POSIX path without traversal")
        with self._db() as db:
            self._study_row(db, study_id)
            stage = db.execute(
                "SELECT run_id FROM study_stages WHERE study_id=? AND id=?", (study_id, stage_id)
            ).fetchone()
            existing = db.execute(
                """SELECT checkpoint_directory,package_json FROM stage_decision_packages
                   WHERE study_id=? AND stage_id=?""", (study_id, stage_id)
            ).fetchone()
        if stage is None:
            raise KeyError(stage_id)
        if stage["run_id"] is None:
            raise DomainError("stage has no linked Run")
        if existing is not None:
            if existing["checkpoint_directory"] != checkpoint_directory:
                raise DomainError("Stage Decision Package already uses a different checkpoint directory")
            return json.loads(existing["package_json"])
        run_id = stage["run_id"]
        self.orchestrator.reconcile()
        if any(item.get("run_id") == run_id for item in self.orchestrator.last_errors):
            raise DomainError("cannot prepare a decision package while the Run state is unavailable")
        run = self.orchestrator.repository.get_run(run_id)
        if not run.status.terminal:
            return {
                "schema_version": 1, "study_id": study_id, "stage_id": stage_id,
                "run_id": run_id, "readiness": "waiting_for_terminal",
                "run_status": run.status.value, "reason": "run_not_terminal",
            }
        try:
            snapshot = self.orchestrator.remote.snapshot(run_id)
        except RemoteRunnerError as exc:
            raise DomainError("cannot prepare a decision package while terminal evidence is unavailable") from exc
        if snapshot.status != run.status:
            raise DomainError("terminal Run status changed while preparing the decision package")
        observation = snapshot.observation if isinstance(snapshot.observation, dict) else {}
        progress = snapshot.progress if isinstance(snapshot.progress, dict) else {}
        gate = snapshot.evidence_gate if isinstance(snapshot.evidence_gate, dict) else {}
        runtime = snapshot.evidence_runtime if isinstance(snapshot.evidence_runtime, dict) else {}
        source = snapshot.source_snapshot if isinstance(snapshot.source_snapshot, dict) else {}
        now = utc_now()
        common = {
            "schema_version": 1,
            "study_id": study_id,
            "stage_id": stage_id,
            "run_id": run_id,
            "prepared_at": now,
            "observation": {
                "state": observation.get("observation_state"),
                "observed_at": observation.get("observed_at"),
            },
            "execution": {"status": snapshot.status.value, "exit_code": snapshot.exit_code},
            "progress": {
                "updated_at": progress.get("updated_at"),
                "phase": progress.get("phase"),
                "step": progress.get("step"),
                "metrics": metric_values(progress.get("metrics")),
            },
            "evidence": {
                "launch_gate": {
                    "verified": gate.get("verified"), "state": gate.get("state"),
                    "manifest_sha256": gate.get("manifest_sha256"),
                },
                "runtime": evidence_status(runtime),
                "source_snapshot": {"root": source.get("root"), "files": source.get("files")},
            },
        }
        if run.status != RunStatus.SUCCEEDED:
            package = {
                **common,
                "readiness": "ready_for_failure_review",
                "checkpoint_candidates": [],
                "checkpoint_selection": None,
            }
            return self._store_decision_package(
                study_id, stage_id, run_id, checkpoint_directory, package, now, None,
            )
        checkpoint = observation.get("checkpoint")
        latest = checkpoint.get("latest") if isinstance(checkpoint, dict) else None
        if not isinstance(checkpoint, dict) or checkpoint.get("state") != "committed" or not isinstance(latest, dict):
            raise DomainError("succeeded Run has no committed managed checkpoint")
        checkpoint_id = latest.get("id")
        checkpoint_sha256 = latest.get("sha256")
        checkpoint_size = latest.get("size")
        checkpoint_state = latest.get("state")
        if (not isinstance(checkpoint_id, str) or not re.fullmatch(r"ckpt_[a-f0-9]{32}", checkpoint_id) or
                not isinstance(checkpoint_sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", checkpoint_sha256) or
                type(checkpoint_size) is not int or checkpoint_size <= 0 or
                not isinstance(latest.get("created_at"), str) or not isinstance(checkpoint_state, dict)):
            raise DomainError("managed checkpoint report is invalid")
        checkpoint_path = checkpoint_directory.rstrip("/") + "/" + checkpoint_id + ".ckpt"
        try:
            artifacts = self.orchestrator.remote.artifacts(run_id, path=checkpoint_path)
        except RemoteRunnerError as exc:
            raise DomainError("managed checkpoint could not be verified by exact path") from exc
        if (len(artifacts) != 1 or artifacts[0].get("path") != checkpoint_path or
                artifacts[0].get("size") != checkpoint_size or
                artifacts[0].get("sha256") != checkpoint_sha256):
            raise DomainError("managed checkpoint exact-path verification does not match its commit record")
        package = {
            **common,
            "readiness": "ready_for_scientific_review",
            "checkpoint_candidates": [{
                "id": checkpoint_id,
                "path": checkpoint_path,
                "created_at": latest["created_at"],
                "sha256": checkpoint_sha256,
                "size": checkpoint_size,
                "state": checkpoint_state,
                "verification": "exact_path_sha256",
            }],
            "checkpoint_selection": None,
        }
        return self._store_decision_package(
            study_id, stage_id, run_id, checkpoint_directory, package, now, checkpoint_sha256,
        )

    def _store_decision_package(self, study_id: str, stage_id: str, run_id: str,
                                checkpoint_directory: str, package: dict[str, Any], created_at: str,
                                checkpoint_sha256: str | None) -> dict[str, Any]:
        package_sha256 = _digest(package)
        package["package_sha256"] = package_sha256
        with self._db() as db:
            existing = db.execute(
                "SELECT package_json FROM stage_decision_packages WHERE study_id=? AND stage_id=?",
                (study_id, stage_id),
            ).fetchone()
            if existing is not None:
                return json.loads(existing["package_json"])
            db.execute(
                """INSERT INTO stage_decision_packages
                   (study_id,stage_id,run_id,checkpoint_directory,package_json,package_sha256,created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (study_id, stage_id, run_id, checkpoint_directory, _canonical_json(package),
                 package_sha256, created_at),
            )
            event = {"run_id": run_id, "package_sha256": package_sha256}
            if checkpoint_sha256 is not None:
                event["checkpoint_sha256"] = checkpoint_sha256
            self._event(db, study_id, stage_id, "stage_decision_package_prepared", event)
            self._action(db, study_id, "prepare_decision_package", {
                "stage_id": stage_id, "run_id": run_id,
                "checkpoint_directory": checkpoint_directory,
            })
        return package

    def _approve(self, study_id: str, stage_id: Any, plan_sha256: Any,
                 recommendation_id: Any, approved_by: Any) -> dict[str, Any]:
        if not isinstance(stage_id, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", stage_id):
            raise DomainError("invalid stage_id")
        if not isinstance(plan_sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", plan_sha256):
            raise DomainError("invalid plan_sha256")
        if (recommendation_id is not None and
                (not isinstance(recommendation_id, str) or not re.fullmatch(r"rec_[a-f0-9]{32}", recommendation_id))):
            raise DomainError("invalid recommendation_id")
        if not isinstance(approved_by, str) or not approved_by.strip() or len(approved_by) > 200:
            raise DomainError("approved_by must contain 1-200 characters")
        with self._db() as db:
            study = self._study_row(db, study_id)
            stage = db.execute(
                "SELECT kind,requires_approval FROM study_stages WHERE study_id=? AND id=?", (study_id, stage_id)
            ).fetchone()
            if stage is None:
                raise KeyError(stage_id)
            if not stage["requires_approval"]:
                raise DomainError("stage does not require approval")
            if study["spec_sha256"] != plan_sha256:
                raise DomainError("approval plan_sha256 does not match the current StudySpec")
            if stage["kind"] == "test":
                recommendation = self._latest_recommendation(db, study_id, stage_id)
                if (recommendation is None or recommendation["id"] != recommendation_id or
                        recommendation["plan_sha256"] != plan_sha256 or recommendation["decision"] != "PROCEED"):
                    raise DomainError("test approval requires the latest matching PROCEED recommendation")
                db.execute(
                    """INSERT OR IGNORE INTO stage_authorizations
                       (study_id,stage_id,recommendation_id,plan_sha256,approved_by,created_at)
                       VALUES (?,?,?,?,?,?)""",
                    (study_id, stage_id, recommendation_id, plan_sha256, approved_by.strip(), utc_now()),
                )
            else:
                if recommendation_id is not None:
                    raise DomainError("Recommendation v1 authorization only applies to test stages")
                db.execute(
                    """INSERT OR IGNORE INTO stage_approvals
                       (study_id,stage_id,plan_sha256,approved_by,created_at) VALUES (?,?,?,?,?)""",
                    (study_id, stage_id, plan_sha256, approved_by.strip(), utc_now()),
                )
            self._event(db, study_id, stage_id, "stage_approved", {
                "approved_by": approved_by.strip(), "recommendation_id": recommendation_id,
            })
            self._action(db, study_id, "approve", {
                "stage_id": stage_id, "recommendation_id": recommendation_id,
                "approved_by": approved_by.strip(),
            })
        return self._view(study_id)

    def _recommend(self, study_id: str, raw_recommendation: Any) -> dict[str, Any]:
        recommendation = Recommendation.from_dict(raw_recommendation)
        normalized = recommendation.to_dict()
        digest = _digest(normalized)
        with self._db() as db:
            study = self._study_row(db, study_id)
            if study["spec_sha256"] != recommendation.plan_sha256:
                raise DomainError("Recommendation plan_sha256 does not match the current StudySpec")
            stage = db.execute(
                "SELECT kind FROM study_stages WHERE study_id=? AND id=?",
                (study_id, recommendation.stage_id),
            ).fetchone()
            if stage is None:
                raise KeyError(recommendation.stage_id)
            if stage["kind"] != "test":
                raise DomainError("Recommendation v1 can only target a test stage")
            existing = db.execute(
                "SELECT id,recommendation_sha256 FROM agent_recommendations WHERE study_id=? AND idempotency_key=?",
                (study_id, recommendation.idempotency_key),
            ).fetchone()
            if existing:
                if existing["recommendation_sha256"] != digest:
                    raise DomainError("Recommendation idempotency key conflicts with different content")
                return self._view(study_id)
        self._validate_recommendation_evidence(study_id, recommendation)
        recommendation_id = f"rec_{uuid4().hex}"
        with self._db() as db:
            db.execute(
                """INSERT INTO agent_recommendations
                   (id,study_id,stage_id,idempotency_key,plan_sha256,decision,recommendation_json,
                    recommendation_sha256,created_at) VALUES (?,?,?,?,?,?,?,?,?)""",
                (recommendation_id, study_id, recommendation.stage_id, recommendation.idempotency_key,
                 recommendation.plan_sha256, recommendation.decision.value, _canonical_json(normalized),
                 digest, utc_now()),
            )
            self._event(db, study_id, recommendation.stage_id, "recommendation_recorded", {
                "recommendation_id": recommendation_id, "decision": recommendation.decision.value,
                "proposer": recommendation.proposer,
            })
            self._action(db, study_id, "recommend", {
                "recommendation_id": recommendation_id, "stage_id": recommendation.stage_id,
                "decision": recommendation.decision.value,
            })
        return self._view(study_id)

    def _validate_recommendation_evidence(self, study_id: str, recommendation: Recommendation) -> None:
        if recommendation.decision != RecommendationDecision.PROCEED:
            return
        if not recommendation.skills_used:
            raise DomainError("PROCEED Recommendation requires a declared review method or skill")
        if not recommendation.evidence:
            raise DomainError("PROCEED Recommendation requires evidence")
        if any(item["observation_state"] != "fresh" for item in recommendation.evidence):
            raise DomainError("PROCEED Recommendation cannot rely on stale evidence")
        if not self._evidence_timestamps_fresh(recommendation.evidence):
            raise DomainError("PROCEED Recommendation evidence timestamp is not fresh")
        with self._db() as db:
            study = self._study_row(db, study_id)
            target = next(stage for stage in StudySpec.from_dict(json.loads(study["spec_json"])).stages
                          if stage.id == recommendation.stage_id)
            bindings_sha256 = self._artifact_bindings_sha256(db, study_id, recommendation.stage_id)
            dependencies = db.execute(
                """SELECT s.run_id,s.status FROM stage_dependencies d
                   JOIN study_stages s ON s.study_id=d.study_id AND s.id=d.depends_on
                   WHERE d.study_id=? AND d.stage_id=?""",
                (study_id, recommendation.stage_id),
            ).fetchall()
            linked = {row["run_id"] for row in db.execute(
                "SELECT run_id FROM study_stages WHERE study_id=? AND run_id IS NOT NULL", (study_id,)
            )}
        if target.bindings and recommendation.artifact_bindings_sha256 != bindings_sha256:
            raise DomainError("PROCEED Recommendation must match the verified artifact bindings")
        if not target.bindings and recommendation.artifact_bindings_sha256 is not None:
            raise DomainError("Recommendation declares artifact bindings for an unbound stage")
        if any(row["status"] != StageStatus.SUCCEEDED.value or row["run_id"] is None for row in dependencies):
            raise DomainError("PROCEED Recommendation requires successful dependency stages")
        evidence_ids = {item["run_id"] for item in recommendation.evidence}
        required_ids = {row["run_id"] for row in dependencies}
        if not required_ids.issubset(evidence_ids):
            raise DomainError("PROCEED Recommendation must reference every dependency Run")
        if not evidence_ids.issubset(linked):
            raise DomainError("Recommendation evidence must reference Runs linked to this Study")

    def _reconcile(self, study_id: str) -> dict[str, Any]:
        with self._db() as db:
            self._study_row(db, study_id)
        self.orchestrator.reconcile()
        self._sync_linked_runs(study_id)
        self._advance_pending_stages(study_id)
        self._refresh_study_status(study_id)
        return self._view(study_id, sync_errors=self.orchestrator.last_errors)

    def _sync_linked_runs(self, study_id: str) -> None:
        with self._db() as db:
            linked = db.execute(
                """SELECT id,run_id,status FROM study_stages
                   WHERE study_id=? AND run_id IS NOT NULL AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED')""",
                (study_id,),
            ).fetchall()
        for stage in linked:
            try:
                run = self.orchestrator.repository.get_run(stage["run_id"])
            except KeyError:
                continue
            target = None
            if run.status == RunStatus.SUCCEEDED:
                target = StageStatus.SUCCEEDED
            elif run.status == RunStatus.FAILED:
                target = StageStatus.FAILED
            elif run.status == RunStatus.CANCELLED:
                target = StageStatus.CANCELLED
            elif stage["status"] != StageStatus.RUNNING.value:
                target = StageStatus.RUNNING
            if target is not None and target.value != stage["status"]:
                with self._db() as db:
                    db.execute(
                        "UPDATE study_stages SET status=?,block_reason=NULL,updated_at=? WHERE study_id=? AND id=?",
                        (target.value, utc_now(), study_id, stage["id"]),
                    )
                    self._event(db, study_id, stage["id"], "stage_status_changed", {
                        "status": target.value, "run_id": run.id, "run_status": run.status.value,
                    })

    def _advance_pending_stages(self, study_id: str) -> None:
        with self._db() as db:
            study = self._study_row(db, study_id)
            if study["stop_requested"]:
                return
            max_runs = json.loads(study["spec_json"])["budget"]["max_runs"]
            run_count = db.execute("SELECT COUNT(*) AS n FROM stage_runs WHERE study_id=?", (study_id,)).fetchone()["n"]
            stages = db.execute(
                """SELECT * FROM study_stages WHERE study_id=?
                   AND status NOT IN ('RUNNING','SUCCEEDED','FAILED','CANCELLED') ORDER BY position""",
                (study_id,),
            ).fetchall()
        study_spec = StudySpec.from_dict(json.loads(study["spec_json"]))
        states, block_reasons = self._stage_states(study_id)
        for stage in stages:
            stage_spec = study_spec.stages[stage["position"]]
            dependencies = self._dependencies(study_id, stage["id"])
            dependency_states = [states[item] for item in dependencies]
            if (any(state in {StageStatus.FAILED, StageStatus.CANCELLED} for state in dependency_states) or
                    any(states[item] == StageStatus.BLOCKED and block_reasons[item] == "dependency_failed"
                        for item in dependencies)):
                self._set_stage_gate(study_id, stage["id"], StageStatus.BLOCKED, "dependency_failed")
                states[stage["id"]] = StageStatus.BLOCKED
                block_reasons[stage["id"]] = "dependency_failed"
                continue
            if not all(state == StageStatus.SUCCEEDED for state in dependency_states):
                self._set_stage_gate(study_id, stage["id"], StageStatus.PENDING, "dependency_pending")
                states[stage["id"]] = StageStatus.PENDING
                block_reasons[stage["id"]] = "dependency_pending"
                continue
            binding_gate = self._artifact_binding_gate(study_id, stage_spec)
            if binding_gate["reason"] is not None:
                self._set_stage_gate(study_id, stage["id"], StageStatus.BLOCKED, binding_gate["reason"])
                states[stage["id"]] = StageStatus.BLOCKED
                block_reasons[stage["id"]] = binding_gate["reason"]
                continue
            recommendation = self._recommendation_gate(study_id, stage["id"], study["spec_sha256"], stage["kind"])
            if recommendation["reason"] is not None:
                self._set_stage_gate(study_id, stage["id"], StageStatus.BLOCKED, recommendation["reason"])
                states[stage["id"]] = StageStatus.BLOCKED
                block_reasons[stage["id"]] = recommendation["reason"]
                continue
            if (stage["requires_approval"] and
                    not self._approved(study_id, stage["id"], study["spec_sha256"], recommendation["id"])):
                self._set_stage_gate(study_id, stage["id"], StageStatus.BLOCKED, "approval_required")
                states[stage["id"]] = StageStatus.BLOCKED
                block_reasons[stage["id"]] = "approval_required"
                continue
            if run_count >= max_runs:
                self._set_stage_gate(study_id, stage["id"], StageStatus.BLOCKED, "run_budget_exhausted")
                states[stage["id"]] = StageStatus.BLOCKED
                block_reasons[stage["id"]] = "run_budget_exhausted"
                continue
            self._set_stage_gate(study_id, stage["id"], StageStatus.READY, None)
            try:
                experiment = self._bound_experiment(stage_spec, binding_gate["bindings"])
                attempt = stage["attempt"] + 1
                key = f"wf:{study_id}:{stage['id']}:{attempt}"
                run = self.orchestrator.submit(experiment, idempotency_key=key)
            except RemoteRunnerError as exc:
                with self._db() as db:
                    self._set_stage(db, study_id, stage["id"], StageStatus.READY, "remote_unavailable")
                    self._event(db, study_id, stage["id"], "stage_submit_deferred", {
                        "code": exc.code, "message": exc.message, "retryable": exc.retryable,
                    })
                states[stage["id"]] = StageStatus.READY
                block_reasons[stage["id"]] = "remote_unavailable"
                continue
            with self._db() as db:
                db.execute(
                    """INSERT OR IGNORE INTO stage_runs(study_id,stage_id,attempt,run_id,created_at)
                       VALUES (?,?,?,?,?)""",
                    (study_id, stage["id"], attempt, run.id, utc_now()),
                )
                db.execute(
                    """UPDATE study_stages SET status=?,block_reason=NULL,attempt=?,run_id=?,updated_at=?
                       WHERE study_id=? AND id=?""",
                    (StageStatus.RUNNING.value, attempt, run.id, utc_now(), study_id, stage["id"]),
                )
                self._event(db, study_id, stage["id"], "stage_run_submitted", {
                    "attempt": attempt, "run_id": run.id, "idempotency_key": key,
                })
            run_count += 1
            states[stage["id"]] = StageStatus.RUNNING
            block_reasons[stage["id"]] = None

    def _artifact_binding_gate(self, study_id: str, stage: StageSpec) -> dict[str, Any]:
        if not stage.bindings:
            return {"reason": None, "bindings": [], "sha256": None}
        resolved = []
        for declaration in stage.bindings:
            with self._db() as db:
                source = db.execute(
                    "SELECT run_id,status FROM study_stages WHERE study_id=? AND id=?",
                    (study_id, declaration.from_stage),
                ).fetchone()
                existing = db.execute(
                    "SELECT * FROM stage_artifact_bindings WHERE study_id=? AND stage_id=? AND role=?",
                    (study_id, stage.id, declaration.role),
                ).fetchone()
            if source is None or source["status"] != StageStatus.SUCCEEDED.value or source["run_id"] is None:
                return {"reason": "artifact_binding_required", "bindings": [], "sha256": None}
            expected_path = declaration.path.replace("${RUN_ID}", source["run_id"])
            try:
                artifacts = self.orchestrator.remote.artifacts(source["run_id"], path=expected_path)
            except (RemoteRunnerError, KeyError):
                return {"reason": "artifact_binding_unavailable", "bindings": [], "sha256": None}
            if not isinstance(artifacts, list) or len(artifacts) > 10000:
                return {"reason": "artifact_binding_invalid", "bindings": [], "sha256": None}
            matches = [item for item in artifacts if isinstance(item, dict) and item.get("path") == expected_path]
            if not matches:
                return {"reason": "artifact_binding_required", "bindings": [], "sha256": None}
            if len(matches) != 1:
                return {"reason": "artifact_binding_invalid", "bindings": [], "sha256": None}
            artifact = matches[0]
            digest = artifact.get("sha256")
            size = artifact.get("size")
            if (not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest) or
                    isinstance(size, bool) or not isinstance(size, int) or size < 0):
                return {"reason": "artifact_binding_invalid", "bindings": [], "sha256": None}
            value = {
                "role": declaration.role,
                "source_stage_id": declaration.from_stage,
                "source_run_id": source["run_id"],
                "path": expected_path,
                "sha256": digest,
                "size": size,
            }
            binding_sha256 = _digest(value)
            if existing is not None and existing["binding_sha256"] != binding_sha256:
                return {"reason": "artifact_binding_changed", "bindings": [], "sha256": None}
            verified_at = utc_now()
            with self._db() as db:
                if existing is None:
                    db.execute(
                        """INSERT INTO stage_artifact_bindings
                           (study_id,stage_id,role,source_stage_id,source_run_id,selector_json,path,sha256,
                            size,binding_sha256,verified_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (study_id, stage.id, declaration.role, declaration.from_stage, source["run_id"],
                         _canonical_json({"path": declaration.path}), expected_path, digest, size,
                         binding_sha256, verified_at),
                    )
                    self._event(db, study_id, stage.id, "artifact_binding_verified", value)
                else:
                    db.execute(
                        "UPDATE stage_artifact_bindings SET verified_at=? WHERE study_id=? AND stage_id=? AND role=?",
                        (verified_at, study_id, stage.id, declaration.role),
                    )
            resolved.append({**value, "binding_sha256": binding_sha256, "verified_at": verified_at})
        with self._db() as db:
            bindings_sha256 = self._artifact_bindings_sha256(db, study_id, stage.id)
        return {"reason": None, "bindings": resolved, "sha256": bindings_sha256}

    @staticmethod
    def _bound_experiment(stage: StageSpec, bindings: list[dict[str, Any]]) -> ExperimentSpec:
        if not bindings:
            return stage.experiment
        raw = stage.experiment.to_dict()
        task = dict(raw.get("task") or {})
        for binding in bindings:
            if binding["role"] == "checkpoint":
                task["source_run_id"] = binding["source_run_id"]
                task["checkpoint"] = {"path": binding["path"], "sha256": binding["sha256"]}
        raw["task"] = task
        return ExperimentSpec.from_dict(raw)

    def _stop(self, study_id: str, reason: Any) -> dict[str, Any]:
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 500:
            raise DomainError("stop reason must contain 1-500 characters")
        with self._db() as db:
            self._study_row(db, study_id)
            db.execute("UPDATE studies SET stop_requested=1,status=?,updated_at=? WHERE id=?",
                       (StudyStatus.STOPPING.value, utc_now(), study_id))
            db.execute(
                """UPDATE study_stages SET status=?,block_reason='study_stopped',updated_at=?
                   WHERE study_id=? AND run_id IS NULL AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED')""",
                (StageStatus.CANCELLED.value, utc_now(), study_id),
            )
            active = db.execute(
                """SELECT id,run_id FROM study_stages WHERE study_id=? AND run_id IS NOT NULL
                   AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED')""", (study_id,)
            ).fetchall()
            self._event(db, study_id, None, "study_stop_requested", {"reason": reason.strip()})
            self._action(db, study_id, "stop", {"reason": reason.strip()})
        errors = []
        for stage in active:
            try:
                run = self.orchestrator.cancel(stage["run_id"], reason.strip())
                if run.status == RunStatus.CANCELLED:
                    with self._db() as db:
                        self._set_stage(db, study_id, stage["id"], StageStatus.CANCELLED, "study_stopped")
                        self._event(db, study_id, stage["id"], "stage_cancelled", {"run_id": run.id})
            except RemoteRunnerError as exc:
                errors.append({"run_id": stage["run_id"], "code": exc.code,
                               "message": exc.message, "retryable": exc.retryable})
        self._sync_linked_runs(study_id)
        self._refresh_study_status(study_id)
        return self._view(study_id, sync_errors=errors)

    def _refresh_study_status(self, study_id: str) -> None:
        with self._db() as db:
            study = self._study_row(db, study_id)
            states = [StageStatus(row["status"]) for row in db.execute(
                "SELECT status FROM study_stages WHERE study_id=?", (study_id,)
            )]
            if study["stop_requested"]:
                target = StudyStatus.CANCELLED if all(state in _TERMINAL_STAGE for state in states) else StudyStatus.STOPPING
            elif all(state == StageStatus.SUCCEEDED for state in states):
                target = StudyStatus.SUCCEEDED
            elif any(state == StageStatus.FAILED for state in states):
                target = StudyStatus.FAILED
            elif any(state in {StageStatus.RUNNING, StageStatus.READY} for state in states):
                target = StudyStatus.RUNNING
            elif any(state == StageStatus.BLOCKED for state in states):
                target = StudyStatus.BLOCKED
            else:
                target = StudyStatus.PENDING
            if target.value != study["status"]:
                db.execute("UPDATE studies SET status=?,updated_at=? WHERE id=?", (target.value, utc_now(), study_id))
                self._event(db, study_id, None, "study_status_changed", {"status": target.value})

    def _view(self, study_id: str, *, sync_errors: list[dict] | None = None) -> dict[str, Any]:
        with self._db() as db:
            study = self._study_row(db, study_id)
            stages = db.execute(
                "SELECT * FROM study_stages WHERE study_id=? ORDER BY position", (study_id,)
            ).fetchall()
            dependencies = db.execute(
                "SELECT stage_id,depends_on FROM stage_dependencies WHERE study_id=? ORDER BY stage_id,depends_on",
                (study_id,),
            ).fetchall()
            approvals = db.execute(
                """SELECT stage_id,recommendation_id,plan_sha256,approved_by,created_at FROM stage_authorizations
                   WHERE study_id=? ORDER BY created_at""", (study_id,)
            ).fetchall()
            legacy_approvals = db.execute(
                """SELECT stage_id,plan_sha256,approved_by,created_at FROM stage_approvals
                   WHERE study_id=? ORDER BY created_at""", (study_id,)
            ).fetchall()
            recommendations = db.execute(
                """SELECT id,stage_id,decision,recommendation_json,recommendation_sha256,created_at
                   FROM agent_recommendations WHERE study_id=? ORDER BY rowid LIMIT 100""",
                (study_id,),
            ).fetchall()
            bindings = db.execute(
                """SELECT stage_id,role,source_stage_id,source_run_id,path,sha256,size,binding_sha256,verified_at
                   FROM stage_artifact_bindings WHERE study_id=? ORDER BY stage_id,role""",
                (study_id,),
            ).fetchall()
            adoptions = db.execute(
                """SELECT stage_id,run_id,run_spec_sha256,adopted_by,metadata_upgrade,created_at
                   FROM stage_run_adoptions WHERE study_id=?""",
                (study_id,),
            ).fetchall()
            decision_packages = db.execute(
                """SELECT stage_id,package_json,package_sha256,created_at
                   FROM stage_decision_packages WHERE study_id=?""",
                (study_id,),
            ).fetchall()
            events = db.execute(
                """SELECT seq,stage_id,kind,payload_json,created_at FROM workflow_events
                   WHERE study_id=? ORDER BY seq DESC LIMIT 100""", (study_id,)
            ).fetchall()
        by_stage: dict[str, list[str]] = {}
        for row in dependencies:
            by_stage.setdefault(row["stage_id"], []).append(row["depends_on"])
        bindings_by_stage: dict[str, list[dict[str, Any]]] = {}
        for row in bindings:
            bindings_by_stage.setdefault(row["stage_id"], []).append({
                "role": row["role"], "source_stage_id": row["source_stage_id"],
                "source_run_id": row["source_run_id"], "path": row["path"],
                "sha256": row["sha256"], "size": row["size"],
                "binding_sha256": row["binding_sha256"], "verified_at": row["verified_at"],
            })
        binding_digests = {
            stage_id: _digest({"bindings": [item["binding_sha256"] for item in items]})
            for stage_id, items in bindings_by_stage.items()
        }
        adopted_by_stage = {row["stage_id"]: {
            "kind": "adopted", "run_id": row["run_id"],
            "run_spec_sha256": row["run_spec_sha256"], "adopted_by": row["adopted_by"],
            "metadata_upgrade": row["metadata_upgrade"],
            "created_at": row["created_at"],
        } for row in adoptions}
        packages_by_stage = {}
        for row in decision_packages:
            package = json.loads(row["package_json"])
            packages_by_stage[row["stage_id"]] = {
                "readiness": package["readiness"],
                "package_sha256": row["package_sha256"],
                "prepared_at": row["created_at"],
                "checkpoint_count": len(package.get("checkpoint_candidates", [])),
            }
        latest_recommendations = {row["stage_id"]: row["id"] for row in recommendations}
        stage_kinds = {row["id"]: row["kind"] for row in stages}
        approved = {
            row["stage_id"]: dict(row) for row in approvals
            if (row["plan_sha256"] == study["spec_sha256"] and
                latest_recommendations.get(row["stage_id"]) == row["recommendation_id"])
        }
        approved.update({
            row["stage_id"]: dict(row) for row in legacy_approvals
            if row["plan_sha256"] == study["spec_sha256"] and stage_kinds.get(row["stage_id"]) != "test"
        })
        spec = json.loads(study["spec_json"])
        return {
            "schema_version": 1,
            "study": {
                "id": study["id"], "name": study["name"], "research_question": study["research_question"],
                "status": study["status"], "stop_requested": bool(study["stop_requested"]),
                "plan_sha256": study["spec_sha256"], "planning": spec["planning"], "budget": spec["budget"],
                "created_at": study["created_at"], "updated_at": study["updated_at"],
            },
            "stages": [{
                "id": row["id"], "name": row["name"], "kind": row["kind"],
                "status": row["status"], "block_reason": row["block_reason"],
                "depends_on": by_stage.get(row["id"], []),
                "requires_approval": bool(row["requires_approval"]),
                "approved": row["id"] in approved,
                "approval": approved.get(row["id"]),
                "attempt": row["attempt"], "run_id": row["run_id"],
                "run_link": (adopted_by_stage.get(row["id"]) or
                             ({"kind": "submitted"} if row["run_id"] else None)),
                "decision_package": packages_by_stage.get(row["id"]),
                "artifact_bindings": bindings_by_stage.get(row["id"], []),
                "artifact_bindings_sha256": binding_digests.get(row["id"]),
                "updated_at": row["updated_at"],
            } for row in stages],
            "sync_errors": list(sync_errors or []),
            "recommendations": [{
                **json.loads(row["recommendation_json"]),
                "id": row["id"], "sha256": row["recommendation_sha256"], "created_at": row["created_at"],
            } for row in recommendations],
            "events": [{
                "seq": row["seq"], "stage_id": row["stage_id"], "kind": row["kind"],
                "payload": json.loads(row["payload_json"]), "created_at": row["created_at"],
            } for row in reversed(events)],
        }

    def _list(self, limit: int) -> list[dict[str, Any]]:
        with self._db() as db:
            rows = db.execute(
                """SELECT id,name,research_question,status,spec_sha256,stop_requested,created_at,updated_at
                   FROM studies ORDER BY created_at DESC LIMIT ?""", (limit,)
            ).fetchall()
        return [{
            "id": row["id"], "name": row["name"], "research_question": row["research_question"],
            "status": row["status"], "plan_sha256": row["spec_sha256"],
            "stop_requested": bool(row["stop_requested"]),
            "created_at": row["created_at"], "updated_at": row["updated_at"],
        } for row in rows]

    def _set_stage_gate(self, study_id: str, stage_id: str, status: StageStatus, reason: str | None) -> None:
        with self._db() as db:
            current = db.execute(
                "SELECT status,block_reason FROM study_stages WHERE study_id=? AND id=?", (study_id, stage_id)
            ).fetchone()
            if current["status"] != status.value or current["block_reason"] != reason:
                self._set_stage(db, study_id, stage_id, status, reason)
                self._event(db, study_id, stage_id, "stage_gate_changed", {"status": status.value, "reason": reason})

    @staticmethod
    def _set_stage(db: sqlite3.Connection, study_id: str, stage_id: str,
                   status: StageStatus, reason: str | None) -> None:
        db.execute(
            "UPDATE study_stages SET status=?,block_reason=?,updated_at=? WHERE study_id=? AND id=?",
            (status.value, reason, utc_now(), study_id, stage_id),
        )

    def _stage_states(self, study_id: str) -> tuple[dict[str, StageStatus], dict[str, str | None]]:
        with self._db() as db:
            rows = db.execute(
                "SELECT id,status,block_reason FROM study_stages WHERE study_id=?", (study_id,)
            ).fetchall()
        return ({row["id"]: StageStatus(row["status"]) for row in rows},
                {row["id"]: row["block_reason"] for row in rows})

    def _dependencies(self, study_id: str, stage_id: str) -> list[str]:
        with self._db() as db:
            return [row["depends_on"] for row in db.execute(
                "SELECT depends_on FROM stage_dependencies WHERE study_id=? AND stage_id=?", (study_id, stage_id)
            )]

    def _approved(self, study_id: str, stage_id: str, plan_sha256: str,
                  recommendation_id: str | None) -> bool:
        if recommendation_id is None:
            with self._db() as db:
                return db.execute(
                    """SELECT 1 FROM stage_approvals
                       WHERE study_id=? AND stage_id=? AND plan_sha256=?""",
                    (study_id, stage_id, plan_sha256),
                ).fetchone() is not None
        with self._db() as db:
            return db.execute(
                """SELECT 1 FROM stage_authorizations
                   WHERE study_id=? AND stage_id=? AND plan_sha256=? AND recommendation_id=?""",
                (study_id, stage_id, plan_sha256, recommendation_id),
            ).fetchone() is not None

    def _recommendation_gate(self, study_id: str, stage_id: str, plan_sha256: str, kind: str) -> dict[str, Any]:
        if kind != "test":
            return {"id": None, "reason": None}
        with self._db() as db:
            recommendation = self._latest_recommendation(db, study_id, stage_id)
        if recommendation is None or recommendation["plan_sha256"] != plan_sha256:
            return {"id": None, "reason": "recommendation_required"}
        reasons = {"WAIT": "recommendation_wait", "STOP": "recommendation_stop"}
        if recommendation["decision"] == "PROCEED":
            raw = json.loads(recommendation["recommendation_json"])
            with self._db() as db:
                bindings_sha256 = self._artifact_bindings_sha256(db, study_id, stage_id)
            if raw.get("artifact_bindings_sha256") != bindings_sha256:
                return {"id": recommendation["id"], "reason": "recommendation_binding_mismatch"}
            if (any(item.get("observation_state") != "fresh" for item in raw.get("evidence", [])) or
                    not self._evidence_timestamps_fresh(raw.get("evidence", []))):
                return {"id": recommendation["id"], "reason": "recommendation_stale"}
        return {"id": recommendation["id"], "reason": reasons.get(recommendation["decision"])}

    @staticmethod
    def _artifact_bindings_sha256(db: sqlite3.Connection, study_id: str, stage_id: str) -> str | None:
        rows = db.execute(
            """SELECT binding_sha256 FROM stage_artifact_bindings
               WHERE study_id=? AND stage_id=? ORDER BY role""",
            (study_id, stage_id),
        ).fetchall()
        if not rows:
            return None
        return _digest({"bindings": [row["binding_sha256"] for row in rows]})

    @staticmethod
    def _evidence_timestamps_fresh(evidence) -> bool:
        now = datetime.now(timezone.utc)
        for item in evidence:
            try:
                observed = datetime.fromisoformat(item["observed_at"].replace("Z", "+00:00"))
                age = (now - observed).total_seconds()
            except (KeyError, TypeError, ValueError):
                return False
            if age > _MAX_RECOMMENDATION_EVIDENCE_AGE_SECONDS or age < -60:
                return False
        return bool(evidence)

    @staticmethod
    def _latest_recommendation(db: sqlite3.Connection, study_id: str, stage_id: str) -> sqlite3.Row | None:
        return db.execute(
            """SELECT * FROM agent_recommendations WHERE study_id=? AND stage_id=?
               ORDER BY rowid DESC LIMIT 1""", (study_id, stage_id)
        ).fetchone()

    @staticmethod
    def _study_row(db: sqlite3.Connection, study_id: str) -> sqlite3.Row:
        row = db.execute("SELECT * FROM studies WHERE id=?", (study_id,)).fetchone()
        if row is None:
            raise KeyError(study_id)
        return row

    @staticmethod
    def _event(db: sqlite3.Connection, study_id: str, stage_id: str | None,
               kind: str, payload: dict[str, Any]) -> None:
        seq = db.execute(
            "SELECT COALESCE(MAX(seq),0)+1 AS next_seq FROM workflow_events WHERE study_id=?", (study_id,)
        ).fetchone()["next_seq"]
        db.execute(
            "INSERT INTO workflow_events VALUES (?,?,?,?,?,?)",
            (study_id, seq, stage_id, kind, _canonical_json(payload), utc_now()),
        )

    @staticmethod
    def _action(db: sqlite3.Connection, study_id: str, action: str, payload: dict[str, Any]) -> None:
        db.execute(
            "INSERT INTO workflow_actions(study_id,action,payload_json,created_at) VALUES (?,?,?,?)",
            (study_id, action, _canonical_json(payload), utc_now()),
        )
