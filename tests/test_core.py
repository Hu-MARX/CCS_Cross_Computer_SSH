import json
import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from controller.codex_planner import CodexDecisionParser
from controller.capabilities import worker_interface
from controller.domain import DomainError, ExperimentSpec, Run, RunStatus
from controller.orchestrator import Orchestrator
from controller.remote import FakeRemoteRunner, OpenSSHRunner
from controller.repository import RunRepository
from controller.policy import PolicyEngine, Verdict


def spec(**automation):
    return ExperimentSpec.from_dict({
        "schema_version": 1, "name": "demo", "project": {}, "runtime": {},
        "command": {"argv": ["python", "train.py"]}, "automation": {
            "max_attempts": 2, "allowed_actions": ["retry_same", "complete", "request_approval"], **automation,
        },
    })


class CoreTests(unittest.TestCase):
    def test_run_state_machine_rejects_skipping_evaluation(self):
        run = Run.new(spec())
        run.transition(RunStatus.PREPARING)
        run.transition(RunStatus.RUNNING)
        with self.assertRaises(DomainError):
            run.transition(RunStatus.SUCCEEDED)

    def test_repository_events_are_ordered_and_remote_events_are_deduplicated(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = RunRepository(Path(directory) / "state.db")
            run = repository.create_run(Run.new(spec()))
            repository.append_event(run.id, "local", {})
            repository.append_event(run.id, "remote", {"x": 1}, remote_seq=7)
            repository.append_event(run.id, "remote", {"x": 1}, remote_seq=7)
            events = repository.events(run.id)
            self.assertEqual([event.seq for event in events], [1, 2])

    def test_orchestrator_submit_is_idempotent_and_reconcile_syncs_fake(self):
        with tempfile.TemporaryDirectory() as directory:
            remote = FakeRemoteRunner()
            orchestrator = Orchestrator(RunRepository(":memory:"), remote)
            first = orchestrator.submit(spec(), idempotency_key="same")
            second = orchestrator.submit(spec(), idempotency_key="same")
            self.assertEqual(first.id, second.id)
            remote.advance(first.id, RunStatus.PREPARING)
            orchestrator.reconcile()
            self.assertEqual(orchestrator.repository.get_run(first.id).status, RunStatus.PREPARING)
            self.assertEqual(len(orchestrator.repository.events(first.id)), 4)

    def test_reconcile_fills_missed_lifecycle_states_for_fast_remote_run(self):
        with tempfile.TemporaryDirectory() as directory:
            remote = FakeRemoteRunner()
            orchestrator = Orchestrator(RunRepository(Path(directory) / "state.db"), remote)
            run = orchestrator.submit(spec(), idempotency_key="fast")
            remote.advance(run.id, RunStatus.SUCCEEDED, exit_code=0)
            orchestrator.reconcile()
            local = orchestrator.repository.get_run(run.id)
            self.assertEqual(local.status, RunStatus.SUCCEEDED)
            self.assertEqual(local.exit_code, 0)

    def test_policy_requires_approval_for_config_change(self):
        run = Run.new(spec(require_approval=["modify_hyperparameters"]))
        decision = {
            "schema_version": 1, "classification": "recoverable_failure", "summary": "oom", "evidence": ["oom"],
            "next_action": {"type": "request_approval", "params": {"proposed_action": "modify_hyperparameters"}},
            "confidence": 0.9, "requires_approval": True,
        }
        result = PolicyEngine().evaluate(__import__("controller.domain", fromlist=["Decision"]).Decision.from_dict(decision), run)
        self.assertEqual(result.verdict, Verdict.NEEDS_APPROVAL)

    def test_codex_parser_uses_last_valid_decision_jsonl_object(self):
        output = '\n'.join([json.dumps({"type": "turn.started"}), json.dumps({"decision": {
            "schema_version": 1, "classification": "success", "summary": "done", "evidence": [],
            "next_action": {"type": "complete"}, "confidence": 1, "requires_approval": False,
        }})])
        self.assertEqual(CodexDecisionParser().parse(output).next_action["type"], "complete")

    def test_openssh_adapter_quotes_remote_arguments_and_parses_jsonl(self):
        calls = []

        def fake_run(command, **kwargs):
            calls.append((command, kwargs))
            remote_command = command[-1]
            if " health " in f" {remote_command} ":
                return SimpleNamespace(returncode=0, stdout=json.dumps({"ok": True, "data": {
                    "ccs_interface": worker_interface(),
                }}), stderr="")
            if " events " in f" {remote_command} ":
                stdout = json.dumps({"seq": 2, "kind": "status_changed", "payload": {"status": "RUNNING"}})
                return SimpleNamespace(returncode=0, stdout=stdout, stderr="")
            return SimpleNamespace(returncode=0, stdout=json.dumps({"ok": True, "data": {
                "run_id": "run_abc", "status": "QUEUED", "updated_at": "now"
            }}), stderr="")

        adapter = OpenSSHRunner("ml-worker", user="mlrunner", command_runner=fake_run)
        self.assertEqual(adapter.submit(spec(), "key-1"), "run_abc")
        events = adapter.events("run_abc", 1)
        self.assertEqual(events[0].seq, 2)
        command, kwargs = next(call for call in calls if ' submit ' in f' {call[0][-1]} ')
        self.assertIn("BatchMode=yes", " ".join(command))
        self.assertIn("StrictHostKeyChecking=yes", " ".join(command))
        self.assertIn('"name": "demo"', kwargs["input"])

    def test_openssh_adapter_rejects_shell_injection_in_run_id(self):
        adapter = OpenSSHRunner("ml-worker", command_runner=lambda *args, **kwargs: None)
        with self.assertRaises(ValueError):
            adapter.snapshot("run-ok;touch-pwned")

    def test_openssh_adapter_supports_absolute_least_privilege_executable(self):
        calls = []
        def command(argv, **kwargs):
            calls.append(argv[-1])
            return SimpleNamespace(returncode=0, stdout=json.dumps({'ok': True, 'data': {}}), stderr='')
        adapter = OpenSSHRunner('ml-worker', user='lab118', remote_root='/home/ccs-runner/.ml-orchestrator',
            trainctl_executable='/home/ccs-runner/.ml-orchestrator/venv/bin/trainctl', command_runner=command)
        adapter.workspace({'action': 'inspect'})
        self.assertTrue(calls[0].startswith('/home/ccs-runner/.ml-orchestrator/venv/bin/trainctl '))
        with self.assertRaises(ValueError):
            OpenSSHRunner('ml-worker', trainctl_executable='relative/trainctl')

    def test_openssh_artifact_download_uses_scp_without_a_remote_shell_path(self):
        calls = []

        def transfer(argv, **kwargs):
            calls.append((argv, kwargs))
            Path(argv[-1]).write_bytes(b'checkpoint')
            return SimpleNamespace(returncode=0, stdout='', stderr='')

        adapter = OpenSSHRunner(
            '100.64.0.8', user='mlrunner', port=2222,
            known_hosts='known_hosts', identity_file='id_ed25519',
            command_runner=lambda *args, **kwargs: None, transfer_runner=transfer,
        )
        artifact = {'path': '/srv/ccs/runs/run_abc/checkpoints/ckpt.ckpt'}
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / 'download.partial'
            result = adapter.download_artifact('run_abc', artifact, destination)

        argv, kwargs = calls[0]
        self.assertEqual(argv[0], 'scp')
        self.assertIn('mlrunner@100.64.0.8:' + artifact['path'], argv)
        self.assertEqual(result['size'], len(b'checkpoint'))
        self.assertEqual(kwargs['timeout'], 24 * 60 * 60)
        with self.assertRaises(ValueError):
            adapter.download_artifact('run_abc', {'path': '/srv/ccs/../secret'}, Path('new-file'))

    def test_openssh_checkpoint_import_prepares_uploads_and_commits_exact_target(self):
        commands = []
        transfers = []
        checkpoint_id = 'ckpt_' + 'a' * 32
        payload = b'checkpoint'
        artifact = {
            'path': '/source/checkpoints/' + checkpoint_id + '.ckpt',
            'size': len(payload),
            'sha256': hashlib.sha256(payload).hexdigest(),
        }

        def command(argv, **kwargs):
            commands.append(argv[-1])
            if ' checkpoint-import prepare ' in ' ' + argv[-1] + ' ':
                request = json.loads(kwargs['input'])
                canonical = json.dumps(
                    request, sort_keys=True, separators=(',', ':'),
                ).encode()
                import_id = 'import_' + hashlib.sha256(canonical).hexdigest()[:32]
                data = {
                    'status': 'ready', 'import_id': import_id,
                    'upload_path': '/srv/ccs/checkpoint-imports/.incoming/' + import_id + '/checkpoint.partial',
                    'upload_required': True, 'size': artifact['size'], 'sha256': artifact['sha256'],
                }
            else:
                import_id = argv[-1].split()[-1]
                data = {
                    'status': 'imported', 'import_id': import_id,
                    'checkpoint_path': '/srv/ccs/checkpoint-imports/' + import_id + '/checkpoint.ckpt',
                    'size': artifact['size'], 'sha256': artifact['sha256'],
                    'import_receipt_sha256': 'f' * 64,
                }
            return SimpleNamespace(
                returncode=0, stdout=json.dumps({'ok': True, 'data': data}), stderr='',
            )

        def transfer(argv, **kwargs):
            transfers.append((argv, kwargs))
            return SimpleNamespace(returncode=0, stdout='', stderr='')

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'checkpoint.ckpt'
            path.write_bytes(payload)
            adapter = OpenSSHRunner(
                'worker', user='runner', remote_root='/srv/ccs',
                command_runner=command, transfer_runner=transfer,
            )
            result = adapter.import_checkpoint({
                'worker_id': 'source', 'run_id': 'run_source',
                'checkpoint_id': checkpoint_id, 'artifact': artifact,
                'size': artifact['size'], 'sha256': artifact['sha256'],
                'checkpoint_path': str(path), 'receipt_sha256': 'e' * 64,
            })

        self.assertEqual(result['status'], 'imported')
        self.assertEqual(len(transfers), 1)
        self.assertEqual(transfers[0][0][0], 'scp')
        self.assertEqual(transfers[0][0][-2], str(path.absolute()))
        self.assertTrue(transfers[0][0][-1].endswith('/checkpoint.partial'))
        self.assertTrue(any(' checkpoint-import commit ' in ' ' + item + ' ' for item in commands))


if __name__ == "__main__":
    unittest.main()
