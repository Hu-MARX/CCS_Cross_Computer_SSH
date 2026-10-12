import json
import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace

from controller.domain import RunStatus
from controller.monitor import watch
from controller.orchestrator import Orchestrator
from controller.remote import FakeRemoteRunner, OpenSSHRunner, RemoteRunnerError
from controller.repository import RunRepository
from test_core import spec


class FlakyRemote(FakeRemoteRunner):
    offline = False

    def events(self, run_id, after_seq):
        if self.offline:
            raise RemoteRunnerError('SSH_TIMEOUT', 'simulated disconnect')
        return super().events(run_id, after_seq)


class MonitorTests(unittest.TestCase):
    def test_cancel_after_remote_finish_preserves_success(self):
        remote = FakeRemoteRunner()
        manager = Orchestrator(RunRepository(':memory:'), remote)
        run = manager.submit(spec(), idempotency_key='finished')
        from controller.remote import RemoteSnapshot
        remote.cancel = lambda *_: RemoteSnapshot(run.id, RunStatus.SUCCEEDED, 0)
        result = manager.cancel(run.id, 'too late')
        self.assertEqual(result.status, RunStatus.SUCCEEDED)
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(manager.cancel(run.id, 'again').status, RunStatus.SUCCEEDED)

    def test_running_to_finished_and_lost_recovery_do_not_regress(self):
        for initial in (RunStatus.QUEUED, RunStatus.PREPARING, RunStatus.RUNNING,
                        RunStatus.EVALUATING, RunStatus.LOST):
            with self.subTest(initial=initial):
                repo = RunRepository(':memory:')
                remote = FakeRemoteRunner()
                manager = Orchestrator(repo, remote)
                run = manager.submit(spec(), idempotency_key='one')
                run.status = initial
                repo.update_run(run)
                remote.advance(run.id, RunStatus.SUCCEEDED, exit_code=0)
                manager.reconcile()
                self.assertEqual(repo.get_run(run.id).status, RunStatus.SUCCEEDED)

    def test_disconnect_restart_and_cursor_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.db'
            remote = FlakyRemote()
            manager = Orchestrator(RunRepository(path), remote)
            run = manager.submit(spec(), idempotency_key='one')
            remote.advance(run.id, RunStatus.RUNNING)
            manager.reconcile()
            remote.offline = True
            report = next(watch(manager, max_polls=1))
            self.assertEqual(report['errors'][0]['code'], 'SSH_TIMEOUT')
            self.assertEqual(manager.repository.get_run(run.id).status, RunStatus.RUNNING)
            remote.offline = False
            remote.advance(run.id, RunStatus.SUCCEEDED, exit_code=0)
            resumed = Orchestrator(RunRepository(path), remote)
            reports = list(watch(resumed, until_idle=True, max_polls=1))
            self.assertEqual(reports[0]['active_runs'], 0)
            seqs = [event.remote_seq for event in resumed.repository.events(run.id) if event.remote_seq]
            self.assertEqual(seqs, [1, 2, 3])

    def test_error_on_one_run_does_not_block_another(self):
        remote = FakeRemoteRunner()
        manager = Orchestrator(RunRepository(':memory:'), remote)
        first = manager.submit(spec(), idempotency_key='a')
        second = manager.submit(spec(), idempotency_key='b')
        original = remote.events
        def events(run_id, after):
            if run_id == first.id:
                raise RemoteRunnerError('RUN_NOT_FOUND', 'missing', retryable=False)
            return original(run_id, after)
        remote.events = events
        remote.advance(second.id, RunStatus.SUCCEEDED, exit_code=0)
        manager.reconcile()
        self.assertEqual(manager.repository.get_run(second.id).status, RunStatus.SUCCEEDED)
        self.assertEqual(len(manager.last_errors), 1)

    def test_remote_error_code_survives_nonzero_exit(self):
        def command(*args, **kwargs):
            return SimpleNamespace(returncode=1, stderr='', stdout=json.dumps({
                'ok': False, 'error': {'code': 'RUN_NOT_FOUND', 'message': 'missing', 'retryable': False}}))
        remote = OpenSSHRunner('host', command_runner=command)
        with self.assertRaises(RemoteRunnerError) as raised:
            remote.snapshot('run_missing')
        self.assertEqual(raised.exception.code, 'RUN_NOT_FOUND')
        self.assertFalse(raised.exception.retryable)

    def test_host_cannot_be_an_ssh_option(self):
        with self.assertRaises(ValueError):
            OpenSSHRunner('-oProxyCommand=bad')

    def test_artifact_response_remains_list(self):
        remote = OpenSSHRunner('host', command_runner=lambda *a, **k: SimpleNamespace(
            returncode=0, stderr='', stdout=json.dumps({'ok': True, 'data': [{'path': 'model.pt'}]})))
        self.assertEqual(remote.artifacts('run_a'), [{'path': 'model.pt'}])
