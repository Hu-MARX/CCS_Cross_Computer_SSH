"""Cooperative pause at the public gateway and injected SSH transport seams."""

from copy import deepcopy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import shlex
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from ccs import CCSClient, CCSError
from ccs.contracts import describe, validate_payload, _validate
from controller.capabilities import requirements_for_spec
from controller.domain import DomainError, ExperimentSpec, RunStatus
from controller.fleet import WorkerBinding, WorkerDirectory, WorkerDirectoryError
from controller.gateway import CCSGateway, CCSRequestError
from controller.remote import FakeRemoteRunner, OpenSSHRunner, RemoteRunnerError
from trainctl.cli import main as worker_main
from trainctl.run_store import RunStore


SPEC = {'schema_version': 1, 'name': 'pause-contract', 'command': {'argv': ['python', 'train.py']},
        'task': {'kind': 'train', 'pause': {
            'schema_version': 1, 'protocol': 'managed-checkpoint-v1', 'contract': 'b' * 64}}}


class RecordingRemote(FakeRemoteRunner):
    def __init__(self):
        super().__init__()
        self.pause_calls = []

    def pause(self, run_id, reason, request_id, timeout_seconds=600):
        self.pause_calls.append((run_id, reason, request_id, timeout_seconds))
        return super().pause(run_id, reason, request_id, timeout_seconds)


class PauseGatewayTests(unittest.TestCase):
    def setUp(self):
        self.remote = RecordingRemote()
        self.run_id = self.remote.submit(ExperimentSpec.from_dict(SPEC), 'fixture-run')
        self.remote.advance(self.run_id, RunStatus.RUNNING)
        self.gateway = CCSGateway(self.remote, worker='worker-a', audit_path=':memory:')
        self.payload = {'run_id': self.run_id, 'worker_id': 'worker-a', 'reason': 'Maintenance pause'}

    def request(self, *, payload=None, dry_run=True):
        return {'schema_version': 1, 'request_id': 'pause-request',
                'actor': {'id': 'test-agent', 'kind': 'agent'}, 'action': 'run.pause',
                'dry_run': dry_run, 'payload': self.payload if payload is None else payload}

    def confirmed(self):
        request = self.request()
        plan = self.gateway.execute(request)
        request.update(dry_run=False, confirm_plan_sha256=plan['plan_sha256'])
        return request

    def test_plan_sends_nothing_and_confirmed_delivery_is_not_paused(self):
        plan = self.gateway.execute(self.request())
        self.assertEqual(plan['classification'], 'mutation')
        self.assertEqual(plan['required_capabilities'], {'run.pause': 1})
        self.assertFalse(plan['executed'])
        self.assertEqual(self.remote.pause_calls, [])
        result = self.gateway.execute(self.confirmed())
        self.assertEqual(self.remote.pause_calls, [(self.run_id, 'Maintenance pause', 'pause-request', 600)])
        self.assertEqual(result['execution_state'], 'confirmed')
        self.assertEqual(result['result']['status'], 'RUNNING')
        self.assertEqual(result['result']['pause']['state'], 'requested')
        schema = describe('run.pause')['output_schema']
        self.assertEqual(_validate(result['result'], schema, 'result', schema.get('$defs', {})), [])
        snapshot = self.gateway.execute({**self.request(), 'action': 'run.snapshot', 'dry_run': False,
                                         'payload': {'run_id': self.run_id, 'worker_id': 'worker-a'}})
        self.assertEqual(snapshot['result']['pause']['request_id'], 'pause-request')
        self.assertEqual(snapshot['result']['status'], 'RUNNING')

    def test_missing_or_changed_confirmation_never_requests_a_pause(self):
        with self.assertRaises(CCSRequestError) as caught:
            self.gateway.execute(self.request(dry_run=False))
        self.assertEqual(caught.exception.code, 'CONFIRMATION_REQUIRED')
        request = self.confirmed()
        request['payload'] = {**self.payload, 'timeout_seconds': 100}
        with self.assertRaises(CCSRequestError) as caught:
            self.gateway.execute(request)
        self.assertEqual(caught.exception.code, 'CONFIRMATION_REQUIRED')
        self.assertEqual(self.remote.pause_calls, [])

    def test_pause_rejects_legacy_instead_of_using_cancel_exception(self):
        class LegacyRemote(RecordingRemote):
            def health(self):
                return {'online': True}
        remote = LegacyRemote()
        gateway = CCSGateway(remote, worker='worker-a', audit_path=':memory:')
        with self.assertRaises(WorkerDirectoryError) as caught:
            gateway.execute(self.request())
        self.assertEqual(caught.exception.code, 'WORKER_CAPABILITIES_UNDECLARED')
        self.assertEqual(remote.pause_calls, [])

    def test_selected_worker_never_falls_back_to_default_or_workspace_account(self):
        other, workspace = RecordingRemote(), RecordingRemote()
        other_run = other.submit(ExperimentSpec.from_dict(SPEC), 'other-run')
        directory = WorkerDirectory([WorkerBinding('worker-a', self.remote),
                                     WorkerBinding('worker-b', other, workspace_remote=workspace)])
        gateway = CCSGateway(self.remote, worker='worker-a', worker_directory=directory, audit_path=':memory:')
        payload = {'run_id': other_run, 'worker_id': 'worker-b', 'reason': 'Selected task pause'}
        request = self.request(payload=payload)
        plan = gateway.execute(request)
        gateway.execute({**request, 'dry_run': False, 'confirm_plan_sha256': plan['plan_sha256']})
        self.assertEqual(other.pause_calls, [(other_run, 'Selected task pause', 'pause-request', 600)])
        self.assertEqual(self.remote.pause_calls, [])
        self.assertEqual(workspace.pause_calls, [])
        with self.assertRaises(CCSRequestError) as caught:
            gateway.execute(self.request(payload={'run_id': other_run, 'reason': 'Missing selection'}))
        self.assertEqual(caught.exception.code, 'WORKER_SELECTION_REQUIRED')

    def test_invalid_payload_is_rejected_before_worker_observation(self):
        with patch.object(self.remote, 'health', side_effect=AssertionError('Must not route invalid payload')):
            for change in ({'timeout_seconds': True}, {'timeout_seconds': 0}, {'timeout_seconds': 3601},
                           {'timeout_seconds': 1.5}, {'force': True}, {'reason': ' '}):
                with self.subTest(change=change), self.assertRaises(CCSRequestError) as caught:
                    self.gateway.execute(self.request(payload={**self.payload, **change}))
                self.assertEqual(caught.exception.code, 'INVALID_REQUEST')
        self.assertEqual(self.remote.pause_calls, [])

    def test_unknown_delivery_is_durable_and_cannot_be_replayed(self):
        original_pause = self.remote.pause
        def lose_response(*args, **kwargs):
            original_pause(*args, **kwargs)
            raise RemoteRunnerError('SSH_TIMEOUT', 'Response lost', retryable=True, execution_state='unknown')
        self.remote.pause = lose_response
        request = self.confirmed()
        with self.assertRaises(CCSRequestError) as caught:
            self.gateway.execute(request)
        error = caught.exception
        self.assertEqual(error.execution_state, 'unknown')
        self.assertFalse(error.retryable)
        query = self.gateway.execute({**self.request(), 'action': 'operation.query', 'dry_run': False,
                                      'payload': {'operation_id': error.operation_id}})
        self.assertEqual(query['result']['state'], 'unknown')
        self.assertEqual(query['result']['worker_id'], 'worker-a')
        self.assertEqual(query['result']['run_id'], self.run_id)
        with self.assertRaises(CCSRequestError) as caught:
            self.gateway.execute(request)
        self.assertEqual(caught.exception.code, 'OPERATION_ALREADY_RECORDED')
        self.assertEqual(len(self.remote.pause_calls), 1)
        self.assertEqual(self.remote.snapshot(self.run_id).pause['request_id'], 'pause-request')

    def test_definite_worker_rejection_does_not_become_ambiguous(self):
        request = self.confirmed()
        with patch.object(self.remote, 'pause', side_effect=RemoteRunnerError(
                'PAUSE_UNSUPPORTED', 'Task did not opt in', retryable=False, execution_state='rejected')):
            with self.assertRaises(CCSRequestError) as caught:
                self.gateway.execute(request)
        self.assertEqual(caught.exception.execution_state, 'rejected')
        self.assertFalse(caught.exception.retryable)

    def test_task_pause_is_explicit_strict_and_required_at_submission(self):
        self.assertEqual(requirements_for_spec(ExperimentSpec.from_dict(SPEC)),
                         {'run.pause': 1, 'run.submit': 1, 'task.metadata': 1})
        self.assertEqual(validate_payload('run.submit', {'spec': SPEC, 'idempotency_key': 'opt-in'}), 'mutation')
        for pause in (None, {}, {'schema_version': True, 'protocol': 'managed-checkpoint-v1', 'contract': 'b' * 64},
                      {'schema_version': 1, 'protocol': 'signal-v1', 'contract': 'b' * 64},
                      {'schema_version': 1, 'protocol': 'managed-checkpoint-v1', 'contract': 'bad'},
                      {**SPEC['task']['pause'], 'force': True}):
            raw = deepcopy(SPEC)
            raw['task']['pause'] = pause
            with self.subTest(pause=pause), self.assertRaises(DomainError):
                ExperimentSpec.from_dict(raw)
        raw = deepcopy(SPEC)
        raw['task']['kind'] = 'analysis'
        with self.assertRaises(DomainError):
            ExperimentSpec.from_dict(raw)


class PauseRemoteTests(unittest.TestCase):
    def test_ssh_pause_binds_original_request_and_preserves_strict_host_keys(self):
        calls = []
        data = {'run_id': 'run-one', 'status': 'RUNNING',
                'pause': {'schema_version': 1, 'request_id': 'request-one', 'state': 'requested'}}
        def transport(command, **kwargs):
            calls.append(command)
            return SimpleNamespace(returncode=0, stdout=json.dumps({'ok': True, 'data': data}), stderr='')
        remote = OpenSSHRunner('worker', command_runner=transport)
        with patch.object(remote, 'require_capabilities') as required:
            self.assertEqual(remote.pause('run-one', 'Save before maintenance', 'request-one', 300), data)
        required.assert_called_once_with({'run.pause': 1})
        self.assertIn('StrictHostKeyChecking=yes', calls[0])
        self.assertIn("pause run-one --reason 'Save before maintenance' --request-id request-one --timeout-seconds 300 --json",
                      calls[0][-1])

    def test_pause_timeout_is_unknown_nonretryable_and_not_reissued(self):
        calls = []
        def transport(command, **kwargs):
            calls.append(command)
            raise subprocess.TimeoutExpired('ssh', 1)
        remote = OpenSSHRunner('worker', command_runner=transport)
        with patch.object(remote, 'require_capabilities'), self.assertRaises(RemoteRunnerError) as caught:
            remote.pause('run-one', 'Save before maintenance', 'request-one')
        self.assertEqual(caught.exception.execution_state, 'unknown')
        self.assertFalse(caught.exception.retryable)
        self.assertEqual(len(calls), 1)

    def test_wrong_receipt_is_unknown_not_confirmed(self):
        for data in (None, {'run_id': 'other', 'status': 'RUNNING'},
                     {'run_id': 'run-one', 'status': 'RUNNING', 'pause': {
                         'schema_version': 1, 'request_id': 'other-request', 'state': 'requested'}}):
            with self.subTest(data=data):
                remote = OpenSSHRunner('worker', command_runner=lambda *args, **kwargs: SimpleNamespace(
                    returncode=0, stdout=json.dumps({'ok': True, 'data': data}), stderr=''))
                with patch.object(remote, 'require_capabilities'), self.assertRaises(RemoteRunnerError) as caught:
                    remote.pause('run-one', 'Maintenance', 'request-one')
                self.assertEqual(caught.exception.execution_state, 'unknown')
                self.assertFalse(caught.exception.retryable)


class PauseSDKTests(unittest.TestCase):
    def test_sdk_pause_failure_preserves_original_identity_and_never_suggests_retry(self):
        request = {'schema_version': 1, 'request_id': 'pause-request',
                   'actor': {'id': 'test-agent', 'kind': 'agent'}, 'action': 'run.pause', 'dry_run': False,
                   'confirm_plan_sha256': 'a' * 64,
                   'payload': {'worker_id': 'worker-a', 'run_id': 'run-one', 'reason': 'Maintenance'}}
        response = SimpleNamespace(returncode=1, stdout=json.dumps({
            'ok': False, 'request_id': 'pause-request', 'data': None,
            'error': {'code': 'SSH_TIMEOUT', 'message': 'Lost response', 'retryable': True},
        }).encode())
        with patch('ccs.client.run_bounded', return_value=response) as transport:
            with self.assertRaises(CCSError) as caught:
                CCSClient(['fake-ccsctl']).execute(request)
        self.assertEqual(caught.exception.execution_state, 'unknown')
        self.assertFalse(caught.exception.retryable)
        self.assertEqual(caught.exception.request_id, 'pause-request')
        self.assertEqual(caught.exception.worker_id, 'worker-a')
        self.assertEqual(caught.exception.run_id, 'run-one')
        self.assertEqual(transport.call_count, 1)


class RealWorkerPauseGatewayTests(unittest.TestCase):
    """Inject only SSH transport: the worker CLI, store and gateway all remain real."""

    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.store = RunStore(self.root / 'worker')
        for name, value in (('_gpu_snapshot', []), ('_cpu_snapshot', {}), ('_memory_snapshot', {}),
                            ('processes', {'state': 'available', 'items': []})):
            mock = patch.object(RunStore, name, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)
        self.calls = []
        self.remote = OpenSSHRunner('local-worker-fixture', remote_root='/fixture-worker',
                                    command_runner=self.transport)
        self.gateway = CCSGateway(self.remote, worker='worker-a', audit_path=':memory:')

    def transport(self, command, **kwargs):
        # Convert the SSH wire argv into a real local CLI invocation. No SSH process is spawned.
        tokens = shlex.split(command[-1])
        self.assertEqual(tokens[:3], ['trainctl', '--root', '/fixture-worker'])
        argv = ['--root', str(self.store.root), *tokens[3:]]
        self.calls.append(tokens[3:])
        output = io.StringIO()
        with redirect_stdout(output):
            code = worker_main(argv)
        return SimpleNamespace(returncode=code, stdout=output.getvalue(), stderr='')

    def submit_fixture(self, *, supports_pause=True, terminal=False):
        raw = deepcopy(SPEC)
        raw['runtime'] = {'workdir': str(self.root)}
        if not supports_pause:
            raw['task'].pop('pause')
        with patch.object(self.store, '_start_runner', return_value=0):
            key = 'fixture-' + str(len(list(self.store.runs.iterdir())))
            run_id = self.store.submit(ExperimentSpec.from_dict(raw), key)['run_id']
        if terminal:
            self.store.finish(self.store.runs / run_id, RunStatus.SUCCEEDED, exit_code=0)
        return run_id

    def request(self, run_id, identity):
        return {'schema_version': 1, 'request_id': identity,
                'actor': {'id': 'test-agent', 'kind': 'agent'}, 'action': 'run.pause', 'dry_run': True,
                'payload': {'run_id': run_id, 'worker_id': 'worker-a', 'reason': 'Maintenance'}}

    def confirmed(self, run_id, identity):
        request = self.request(run_id, identity)
        plan = self.gateway.execute(request)
        return {**request, 'dry_run': False, 'confirm_plan_sha256': plan['plan_sha256']}

    def operation(self, operation_id):
        return self.gateway.execute({'schema_version': 1, 'request_id': 'query-' + operation_id,
                                     'actor': {'id': 'test-agent', 'kind': 'agent'},
                                     'action': 'operation.query', 'dry_run': False,
                                     'payload': {'operation_id': operation_id}})['result']

    def test_real_worker_rejections_stay_rejected_in_the_gateway_operation_ledger(self):
        cases = [(self.submit_fixture(supports_pause=False), 'unsupported', 'PAUSE_NOT_SUPPORTED'),
                 (self.submit_fixture(terminal=True), 'inactive', 'PAUSE_NOT_ACTIVE'),
                 ('not-a-managed-run', 'invalid-run', 'INVALID_RUN_ID')]
        for run_id, identity, expected_code in cases:
            with self.subTest(case=identity):
                request = self.confirmed(run_id, 'pause-' + identity)
                with self.assertRaises(CCSRequestError) as caught:
                    self.gateway.execute(request)
                error = caught.exception
                self.assertEqual(error.code, expected_code)
                self.assertEqual(error.execution_state, 'rejected')
                self.assertFalse(error.retryable)
                self.assertEqual(self.operation(error.operation_id)['execution_state'], 'rejected')
                if run_id.startswith('run_'):
                    self.assertFalse((self.store.runs / run_id / 'pause-request.json').exists())
        self.assertEqual(sum(call[0] == 'pause' for call in self.calls), 3)
        self.assertFalse(any(call[0] == 'cancel' for call in self.calls))

    def test_illegal_timeout_is_local_not_sent_and_worker_cli_also_rejects_it(self):
        run_id = self.submit_fixture()
        request = self.request(run_id, 'pause-invalid-timeout')
        request['payload']['timeout_seconds'] = 0
        with self.assertRaises(CCSRequestError) as caught:
            self.gateway.execute(request)
        self.assertEqual(caught.exception.execution_state, 'not_sent')
        self.assertEqual(self.calls, [])
        output = io.StringIO()
        with redirect_stdout(output):
            code = worker_main(['--root', str(self.store.root), 'pause', run_id,
                                '--reason', 'Maintenance', '--request-id', 'pause-invalid-timeout',
                                '--timeout-seconds', '0'])
        response = json.loads(output.getvalue())
        self.assertEqual(code, 1)
        self.assertEqual(response['error']['code'], 'INVALID_PAUSE_REQUEST')
        self.assertEqual(response['error']['execution_state'], 'rejected')
        self.assertFalse((self.store.runs / run_id / 'pause-request.json').exists())

    def test_snapshot_failure_after_request_publication_stays_unknown(self):
        run_id = self.submit_fixture()
        request = self.confirmed(run_id, 'pause-published')
        with patch.object(RunStore, 'snapshot', side_effect=OSError('Snapshot unavailable after publication')):
            with self.assertRaises(CCSRequestError) as caught:
                self.gateway.execute(request)
        error = caught.exception
        self.assertEqual(error.execution_state, 'unknown')
        self.assertFalse(error.retryable)
        operation = self.operation(error.operation_id)
        self.assertEqual(operation['execution_state'], 'unknown')
        self.assertEqual(operation['run_id'], run_id)
        published = json.loads((self.store.runs / run_id / 'pause-request.json').read_text())
        self.assertEqual(published['request_id'], 'pause-published')
        with self.assertRaises(CCSRequestError) as caught:
            self.gateway.execute(request)
        self.assertEqual(caught.exception.code, 'OPERATION_ALREADY_RECORDED')
        self.assertEqual(sum(call[0] == 'pause' for call in self.calls), 1)


if __name__ == '__main__':
    unittest.main()
