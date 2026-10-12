"""Outcome recovery stays on the gateway seam and never replays remote effects."""

import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from controller.domain import ExperimentSpec
from controller.gateway import CCSGateway, CCSRequestError
from controller.gateway_cli import main
from controller.remote import FakeRemoteRunner, RemoteRunnerError
from controller.repository import RunRepository, SubmissionError


SPEC = {'schema_version': 1, 'name': 'outcome-test', 'project': {}, 'runtime': {},
        'command': {'argv': ['python', 'private_training_content.py']}}


class LostResponse(FakeRemoteRunner):
    def __init__(self):
        super().__init__()
        self.calls = 0

    def submit(self, spec, idempotency_key):
        self.calls += 1
        super().submit(spec, idempotency_key)
        raise RemoteRunnerError('SSH_TIMEOUT', 'response lost', execution_state='unknown')


class GatewayOperationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.remote = LostResponse()
        self.repository = RunRepository(self.root / 'runs.db')
        self.gateway = CCSGateway(self.remote, repository=self.repository, worker='worker-a',
                                  audit_path=str(self.root / 'audit.db'))
        self.payload = {'worker_id': 'worker-a', 'idempotency_key': 'recover-key', 'spec': SPEC}

    @staticmethod
    def request(action, payload, *, dry_run=False, request_id='req-outcome'):
        return {'schema_version': 1, 'request_id': request_id,
                'actor': {'id': 'test-agent', 'kind': 'agent'}, 'action': action,
                'dry_run': dry_run, 'payload': payload}

    def confirmed(self, action='run.submit', payload=None):
        request = self.request(action, self.payload if payload is None else payload, dry_run=True)
        plan = self.gateway.execute(request)
        request.update(dry_run=False, confirm_plan_sha256=plan['plan_sha256'])
        return request

    def test_lost_response_is_unknown_and_restart_reconcile_never_submits(self):
        request = self.confirmed()
        with self.assertRaises(SubmissionError) as caught:
            self.gateway.execute(request)
        error = caught.exception
        self.assertEqual(error.execution_state, 'unknown')
        self.assertFalse(error.retryable)
        self.assertIsNotNone(error.operation_id)
        restarted = CCSGateway(self.remote, repository=RunRepository(self.root / 'runs.db'),
                               worker='worker-a', audit_path=str(self.root / 'audit.db'))
        with patch.object(self.remote, 'health', side_effect=AssertionError('query must be offline')):
            operation = restarted.execute(self.request('operation.query', {'operation_id': error.operation_id}))['result']
        self.assertEqual(operation['execution_state'], 'unknown')
        self.assertNotIn('private_training_content', json.dumps(operation))
        result = restarted.execute(self.request('operation.reconcile', {'idempotency_key': 'recover-key'}))
        self.assertEqual(result['result']['execution_state'], 'confirmed')
        self.assertEqual(result['result']['run_id'], 'remote_0001')
        self.assertEqual(self.remote.calls, 1)
        self.assertEqual(restarted.execute(request)['result']['id'], 'remote_0001')
        self.assertEqual(self.remote.calls, 1)

    def test_started_but_no_worker_receipt_remains_unknown_and_cannot_replay(self):
        request = self.confirmed()
        op, _ = self.repository.reserve_submission(ExperimentSpec.from_dict(SPEC), 'worker-a', 'recover-key')
        self.repository.begin_submission(op['operation_id'])
        result = self.gateway.execute(self.request('operation.reconcile', {'operation_id': op['operation_id']}))
        self.assertEqual(result['result']['execution_state'], 'unknown')
        with self.assertRaises(SubmissionError) as caught:
            self.gateway.execute(request)
        self.assertEqual(caught.exception.execution_state, 'unknown')
        self.assertEqual(self.remote.calls, 0)

    def test_pre_dispatch_audit_failure_sends_nothing(self):
        request = self.confirmed()
        with patch.object(self.gateway.audit, 'record', side_effect=sqlite3.OperationalError('disk full')):
            with self.assertRaises(CCSRequestError) as caught:
                self.gateway.execute(request)
        self.assertEqual(caught.exception.execution_state, 'not_sent')
        self.assertEqual(self.remote.calls, 0)
        self.assertEqual(self.repository.get_operation(idempotency_key='recover-key')['state'], 'pending')

    def test_restart_with_broken_audit_can_still_query_unknown_operation(self):
        with self.assertRaises(SubmissionError) as caught:
            self.gateway.execute(self.confirmed())
        broken = self.root / 'broken-audit.db'
        broken.write_bytes(b'not a sqlite database')
        restarted = CCSGateway(self.remote, repository=RunRepository(self.root / 'runs.db'),
                               worker='worker-a', audit_path=str(broken))
        with patch.object(self.remote, 'health', side_effect=AssertionError('query must be offline')):
            result = restarted.execute(self.request('operation.query', {'operation_id': caught.exception.operation_id}))
        self.assertEqual(result['result']['execution_state'], 'unknown')
        self.assertEqual(result['warnings'][0]['code'], 'AUDIT_UNAVAILABLE')

    def test_final_audit_failure_preserves_confirmed_success(self):
        self.remote = FakeRemoteRunner()
        self.gateway.remote = self.remote
        self.gateway.worker_directory._bindings['worker-a'] = self.gateway.worker_directory._bindings['worker-a'].__class__(
            'worker-a', self.remote)
        request = self.confirmed()
        record = self.gateway.audit.record

        def fail_final(*args, **kwargs):
            if args[3] == 'succeeded':
                raise sqlite3.OperationalError('audit full')
            return record(*args, **kwargs)

        with patch.object(self.gateway.audit, 'record', side_effect=fail_final):
            result = self.gateway.execute(request)
        self.assertEqual(result['execution_state'], 'confirmed')
        self.assertTrue(result['executed'])
        self.assertEqual(result['warnings'][0]['code'], 'AUDIT_UNAVAILABLE')
        self.assertEqual(self.repository.get_operation(idempotency_key='recover-key')['state'], 'confirmed')

    def test_audit_unknown_records_are_correlated_and_hide_task_body(self):
        with self.assertRaises(SubmissionError):
            self.gateway.execute(self.confirmed())
        operation = self.repository.get_operation(idempotency_key='recover-key')
        records = self.gateway.execute(self.request('gateway.audit', {'operation_id': operation['operation_id']}))['result']['records']
        self.assertEqual(records[0]['outcome'], 'unknown')
        self.assertEqual(records[0]['execution_state'], 'unknown')
        self.assertEqual(records[0]['worker_id'], 'worker-a')
        self.assertNotIn('private_training_content', json.dumps(records))

    def test_generic_unknown_workspace_mutation_is_recorded_and_not_replayed(self):
        payload = {'worker_id': 'worker-a', 'request': {'action': 'stage', 'project': '/workspace/demo', 'operations': [
                {'op': 'write', 'path': 'draft.py', 'content': '# staged\n', 'expected_sha256': None}]}}
        request = self.confirmed('workspace.execute', payload)
        with patch.object(self.remote, 'workspace', side_effect=RemoteRunnerError('SSH_TIMEOUT', 'lost')) as remote:
            with self.assertRaises(CCSRequestError) as first:
                self.gateway.execute(request)
            self.assertEqual(first.exception.execution_state, 'unknown')
            with self.assertRaises(CCSRequestError) as second:
                self.gateway.execute(request)
            self.assertEqual(second.exception.operation_id, first.exception.operation_id)
            self.assertEqual(remote.call_count, 1)
        with self.assertRaises(CCSRequestError) as caught:
            self.gateway.execute(self.request('operation.reconcile', {'operation_id': first.exception.operation_id}))
        self.assertEqual(caught.exception.code, 'RECONCILIATION_UNSUPPORTED')

    def test_cli_error_envelope_preserves_operation_state(self):
        request = self.confirmed()
        path = self.root / 'request.json'
        path.write_text(json.dumps(request), encoding='utf-8')
        output = io.StringIO()
        with patch('controller.gateway_cli._worker_targets', return_value=(self.remote, self.remote,
                  self.gateway.worker_directory, 'worker-a')), redirect_stdout(output):
            code = main(['--host', 'fake', '--db', str(self.root / 'runs.db'),
                         '--audit-db', str(self.root / 'audit.db'), 'request', '--file', str(path)])
        error = json.loads(output.getvalue())['error']
        self.assertEqual(code, 1)
        self.assertFalse(error['retryable'])
        self.assertEqual(error['execution_state'], 'unknown')
        self.assertEqual(error['worker_id'], 'worker-a')
        self.assertTrue(error['operation_id'])


if __name__ == '__main__':
    unittest.main()
