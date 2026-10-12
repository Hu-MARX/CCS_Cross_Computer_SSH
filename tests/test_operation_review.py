"""Focused regressions for reviewed operation outcome persistence boundaries."""

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from controller.domain import ExperimentSpec
from controller.gateway import CCSGateway, CCSRequestError
from controller.orchestrator import Orchestrator
from controller.remote import FakeRemoteRunner, RemoteRunnerError
from controller.repository import RunRepository


SPEC = {'schema_version': 1, 'name': 'operation-review', 'project': {}, 'runtime': {},
        'command': {'argv': ['python', 'train.py']}}


class OperationPersistenceReviewTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.remote = FakeRemoteRunner()
        self.repository = RunRepository(self.root / 'runs.db')
        self.gateway = CCSGateway(self.remote, repository=self.repository, worker='worker-a',
                                  audit_path=str(self.root / 'audit.db'))

    def confirmed(self, action, payload):
        request = {'schema_version': 1, 'request_id': 'review-request',
                   'actor': {'id': 'review-agent', 'kind': 'agent'},
                   'action': action, 'dry_run': True, 'payload': payload}
        plan = self.gateway.execute(request)
        return {**request, 'dry_run': False, 'confirm_plan_sha256': plan['plan_sha256']}

    def test_pre_dispatch_audit_failure_uses_latest_concurrent_submission_state(self):
        request = self.confirmed('run.submit', {'worker_id': 'worker-a',
                                              'idempotency_key': 'review-key', 'spec': SPEC})
        # Model another caller winning the reservation CAS during this caller's
        # audit write. Its accepted run is durable before the failed audit returns.
        def concurrent_acceptance_then_audit_error(*args, **kwargs):
            Orchestrator(self.repository, self.remote, worker='worker-a').submit(
                ExperimentSpec.from_dict(SPEC), idempotency_key='review-key',
                request_id=request['request_id'], actor=request['actor'],
                plan_sha256=request['confirm_plan_sha256'],
            )
            raise sqlite3.OperationalError('audit disk full')

        with patch.object(self.gateway.audit, 'record', side_effect=concurrent_acceptance_then_audit_error):
            with self.assertRaises(CCSRequestError) as raised:
                self.gateway.execute(request)
        operation = self.repository.get_operation(idempotency_key='review-key')
        self.assertEqual(operation['execution_state'], 'confirmed')
        self.assertEqual(raised.exception.execution_state, 'confirmed')
        self.assertEqual(raised.exception.operation_id, operation['operation_id'])
        self.assertEqual(raised.exception.run_id, operation['run_id'])
        self.assertEqual(len(self.remote.runs), 1)

    def test_remote_error_and_failed_local_marker_keep_unknown_correlation(self):
        request = self.confirmed('workspace.execute', {
            'worker_id': 'worker-a', 'request': {'action': 'stage', 'project': '/workspace/demo', 'operations': [
                {'op': 'write', 'path': 'draft.py', 'content': '# staged\n', 'expected_sha256': None}]},
        })
        remote = Mock(side_effect=RemoteRunnerError('SSH_TIMEOUT', 'response lost', execution_state='unknown'))
        with patch.object(self.remote, 'workspace', remote), \
                patch.object(self.repository, 'mark_operation', side_effect=sqlite3.OperationalError('state disk full')):
            with self.assertRaises(CCSRequestError) as raised:
                self.gateway.execute(request)
        operation = self.repository.get_operation(request_id='review-request')
        self.assertEqual(operation['state'], 'started')
        self.assertEqual(raised.exception.execution_state, 'unknown')
        self.assertEqual(raised.exception.operation_id, operation['operation_id'])
        self.assertEqual(raised.exception.worker_id, 'worker-a')
        self.assertFalse(raised.exception.retryable)
        self.assertEqual(remote.call_count, 1)

    def test_confirmed_commit_followed_by_read_failure_keeps_confirmed_correlation(self):
        request = self.confirmed('workspace.execute', {
            'worker_id': 'worker-a', 'request': {'action': 'stage', 'project': '/workspace/demo', 'operations': [
                {'op': 'write', 'path': 'draft.py', 'content': '# staged\n', 'expected_sha256': None}]},
        })
        original_get = self.repository.get_operation

        def fail_confirmed_read(*args, **kwargs):
            result = original_get(*args, **kwargs)
            if result and result['state'] == 'confirmed':
                raise sqlite3.OperationalError('state read unavailable')
            return result

        remote = Mock(return_value={'staged': True})
        with patch.object(self.remote, 'workspace', remote), \
                patch.object(self.repository, 'get_operation', side_effect=fail_confirmed_read):
            try:
                response = self.gateway.execute(request)
            except CCSRequestError as error:
                reported = {'execution_state': error.execution_state,
                            'operation_id': error.operation_id, 'worker_id': error.worker_id}
                self.assertFalse(error.retryable)
            else:
                # Returning the committed row from mark_operation avoids the
                # unnecessary reread entirely and is also a correct outcome.
                reported = response['operation']
                self.assertEqual(response['execution_state'], 'confirmed')
        operation = original_get(request_id='review-request')
        self.assertEqual(operation['state'], 'confirmed')
        self.assertEqual(reported['execution_state'], 'confirmed')
        self.assertEqual(reported['operation_id'], operation['operation_id'])
        self.assertEqual(reported['worker_id'], 'worker-a')
        self.assertEqual(remote.call_count, 1)


if __name__ == '__main__':
    unittest.main()
