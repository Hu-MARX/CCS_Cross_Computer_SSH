"""Contracts are useful from CLI/SDK/MCP without a configured worker."""

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from ccs import CCSClient, CCSError
from ccs.contracts import describe, request_schema
from controller.gateway import CCSGateway
from controller.gateway_cli import main
from controller.remote import FakeRemoteRunner


class OfflineDescribeTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='CCS 合同 ')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    @staticmethod
    def request(action='gateway.describe', payload=None):
        return {'schema_version': 1, 'request_id': 'describe-request',
                'actor': {'id': 'contract-agent', 'kind': 'agent'}, 'action': action,
                'dry_run': False, 'payload': payload if payload is not None else {}}

    def invoke(self, args):
        output = io.StringIO()
        with patch('controller.gateway_cli._worker_targets', side_effect=AssertionError('must stay offline')), \
                patch('controller.gateway_cli.RunRepository', side_effect=AssertionError('must not create DB')), \
                redirect_stdout(output):
            code = main(args)
        return code, json.loads(output.getvalue())

    def test_describe_without_target_lists_all_actions(self):
        code, response = self.invoke(['describe'])
        self.assertEqual(code, 0)
        self.assertEqual(response['data'], describe())
        self.assertIsNone(response['request_id'])
        self.assertIn('run.submit', response['data']['actions'])
        self.assertIn('gateway.describe', response['data']['actions'])

    def test_invalid_target_configuration_is_ignored_for_offline_contract(self):
        code, response = self.invoke(['--fleet', str(self.root / 'missing fleet.json'),
                                      '--db', str(self.root / 'should-not-exist.db'),
                                      'describe', '--action', 'worker.health'])
        self.assertEqual(code, 0)
        self.assertEqual(response['data']['action'], 'worker.health')
        self.assertFalse((self.root / 'should-not-exist.db').exists())

    def test_request_schema_is_exported_from_packaged_source(self):
        code, response = self.invoke(['describe', '--schema', 'request'])
        self.assertEqual(code, 0)
        self.assertEqual(response['data'], request_schema())

    def test_workspace_contract_and_unknown_action_error_are_offline(self):
        code, response = self.invoke(['describe', '--action', 'workspace.execute', '--workspace-action', 'stage'])
        self.assertEqual(code, 0)
        self.assertEqual(response['data']['workspace_action'], 'stage')
        code, response = self.invoke(['describe', '--action', 'run.typo'])
        self.assertEqual(code, 1)
        self.assertFalse(response['error']['retryable'])
        self.assertTrue(response['error']['field_errors'])

    def test_gateway_json_describe_has_normal_response_and_no_target(self):
        path = self.root / 'request.json'
        path.write_text(json.dumps(self.request(payload={'action': 'run.submit'})), encoding='utf-8')
        code, response = self.invoke(['request', '--file', str(path)])
        self.assertEqual(code, 0)
        self.assertEqual(response['request_id'], 'describe-request')
        self.assertEqual(response['data']['classification'], 'read')
        self.assertTrue(response['data']['executed'])
        self.assertEqual(response['data']['result']['action'], 'run.submit')

    def test_regular_request_still_requires_target(self):
        path = self.root / 'regular.json'
        path.write_text(json.dumps(self.request('fleet.status')), encoding='utf-8')
        code, response = self.invoke(['request', '--file', str(path)])
        self.assertEqual(code, 1)
        self.assertEqual(response['error']['code'], 'TARGET_REQUIRED')

    def test_missing_envelope_field_and_boolean_version_return_field_errors(self):
        for change, expected in ((lambda request: request.pop('actor'), 'request.actor'),
                                 (lambda request: request.update(schema_version=True), 'request.schema_version')):
            request = self.request()
            change(request)
            path = self.root / 'invalid.json'
            path.write_text(json.dumps(request), encoding='utf-8')
            with self.subTest(field=expected):
                code, response = self.invoke(['request', '--file', str(path)])
                self.assertEqual(code, 1)
                self.assertEqual(response['error']['code'], 'INVALID_REQUEST')
                self.assertIn(expected, [item['path'] for item in response['error']['field_errors']])

    def test_payload_field_errors_precede_any_worker_observation(self):
        remote = FakeRemoteRunner()
        gateway = CCSGateway(remote, audit_path=':memory:')
        with patch.object(remote, 'health', side_effect=AssertionError('invalid payload must not reach worker')):
            for action, payload, path in (
                    ('worker.health', {'workre_id': 'default'}, 'payload.workre_id'),
                    ('run.events', {'run_id': 'run_1', 'after_seq': True}, 'payload.after_seq'),
                    ('run.submit', {'idempotency_key': 'key'}, 'payload.spec'),
                    ('workspace.execute', {'request': {'action': 'stage'}}, 'payload.request.project')):
                with self.subTest(action=action), self.assertRaises(Exception) as caught:
                    gateway.execute(self.request(action, payload))
                self.assertEqual(caught.exception.code, 'INVALID_REQUEST')
                self.assertIn(path, [item['path'] for item in caught.exception.field_errors])

    def test_real_sdk_process_can_describe_and_preserves_field_errors(self):
        sdk = CCSClient([sys.executable, '-m', 'controller.gateway_cli'], cwd=self.root)
        contract = sdk.describe('workspace.execute', workspace_action='inspect')
        self.assertEqual(contract['workspace_action'], 'inspect')
        with self.assertRaises(CCSError) as caught:
            sdk.request('gateway.describe', {'action': 17}, dry_run=False)
        self.assertEqual(caught.exception.execution_state, 'not_sent')
        self.assertEqual(caught.exception.field_errors[0]['path'], 'payload.action')
        self.assertFalse(any(self.root.glob('*.db')))


if __name__ == '__main__':
    unittest.main()
