import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from controller.gateway_cli import main
from controller.remote import FakeRemoteRunner


class GatewayCliTests(unittest.TestCase):
    @staticmethod
    def request():
        return {
            'schema_version': 1,
            'request_id': 'cli-snapshot-plan',
            'actor': {'id': 'opencode-local', 'kind': 'agent'},
            'action': 'run.snapshot',
            'dry_run': True,
            'payload': {'run_id': 'remote_missing'},
        }

    def invoke(self, request):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'request.json'
            path.write_text(json.dumps(request), encoding='utf-8')
            output = io.StringIO()
            with patch('controller.gateway_cli.OpenSSHRunner', return_value=FakeRemoteRunner()), redirect_stdout(output):
                code = main([
                    '--host', 'worker', '--db', ':memory:', '--audit-db', ':memory:',
                    'request', '--file', str(path),
                ])
            return code, json.loads(output.getvalue())

    def test_request_prints_one_machine_readable_success_envelope(self):
        code, response = self.invoke(self.request())

        self.assertEqual(code, 0)
        self.assertTrue(response['ok'])
        self.assertEqual(response['request_id'], 'cli-snapshot-plan')
        self.assertEqual(response['data']['action'], 'run.snapshot')
        self.assertIsNone(response['error'])

    def test_request_errors_are_structured_and_have_nonzero_exit_status(self):
        request = self.request()
        request['schema_version'] = 999

        code, response = self.invoke(request)

        self.assertEqual(code, 1)
        self.assertFalse(response['ok'])
        self.assertEqual(response['request_id'], 'cli-snapshot-plan')
        self.assertEqual(response['error']['code'], 'INVALID_REQUEST')
        self.assertFalse(response['error']['retryable'])
        self.assertIsNone(response['data'])

    def test_help_uses_standard_successful_argparse_exit(self):
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaises(SystemExit) as raised:
            main(['--help'])

        self.assertEqual(raised.exception.code, 0)
        self.assertIn('usage: ccsctl', output.getvalue())
        self.assertNotIn('"ok":false', output.getvalue())

    def test_fleet_file_exposes_multiple_workers_to_the_same_request_interface(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            fleet = root / 'fleet.json'
            fleet.write_text(json.dumps({
                'schema_version': 1,
                'workers': [
                    {'id': 'lab118', 'host': '192.0.2.10', 'user': 'mlrunner',
                     'workspace_user': 'lab118', 'labels': ['cuda', 'vision']},
                    {'id': 'lab119', 'host': '100.64.0.2', 'user': 'mlrunner',
                     'workspace_user': 'researcher', 'labels': ['cuda', 'embodied']},
                ],
            }), encoding='utf-8')
            request = root / 'request.json'
            request.write_text(json.dumps({
                'schema_version': 1, 'request_id': 'fleet-status-cli',
                'actor': {'id': 'codex-local', 'kind': 'agent'},
                'action': 'fleet.status', 'dry_run': False, 'payload': {},
            }), encoding='utf-8')
            remotes = {'192.0.2.10': FakeRemoteRunner(), '100.64.0.2': FakeRemoteRunner()}

            output = io.StringIO()
            with patch('controller.gateway_cli.OpenSSHRunner',
                       side_effect=lambda host, **kwargs: remotes[host]), redirect_stdout(output):
                code = main([
                    '--fleet', str(fleet), '--db', ':memory:', '--audit-db', ':memory:',
                    'request', '--file', str(request),
                ])

        response = json.loads(output.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(
            [item['worker_id'] for item in response['data']['result']['workers']],
            ['lab118', 'lab119'],
        )
        self.assertEqual(response['data']['result']['selection_policy'], 'upper_agent_explicit')


if __name__ == '__main__':
    unittest.main()
