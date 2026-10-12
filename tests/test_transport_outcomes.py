"""Submission transport outcomes; all transports are local injected fakes."""

import json
import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ccs.process import ProcessStopped
from ccs import CCSClient, CCSError
from controller.capabilities import worker_interface
from controller.domain import ExperimentSpec
from controller.remote import OpenSSHRunner, RemoteRunnerError


def remote_response(data=None, *, error=None, returncode=None):
    return SimpleNamespace(
        returncode=(1 if error else 0) if returncode is None else returncode,
        stdout=json.dumps({'ok': error is None, 'data': data, 'error': error}), stderr='',
    )


class RemoteTransportOutcomeTests(unittest.TestCase):
    mutation_commands = [
        (['submit', '--spec', '-', '--idempotency-key', 'key-one'], '{}'),
        (['cancel', 'run_' + 'a' * 32, '--reason', 'test'], None),
        (['checkpoint-import', 'prepare', '--request', '-'], '{}'),
        (['workspace', '--request', '-'], '{"action":"apply"}'),
    ]

    def test_started_mutation_timeout_is_unknown_and_not_retryable(self):
        for command, stdin in self.mutation_commands:
            with self.subTest(command=command):
                transport = Mock(side_effect=subprocess.TimeoutExpired('ssh', 1))
                runner = OpenSSHRunner('worker', command_runner=transport)
                with self.assertRaises(RemoteRunnerError) as raised:
                    runner._json_command(command, stdin=stdin)
                self.assertEqual(raised.exception.code, 'SSH_TIMEOUT')
                self.assertEqual(raised.exception.execution_state, 'unknown')
                self.assertFalse(raised.exception.retryable)
                self.assertEqual(transport.call_count, 1)

    def test_mutation_nonjson_and_ambiguous_exit_are_unknown(self):
        cases = [(0, 'response disappeared', 'INVALID_REMOTE_RESPONSE'),
                 (255, '', 'SSH_COMMAND_FAILED'),
                 (255, '{"ok":true,"data":{}}', 'SSH_COMMAND_FAILED')]
        for command, stdin in self.mutation_commands:
            for exit_code, stdout, error_code in cases:
                with self.subTest(command=command, exit_code=exit_code, stdout=stdout):
                    transport = Mock(return_value=SimpleNamespace(
                        returncode=exit_code, stdout=stdout, stderr='connection lost'))
                    runner = OpenSSHRunner('worker', command_runner=transport)
                    with self.assertRaises(RemoteRunnerError) as raised:
                        runner._json_command(command, stdin=stdin)
                    self.assertEqual(raised.exception.code, error_code)
                    self.assertEqual(raised.exception.execution_state, 'unknown')
                    self.assertFalse(raised.exception.retryable)
                    self.assertEqual(transport.call_count, 1)

    def test_process_spawn_failure_means_mutation_was_not_sent(self):
        runner = OpenSSHRunner('worker', command_runner=Mock(side_effect=FileNotFoundError('no ssh')))
        with self.assertRaises(RemoteRunnerError) as raised:
            runner._json_command(['submit', '--spec', '-'], stdin='{}')
        self.assertEqual(raised.exception.code, 'SSH_UNAVAILABLE')
        self.assertEqual(raised.exception.execution_state, 'not_sent')
        self.assertFalse(raised.exception.retryable)

    def test_read_transport_failures_preserve_retryability(self):
        for command, stdin in [(['health'], None),
                               (['workspace', '--request', '-'], '{"action":"inspect"}'),
                               (['lookup-submission', '--idempotency-key', 'one'], None)]:
            cases = [Mock(side_effect=subprocess.TimeoutExpired('ssh', 1)),
                     Mock(side_effect=FileNotFoundError('no ssh')),
                     Mock(return_value=SimpleNamespace(returncode=255, stdout='', stderr='lost'))]
            for transport in cases:
                with self.subTest(command=command, transport=transport):
                    runner = OpenSSHRunner('worker', command_runner=transport)
                    with self.assertRaises(RemoteRunnerError) as raised:
                        runner._json_command(command, stdin=stdin)
                    self.assertTrue(raised.exception.retryable)
                    self.assertIsNone(raised.exception.execution_state)
                    self.assertEqual(transport.call_count, 1)

    def test_worker_retryable_mutation_error_is_suppressed(self):
        error = {'code': 'START_UNCERTAIN', 'message': 'response lost', 'retryable': True}
        for returncode in (0, 1):
            with self.subTest(returncode=returncode):
                runner = OpenSSHRunner('worker', command_runner=lambda *a, **k: remote_response(
                    error=error, returncode=returncode))
                with self.assertRaises(RemoteRunnerError) as raised:
                    runner._json_command(['submit', '--spec', '-'], stdin='{}')
                self.assertEqual(raised.exception.code, 'START_UNCERTAIN')
                self.assertEqual(raised.exception.execution_state, 'unknown')
                self.assertFalse(raised.exception.retryable)
        runner = OpenSSHRunner('worker', command_runner=lambda *a, **k: remote_response(error=error))
        with self.assertRaises(RemoteRunnerError) as raised:
            runner._json_command(['health'])
        self.assertTrue(raised.exception.retryable)

    def test_malformed_mutation_envelopes_do_not_confirm_or_raise_raw_errors(self):
        cases = [{'ok': 'yes', 'data': {'run_id': 'run_' + 'a' * 32}},
                 {'ok': False, 'error': 'lost response'},
                 {'ok': False, 'error': ['lost response']},
                 ['not an envelope']]
        for envelope in cases:
            with self.subTest(envelope=envelope):
                runner = OpenSSHRunner('worker', command_runner=lambda *a, **k: SimpleNamespace(
                    returncode=0, stdout=json.dumps(envelope), stderr=''))
                with self.assertRaises(RemoteRunnerError) as raised:
                    runner._json_command(['submit', '--spec', '-'], stdin='{}')
                self.assertEqual(raised.exception.code, 'INVALID_REMOTE_RESPONSE')
                self.assertEqual(raised.exception.execution_state, 'unknown')
                self.assertFalse(raised.exception.retryable)

    def test_invalid_cancel_result_is_unknown(self):
        for data in (None, {}, {'status': 'not-a-status'}):
            with self.subTest(data=data):
                runner = OpenSSHRunner('worker', command_runner=lambda *a, **k: remote_response(data))
                with self.assertRaises(RemoteRunnerError) as raised:
                    runner.cancel('run_' + 'a' * 32, 'test')
                self.assertEqual(raised.exception.code, 'INVALID_REMOTE_RESPONSE')
                self.assertEqual(raised.exception.execution_state, 'unknown')
                self.assertFalse(raised.exception.retryable)

    def test_submit_invalid_run_identity_is_unknown(self):
        spec = ExperimentSpec.from_dict({
            'schema_version': 1, 'name': 'transport', 'project': {}, 'runtime': {},
            'command': {'argv': ['python', 'train.py']},
        })
        for data in (None, {}, {'run_id': None}, {'run_id': '../unsafe'}):
            with self.subTest(data=data):
                runner = OpenSSHRunner('worker', command_runner=lambda *a, **k: remote_response(data))
                runner.require_capabilities = Mock()
                with self.assertRaises(RemoteRunnerError) as raised:
                    runner.submit(spec, 'one-key')
                self.assertEqual(raised.exception.code, 'INVALID_REMOTE_RESPONSE')
                self.assertEqual(raised.exception.execution_state, 'unknown')
                self.assertFalse(raised.exception.retryable)

    def test_lookup_requires_capability_and_preserves_query_identity(self):
        descriptor = worker_interface()
        descriptor['capabilities'].pop('run.lookup')
        transport = Mock(return_value=remote_response({'ccs_interface': descriptor}))
        runner = OpenSSHRunner('worker', command_runner=transport)
        with self.assertRaises(RemoteRunnerError) as raised:
            runner.lookup_submission('key-one')
        self.assertEqual(raised.exception.code, 'WORKER_INCOMPATIBLE')
        self.assertEqual(transport.call_count, 1)
        self.assertNotIn('lookup-submission', transport.call_args.args[0][-1])

        transport = Mock(return_value=remote_response({
            'idempotency_key': 'key-one', 'state': 'found', 'run_id': 'run_' + 'a' * 32,
        }))
        runner = OpenSSHRunner('worker', command_runner=transport)
        runner.require_capabilities = Mock()
        self.assertEqual(runner.lookup_submission('key-one'), 'run_' + 'a' * 32)
        runner.require_capabilities.assert_called_once_with({'run.lookup': 1})
        self.assertIn('lookup-submission --idempotency-key key-one --json', transport.call_args.args[0][-1])

    def test_lookup_absence_and_invalid_receipts_never_imply_submission(self):
        base = {'idempotency_key': 'key-one', 'state': 'absent', 'run_id': None}
        runner = OpenSSHRunner('worker', command_runner=lambda *a, **k: remote_response(base))
        runner.require_capabilities = Mock()
        self.assertIsNone(runner.lookup_submission('key-one'))
        for change in ({'idempotency_key': 'other'}, {'run_id': 'run_' + 'a' * 32},
                       {'state': 'found'}, {'state': 'found', 'run_id': 1},
                       {'state': 'found', 'run_id': '../unsafe'}):
            with self.subTest(change=change):
                runner = OpenSSHRunner('worker', command_runner=lambda *a, **k: remote_response({**base, **change}))
                runner.require_capabilities = Mock()
                with self.assertRaises(RemoteRunnerError) as raised:
                    runner.lookup_submission('key-one')
                self.assertEqual(raised.exception.code, 'INVALID_REMOTE_RESPONSE')
                self.assertFalse(raised.exception.retryable)


class SDKTransportOutcomeTests(unittest.TestCase):
    @staticmethod
    def request(*, action='run.submit', dry_run=False):
        return {'schema_version': 1, 'request_id': 'request-one',
                'actor': {'id': 'test-agent', 'kind': 'agent'},
                'action': action, 'dry_run': dry_run,
                'payload': {'worker_id': 'lab118', 'run_id': 'run_' + 'a' * 32,
                            'idempotency_key': 'key-one'}}

    @staticmethod
    def error_response(error, *, request_id='request-one', returncode=1):
        return SimpleNamespace(returncode=returncode, stdout=json.dumps({
            'ok': False, 'request_id': request_id, 'data': None, 'error': error,
        }).encode('utf-8'))

    def test_legacy_retryable_mutation_error_becomes_unknown_with_original_ids(self):
        response = self.error_response({'code': 'SSH_TIMEOUT', 'message': 'lost', 'retryable': True})
        transport = Mock(return_value=response)
        with patch('ccs.client.run_bounded', transport):
            with self.assertRaises(CCSError) as raised:
                CCSClient(['fake-ccsctl']).execute(self.request())
        error = raised.exception
        self.assertFalse(error.retryable)
        self.assertEqual(error.execution_state, 'unknown')
        self.assertEqual(error.request_id, 'request-one')
        self.assertEqual(error.worker_id, 'lab118')
        self.assertEqual(error.run_id, 'run_' + 'a' * 32)
        self.assertEqual(error.idempotency_key, 'key-one')
        self.assertEqual(transport.call_count, 1)

    def test_error_operation_and_remote_run_ids_are_preserved(self):
        response = self.error_response({
            'code': 'OPERATION_UNKNOWN', 'message': 'query original operation', 'retryable': False,
            'execution_state': 'unknown', 'operation_id': 'op_' + 'b' * 32,
            'worker_id': 'resolved-worker', 'run_id': 'run_' + 'c' * 32,
            'idempotency_key': 'resolved-key',
        })
        with self.assertRaises(CCSError) as raised:
            CCSClient._response(response, self.request())
        metadata = raised.exception.to_dict()
        self.assertEqual(metadata['operation_id'], 'op_' + 'b' * 32)
        self.assertEqual(metadata['worker_id'], 'resolved-worker')
        self.assertEqual(metadata['run_id'], 'run_' + 'c' * 32)
        self.assertEqual(metadata['idempotency_key'], 'resolved-key')
        self.assertEqual(metadata['request_id'], 'request-one')

    def test_read_and_dry_run_keep_server_retryability(self):
        response = self.error_response({'code': 'SSH_TIMEOUT', 'message': 'lost', 'retryable': True})
        for request in (self.request(action='fleet.status'), self.request(dry_run=True)):
            with self.subTest(request=request):
                with self.assertRaises(CCSError) as raised:
                    CCSClient._response(response, request)
                self.assertTrue(raised.exception.retryable)
                self.assertIsNone(raised.exception.execution_state)

    def test_malformed_gateway_mutation_response_is_unknown(self):
        cases = [SimpleNamespace(returncode=0, stdout=b'not json'),
                 SimpleNamespace(returncode=0, stdout=b'\xff'),
                 SimpleNamespace(returncode=0, stdout=b'[]'),
                 self.error_response({'code': 'X', 'message': 'lost', 'retryable': True},
                                     request_id='another-request'),
                 self.error_response({'code': 'X', 'message': 'lost', 'retryable': True}, returncode=0)]
        for response in cases:
            with self.subTest(response=response):
                transport = Mock(return_value=response)
                with patch('ccs.client.run_bounded', transport):
                    with self.assertRaises(CCSError) as raised:
                        CCSClient(['fake-ccsctl']).execute(self.request())
                self.assertEqual(raised.exception.code, 'INVALID_GATEWAY_RESPONSE')
                self.assertEqual(raised.exception.execution_state, 'unknown')
                self.assertFalse(raised.exception.retryable)
                self.assertEqual(raised.exception.request_id, 'request-one')
                self.assertEqual(raised.exception.idempotency_key, 'key-one')
                self.assertEqual(transport.call_count, 1)

    def test_timeout_and_spawn_error_have_distinct_mutation_states(self):
        for failure, expected in ((ProcessStopped('TIMEOUT', started=True), 'unknown'),
                                  (FileNotFoundError('no ccsctl'), 'not_sent')):
            with self.subTest(failure=failure):
                transport = Mock(side_effect=failure)
                with patch('ccs.client.run_bounded', transport):
                    with self.assertRaises(CCSError) as raised:
                        CCSClient(['fake-ccsctl']).execute(self.request())
                self.assertEqual(raised.exception.execution_state, expected)
                self.assertFalse(raised.exception.retryable)
                self.assertEqual(raised.exception.request_id, 'request-one')
                self.assertEqual(raised.exception.worker_id, 'lab118')
                self.assertEqual(transport.call_count, 1)


if __name__ == '__main__':
    unittest.main()
