"""Offline upgrade preview uses captured public replies, never remote execution."""
from copy import deepcopy
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ccs.upgrade_preflight import main, upgrade_preview


def declaration(version, capabilities=None):
    return {'schema_version': 1, 'protocol': {'name': 'ccs-worker', 'major': 1, 'minor': 0},
            'software': {'name': 'cross-computer-ssh', 'version': version},
            'capabilities': capabilities or {}}


def observation(action, result, *, worker='worker-a', compatibility=None, failed=False):
    payload = {} if action == 'fleet.status' else {'worker_id': worker}
    if action == 'workspace.execute':
        payload['request'] = {'action': 'changes', 'project': '/home/runner/project'}
    request = {'schema_version': 1, 'request_id': f'read-{action}',
               'actor': {'id': 'reader', 'kind': 'agent'}, 'action': action,
               'dry_run': False, 'payload': payload}
    if failed:
        response = {'ok': False, 'data': None, 'error': {'code': 'SSH_TIMEOUT'}}
    else:
        if action != 'fleet.status':
            result = {**result, 'worker_id': worker}
        response = {'ok': True, 'data': {'request_id': request['request_id'],
                    'action': action, 'classification': 'read', 'executed': True,
                    'result': result, 'compatibility': compatibility or {}}}
    return {'request': request, 'response': response}


def evidence():
    return {'schema_version': 1, 'controller': {'version': '0.6.1'}, 'observations': [
        observation('fleet.status', {'workers': [{'worker_id': 'worker-a', 'state': 'available',
            'workload_state': 'available', 'workload_count': 1,
            'compatibility': {'software': {'version': '0.6.0'}, 'available_capabilities': {'run.submit': 1}},
            'node': {'python': '3.12.3'}}]}),
        observation('worker.health', {'ccs_interface': declaration('0.6.1', {'run.submit': 1}), 'python': '3.12.3'}),
        observation('worker.monitor', {'health': {'ccs_interface': declaration('0.5.2')},
            'observation_state': 'stale', 'observed_at': '2026-01-01T00:00:00Z', 'pid': 123}),
        observation('workspace.execute', {'action': 'changes'},
            compatibility={'software': {'version': '0.5.0'}, 'available_capabilities': {'workspace.control': 1}}),
    ]}


def preview(value):
    return upgrade_preview(value, worker_ids=['worker-a'], target_version='0.6.1', artifact_sha256='a' * 64)


class UpgradePreflightTests(unittest.TestCase):
    def test_role_versions_are_independent_and_preview_cannot_authorize_mutations(self):
        value = evidence()
        untouched = deepcopy(value)
        result = preview(value)
        roles = {row['role']: row for row in result['role_matrix']}
        self.assertEqual(roles['controller']['version'], '0.6.1')
        self.assertEqual(roles['runner_cli']['version'], '0.6.1')
        self.assertEqual(roles['observer_daemon']['version'], '0.5.2')
        self.assertEqual(roles['workspace']['version'], '0.5.0')
        self.assertEqual(roles['observer_daemon']['freshness'], 'stale')
        self.assertFalse(result['executed'])
        self.assertEqual(result['authorization'], 'not_granted')
        self.assertEqual(result['execution_state'], 'not_sent')
        self.assertNotIn('plan_sha256', result)
        self.assertEqual(result['artifact']['verification'], 'caller_declared_not_verified')
        self.assertEqual(value, untouched)
        self.assertEqual(result['workloads']['worker-a']['state'], 'busy')
        self.assertTrue(result['guards']['always_preserve_active_runtime'])

    def test_unknown_daemon_workspace_conda_and_venv_are_not_inferred_from_cli_version(self):
        value = evidence()
        value['observations'] = value['observations'][:2]
        value['observations'][1]['response']['data']['result']['ccs_interface'] = declaration('99.0.0')
        roles = {row['role']: row for row in preview(value)['role_matrix']}
        self.assertEqual(roles['runner_cli']['declared_capabilities'], {})
        self.assertEqual(roles['observer_daemon']['state'], 'unknown')
        self.assertEqual(roles['workspace']['state'], 'unknown')
        self.assertEqual(preview(value)['environment']['worker_conda_available'], 'unknown')
        self.assertEqual(preview(value)['environment']['worker_venv_available'], 'unknown')

    def test_doctor_does_not_invent_controller_version(self):
        value = evidence()
        value['controller'] = {'doctor': {'ok': True, 'data': {'ok': True,
            'interpreter': {'python': [3, 12, 3], 'ccs_available': True}}}}
        row = preview(value)['role_matrix'][0]
        self.assertIsNone(row['version'])
        self.assertEqual(row['state'], 'unknown')
        self.assertEqual(row['python'], [3, 12, 3])

    def test_failed_last_observation_replaces_previous_version_with_unknown(self):
        value = evidence()
        value['observations'].append(observation('worker.monitor', {}, failed=True))
        roles = {row['role']: row for row in preview(value)['role_matrix']}
        self.assertEqual(roles['observer_daemon']['state'], 'unknown')
        self.assertIsNone(roles['observer_daemon']['version'])
        self.assertEqual(roles['runner_cli']['version'], '0.6.1')

    def test_incompatible_declaration_is_not_masked_by_equal_target_version(self):
        value = evidence()
        data = value['observations'][1]['response']['data']
        data['compatibility'] = {'state': 'incompatible'}
        roles = {row['role']: row for row in preview(value)['role_matrix']}
        self.assertTrue(roles['runner_cli']['matches_target'])
        self.assertEqual(roles['runner_cli']['compatibility_state'], 'incompatible')
        self.assertIsNone(roles['observer_daemon']['compatibility_state'])

    def test_failed_fleet_or_malformed_monitor_cannot_reuse_healthy_evidence(self):
        value = evidence()
        value['observations'].append(observation('fleet.status', {}, failed=True))
        result = preview(value)
        self.assertEqual(result['workloads']['worker-a']['state'], 'unknown')
        roles = {row['role']: row for row in result['role_matrix']}
        self.assertEqual(roles['runner_cli']['state'], 'unknown')
        value = evidence()
        value['observations'][2]['response']['data']['result']['health'] = ['not-an-object']
        with self.assertRaises(ValueError):
            preview(value)

    def test_partial_process_inventory_cannot_claim_idle(self):
        value = evidence()
        value['observations'].append(observation('worker.processes', {
            'state': 'available', 'items': [], 'truncated': True}))
        self.assertEqual(preview(value)['workloads']['worker-a']['state'], 'unknown')
        value['observations'][-1]['response']['data']['result']['truncated'] = False
        self.assertEqual(preview(value)['workloads']['worker-a']['state'], 'idle')
        self.assertTrue(preview(value)['guards']['always_preserve_active_runtime'])

    def test_requires_explicit_worker_matching_public_reply_and_read_only_actions(self):
        variants = []
        value = evidence()
        value['observations'][1]['request']['payload'].pop('worker_id')
        variants.append(value)
        value = evidence()
        value['observations'][1]['response']['data']['result']['worker_id'] = 'worker-b'
        variants.append(value)
        value = evidence()
        value['observations'][1]['response']['data']['request_id'] = 'different-request'
        variants.append(value)
        value = evidence()
        value['observations'][3]['request']['payload']['request']['action'] = 'apply'
        variants.append(value)
        value = evidence()
        value['observations'][1]['request']['action'] = 'run.submit'
        variants.append(value)
        for value in variants:
            with self.subTest(value=value), self.assertRaises(ValueError):
                preview(value)

    def test_digest_is_deterministic_and_changes_with_evidence_or_target(self):
        value = evidence()
        result = preview(value)
        self.assertEqual(result['preview_sha256'], preview(deepcopy(value))['preview_sha256'])
        value['controller']['version'] = '0.6.0'
        self.assertNotEqual(result['preview_sha256'], preview(value)['preview_sha256'])
        changed = upgrade_preview(evidence(), worker_ids=['worker-a'], target_version='0.6.2', artifact_sha256='a' * 64)
        self.assertNotEqual(result['preview_sha256'], changed['preview_sha256'])

    def test_cli_only_reads_local_capture_and_outputs_preview(self):
        with tempfile.TemporaryDirectory() as directory:
            capture = Path(directory) / 'captured-public-json.json'
            capture.write_text(json.dumps(evidence()), encoding='utf-8')
            output = io.StringIO()
            with (patch('subprocess.run', side_effect=AssertionError('process invoked')),
                  patch('socket.create_connection', side_effect=AssertionError('network invoked')),
                  contextlib.redirect_stdout(output)):
                status = main(['--evidence', str(capture), '--worker-id', 'worker-a',
                               '--target-version', '0.6.1', '--artifact-sha256', 'a' * 64])
            self.assertEqual(status, 0)
            self.assertEqual(json.loads(output.getvalue())['data']['status'], 'preview_only')
            self.assertEqual(list(Path(directory).iterdir()), [capture])

    def test_invalid_cli_input_is_structured_and_never_sent(self):
        with tempfile.TemporaryDirectory() as directory:
            capture = Path(directory) / 'capture.json'
            capture.write_text('{"schema_version":1,"observations":NaN}', encoding='utf-8')
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                status = main(['--evidence', str(capture), '--worker-id', 'worker-a',
                               '--target-version', '0.6.1', '--artifact-sha256', 'a' * 64])
            self.assertEqual(status, 1)
            error = json.loads(output.getvalue())['error']
            self.assertEqual(error['code'], 'INVALID_UPGRADE_EVIDENCE')
            self.assertEqual(error['execution_state'], 'not_sent')


if __name__ == '__main__':
    unittest.main()
