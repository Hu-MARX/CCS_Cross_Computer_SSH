import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import Mock, patch

from controller.capabilities import negotiate_worker, requirements_for_spec, worker_interface
from controller.cli import main as controller_main
from controller.domain import ExperimentSpec
from controller.remote import OpenSSHRunner, RemoteRunnerError
from trainctl.cli import main as trainctl_main
from trainctl.run_store import RunStore


class CapabilityContractTests(unittest.TestCase):
    @staticmethod
    def spec(*, task=False, snapshot=False, checkpoint=False):
        raw = {
            'schema_version': 1, 'name': 'contract', 'project': {}, 'runtime': {},
            'command': {'argv': ['/usr/bin/python3', '/srv/train.py']},
        }
        if task or checkpoint:
            raw['task'] = {'kind': 'train', 'inputs': []}
        if checkpoint:
            raw['task']['checkpoint'] = {'path': '/imports/checkpoint.ckpt', 'sha256': 'd' * 64}
        if snapshot:
            raw['evidence'] = {
                'bundle_id': 'bundle_' + 'a' * 32,
                'manifest_sha256': 'b' * 64,
                'execution_mode': 'snapshot',
            }
        return ExperimentSpec.from_dict(raw)

    def test_worker_exposes_same_versioned_interface_in_health_and_cli(self):
        with tempfile.TemporaryDirectory() as directory:
            health_interface = RunStore(directory).health()['ccs_interface']
            output = io.StringIO()
            with redirect_stdout(output):
                code = trainctl_main(['--root', directory, 'capabilities', '--json'])

        envelope = json.loads(output.getvalue())
        self.assertEqual(code, 0)
        self.assertTrue(envelope['ok'])
        self.assertEqual(envelope['data'], health_interface)
        self.assertEqual(health_interface['protocol'], {'name': 'ccs-worker', 'major': 1, 'minor': 0})
        self.assertEqual(health_interface['software']['version'], '0.6.1')
        self.assertGreaterEqual(health_interface['capabilities']['run.submit'], 1)
        self.assertGreaterEqual(health_interface['capabilities']['evidence.runtime'], 2)
        self.assertGreaterEqual(health_interface['capabilities']['checkpoint.export'], 1)
        self.assertGreaterEqual(health_interface['capabilities']['checkpoint.import'], 1)

    def test_negotiation_distinguishes_legacy_from_incompatible(self):
        legacy = negotiate_worker({'hostname': 'old-worker'}, {'run.submit': 1})
        self.assertEqual(legacy['state'], 'legacy')
        self.assertTrue(legacy['reachable'])
        self.assertFalse(legacy['compatible'])

        descriptor = worker_interface()
        descriptor['protocol']['major'] = 2
        incompatible = negotiate_worker({'ccs_interface': descriptor}, {'run.submit': 1})
        self.assertEqual(incompatible['state'], 'incompatible')
        self.assertIn('protocol major', incompatible['reason'])

        descriptor = worker_interface()
        descriptor['capabilities'].pop('run.submit')
        missing = negotiate_worker({'ccs_interface': descriptor}, {'run.submit': 1})
        self.assertEqual(missing['state'], 'incompatible')
        self.assertEqual(missing['missing_capabilities'], [
            {'name': 'run.submit', 'required': 1, 'available': None},
        ])

    def test_negotiation_accepts_capability_versions_at_or_above_minimum(self):
        result = negotiate_worker({'ccs_interface': worker_interface()}, {
            'run.submit': 1,
            'evidence.runtime': 2,
        })
        self.assertEqual(result['state'], 'compatible')
        self.assertTrue(result['compatible'])
        self.assertEqual(result['missing_capabilities'], [])

    def test_remote_compatibility_uses_health_and_does_not_probe_legacy_command(self):
        calls = []

        def legacy_worker(command, **kwargs):
            calls.append(command[-1])
            return SimpleNamespace(
                returncode=0,
                stdout=json.dumps({'ok': True, 'data': {'hostname': 'old-worker'}}),
                stderr='',
            )

        result = OpenSSHRunner('old-worker', command_runner=legacy_worker).compatibility({'run.submit': 1})
        self.assertEqual(result['state'], 'legacy')
        self.assertEqual(len(calls), 1)
        self.assertIn(' health ', f' {calls[0]} ')
        self.assertNotIn('capabilities', calls[0])

    def test_controller_cli_exposes_machine_readable_negotiation(self):
        output = io.StringIO()
        expected = {'state': 'compatible', 'compatible': True}
        with patch('controller.cli.OpenSSHRunner') as remote, redirect_stdout(output):
            remote.return_value.compatibility.return_value = expected
            code = controller_main([
                '--host', 'worker', '--db', ':memory:', 'compatibility',
                '--require', 'run.submit=1', '--require', 'evidence.runtime=2',
            ])

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue())['data'], expected)
        remote.return_value.compatibility.assert_called_once_with({
            'evidence.runtime': 2,
            'run.submit': 1,
        })

    def test_submit_requirements_follow_the_spec_features(self):
        self.assertEqual(requirements_for_spec(self.spec()), {'run.submit': 1})
        self.assertEqual(requirements_for_spec(self.spec(task=True, snapshot=True)), {
            'evidence.bundle': 1,
            'evidence.launch_gate': 1,
            'run.submit': 1,
            'source.snapshot': 1,
            'task.metadata': 1,
        })
        self.assertEqual(requirements_for_spec(self.spec(checkpoint=True)), {
            'checkpoint.input_gate': 1,
            'run.submit': 1,
            'task.metadata': 1,
        })

    def test_submit_refuses_legacy_worker_before_mutating_command(self):
        calls = []

        def legacy_worker(command, **kwargs):
            calls.append(command[-1])
            return SimpleNamespace(returncode=0, stdout=json.dumps({
                'ok': True, 'data': {'hostname': 'old-worker'},
            }), stderr='')

        adapter = OpenSSHRunner('old-worker', command_runner=legacy_worker)
        with self.assertRaises(RemoteRunnerError) as raised:
            adapter.submit(self.spec(), 'contract-key')
        self.assertEqual(raised.exception.code, 'WORKER_CAPABILITIES_UNDECLARED')
        self.assertEqual(len(calls), 1)
        self.assertIn(' health ', f' {calls[0]} ')

    def test_submit_negotiates_exact_requirements_before_dispatch(self):
        response = SimpleNamespace(returncode=0, stdout=json.dumps({
            'ok': True, 'data': {'run_id': 'run_' + 'c' * 32},
        }), stderr='')
        adapter = OpenSSHRunner('worker', command_runner=lambda *args, **kwargs: response)
        adapter.compatibility = Mock(return_value={
            'state': 'compatible', 'compatible': True, 'reason': 'ok',
        })
        run_id = adapter.submit(self.spec(task=True, snapshot=True), 'contract-key')
        self.assertEqual(run_id, 'run_' + 'c' * 32)
        adapter.compatibility.assert_called_once_with(requirements_for_spec(self.spec(task=True, snapshot=True)))


if __name__ == '__main__':
    unittest.main()
