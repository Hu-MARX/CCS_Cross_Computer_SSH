"""Public entrypoints require explicit targets; fixtures never contact workers."""
import contextlib
import importlib.util
import io
import json
import os
import shlex
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dashboard import server as dashboard_server


ROOT = Path(__file__).resolve().parents[1]


def load_recipe(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PublicConfigurationTests(unittest.TestCase):
    def test_dashboard_missing_target_cannot_initialize_a_database_or_remote(self):
        with patch.object(dashboard_server, 'OpenSSHRunner') as remote, \
                patch.object(dashboard_server, 'RunRepository') as repository, \
                contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                dashboard_server.main([])
        self.assertEqual(error.exception.code, 2)
        remote.assert_not_called()
        repository.assert_not_called()

    def test_dashboard_explicit_host_routes_to_requested_root_and_executable(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(dashboard_server, 'OpenSSHRunner') as remote, \
                    patch.object(dashboard_server, 'RunRepository') as repository, \
                    patch.object(dashboard_server, 'WorkflowEngine'), \
                    contextlib.redirect_stderr(io.StringIO()):
                repository.catalog.return_value = []
                with self.assertRaises(SystemExit):
                    dashboard_server.main(['--host', 'worker.example', '--user', 'ccs-runner',
                        '--remote-root', '/srv/test-worker', '--trainctl-executable', '/opt/ccs/bin/trainctl',
                        '--local-root', directory])
            self.assertEqual(remote.call_args.args, ('worker.example',))
            self.assertEqual(remote.call_args.kwargs['user'], 'ccs-runner')
            self.assertEqual(remote.call_args.kwargs['remote_root'], '/srv/test-worker')
            self.assertEqual(remote.call_args.kwargs['trainctl_executable'], '/opt/ccs/bin/trainctl')
            repository.assert_called_once_with(Path(directory).resolve() / 'data/orchestrator.db')

    def test_dashboard_fleet_does_not_silently_ignore_single_host_settings(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            dashboard_server.parse_args(['--fleet', 'pool.json', '--user', 'another-account'])

    def test_rf_detr_rejects_unconfigured_locations_before_loading_training(self):
        module = load_recipe('public_rf_detr', 'examples/rfdetr_managed.py')
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, '--project'):
                module.main(['--epochs', '1'])

    def test_rf_detr_paths_can_be_supplied_without_reading_models(self):
        module = load_recipe('configured_rf_detr', 'examples/rfdetr_managed.py')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            with patch.dict(os.environ, {'CCS_RFDETR_PROJECT': str(root / 'source'),
                    'CCS_RFDETR_DATASET': str(root / 'dataset'),
                    'CCS_RFDETR_PRETRAIN': str(root / 'weights.pth')}, clear=True):
                paths = module.training_paths(module.parse_args(['--epochs', '1']))
            self.assertEqual(paths, (root / 'source', root / 'dataset', root / 'weights.pth'))
            self.assertFalse((root / 'weights.pth').exists())

    def test_capture_requires_explicit_directories_before_opening_evidence_store(self):
        module = load_recipe('public_capture', 'scripts/capture_cvfdetr_evidence.py')
        with patch.object(module, 'EvidenceStore') as store, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                module.main([])
        self.assertEqual(error.exception.code, 2)
        store.assert_not_called()

    def test_fault_recipe_preview_never_constructs_a_remote_or_runs_ssh(self):
        module = load_recipe('public_fault_recipe', 'scripts/verify_periodic_cvfdetr.py')
        output = io.StringIO()
        with patch.object(module, 'OpenSSHRunner') as remote, \
                patch.object(module.subprocess, 'run') as child, contextlib.redirect_stdout(output):
            result = module.main(['--host', 'worker.example', '--user', 'ccs-runner',
                '--stage', "/srv/stage with 'quote", '--spec', str(ROOT / 'examples/agent-smoke-spec.json'),
                '--probe-python', '/opt/ccs/bin/python'])
        self.assertEqual(result, 0)
        preview = json.loads(output.getvalue())
        self.assertEqual(preview['execution_state'], 'not_sent')
        self.assertEqual(preview['host'], 'worker.example')
        command = shlex.split(preview['fault_command'])
        self.assertIn("/srv/stage with 'quote/damage_checkpoint_probe.py", command)
        self.assertEqual(command[-1], '<source_run>')
        remote.assert_not_called()
        child.assert_not_called()

    def test_fault_recipe_missing_target_is_rejected_before_ssh(self):
        module = load_recipe('unconfigured_fault_recipe', 'scripts/verify_periodic_cvfdetr.py')
        with patch.object(module, 'OpenSSHRunner') as remote, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                module.main(['--confirm-run'])
        self.assertEqual(error.exception.code, 2)
        remote.assert_not_called()


if __name__ == '__main__':
    unittest.main()
