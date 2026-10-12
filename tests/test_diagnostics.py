import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ccs.diagnostics import DiagnosticError, configuration_plan, doctor, write_configuration
from controller.gateway_cli import main


class DiagnosticsTests(unittest.TestCase):
    def test_unicode_paths_preview_exact_confirm_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as temporary:
            cwd = Path(temporary) / '中文 空格项目'
            cwd.mkdir()
            script = cwd / '启动 脚本.py'
            script.write_text('pass', encoding='utf-8')
            target = cwd / 'mcp 配置.json'
            plan = configuration_plan(command=[sys.executable, script.name, '--fleet', 'worker pool.json'],
                                      cwd=str(cwd), output=str(target))
            self.assertFalse(target.exists())
            launcher = plan['content']['mcpServers']['ccs']
            self.assertTrue(Path(launcher['command']).is_absolute())
            self.assertIn('--read-only', launcher['args'])
            argv = json.loads(launcher['args'][launcher['args'].index('--command-json') + 1])
            self.assertEqual(argv[1], str(script))
            self.assertEqual(argv[3], str(cwd / 'worker pool.json'))
            with self.assertRaises(DiagnosticError) as error:
                write_configuration(plan, '0' * 64)
            self.assertEqual(error.exception.code, 'CONFIG_CONFIRMATION_REQUIRED')
            self.assertFalse(target.exists())
            write_configuration(plan, plan['plan_sha256'])
            original = target.read_bytes()
            with self.assertRaises(DiagnosticError) as error:
                write_configuration(plan, plan['plan_sha256'])
            self.assertEqual(error.exception.code, 'CONFIG_ALREADY_EXISTS')
            self.assertEqual(original, target.read_bytes())

    def test_missing_program_wrong_cwd_and_dependency_mismatch(self):
        with patch('ccs.diagnostics._probe_python', return_value={
                'python': [3, 10, 0], 'mcp_version': '3.0.0', 'ccs_available': True}):
            result = doctor(command=['ccs-missing-unique-program'], cwd='missing-doctor-directory', require_mcp=True)
        codes = {item['code'] for item in result['checks']}
        self.assertTrue({'CWD_NOT_FOUND', 'PROGRAM_NOT_FOUND', 'PYTHON_VERSION_UNSUPPORTED',
                         'MCP_DEPENDENCY_INCOMPATIBLE'} <= codes)
        self.assertFalse(result['ok'])

    def test_doctor_never_opens_private_key_or_runs_gateway(self):
        secret = 'PRIVATE_KEY_CONTENT_DO_NOT_READ'
        with tempfile.TemporaryDirectory() as directory:
            key = Path(directory) / 'identity'
            key.write_text(secret)
            with patch('ccs.diagnostics._probe_python', return_value={'python': [3, 11, 0]}):
                result = doctor(command=[sys.executable, '--identity-file', str(key)], cwd=directory)
            self.assertTrue(result['ok'])
            self.assertNotIn(secret, json.dumps(result))
            self.assertEqual(result['remote_probe'], 'not_requested')

    def test_cli_offline_does_not_construct_workers_or_databases(self):
        output = io.StringIO()
        with (patch('controller.gateway_cli._worker_targets', side_effect=AssertionError('worker touched')),
             patch('controller.gateway_cli.RunRepository', side_effect=AssertionError('database touched')),
             patch('ccs.diagnostics._probe_python', return_value={'python': [3, 11, 0]}),
             contextlib.redirect_stdout(output)):
            status = main(['doctor', '--command-json', json.dumps([sys.executable]), '--cwd', str(Path.cwd())])
        self.assertEqual(status, 0)
        self.assertTrue(json.loads(output.getvalue())['data']['ok'])

    def test_remote_probe_requires_explicit_target_and_worker(self):
        output = io.StringIO()
        with patch('ccs.diagnostics._probe_python', return_value={'python': [3, 11, 0]}), contextlib.redirect_stdout(output):
            status = main(['doctor', '--probe-remote'])
        self.assertEqual(status, 1)
        self.assertEqual(json.loads(output.getvalue())['error']['code'], 'TARGET_REQUIRED')

    def test_relative_fleet_and_module_install_are_diagnosed(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / 'workers.json'
            config.write_text('{"schema_version":1,"workers":[]}', encoding='utf-8')
            with patch('ccs.diagnostics._probe_python', return_value={'python': [3, 11, 0], 'ccs_available': False}):
                result = doctor(command=[sys.executable, '-m', 'controller.gateway_cli', '--fleet', config.name], cwd=directory)
            self.assertTrue({'INVALID_FLEET_CONFIG', 'CCS_PACKAGE_NOT_INSTALLED'} <= {item['code'] for item in result['checks']})

    def test_existing_ccs_config_checks_actual_gateway_and_cwd(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / 'mcp.json'
            config.write_text(json.dumps({'mcpServers': {'ccs': {'command': sys.executable, 'args': [
                '-m', 'ccs.mcp_server', '--command-json', json.dumps([sys.executable, 'missing-entry.py']),
                '--cwd', str(Path(directory) / 'missing-cwd')]}}}), encoding='utf-8')
            with patch('ccs.diagnostics._probe_python', return_value={
                    'python': [3, 11, 0], 'mcp_version': '2.2.0', 'ccs_available': True}):
                result = doctor(mcp_config=str(config), cwd=directory)
            self.assertFalse(result['ok'])
            self.assertTrue({'CWD_NOT_FOUND', 'ENTRY_FILE_NOT_FOUND'} <= {item['code'] for item in result['checks']})


if __name__ == '__main__':
    unittest.main()
