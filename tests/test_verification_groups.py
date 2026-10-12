"""Acceptance boundaries for the grouped verifier, without rerunning its suites."""

import importlib.metadata
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile


_spec = importlib.util.spec_from_file_location('ccs_verification_groups',
    Path(__file__).resolve().parents[1] / 'scripts' / 'verify.py')
verify = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(verify)
_release_spec = importlib.util.spec_from_file_location('ccs_release_verification',
    Path(__file__).resolve().parents[1] / 'scripts' / 'verify-release.py')
release = importlib.util.module_from_spec(_release_spec)
_release_spec.loader.exec_module(release)


class VerificationGroupTests(unittest.TestCase):
    def test_external_runner_imports_bundled_examples_and_restores_caller_paths(self):
        child_code = '''
import importlib
import importlib.util
from pathlib import Path
import sys
import unittest
path = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location('external_ccs_verify', path)
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)
original_paths = list(sys.path)
original_cwd = Path.cwd()
assert str(verify.ROOT) not in original_paths
def import_fixture():
    module = importlib.import_module('examples.mock_workflow')
    assert Path(module.__file__).resolve() == verify.ROOT / 'examples' / 'mock_workflow.py'
    assert issubclass(module.MockRemote, module.FakeRemoteRunner)
verify.load_group = lambda group: unittest.TestSuite([unittest.FunctionTestCase(import_fixture)])
code = verify.main(['--group', 'core'])
assert code == 0
assert sys.path == original_paths, (sys.path, original_paths)
assert Path.cwd() == original_cwd
'''
        env = {key: value for key, value in os.environ.items()
               if key.upper() not in {'PYTHONPATH', 'PYTHONHOME'}}
        with tempfile.TemporaryDirectory() as unrelated:
            result = subprocess.run([sys.executable, '-I', '-c', child_code,
                                     str(verify.ROOT / 'scripts' / 'verify.py')],
                                    cwd=unrelated, env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(report['groups'][0]['passed_tests'], 1)

    def test_groups_partition_all_python_modules_and_keep_core_independent(self):
        core = set(verify.selected_modules('core'))
        mcp = set(verify.selected_modules('mcp'))
        research = set(verify.selected_modules('research'))
        self.assertTrue(core)
        self.assertEqual({path.name for path in mcp}, {'test_mcp_server.py'})
        self.assertEqual({path.name for path in research}, {'test_online_joint_registration.py'})
        self.assertFalse(core & mcp or core & research or mcp & research)
        self.assertEqual(set(verify.selected_modules('all')), core | mcp | research)
        self.assertIn('test_replay_data.py', {path.name for path in core})
        with patch.object(verify.importlib.metadata, 'version', side_effect=AssertionError('optional import')):
            self.assertEqual(verify.preflight('core', None), {})

    def test_missing_or_incompatible_mcp_dependency_is_a_failure(self):
        with patch.object(verify.importlib.metadata, 'version',
                          side_effect=importlib.metadata.PackageNotFoundError('mcp')):
            with self.assertRaisesRegex(ValueError, 'requires mcp'):
                verify.preflight('mcp', None)
        for version in ('1.26.0', '2.1.9', '3.0.0', '2.2.0rc1'):
            with self.subTest(version=version), patch.object(verify.importlib.metadata, 'version', return_value=version):
                with self.assertRaisesRegex(ValueError, 'Unsupported MCP SDK'):
                    verify.preflight('mcp', None)
        for version in ('2.2.0', '2.3.0'):
            with self.subTest(version=version), patch.object(verify.importlib.metadata, 'version', return_value=version):
                self.assertEqual(verify.preflight('mcp', None), {'mcp': version})

    def test_research_requires_explicit_existing_input(self):
        with self.assertRaisesRegex(ValueError, 'explicit --research-root'):
            verify.preflight('research', None)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'lacks scripts'):
                verify.preflight('research', directory)
            root = Path(directory)
            (root / 'scripts').mkdir()
            (root / 'scripts' / 'register_online_joint_dashboard.py').touch()
            self.assertEqual(verify.preflight('research', directory), {'research_root': str(root.resolve())})

    def test_windows_cannot_satisfy_linux_acceptance(self):
        with patch.object(verify.sys, 'platform', 'win32'):
            with self.assertRaisesRegex(ValueError, 'actual Linux interpreter'):
                verify.preflight('core', None, require_linux=True)

    def test_installed_child_environment_removes_source_import_and_user_site(self):
        with patch.dict(os.environ, {'PYTHONPATH': 'source', 'PYTHONHOME': 'another-interpreter',
                                     'CCS_RESEARCH_ROOT': 'external-project'}):
            env = release.clean_environment(Path('/evidence/temp'))
        self.assertNotIn('PYTHONPATH', env)
        self.assertNotIn('PYTHONHOME', env)
        self.assertNotIn('CCS_RESEARCH_ROOT', env)
        self.assertEqual(env['PYTHONNOUSERSITE'], '1')
        self.assertEqual(env['TEMP'], str(Path('/evidence/temp')))

    def test_package_inventory_requires_resource_fixtures_and_zero_core_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            wheel, sdist = Path(directory) / 'ccs.whl', Path(directory) / 'ccs.tar.gz'
            with tarfile.open(sdist, 'w:gz') as archive:
                for name in release.REQUIRED_SOURCE_FILES:
                    entry = tarfile.TarInfo('ccs/' + name)
                    entry.size = 2
                    archive.addfile(entry, io.BytesIO(b'{}'))
            def write_wheel(requirement, resource=True):
                with zipfile.ZipFile(wheel, 'w') as archive:
                    archive.writestr('ccs.dist-info/METADATA',
                        'Name: cross-computer-ssh\nVersion: 0.5.2\nRequires-Dist: ' + requirement + '\n')
                    if resource:
                        archive.writestr(release.REQUIRED_RESOURCE, '{}')
            write_wheel('mcp>=2.2,<3; extra == "mcp"')
            self.assertEqual(release.archive_inventory(wheel, sdist)['version'], '0.5.2')
            write_wheel('mcp>=2.2,<3; extra == "mcp"', resource=False)
            with self.assertRaisesRegex(ValueError, 'missing installed contract resource'):
                release.archive_inventory(wheel, sdist)
            write_wheel('mcp>=2.2,<3')
            with self.assertRaisesRegex(ValueError, 'third-party'):
                release.archive_inventory(wheel, sdist)


if __name__ == '__main__':
    unittest.main()
