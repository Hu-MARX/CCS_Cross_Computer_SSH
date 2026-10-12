import copy
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from trainctl.cli import main
from trainctl.evidence import EvidenceStore


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.source = self.root / 'research'
        self.source.mkdir()
        (self.source / 'code.py').write_text('original code', encoding='utf-8')
        (self.source / 'data.bin').write_bytes(b'dataset')
        self.store = EvidenceStore(self.root / 'evidence')
        self.plan = {'schema_version': 1, 'min_free_bytes': 0, 'groups': [
            {'name': 'code', 'root': str(self.source), 'mode': 'copy', 'files': ['code.py']},
            {'name': 'data', 'root': str(self.source), 'mode': 'hash', 'files': ['data.bin']}]}

    def capture(self):
        with patch('trainctl.evidence.environment_record', return_value={'python': 'fixture'}):
            return self.store.capture(self.plan)

    def verify(self, receipt, **kwargs):
        return self.store.verify(receipt['bundle_id'], receipt['manifest_sha256'], **kwargs)

    def test_copies_code_without_copying_or_changing_dataset(self):
        result = self.capture()
        directory = Path(result['path'])
        self.assertEqual((directory / 'files/code/code.py').read_text(), 'original code')
        self.assertFalse((directory / 'files/data').exists())
        self.assertEqual((self.source / 'data.bin').read_bytes(), b'dataset')
        self.assertTrue(self.verify(result, check_sources=True)['verified'])

    def test_source_drift_and_missing_source_preserve_archive(self):
        result = self.capture()
        (self.source / 'code.py').write_text('changed code')
        (self.source / 'data.bin').unlink()
        self.assertTrue(self.verify(result)['verified'])
        report = self.verify(result, check_sources=True)
        self.assertFalse(report['verified'])
        self.assertEqual(len(report['issues']), 2)
        self.assertTrue(all(row['kind'] == 'source' for row in report['issues']))

    def test_copied_file_corruption_detected(self):
        result = self.capture()
        (Path(result['path']) / 'files/code/code.py').write_text('tampered')
        self.assertFalse(self.verify(result)['verified'])

    def test_external_digest_rejects_manifest_rewrite(self):
        result = self.capture()
        manifest = Path(result['path']) / 'manifest.json'
        manifest.write_text(manifest.read_text() + ' ')
        with self.assertRaisesRegex(ValueError, 'caller digest'):
            self.verify(result)

    def test_new_capture_never_overwrites_existing_bundle(self):
        first = self.capture()
        raw = (Path(first['path']) / 'manifest.json').read_bytes()
        second = self.capture()
        self.assertNotEqual(first['bundle_id'], second['bundle_id'])
        self.assertEqual(raw, (Path(first['path']) / 'manifest.json').read_bytes())

    def test_limits_and_disk_floor_fail_before_commit(self):
        self.plan['max_files'] = 1
        with self.assertRaisesRegex(ValueError, 'budget'):
            self.capture()
        del self.plan['max_files']
        self.plan['max_bytes'] = 1
        with self.assertRaisesRegex(ValueError, 'budget'):
            self.capture()
        del self.plan['max_bytes']
        with patch('trainctl.evidence.shutil.disk_usage') as disk:
            disk.return_value.free = 0
            with self.assertRaisesRegex(ValueError, 'free space'):
                self.capture()
        self.assertFalse(list(self.store.root.glob('bundle_*')))

    def test_traversal_duplicate_and_invalid_plans_rejected(self):
        baseline = copy.deepcopy(self.plan)
        for value in ('../data.bin', '/etc/passwd', 'a/../code.py', 'a\\code.py', './code.py', 'C:/secret'):
            self.plan = copy.deepcopy(baseline)
            self.plan['groups'][0]['files'] = [value]
            with self.assertRaises(ValueError):
                self.capture()
        self.plan = baseline
        self.plan['groups'][0]['files'] *= 2
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            self.capture()

    def test_symlink_and_special_file_rejected(self):
        link = self.source / 'linked.py'
        try:
            link.symlink_to(self.source / 'code.py')
        except OSError:
            self.skipTest('Symlink privilege unavailable')
        self.plan['groups'][0]['files'] = ['linked.py']
        with self.assertRaisesRegex(ValueError, 'Symlinks'):
            self.capture()
        if os.name != 'nt':
            fifo = self.source / 'pipe'
            os.mkfifo(fifo)
            self.plan['groups'][0]['files'] = ['pipe']
            with self.assertRaisesRegex(ValueError, 'regular'):
                self.capture()

    def test_drift_during_capture_never_publishes_bundle(self):
        from trainctl import evidence
        original = evidence._read
        def changing(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            if path.name == 'data.bin':
                (self.source / 'code.py').write_text('changed during capture')
            return result
        with patch('trainctl.evidence._read', side_effect=changing):
            with self.assertRaisesRegex(ValueError, 'drift'):
                self.capture()
        self.assertFalse(list(self.store.root.glob('bundle_*')))
        self.assertTrue(list(self.store.root.glob('.partial-*')))

    def test_environment_inventory_does_not_dump_env_or_package_urls(self):
        from trainctl.evidence import environment_record
        with patch.dict(os.environ, {'SECRET_TOKEN': 'must-not-appear'}):
            result = environment_record()
        raw = json.dumps(result)
        self.assertNotIn('must-not-appear', raw)
        self.assertNotIn('direct_url', raw)
        self.assertTrue(result['executable'])

    def test_cli_returns_nonzero_for_drift(self):
        plan = self.root / 'plan.json'
        plan.write_text(json.dumps(self.plan))
        stream = io.StringIO()
        with redirect_stdout(stream), patch('trainctl.evidence.environment_record', return_value={}):
            code = main(['--root', str(self.root / 'worker'), 'evidence', 'capture', '--plan', str(plan)])
        self.assertEqual(code, 0)
        receipt = json.loads(stream.getvalue())['data']
        (self.source / 'data.bin').write_bytes(b'mutated')
        stream = io.StringIO()
        with redirect_stdout(stream):
            code = main(['--root', str(self.root / 'worker'), 'evidence', 'verify', receipt['bundle_id'],
                         '--expected-sha256', receipt['manifest_sha256'], '--check-sources'])
        self.assertEqual(code, 1)
        self.assertFalse(json.loads(stream.getvalue())['data']['verified'])

    def test_environment_drift_is_explicit_and_optional(self):
        result = self.capture()
        with patch('trainctl.evidence.environment_record', return_value={'python': 'fixture'}):
            self.assertTrue(self.verify(result, check_environment=True)['verified'])
        with patch('trainctl.evidence.environment_record', return_value={'python': 'changed'}):
            self.assertFalse(self.verify(result, check_environment=True)['verified'])
            self.assertTrue(self.verify(result)['verified'])


if __name__ == '__main__':
    unittest.main()
