import json
import os
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from controller.domain import ExperimentSpec
from trainctl.evidence import EvidenceStore
from trainctl import evidence_watch
from trainctl.evidence_watch import Auditor
from trainctl.source_snapshot import launch, prepare
from trainctl.telemetry import Telemetry
from trainctl.worker import observe
from test_worker import WorkerFixture


class SnapshotTests(WorkerFixture, unittest.TestCase):
    def tearDown(self):
        # Only our validated TemporaryDirectory; allow cleanup of protected copies.
        root = Path(self.directory.name).resolve()
        for path in root.rglob('*'):
            if path.is_relative_to(root) and not path.is_symlink():
                path.chmod(0o755 if path.is_dir() else 0o644)
        super().tearDown()

    def fixture(self, script="print('snapshot')"):
        root = Path(self.directory.name).resolve()
        source = root / 'project'
        source.mkdir()
        (source / 'train.py').write_text(script)
        (source / 'lazy.py').write_text("VALUE = 'frozen'")
        (source / 'large.bin').write_bytes(b'a' * (1024**2 + 100))
        receipt = EvidenceStore(self.store.root / 'evidence').capture({
            'schema_version': 1, 'min_free_bytes': 0, 'groups': [
                {'name': 'code', 'root': str(source), 'mode': 'copy', 'files': ['train.py', 'lazy.py']},
                {'name': 'data', 'root': str(source), 'mode': 'hash', 'files': ['large.bin']}]})
        binding = {k: receipt[k] for k in ('bundle_id', 'manifest_sha256')}
        spec = ExperimentSpec.from_dict({'schema_version': 1, 'name': 'snapshot-test',
            'runtime': {'workdir': str(source)},
            'command': {'argv': [sys.executable, '-u', str(source / 'train.py')]},
            'resources': {'max_duration_seconds': 10}, 'evidence': binding})
        return source, spec

    def test_incremental_hash_detects_same_size_edit_and_remembers_restoration(self):
        source, spec = self.fixture()
        run_dir = self.store.runs / ('run_' + 'a' * 32)
        run_dir.mkdir()
        audit = Auditor(run_dir, spec.evidence)
        audit.fast_pass()
        self.assertFalse(audit.report()['drift_detected'])
        original = (source / 'large.bin').read_bytes()
        (source / 'large.bin').write_bytes(b'b' * len(original))
        audit.step(17)
        self.assertEqual(audit.bytes_read, 17)
        self.assertEqual(audit.checked, 0)
        while not audit.checked:
            audit.step(64000)
        self.assertTrue(audit.report()['drift_detected'])
        (source / 'large.bin').write_bytes(original)
        while audit.rounds < 2:
            audit.step()
        self.assertTrue(audit.report()['drift_detected'])
        self.assertEqual(audit.report()['issue_count'], 1)

    def test_environment_and_small_code_drift(self):
        source, spec = self.fixture()
        audit = Auditor(self.store.runs / 'unused', spec.evidence)
        (source / 'lazy.py').write_text("VALUE = 'edited'")
        with patch('trainctl.evidence_watch.environment_record', return_value={}):
            audit.fast_pass()
        self.assertEqual({i['kind'] for i in audit.issues}, {'source', 'environment'})

    def test_fast_size_change_is_reported_as_size_drift(self):
        source, spec = self.fixture()
        audit = Auditor(self.store.runs / 'unused', spec.evidence)
        (source / 'lazy.py').write_text("VALUE = 'edited and longer'")
        audit.fast_pass()
        issue = next(item for item in audit.issues if item['kind'] == 'source')
        self.assertEqual(issue['error'], 'Size differs')

    def test_snapshot_audit_ignores_mutable_original_copies(self):
        source, spec = self.fixture()
        spec.evidence['execution_mode'] = 'snapshot'
        run_dir = self.store.runs / ('run_' + 'c' * 32)
        run_dir.mkdir()
        evidence = EvidenceStore(self.store.root / 'evidence')
        manifest = evidence.manifest(
            spec.evidence['bundle_id'], spec.evidence['manifest_sha256'])
        for row in manifest['files']:
            if row['mode'] != 'copy':
                continue
            copied = evidence.root / spec.evidence['bundle_id'] / 'files' / row['group'] / row['path']
            snapshot = run_dir / 'source-snapshot' / row['group'] / row['path']
            snapshot.parent.mkdir(parents=True, exist_ok=True)
            snapshot.write_bytes(copied.read_bytes())
        audit = Auditor(run_dir, spec.evidence)
        (source / 'lazy.py').write_text("VALUE = 'new development'")
        audit.fast_pass()
        report = audit.report()
        self.assertEqual(report['schema_version'], 2)
        self.assertEqual(report['execution_integrity']['state'], 'no_violation_observed')
        self.assertEqual(report['execution_integrity']['issue_count'], 0)
        self.assertEqual(report['deployment']['state'], 'changed')
        self.assertEqual(report['deployment']['issue_count'], 1)
        self.assertFalse(report['drift_detected'])

    def test_snapshot_verification_race_degrades_observer_without_claiming_drift(self):
        source, spec = self.fixture()
        spec.evidence['execution_mode'] = 'snapshot'
        run_dir = self.store.runs / ('run_' + 'd' * 32)
        run_dir.mkdir()
        evidence = EvidenceStore(self.store.root / 'evidence')
        manifest = evidence.manifest(spec.evidence['bundle_id'], spec.evidence['manifest_sha256'])
        for row in manifest['files']:
            if row['mode'] == 'copy':
                copied = evidence.root / spec.evidence['bundle_id'] / 'files' / row['group'] / row['path']
                snapshot = run_dir / 'source-snapshot' / row['group'] / row['path']
                snapshot.parent.mkdir(parents=True, exist_ok=True)
                snapshot.write_bytes(copied.read_bytes())
        original_read = evidence_watch._read

        def read_with_deployment_race(path, *args, **kwargs):
            if Path(path) == source / 'lazy.py':
                raise ValueError('Evidence byte budget exceeded')
            return original_read(path, *args, **kwargs)

        with patch('trainctl.evidence_watch._read', side_effect=read_with_deployment_race):
            audit = Auditor(run_dir, spec.evidence)
            audit.fast_pass()
        report = audit.report()
        self.assertEqual(report['execution_integrity']['state'], 'no_violation_observed')
        self.assertEqual(report['deployment']['state'], 'unknown')
        self.assertEqual(report['observer']['health'], 'degraded')
        self.assertEqual(report['observer']['warning_count'], 1)
        self.assertEqual(report['observer']['warnings'][0]['code'], 'verification_race')
        self.assertFalse(report['drift_detected'])

    def test_large_file_rotation_pauses_after_a_complete_pass(self):
        _, spec = self.fixture()
        audit = Auditor(self.store.runs / 'unused', spec.evidence, rotation_interval_seconds=3600)
        while audit.rounds == 0:
            audit.step()
        self.assertFalse(audit.rotation_due(audit.next_rotation_monotonic - 1))
        self.assertTrue(audit.rotation_due(audit.next_rotation_monotonic))
        report = audit.report()
        self.assertEqual(report['rotation_interval_seconds'], 3600)
        self.assertGreater(report['seconds_until_next_rotation'], 0)

    def test_large_file_change_during_scan_is_unknown_until_stable_rescan(self):
        source, spec = self.fixture()
        spec.evidence['execution_mode'] = 'snapshot'
        audit = Auditor(self.store.runs / 'unused', spec.evidence)
        audit.step(17)
        (source / 'large.bin').write_bytes(b'b' * (1024**2 + 100))
        while audit.rounds < 1:
            audit.step(64000)
        report = audit.report()
        self.assertEqual(report['execution_integrity']['state'], 'unknown')
        self.assertEqual(report['observer']['warnings'][0]['code'], 'verification_race')
        self.assertFalse(report['drift_detected'])

        while audit.rounds < 2:
            audit.step(64000)
        report = audit.report()
        self.assertEqual(report['execution_integrity']['state'], 'violation_observed')
        self.assertEqual(report['observer']['health'], 'healthy')
        self.assertEqual(report['observer']['warning_count'], 0)
        self.assertTrue(report['drift_detected'])

    @unittest.skipUnless(os.name == 'posix', 'POSIX read-only permissions')
    def test_snapshot_copies_seal_and_rewrite_only_covered_paths(self):
        source, spec = self.fixture()
        spec.evidence['execution_mode'] = 'snapshot'
        run_dir = self.store.runs / ('run_' + 'b' * 32)
        run_dir.mkdir()
        evidence = EvidenceStore(self.store.root / 'evidence')
        manifest = evidence.manifest(spec.evidence['bundle_id'], spec.evidence['manifest_sha256'])
        prepare(run_dir, spec.evidence, manifest)
        argv, env = launch(run_dir, spec, {'PYTHONPATH': str(source)})
        self.assertEqual(Path(argv[-1]), run_dir / 'source-snapshot/code/train.py')
        self.assertFalse(Path(argv[-1]).stat().st_mode & 0o222)
        self.assertEqual(env['PYTHONPATH'], str(run_dir / 'source-snapshot/code'))
        with self.assertRaises(ValueError):
            launch(run_dir, spec, {'PYTHONPATH': '/not-captured'})
        (source / 'lazy.py').write_text("VALUE = 'edited'")
        self.assertIn('frozen', (run_dir / 'source-snapshot/code/lazy.py').read_text())

    @unittest.skipUnless(os.name == 'posix', 'Linux snapshot subprocess acceptance')
    def test_real_run_imports_frozen_lazy_code_and_refuses_writes(self):
        source, spec = self.fixture("""import os, time
from pathlib import Path
Path('ready').touch()
while not Path('continue').exists(): time.sleep(.05)
import lazy
assert lazy.VALUE == 'frozen'
try:
    Path(__file__).write_text('bad')
except PermissionError:
    print('READ_ONLY_AND_FROZEN', flush=True)
else:
    raise AssertionError('snapshot was writable')
""")
        spec.evidence['execution_mode'] = 'snapshot'
        spec.runtime['environment_variables'] = {'PYTHONPATH': str(source)}
        run = self.submit(spec)
        for _ in range(100):
            if (source / 'ready').exists():
                break
            time.sleep(.1)
        self.assertTrue((source / 'ready').exists(), self.store.snapshot(run))
        (source / 'lazy.py').write_text("VALUE = 'changed'")
        (source / 'continue').touch()
        self.assertEqual(self.wait(run)['status'], 'SUCCEEDED', self.store.logs(run))
        self.assertIn('READ_ONLY_AND_FROZEN', self.store.logs(run)['text'])
        result = json.loads((self.store.runs / run / 'execution-result.json').read_text())
        self.assertEqual(result['evidence_runtime']['state'], 'closed')
        self.assertIn('source_snapshot', result)

    def test_execution_integrity_alert_does_not_change_training_result(self):
        source, spec = self.fixture()
        run = self.submit(spec)
        self.assertEqual(self.wait(run)['status'], 'SUCCEEDED')
        directory = self.store.runs / run
        audit = Auditor(directory, spec.evidence)
        (source / 'lazy.py').write_text('edited')
        audit.fast_pass()
        self.store._atomic_json(directory / 'evidence-runtime.json', audit.report('closed'))
        telemetry = Telemetry(self.store.root)
        observe(self.store, telemetry)
        alerts = telemetry.alerts()
        self.assertTrue(any(a['kind'] == 'evidence_execution_integrity' for a in alerts))
        self.assertEqual(self.store.snapshot(run)['status'], 'SUCCEEDED')
