import json
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from controller.domain import ExperimentSpec, DomainError
from trainctl.evidence import EvidenceStore
from trainctl.evidence_gate import check
from trainctl.resources import Interrupted
from test_worker import WorkerFixture


class EvidenceGateTests(WorkerFixture, unittest.TestCase):
    def fixture(self):
        root = Path(self.directory.name).resolve()
        script = root / 'train.py'
        script.write_text("import os; from pathlib import Path; Path('started-marker').write_text(os.environ['TRAINCTL_EVIDENCE_ID']); print('REAL_TRAINING_ENTRY')", encoding='utf-8')
        (root / 'data.bin').write_bytes(b'fixture')
        plan = {'schema_version': 1, 'min_free_bytes': 0, 'groups': [
            {'name': 'code', 'root': str(root), 'mode': 'copy', 'files': ['train.py']},
            {'name': 'data', 'root': str(root), 'mode': 'hash', 'files': ['data.bin']}]}
        receipt = EvidenceStore(self.store.root / 'evidence').capture(plan)
        raw = {'schema_version': 1, 'name': 'gate-test', 'runtime': {'workdir': str(root)},
               'command': {'argv': [sys.executable, '-u', str(script)]},
               'resources': {'gpu_index': 0, 'max_duration_seconds': 10, 'max_queue_seconds': 10},
               'evidence': {k: receipt[k] for k in ('bundle_id', 'manifest_sha256')}}
        raw['evidence']['timeout_seconds'] = 10
        return root, raw

    def test_pass_binds_result_and_checks_before_gpu_and_training(self):
        root, raw = self.fixture()
        run = self.submit(ExperimentSpec.from_dict(raw))
        result = self.wait(run)
        self.assertEqual(result['status'], 'SUCCEEDED', result)
        self.assertEqual((root / 'started-marker').read_text(), raw['evidence']['bundle_id'])
        events = self.store.events(run)
        passes = [i for i, e in enumerate(events) if e['kind'] == 'evidence_check_passed']
        acquire = next(i for i, e in enumerate(events) if e['kind'] == 'resource_acquired')
        child = next(i for i, e in enumerate(events) if e['kind'] == 'child_started')
        self.assertEqual(len(passes), 2)
        self.assertLess(passes[-1], acquire)
        self.assertLess(acquire, child)
        final = json.loads((self.store.runs / run / 'execution-result.json').read_text())
        self.assertEqual(final['evidence'], raw['evidence'])
        self.assertTrue(final['evidence_gate']['verified'])
        self.assertIn('spec_sha256', final['evidence_gate'])

    def test_source_drift_rejects_without_gpu_lease_or_training(self):
        root, raw = self.fixture()
        (root / 'data.bin').write_bytes(b'changed')
        run = self.submit(ExperimentSpec.from_dict(raw))
        result = self.wait(run)
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('EVIDENCE_REJECTED', result['error'])
        self.assertFalse((root / 'started-marker').exists())
        self.assertFalse(any(e['kind'] in ('resource_acquired', 'child_started') for e in self.store.events(run)))

    def test_changed_while_queued_rejected_at_launch(self):
        root, raw = self.fixture()
        busy = self.spec("import time; print('BUSY',flush=True); time.sleep(4)")
        busy.resources['gpu_index'] = 0
        first = self.submit(busy, 'busy')
        for _ in range(50):
            if 'BUSY' in self.store.logs(first)['text']:
                break
            time.sleep(.1)
        run = self.submit(ExperimentSpec.from_dict(raw), 'queued')
        for _ in range(50):
            if any(e['kind'] == 'resource_wait' for e in self.store.events(run)):
                break
            time.sleep(.1)
        self.assertTrue(any(e['kind'] == 'evidence_check_passed' for e in self.store.events(run)))
        (root / 'data.bin').write_bytes(b'changed while queued')
        self.assertEqual(self.wait(first)['status'], 'SUCCEEDED')
        result = self.wait(run)
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('launch', result['error'])
        self.assertFalse((root / 'started-marker').exists())
        self.assertFalse(any(e['kind'] == 'resource_acquired' for e in self.store.events(run)))

    def test_manifest_digest_or_environment_mismatch_blocks(self):
        root, raw = self.fixture()
        raw['evidence']['manifest_sha256'] = '0' * 64
        run = self.submit(ExperimentSpec.from_dict(raw), 'digest')
        self.assertEqual(self.wait(run)['status'], 'FAILED')
        with patch('trainctl.evidence.environment_record', return_value={'python': 'different environment'}):
            _, raw = self.fixture()
        run = self.submit(ExperimentSpec.from_dict(raw), 'environment')
        self.assertEqual(self.wait(run)['status'], 'FAILED')
        report = json.loads((self.store.runs / run / 'evidence-preflight.json').read_text())
        self.assertEqual(report['issues'][0]['kind'], 'environment')
        self.assertFalse((root / 'started-marker').exists())

    def test_uncovered_entry_rejected(self):
        root, raw = self.fixture()
        other = root / 'other.py'
        other.write_text("raise RuntimeError('must not execute')")
        raw['command']['argv'][-1] = str(other)
        run = self.submit(ExperimentSpec.from_dict(raw))
        self.assertEqual(self.wait(run)['status'], 'FAILED')
        self.assertFalse(any(e['kind'] == 'child_started' for e in self.store.events(run)))

    def test_spec_binding_is_strict_and_legacy_serialization_unchanged(self):
        root, raw = self.fixture()
        self.assertEqual(ExperimentSpec.from_dict(raw).to_dict()['evidence'], raw['evidence'])
        self.assertNotIn('evidence', self.spec('pass').to_dict())
        for binding in ({}, [], {'bundle_id': '../escape'}, {**raw['evidence'], 'timeout_seconds': True}):
            with self.assertRaises(DomainError):
                ExperimentSpec.from_dict({**raw, 'evidence': binding})
        with self.assertRaises(DomainError):
            ExperimentSpec.from_dict({**raw, 'command': {'argv': [sys.executable, '-c', 'pass']}})

    def test_check_timeout_and_cancellation_are_distinct(self):
        root, raw = self.fixture()
        spec = ExperimentSpec.from_dict(raw)
        for failure, expected in [('TIMEOUT', 'EVIDENCE_TIMEOUT'), ('CANCELLED', 'CANCELLED')]:
            directory = root / failure
            directory.mkdir()
            def interrupted(*args, **kwargs):
                raise Interrupted(failure)
            with self.assertRaisesRegex(Interrupted, expected):
                check(self.store, directory, spec, root, {}, interrupted, 'preflight')
            self.assertEqual(json.loads((directory / 'evidence-gate.json').read_text())['state'], 'failed')
