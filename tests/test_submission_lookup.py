import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from controller.capabilities import worker_interface
from controller.domain import ExperimentSpec
from trainctl.cli import main
from trainctl.run_store import RunStore, TrainCtlError


class SubmissionLookupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / 'worker'

    @staticmethod
    def spec():
        return ExperimentSpec.from_dict({
            'schema_version': 1, 'name': 'lookup', 'project': {}, 'runtime': {},
            'command': {'argv': ['python', 'train.py']},
        })

    def submit_receipt(self):
        store = RunStore(self.root)
        with patch.object(store, '_start_runner', return_value=0), \
                patch.object(store, '_gpu_snapshot', return_value=[]):
            run_id = store.submit(self.spec(), 'key-one')['run_id']
        return store, run_id

    def test_cli_absence_does_not_initialize_worker(self):
        output = io.StringIO()
        with redirect_stdout(output):
            code = main(['--root', str(self.root), 'lookup-submission',
                         '--idempotency-key', 'not-published', '--json'])
        response = json.loads(output.getvalue())
        self.assertEqual(code, 0)
        self.assertTrue(response['ok'])
        self.assertEqual(response['data']['state'], 'absent')
        self.assertEqual(response['data']['idempotency_key'], 'not-published')
        self.assertIsNone(response['data']['run_id'])
        self.assertTrue(response['data']['observed_at'])
        self.assertFalse(self.root.exists())

    def test_published_run_is_found_without_launch_health_or_file_changes(self):
        store, run_id = self.submit_receipt()
        before = {str(path.relative_to(self.root)): (path.read_bytes(), path.stat().st_mtime_ns)
                  for path in self.root.rglob('*') if path.is_file()}
        with patch.object(store, '_start_runner') as start, \
                patch.object(store, '_gpu_snapshot') as gpu:
            result = store.lookup_submission('key-one')
        after = {str(path.relative_to(self.root)): (path.read_bytes(), path.stat().st_mtime_ns)
                 for path in self.root.rglob('*') if path.is_file()}
        self.assertEqual(result['state'], 'found')
        self.assertEqual(result['run_id'], run_id)
        self.assertEqual(before, after)
        start.assert_not_called()
        gpu.assert_not_called()

    def test_absent_key_is_observation_even_if_other_runs_exist(self):
        store, _ = self.submit_receipt()
        result = store.lookup_submission('not-published')
        self.assertEqual(result['state'], 'absent')
        self.assertIsNone(result['run_id'])
        self.assertNotIn('safe_to_resubmit', result)

    def test_corrupt_map_is_error_and_never_absence(self):
        store = RunStore(self.root)
        for content in ('{bad json', '[]', 'null'):
            with self.subTest(content=content):
                store.idempotency_file.write_text(content, encoding='utf-8')
                with self.assertRaises(TrainCtlError) as raised:
                    store.lookup_submission('key-one')
                self.assertEqual(raised.exception.code, 'SUBMISSION_LOOKUP_INCONSISTENT')

    def test_invalid_or_missing_mapped_run_is_error(self):
        store = RunStore(self.root)
        for run_id in (None, 3, '../other', 'run_' + 'a' * 32):
            with self.subTest(run_id=run_id):
                store._atomic_json(store.idempotency_file, {'key-one': run_id})
                with self.assertRaises(TrainCtlError) as raised:
                    store.lookup_submission('key-one')
                self.assertEqual(raised.exception.code, 'SUBMISSION_LOOKUP_INCONSISTENT')

    def test_receipt_identity_and_missing_files_are_rejected(self):
        store, run_id = self.submit_receipt()
        run_dir = store.runs / run_id
        for filename in ('manifest.json', 'status.json', 'events.jsonl'):
            path = run_dir / filename
            original = path.read_bytes()
            try:
                for content in (None, '{}', '{bad json'):
                    with self.subTest(filename=filename, content=content):
                        if content is None:
                            path.unlink()
                        else:
                            path.write_text(content, encoding='utf-8')
                        with self.assertRaises(TrainCtlError) as raised:
                            store.lookup_submission('key-one')
                        self.assertEqual(raised.exception.code, 'SUBMISSION_LOOKUP_INCONSISTENT')
            finally:
                path.write_bytes(original)
        store._atomic_json(store.idempotency_file, {'other-key': run_id})
        with self.assertRaises(TrainCtlError) as raised:
            store.lookup_submission('other-key')
        self.assertEqual(raised.exception.code, 'SUBMISSION_LOOKUP_INCONSISTENT')

    def test_later_partial_event_does_not_hide_published_submission(self):
        store, run_id = self.submit_receipt()
        with (store.runs / run_id / 'events.jsonl').open('a', encoding='utf-8') as handle:
            handle.write('{unfinished runner event')
        self.assertEqual(store.lookup_submission('key-one')['run_id'], run_id)

    def test_read_failure_is_unavailable_not_absent(self):
        store = RunStore(self.root, initialize=False)
        with patch.object(store, '_read_json', side_effect=PermissionError('denied')):
            with self.assertRaises(TrainCtlError) as raised:
                store.lookup_submission('key-one')
        self.assertEqual(raised.exception.code, 'SUBMISSION_LOOKUP_UNAVAILABLE')
        self.assertTrue(raised.exception.retryable)

    def test_start_uncertain_receipt_can_be_recovered_by_key(self):
        store = RunStore(self.root)
        store._atomic_json(store.root / 'worker.json', {'backend': 'systemd'})
        with patch('trainctl.systemd_backend.boot_id', return_value='test-boot'), \
                patch.object(store, '_start_runner', side_effect=TimeoutError('lost response')):
            with self.assertRaises(TrainCtlError) as raised:
                store.submit(self.spec(), 'key-one')
        self.assertEqual(raised.exception.code, 'START_UNCERTAIN')
        recovered = store.lookup_submission('key-one')
        self.assertEqual(recovered['state'], 'found')
        self.assertEqual(recovered['run_id'], store._read_json(store.idempotency_file, {})['key-one'])

    def test_lookup_rejects_invalid_key_and_declares_capability(self):
        store = RunStore(self.root, initialize=False)
        for key in (None, 1, '', '  '):
            with self.subTest(key=key):
                with self.assertRaises(TrainCtlError) as raised:
                    store.lookup_submission(key)
                self.assertEqual(raised.exception.code, 'INVALID_IDEMPOTENCY_KEY')
        self.assertEqual(worker_interface()['capabilities']['run.lookup'], 1)


if __name__ == '__main__':
    unittest.main()
