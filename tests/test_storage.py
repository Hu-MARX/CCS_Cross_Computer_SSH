import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from trainctl.logs import SegmentedWriter, read_log, log_limits
from trainctl.run_store import RunStore
from trainctl.storage import maintain
from trainctl.telemetry import Telemetry
from test_worker import WorkerFixture


class LogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def writer(self, segment=7, maximum=10000):
        return SegmentedWriter(self.root, 'stdout', segment_bytes=segment, max_stream_bytes=maximum)

    def test_cross_segments_utf8_resume_and_reopen(self):
        expected = '训练🚀第1步\n' * 100
        raw = expected.encode()
        writer = self.writer()
        writer.write(raw[:13])
        first = read_log(self.root, 'stdout', limit=8)
        writer.close()
        writer = self.writer()
        writer.write(raw[13:])
        writer.close()
        offset, text = first['next_offset'], first['text']
        while offset < len(raw):
            response = read_log(self.root, 'stdout', offset, terminal=True, limit=8)
            self.assertGreater(response['next_offset'], offset)
            offset = response['next_offset']
            text += response['text']
        self.assertEqual(text, expected)
        self.assertTrue(response['eof'])
        self.assertTrue(all(p.stat().st_size <= 7 for p in (self.root / 'logs/stdout').glob('*.log')))

    def test_pending_character_not_acknowledged_until_complete(self):
        writer = self.writer()
        writer.write('中'.encode()[:2])
        self.assertEqual(read_log(self.root, 'stdout')['next_offset'], 0)
        writer.write('中'.encode()[2:])
        writer.close()
        self.assertEqual(read_log(self.root, 'stdout', terminal=True)['text'], '中')

    def test_invalid_final_bytes_and_legacy_compatibility(self):
        (self.root / 'stdout.log').write_bytes(b'ok\xe4\xb8')
        self.assertEqual(read_log(self.root, 'stdout')['next_offset'], 2)
        self.assertEqual(read_log(self.root, 'stdout', 2, terminal=True)['text'], '\ufffd')

    def test_quota_retains_prefix_and_raises(self):
        writer = self.writer(segment=4, maximum=8)
        try:
            with self.assertRaisesRegex(RuntimeError, 'LOG_LIMIT_EXCEEDED'):
                writer.write(b'0123456789')
        finally:
            writer.close()
        self.assertEqual(read_log(self.root, 'stdout', terminal=True)['text'], '01234567')

    def test_invalid_cursor_and_missing_segment_are_explicit(self):
        with self.assertRaises(ValueError):
            read_log(self.root, 'stdout', -1)
        with self.assertRaisesRegex(ValueError, 'CURSOR_AHEAD'):
            read_log(self.root, 'stdout', 1)
        directory = self.root / 'logs/stdout'
        directory.mkdir(parents=True)
        (directory / '00000000000000000007.log').write_bytes(b'gap')
        with self.assertRaisesRegex(ValueError, 'LOG_GAP'):
            read_log(self.root, 'stdout')

    def test_invalid_limits(self):
        for values in ({'segment_bytes': 0}, {'max_stream_bytes': True}, {'max_stream_bytes': 1}):
            with self.assertRaises(ValueError):
                log_limits({'logs': values})

    def test_live_read_during_rotation_has_no_gaps(self):
        writer = self.writer(segment=17, maximum=100000)
        expected = b'live-log\n' * 500
        def produce():
            try:
                for start in range(0, len(expected), 41):
                    writer.write(expected[start:start+41])
            finally:
                writer.close()
        text, offset = '', 0
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(produce)
            while not future.done():
                result = read_log(self.root, 'stdout', offset, limit=127)
                text += result['text']
                offset = result['next_offset']
            future.result()
        result = read_log(self.root, 'stdout', offset, terminal=True)
        self.assertEqual((text + result['text']).encode(), expected)


class LogIntegrationTests(WorkerFixture, unittest.TestCase):
    def test_both_streams_over_pipe_capacity_and_rotation(self):
        self.store._atomic_json(self.store.root / 'worker.json', {'logs': {'segment_bytes': 4096, 'max_stream_bytes': 2000000}})
        run = self.submit(self.spec("import os; [(os.write(1,b'x'*4096),os.write(2,b'y'*4096)) for _ in range(100)]"))
        self.assertEqual(self.wait(run)['status'], 'SUCCEEDED')
        for stream, letter in (('stdout', 'x'), ('stderr', 'y')):
            offset, text = 0, ''
            while True:
                response = self.store.logs(run, stream, offset)
                text += response['text']
                offset = response['next_offset']
                if response['eof']:
                    break
            self.assertEqual(text, letter * 409600)

    def test_log_failure_marks_run_failed_and_skips_evaluation(self):
        self.store._atomic_json(self.store.root / 'worker.json', {'logs': {'segment_bytes': 1024, 'max_stream_bytes': 2048}})
        run = self.submit(self.spec("import os,time; os.write(1,b'x'*10000); time.sleep(30)", evaluation="print('not allowed')"))
        result = self.wait(run)
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('LOG_LIMIT_EXCEEDED', result['error'])
        self.assertEqual(len(self.store.logs(run)['text']), 2048)
        self.assertNotIn('evaluation_started', [e['kind'] for e in self.store.events(run)])


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = RunStore(self.temp.name)
        self.store._atomic_json(self.store.root / 'worker.json', {'min_free_bytes': 0, 'storage': {'backup_keep': 2}})
        self.telemetry = Telemetry(self.store.root)
        self.run = 'run_' + 'a'*32
        directory = self.store.runs / self.run
        directory.mkdir()
        (directory / 'monitor-finalized.json').write_text('{}')

    def tearDown(self):
        self.temp.cleanup()

    def test_backup_before_retention_preserves_active_latest_and_outbox(self):
        for run in (self.run, 'active', '_worker'):
            for i in range(3):
                self.telemetry.record(run, '2000-01-01T00:00:00Z', {'i': i})
        self.telemetry.alert(self.run, 'failed', '2000-01-01T00:00:00Z', {})
        result = maintain(self.store)
        self.assertEqual(result['deleted_samples'], 4)
        self.assertEqual(len(self.telemetry.samples(self.run)), 1)
        self.assertEqual(len(self.telemetry.samples('active')), 3)
        connection = sqlite3.connect(result['backup_path'])
        try:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM samples').fetchone()[0], 9)
            self.assertEqual(connection.execute('PRAGMA quick_check').fetchone()[0], 'ok')
        finally:
            connection.close()
        with self.telemetry.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM notification_outbox').fetchone()[0], 1)
        self.assertEqual(maintain(self.store), result)

    def test_backup_failure_prevents_deletion(self):
        for _ in range(3):
            self.telemetry.record(self.run, '2000-01-01T00:00:00Z', {})
        with patch('trainctl.storage._backup', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                maintain(self.store)
        self.assertEqual(len(self.telemetry.samples(self.run)), 3)
        self.assertEqual(self.store.storage()['maintenance']['state'], 'failed')
        self.assertEqual(self.telemetry.alerts()[0]['kind'], 'storage_failed')

    def test_only_managed_backups_pruned(self):
        maintain(self.store)
        other = self.store.root / 'telemetry-backups' / 'my-research.sqlite3'
        other.write_bytes(b'preserve')
        maintain(self.store, force=True)
        result = maintain(self.store, force=True)
        self.assertEqual(len(result['removed_backups']), 1)
        self.assertEqual(other.read_bytes(), b'preserve')
        self.assertEqual(len(list(other.parent.glob('telemetry-*.sqlite3'))), 2)

    def test_sample_ids_never_reused_after_delete_and_restart(self):
        self.telemetry.record('run', 'now', {})
        before = self.telemetry.samples('run')[-1]['id']
        with self.telemetry.db() as db:
            db.execute('DELETE FROM samples')
        self.telemetry = Telemetry(self.store.root)
        self.telemetry.record('run', 'later', {})
        self.assertGreater(self.telemetry.samples('run')[0]['id'], before)

    def test_legacy_sequence_migrates(self):
        with self.telemetry.db() as db:
            db.execute('INSERT INTO samples VALUES(100,?,?,?)', ('legacy', 'now', '{}'))
            db.execute('DROP TABLE sample_sequence')
        self.telemetry = Telemetry(self.store.root)
        self.telemetry.record('legacy', 'later', {})
        self.assertEqual(self.telemetry.samples('legacy')[-1]['id'], 101)

    def test_samples_added_after_backup_are_not_deleted(self):
        from trainctl.storage import _backup
        self.telemetry.record(self.run, '2000-01-01T00:00:00Z', {'old': True})
        def backup_then_append(*args):
            result = _backup(*args)
            self.telemetry.record(self.run, '2000-01-01T00:00:00Z', {'new': 1})
            self.telemetry.record(self.run, '2000-01-01T00:00:00Z', {'new': 2})
            return result
        with patch('trainctl.storage._backup', side_effect=backup_then_append):
            maintain(self.store)
        self.assertEqual([s['payload'] for s in self.telemetry.samples(self.run)], [{'new': 1}, {'new': 2}])


if __name__ == '__main__':
    unittest.main()
