import base64
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from controller.log_sync import sync_logs


class LogSyncTests(unittest.TestCase):
    def test_disconnect_then_restart_resumes_from_disk_without_duplicate(self):
        run = 'run_' + 'c'*32
        raw = b'first\n\xff\x00second\n'
        calls = []

        def fetch(run_id, stream, offset):
            calls.append(offset)
            if len(calls) == 2:
                raise OSError('SSH disconnected')
            data = raw[offset:offset+6]
            return {'run_id': run_id, 'stream': stream, 'offset': offset,
                    'next_offset': offset+len(data), 'data_base64': base64.b64encode(data).decode(),
                    'eof': offset+len(data) == len(raw)}

        remote = SimpleNamespace(host='worker', user='mlrunner', port=None, remote_root='/srv/test', logs=fetch)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(OSError):
                sync_logs(remote, run, 'stdout', directory)
            result = sync_logs(remote, run, 'stdout', directory)
            self.assertEqual(calls[:3], [0, 6, 6])
            self.assertEqual(Path(result['path']).read_bytes(), raw)
            self.assertTrue(result['eof'])
            self.assertEqual(sync_logs(remote, run, 'stdout', directory)['downloaded_bytes'], 0)

    def test_page_budget_is_bounded_and_workers_have_separate_caches(self):
        run = 'run_' + 'd'*32
        def fetch(run_id, stream, offset):
            return {'run_id': run_id, 'stream': stream, 'offset': offset,
                    'next_offset': offset+1, 'data_base64': 'eA==', 'eof': False}
        remote = SimpleNamespace(host='one', user='u', port=None, remote_root='/srv', logs=fetch)
        with tempfile.TemporaryDirectory() as directory:
            first = sync_logs(remote, run, 'stdout', directory, max_pages=2)
            self.assertEqual(first['downloaded_bytes'], 2)
            remote.host = 'two'
            second = sync_logs(remote, run, 'stdout', directory, max_pages=1)
            self.assertNotEqual(first['path'], second['path'])
            self.assertEqual(second['next_offset'], 1)
