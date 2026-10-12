import tempfile
import unittest
from pathlib import Path

from trainctl.processes import discover


class WorkloadDiscoveryTests(unittest.TestCase):
    def test_discovery_is_bounded_and_does_not_expose_command_line(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            runs, proc = root / 'runs', root / 'proc'
            runs.mkdir(); process = proc / '123'; process.mkdir(parents=True)
            (process / 'comm').write_text('python3\n', encoding='utf-8')
            (process / 'cmdline').write_bytes(
                b'/opt/conda/bin/python\0/home/user/project/train_secret.py\0--token\0secret-value\0')
            (process / 'status').write_text('State:\tR (running)\nVmRSS:\t2048 kB\n', encoding='utf-8')
            report = discover(runs, proc, supported=True)
            self.assertEqual(report['state'], 'available')
            self.assertEqual(report['items'][0]['script'], 'train_secret.py')
            self.assertEqual(report['items'][0]['memory_bytes'], 2048 * 1024)
            self.assertNotIn('secret-value', str(report))
            self.assertNotIn('argv', report['items'][0])

    def test_managed_descendants_remain_managed_across_process_groups(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); runs, proc = root / 'runs', root / 'proc'; runs.mkdir()
            run = runs / ('run_' + 'a' * 32); run.mkdir()
            (run / 'status.json').write_text('{"status":"RUNNING","pid":100}', encoding='utf-8')
            for pid, parent, command in [(100, 1, b'python\0-m\0trainctl.runner\0'),
                                         (101, 100, b'python\0/project/train.py\0')]:
                process = proc / str(pid); process.mkdir(parents=True)
                (process / 'comm').write_text('python\n', encoding='utf-8')
                (process / 'cmdline').write_bytes(command)
                (process / 'status').write_text(f'State:\tS (sleeping)\nPPid:\t{parent}\nVmRSS:\t1 kB\n', encoding='utf-8')
            report = discover(runs, proc, supported=True)
            self.assertEqual({item['pid'] for item in report['items'] if item['managed']}, {100, 101})

    def test_non_posix_worker_reports_unsupported(self):
        with tempfile.TemporaryDirectory() as folder:
            report = discover(Path(folder), Path(folder), supported=False)
            self.assertEqual(report['state'], 'unsupported')
            self.assertEqual(report['items'], [])
