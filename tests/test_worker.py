import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path
from unittest.mock import patch

from controller.domain import ExperimentSpec
from trainctl.run_store import RunStore, TrainCtlError


def submit_in_process(arguments):
    root, spec = arguments
    with patch.object(RunStore, '_gpu_snapshot', return_value=[]):
        return RunStore(root).submit(ExperimentSpec.from_dict(spec), 'multiprocess')['run_id']


class WorkerFixture:
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = RunStore(self.directory.name)
        self.gpu = patch.object(RunStore, '_gpu_snapshot', return_value=[])
        self.gpu.start()
        self.created = []

    def tearDown(self):
        for run_id in self.created:
            self.store.cancel(run_id, 'test cleanup')
        time.sleep(0.2)
        self.gpu.stop()
        self.directory.cleanup()

    def spec(self, code, limit=10, evaluation=None):
        return ExperimentSpec.from_dict({'schema_version': 1, 'name': 'worker-test',
            'runtime': {'workdir': self.directory.name}, 'command': {'argv': [sys.executable, '-c', code]},
            'resources': {'max_duration_seconds': limit},
            'evaluation': {'argv': [sys.executable, '-c', evaluation]} if evaluation else {}})

    def submit(self, spec, key='key'):
        run = self.store.submit(spec, key)
        self.created.append(run['run_id'])
        return run['run_id']

    def wait(self, run_id):
        for _ in range(150):
            status = self.store.snapshot(run_id)
            if status['status'] in {'FAILED', 'SUCCEEDED', 'CANCELLED'}:
                return status
            time.sleep(0.1)
        self.fail('runner did not finish')

class WorkerTests(WorkerFixture, unittest.TestCase):
    def test_health_exposes_cpu_and_system_memory(self):
        cpu = {'model': 'Test CPU', 'logical_cores': 16, 'utilization_percent': 37.5,
               'load_1m': 2.25, 'load_5m': 1.75, 'load_15m': 1.5}
        memory = {'total_bytes': 32 * 1024**3, 'available_bytes': 20 * 1024**3,
                  'used_bytes': 12 * 1024**3, 'utilization_percent': 37.5}
        with patch.object(RunStore, '_cpu_snapshot', return_value=cpu), patch.object(
                RunStore, '_memory_snapshot', return_value=memory):
            report = self.store.health()

        self.assertEqual(report['cpu'], cpu)
        self.assertEqual(report['memory'], memory)

    def test_linux_cpu_and_memory_parsers_report_real_units(self):
        before = RunStore._parse_cpu_stat(
            'cpu  100 20 30 400 50 10 5 15 1000 1000\n')
        after = RunStore._parse_cpu_stat(
            'cpu  150 20 50 450 60 10 10 20 2000 2000\n')
        memory = RunStore._parse_meminfo(
            'MemTotal:       1000 kB\nMemAvailable:    400 kB\n'
            'SwapTotal:       200 kB\nSwapFree:        150 kB\n')

        self.assertEqual(before, (630, 450))
        self.assertAlmostEqual(RunStore._cpu_utilization(before, after), 57.1, places=1)
        self.assertEqual(memory, {
            'total_bytes': 1000 * 1024,
            'available_bytes': 400 * 1024,
            'used_bytes': 600 * 1024,
            'utilization_percent': 60.0,
            'swap_total_bytes': 200 * 1024,
            'swap_used_bytes': 50 * 1024,
        })

    def test_success_and_evaluation(self):
        run = self.submit(self.spec("print('train')", evaluation="print('evaluation')"))
        self.assertEqual(self.wait(run)['status'], 'SUCCEEDED')
        self.assertIn('evaluation', self.store.logs(run)['text'])

    def test_training_failure_skips_evaluation(self):
        run = self.submit(self.spec('raise SystemExit(7)', evaluation="print('must not run')"))
        self.assertEqual(self.wait(run)['exit_code'], 7)
        self.assertNotIn('must not run', self.store.logs(run)['text'])

    def test_timeout_covers_evaluation(self):
        run = self.submit(self.spec("print('train')", limit=0.7,
                                    evaluation='import time; time.sleep(30)'))
        status = self.wait(run)
        self.assertEqual(status['status'], 'FAILED')
        self.assertEqual(status['error'], 'TIMEOUT')
        self.assertEqual(status['exit_code'], 124)

    @unittest.skipUnless(os.name == 'posix', 'Ubuntu process-group verification')
    def test_cancel_stops_descendant_and_skips_evaluation(self):
        code = "import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); print(p.pid,flush=True); time.sleep(30)"
        run = self.submit(self.spec(code, evaluation="print('must not run')"))
        for _ in range(60):
            output = self.store.logs(run)['text'].strip()
            if output:
                break
            time.sleep(0.1)
        child_pid = int(output)
        self.assertEqual(self.store.cancel(run, 'test')['status'], 'CANCELLED')
        proc = Path(f'/proc/{child_pid}/stat')
        if proc.exists():
            self.assertEqual(proc.read_text().split(') ')[1].split()[0], 'Z')
        self.assertNotIn('must not run', self.store.logs(run)['text'])

    def test_concurrent_submit_and_conflicting_key(self):
        spec = self.spec("print('once')")
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: self.store.submit(spec, 'same'), range(4)))
        ids = {result['run_id'] for result in results}
        self.assertEqual(len(ids), 1)
        self.created.extend(ids)
        run = next(iter(ids))
        self.assertEqual(self.wait(run)['status'], 'SUCCEEDED')
        self.assertEqual(self.store.logs(run)['text'].count('once'), 1)
        with self.assertRaises(TrainCtlError) as error:
            self.store.submit(self.spec("print('other')"), 'same')
        self.assertEqual(error.exception.code, 'IDEMPOTENCY_CONFLICT')

    def test_path_traversal_rejected(self):
        with self.assertRaises(TrainCtlError):
            self.store.snapshot('../runs')

    def test_artifacts_are_scoped_to_current_run(self):
        spec = self.spec("print('ok')")
        spec.outputs['artifact_glob'] = 'outputs/${RUN_ID}/*.json'
        run = self.submit(spec)
        self.wait(run)
        directory = Path(self.directory.name) / 'outputs' / run
        directory.mkdir(parents=True)
        (directory / 'result.json').write_text('{}')
        (directory / 'unselected.json').write_text('{"large": false}')
        other = directory.parent / 'run_other'
        other.mkdir()
        (other / 'result.json').write_text('{}')
        artifacts = self.store.artifacts(run, path=str(directory / 'result.json'))
        self.assertEqual(len(artifacts), 1)
        self.assertEqual(Path(artifacts[0]['path']), directory / 'result.json')
        self.assertEqual(artifacts[0]['size'], 2)
        self.assertEqual(artifacts[0]['sha256'],
                         '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a')

    def test_exact_path_can_verify_the_runs_reported_managed_checkpoint(self):
        spec = self.spec("print('ok')")
        run = self.submit(spec)
        self.wait(run)
        identifier = 'ckpt_' + '1' * 32
        checkpoint = (Path(self.directory.name) / 'orchestrator_runs' / run /
                      'checkpoints' / (identifier + '.ckpt'))
        checkpoint.parent.mkdir(parents=True)
        checkpoint.write_bytes(b'checkpoint')
        self.store._atomic_json(self.store.runs / run / 'checkpoint-status.json', {
            'state': 'committed',
            'latest': {
                'id': identifier,
                'created_at': '2026-09-05T12:00:00Z',
                'size': 10,
                'sha256': '47320987f9a49d5b00119b960f247a956773f57543982b8bfcb6da5bb3afd9ef',
                'state': {'global_step': 5000},
            },
        })

        artifacts = self.store.artifacts(run, path=str(checkpoint))

        self.assertEqual(artifacts, [{
            'path': str(checkpoint),
            'size': 10,
            'sha256': '47320987f9a49d5b00119b960f247a956773f57543982b8bfcb6da5bb3afd9ef',
        }])

    def test_independent_processes_submit_once(self):
        arguments = (self.directory.name, self.spec("print('once')").to_dict())
        with ProcessPoolExecutor(max_workers=4) as pool:
            ids = set(pool.map(submit_in_process, [arguments] * 4))
        self.created.extend(ids)
        self.assertEqual(len(ids), 1)
        run = next(iter(ids))
        self.assertEqual(self.wait(run)['status'], 'SUCCEEDED')
        self.assertEqual(self.store.logs(run)['text'].count('once'), 1)
