import time
import unittest
from pathlib import Path

from controller.domain import DomainError, ExperimentSpec

from test_worker import WorkerFixture


class GPUQueueTests(WorkerFixture, unittest.TestCase):
    def test_invalid_resource_limits_are_rejected(self):
        for value in (-1, True, '0', 0.5):
            spec = self.spec("print('unused')")
            spec.resources['gpu_index'] = value
            with self.subTest(gpu=value), self.assertRaises(DomainError):
                spec.validate()
        for value in (0, -1, True, float('nan'), float('inf'), '1'):
            spec = self.spec("print('unused')")
            spec.resources['max_queue_seconds'] = value
            with self.subTest(queue=value), self.assertRaises(DomainError):
                spec.validate()

    def gpu_spec(self, code, limit=10):
        spec = self.spec(code, limit=limit)
        spec.resources.update(gpu_index=0, max_queue_seconds=5)
        return spec

    def wait_started(self, run):
        for _ in range(60):
            if self.store.logs(run)['text']:
                return
            time.sleep(0.1)
        self.fail('first job did not start')

    def test_same_gpu_is_serialized_and_budget_starts_after_wait(self):
        first = self.submit(self.gpu_spec("import time; print('started',flush=True); time.sleep(2)"), 'first')
        self.wait_started(first)
        second = self.submit(self.gpu_spec("import os; print(os.environ['CUDA_VISIBLE_DEVICES'])", limit=0.8), 'second')
        time.sleep(0.4)
        self.assertEqual(self.store.snapshot(second)['status'], 'PREPARING')
        self.assertEqual(self.store.logs(second)['text'], '')
        self.assertEqual(self.wait(first)['status'], 'SUCCEEDED')
        self.assertEqual(self.wait(second)['status'], 'SUCCEEDED')
        self.assertEqual(self.store.logs(second)['text'].strip(), '0')

    def test_cancel_waiting_job_does_not_start_child(self):
        first = self.submit(self.gpu_spec("import time; print('started',flush=True); time.sleep(4)"), 'first')
        self.wait_started(first)
        second = self.submit(self.gpu_spec("print('must not run')"), 'second')
        self.assertEqual(self.store.cancel(second, 'cancel queued')['status'], 'CANCELLED')
        self.assertFalse(any(event['kind'] == 'child_started' for event in self.store.events(second)))
        self.assertEqual(self.store.snapshot(first)['status'], 'RUNNING')

    def test_queue_timeout(self):
        first = self.submit(self.gpu_spec("import time; print('started',flush=True); time.sleep(3)"), 'first')
        self.wait_started(first)
        spec = self.gpu_spec("print('must not run')")
        spec.resources['max_queue_seconds'] = 0.2
        second = self.submit(spec, 'second')
        self.assertEqual(self.wait(second)['error'], 'QUEUE_TIMEOUT')
        self.assertEqual(self.store.logs(second)['text'], '')

    def test_checkpoint_input_gate_fails_before_gpu_lease_or_child_start(self):
        raw = self.spec("print('must not run')").to_dict()
        raw['resources']['gpu_index'] = 0
        raw['task'] = {
            'kind': 'train',
            'inputs': [],
            'checkpoint': {
                'path': str(Path(self.directory.name).resolve() / 'missing.ckpt'),
                'sha256': 'a' * 64,
            },
        }
        run = self.submit(ExperimentSpec.from_dict(raw), 'missing-checkpoint')

        result = self.wait(run)

        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('TASK_CHECKPOINT_MISSING', result['error'])
        self.assertFalse(any(
            event['kind'] in {'resource_acquired', 'child_started'}
            for event in self.store.events(run)
        ))
