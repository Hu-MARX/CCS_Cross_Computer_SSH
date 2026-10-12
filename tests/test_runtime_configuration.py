import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from controller.domain import RunStatus
from trainctl.run_store import RunStore
from trainctl.runtime_configuration import learning_rate_metrics, project, write
from trainctl.telemetry import Telemetry
from trainctl.worker import observe


class DemoOptimizer:
    def __init__(self, groups):
        self.param_groups = groups


class DemoScheduler:
    pass


def trainer(groups=None):
    optimizer = DemoOptimizer(groups or [
        {'params': [object()], 'lr': 1e-4, 'initial_lr': 1e-4, 'weight_decay': 1e-4,
         'betas': (.9, .999), 'name': 'head'},
        {'params': [object()], 'lr': 7e-5, 'weight_decay': 0.0}])
    return SimpleNamespace(optimizers=[optimizer],
        lr_scheduler_configs=[SimpleNamespace(scheduler=DemoScheduler(), interval='step', frequency=1)],
        precision='16-mixed', world_size=1, accumulate_grad_batches=2, max_epochs=50,
        lightning_module=SimpleNamespace(), datamodule=SimpleNamespace(train_config=SimpleNamespace(
            batch_size=1, grad_accum_steps=2, num_workers=1, resolution=560)))


class RuntimeConfigurationTests(unittest.TestCase):
    def test_projects_effective_groups_without_parameters(self):
        value = project(trainer(), '2026-09-05T00:00:00Z')
        self.assertEqual(value['schema_version'], 1)
        self.assertEqual(value['source'], 'constructed_trainer_state')
        self.assertEqual(value['trainer']['precision'], '16-mixed')
        self.assertEqual(value['datamodule']['batch_size'], 1)
        self.assertEqual(value['optimizers'][0]['groups'][0]['lr'], 1e-4)
        self.assertEqual(value['optimizers'][0]['groups'][1]['weight_decay'], 0.0)
        self.assertNotIn('params', json.dumps(value))
        self.assertIn('DemoOptimizer', value['optimizers'][0]['type'])

    def test_nonfinite_unsupported_values_dropped_and_groups_bounded(self):
        groups = [{'lr': float('nan'), 'weight_decay': object()}] + [{'lr': index + 1} for index in range(100)]
        value = project(trainer(groups), 'now')
        self.assertNotIn('lr', value['optimizers'][0]['groups'][0])
        self.assertEqual(value['limits']['parameter_group_count'], 64)
        self.assertTrue(value['limits']['truncated'])
        metrics = learning_rate_metrics(SimpleNamespace(optimizers=[DemoOptimizer(groups)]))
        self.assertEqual(len(metrics), 63)
        self.assertNotIn('optimizer/0/group/0/lr', metrics)

    def test_atomic_write_roundtrip_and_size_bound(self):
        with tempfile.TemporaryDirectory() as folder:
            value = project(trainer(), 'now')
            write(folder, value)
            self.assertEqual(json.loads((Path(folder)/'runtime-configuration.json').read_text()), value)
            self.assertFalse(list(Path(folder).glob('*.tmp')))

    def test_worker_carries_valid_runtime_record_into_sample(self):
        with tempfile.TemporaryDirectory() as folder:
            store, telemetry = RunStore(folder), Telemetry(folder)
            run_id = 'run_' + 'e'*32
            run_dir = store.runs/run_id
            run_dir.mkdir()
            store._atomic_json(run_dir/'status.json', {'status': RunStatus.RUNNING.value})
            store._atomic_json(run_dir/'manifest.json', {'execution': {'backend': 'process'}})
            value = project(trainer(), 'now')
            write(run_dir, value)
            from unittest.mock import patch
            with patch.object(store, '_gpu_snapshot', return_value=[]), patch.object(
                    store, 'reconcile_execution', return_value={'backend': 'process', 'quiescent': False}):
                observe(store, telemetry)
            self.assertEqual(telemetry.samples(run_id)[0]['payload']['runtime_configuration'], value)
            self.assertEqual(store.snapshot(run_id)['runtime_configuration'], value)

    def test_bad_optional_record_does_not_fail_lifecycle_or_observation(self):
        with tempfile.TemporaryDirectory() as folder:
            store, telemetry = RunStore(folder), Telemetry(folder)
            run_id = 'run_' + 'f'*32
            run_dir = store.runs/run_id
            run_dir.mkdir()
            store._atomic_json(run_dir/'status.json', {'status': RunStatus.RUNNING.value})
            store._atomic_json(run_dir/'manifest.json', {'execution': {'backend': 'process'}})
            (run_dir/'runtime-configuration.json').write_text('{broken')
            from unittest.mock import patch
            with patch.object(store, '_gpu_snapshot', return_value=[]), patch.object(
                    store, 'reconcile_execution', return_value={'backend': 'process', 'quiescent': False}):
                observe(store, telemetry)
            sample = telemetry.samples(run_id)[0]['payload']
            self.assertEqual(sample['runtime_configuration']['state'], 'invalid')
            self.assertEqual(sample['observation_state'], 'fresh')
            self.assertEqual(store.snapshot(run_id)['status'], 'RUNNING')
