import subprocess
import sys
import unittest
import importlib.util
import tempfile
from types import SimpleNamespace
from pathlib import Path


class RFDETRAdapterContractTests(unittest.TestCase):
    @staticmethod
    def module():
        script = Path(__file__).resolve().parents[1] / 'examples' / 'rfdetr_managed.py'
        spec = importlib.util.spec_from_file_location('rfdetr_managed_contract', script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_help_exposes_explicit_long_run_parameters_without_loading_training_stack(self):
        script = Path(__file__).resolve().parents[1] / 'examples' / 'rfdetr_managed.py'

        result = subprocess.run(
            [sys.executable, str(script), '--help'],
            capture_output=True, text=True, encoding='utf-8', timeout=10,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('--epochs', result.stdout)
        self.assertIn('--batch-size', result.stdout)
        self.assertIn('--grad-accum', result.stdout)
        self.assertIn('--num-workers', result.stdout)
        self.assertIn('--resume-from-run', result.stdout)
        self.assertIn('--checkpoint-steps', result.stdout)
        self.assertIn('--checkpoint-seconds', result.stdout)
        self.assertIn('--audit-data', result.stdout)

    def test_dataset_view_maps_single_view_aliases_to_coco_directories(self):
        aliases = self.module().dataset_aliases(Path('/dataset'))

        self.assertEqual(aliases['annotations_oneview'], Path('/dataset/annotations_oneview'))
        self.assertEqual(aliases['train'], Path('/dataset/train2017'))
        self.assertEqual(aliases['val'], Path('/dataset/val2017'))

    def test_contract_binds_scientific_state_but_not_checkpoint_frequency(self):
        module = self.module()
        base = module.parse_args([
            '--epochs', '3', '--num-workers', '0', '--max-steps', '4',
            '--limit-train-batches', '2', '--checkpoint-steps', '1',
        ])
        changed_interval = module.parse_args([
            '--epochs', '3', '--num-workers', '0', '--max-steps', '4',
            '--limit-train-batches', '2', '--checkpoint-steps', '9',
        ])
        changed_lr = module.parse_args([
            '--epochs', '3', '--num-workers', '0', '--max-steps', '4',
            '--limit-train-batches', '2', '--checkpoint-steps', '1',
            '--lr', '0.0002',
        ])

        self.assertEqual(module.recovery_recipe(base), module.recovery_recipe(changed_interval))
        self.assertNotEqual(module.recovery_recipe(base), module.recovery_recipe(changed_lr))

    def test_contract_hashes_source_weights_annotations_and_runtime(self):
        module = self.module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = root / 'project'
            dataset = root / 'dataset'
            source = project / 'src/rfdetr/model.py'
            annotation_root = dataset / 'annotations_oneview'
            annotation_root.mkdir(parents=True)
            source.parent.mkdir(parents=True)
            (project / 'train_single_view.py').write_text('entry', encoding='utf-8')
            source.write_text('model = 1', encoding='utf-8')
            for split in ('train', 'val'):
                (annotation_root / f'{split}_cocoformat_master.json').write_text('{}', encoding='utf-8')
            pretrain = project / 'model.pth'
            pretrain.write_bytes(b'weights')
            adapter = root / 'adapter.py'
            adapter.write_text('adapter', encoding='utf-8')
            checkpoint_module = root / 'checkpoints.py'
            checkpoint_module.write_text('checkpoint store', encoding='utf-8')
            options = dict(
                project=project, dataset=dataset, pretrain=pretrain,
                adapter=adapter, recipe={'epochs': 3},
                runtime={'python': '3.12', 'torch': '2.8'},
                recovery_modules={'checkpoints.py': checkpoint_module},
            )

            first = module.contract_hash(module.build_contract_data(**options))
            source.write_text('model = 2', encoding='utf-8')
            second = module.contract_hash(module.build_contract_data(**options))

            self.assertNotEqual(first, second)

    def test_optimizer_progress_requires_step_model_and_optimizer_change(self):
        module = self.module()
        before = {'global_step': 1, 'model': 'model-a', 'optimizers': 'optimizer-a'}
        after = {'global_step': 2, 'model': 'model-b', 'optimizers': 'optimizer-b'}

        self.assertTrue(module.optimizer_progressed(before, after))
        self.assertFalse(module.optimizer_progressed(before, {**after, 'model': 'model-a'}))
        self.assertFalse(module.optimizer_progressed(before, {**after, 'optimizers': 'optimizer-a'}))
        self.assertFalse(module.optimizer_progressed(before, {**after, 'global_step': 1}))

    def test_resume_source_must_be_terminal(self):
        module = self.module()

        for status in ('CANCELLED', 'FAILED', 'SUCCEEDED'):
            self.assertTrue(module.resume_source_is_terminal(status))
        for status in ('QUEUED', 'PREPARING', 'RUNNING', 'EVALUATING', 'LOST', None):
            self.assertFalse(module.resume_source_is_terminal(status))

    def test_replay_datamodule_is_a_thin_loader_adapter(self):
        module = self.module()
        calls = []

        class Original:
            def __init__(self):
                self.train_config = SimpleNamespace(seed=73)

            def train_dataloader(self):
                return 'research-loader'

        research = SimpleNamespace(SingleViewRFDETRDataModule=Original)

        def replay(loader, **kwargs):
            calls.append((loader, kwargs))
            return 'replay-loader'

        adapted = module.install_replay_datamodule(
            research, replay, identity='contract', max_batches=8,
        )

        self.assertIs(research.SingleViewRFDETRDataModule, adapted)
        self.assertEqual(adapted().train_dataloader(), 'replay-loader')
        self.assertEqual(calls, [('research-loader', {
            'seed': 73, 'identity': 'contract', 'max_batches': 8,
        })])


if __name__ == '__main__':
    unittest.main()
