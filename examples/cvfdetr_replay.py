"""Replay-v1 CVFDETR recipe; legacy cvfdetr_managed.py remains unchanged.

The harness always creates a new per-run output. Checkpoint restore is explicit;
incompatible/corrupt points never silently trigger training from scratch.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import sys
import time
from pathlib import Path


def main(*, source_project=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--resume-from-run')
    parser.add_argument('--epochs', type=int, default=100)
    parser.add_argument('--batch-size', type=int, default=2)
    parser.add_argument('--grad-accum', type=int, default=4)
    parser.add_argument('--num-workers', type=int, default=1)
    parser.add_argument('--checkpoint-steps', type=int, default=1000)
    parser.add_argument('--checkpoint-seconds', type=float, default=1800)
    parser.add_argument('--limit-train-batches', type=int)
    parser.add_argument('--max-steps', type=int, default=-1)
    parser.add_argument('--stop-after-seconds', type=float)
    parser.add_argument('--probe-hold-step', type=int)
    parser.add_argument('--audit-data', action='store_true')
    args = parser.parse_args()
    project = Path('/srv/research/cvfdetr')
    source_project = Path(source_project) if source_project else project
    base = project / 'orchestrator_runs'
    run_id = os.environ.get('TRAINCTL_RUN_ID', '')
    for value in (run_id, args.resume_from_run):
        if value is not None and not re.fullmatch(r'run_[a-f0-9]{32}', value):
            raise ValueError('Invalid run identifier')
    if Path.cwd().resolve() != project:
        raise ValueError('Unexpected project working directory')
    output = base / run_id
    if output.resolve() != output:
        raise ValueError('Unexpected output symlink')
    output.mkdir(exist_ok=False)
    for name, suffix in {'XDG_CACHE_HOME': 'cache', 'MPLCONFIGDIR': 'matplotlib',
                         'TORCH_HOME': 'torch', 'HF_HOME': 'huggingface'}.items():
        directory = output / suffix
        directory.mkdir()
        os.environ[name] = str(directory)
    (output / 'cache/torch/kernels').mkdir(parents=True)
    os.environ['HF_HUB_OFFLINE'] = '1'
    os.environ['WANDB_MODE'] = 'disabled'
    import torch
    import pytorch_lightning as pl
    from trainctl.checkpoints import CheckpointStore, sha256
    from trainctl.lightning_checkpoints import PeriodicCheckpoint, inspect_checkpoint, live_state
    from trainctl.lightning_progress import TrainingProgressCallback
    from trainctl.run_store import RunStore
    from trainctl.replay_data import ReplayLoader
    from trainctl.lightning_replay import ReplayProgress, inspect_replay_checkpoint
    from trainctl.recovery_input import RecoveryInput
    from trainctl.training_budget import TrainingTimeBudget
    wall_budget = TrainingTimeBudget(args.stop_after_seconds)

    recipe = {key: getattr(args, key) for key in ('batch_size', 'grad_accum', 'num_workers', 'limit_train_batches')}
    sources = {str(path.relative_to(source_project)): sha256(path) for path in sorted((source_project / 'src').rglob('*.py'))
               if not path.is_symlink()}
    sources['train_gcr.py'] = sha256(source_project / 'train_gcr.py')
    # Detect source/recipe/runtime drift; not an immutable code or dataset snapshot.
    contract_data = {'schema_version': 1, 'sources': sources, 'recipe': recipe,
        'torch': torch.__version__, 'lightning': pl.__version__, 'python': sys.version.split()[0],
        'adapter_sha256': sha256(Path(__file__)), 'data_recipe': 'replay-v1',
        'data_modules': {name: sha256(Path(__import__('trainctl').__file__).parent / name) for name in
                         ('replay_data.py', 'lightning_replay.py', 'lightning_checkpoints.py')}, 'dataset': '/srv/datasets/example-coco'}
    annotations = {}
    for split in ('train', 'val'):
        path = Path(contract_data['dataset']) / 'annotations' / (split + '_cocoformat.json')
        if not path.exists():
            path = Path(contract_data['dataset']) / split / '_annotations.coco.json'
        annotations[split] = sha256(path)
    contract_data['annotations_sha256'] = annotations
    contract = hashlib.sha256(json.dumps(contract_data, sort_keys=True).encode()).hexdigest()
    RunStore._atomic_json(output / 'experiment-contract.json', {'hash': contract, **contract_data})
    selected = None
    checkpoint = None
    local_store = None
    if args.resume_from_run:
        source = base / args.resume_from_run / 'checkpoints'
        if not source.is_dir() or source.resolve() != source:
            raise ValueError('Managed source checkpoint directory missing or unsafe')
        local_store = CheckpointStore(source)
    selected = RecoveryInput(inspect_replay_checkpoint).select(
        output / 'resume-source.ckpt', contract, local_store=local_store,
    )
    resume_from = args.resume_from_run
    if selected:
        resume_from = resume_from or selected.get('source', {}).get('source_run_id')
        if selected['state']['contract'] != contract:
            raise ValueError('Serialized checkpoint contract mismatch')
        checkpoint = selected['path']
        RunStore._atomic_json(output / 'resume-selection.json', {'from_run': resume_from, **selected})
        print('CHECKPOINT_SELECTED ' + json.dumps({'step': selected['state']['global_step'], 'rejected': selected['rejected']}), flush=True)

    module_spec = importlib.util.spec_from_file_location('research_train_gcr', source_project / 'train_gcr.py')
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    if source_project != project:
        origin = Path(sys.modules['rfdetr'].__file__).resolve()
        if not origin.is_relative_to(source_project):
            raise ValueError('Research package imported outside snapshot')
        RunStore._atomic_json(Path(os.environ['TRAINCTL_RUN_DIR']) / 'snapshot-imports.json', {
            'entry': str(Path(sys.argv[0]).resolve()), 'research_package': str(origin),
            'adapter': str(Path(__file__).resolve()),
            'trainctl': str(Path(__import__('trainctl').__file__).resolve()),
            'research_source': str(source_project), 'data_and_output_project': str(project)})
        print('SNAPSHOT_IMPORTS ' + json.dumps({'rfdetr': str(origin)}), flush=True)
    class ReplayDataModule(module.GCRDataModule):
        def train_dataloader(self):
            return ReplayLoader(super().train_dataloader(), seed=self.train_config.seed,
                                identity=contract, max_batches=args.limit_train_batches)
    module.GCRDataModule = ReplayDataModule
    replay = ReplayProgress(output / 'data-trace.jsonl' if args.audit_data else None)
    periodic = PeriodicCheckpoint(output / 'checkpoints', contract=contract,
        every_steps=args.checkpoint_steps, every_seconds=args.checkpoint_seconds)

    class ManagedEMA(module.RFDETREMACallback):
        def load_state_dict(self, state_dict):
            super().load_state_dict(state_dict)
            # Current Lightning restores callbacks after setup. The research
            # callback otherwise leaves restored EMA tensors pending indefinitely.
            if self._average_model is not None and self._pending_average_state_dict is not None:
                self._average_model.load_state_dict(self._pending_average_state_dict)
                self._pending_average_state_dict = None

        def on_train_end(self, trainer, pl_module):
            # Save live training weights paired with their optimizer, before the
            # research callback swaps them to EMA inference weights.
            periodic.save_now(trainer, reason='train_end_before_ema_swap', pinned=True)
            super().on_train_end(trainer, pl_module)

    class Evidence(pl.Callback):
        def __init__(self):
            self.updates = 0
            self.stop_reason = None

        def on_train_start(self, trainer, pl_module):
            wall_budget.start(trainer.global_step)
            initial = live_state(trainer)
            if selected:
                for key in ('global_step', 'model', 'optimizers', 'schedulers', 'precision', 'callbacks', 'rng'):
                    if initial[key] != selected['state'][key]:
                        raise AssertionError('Restore mismatch: ' + key)
            self.initial = initial
            self.handle = trainer.optimizers[0].register_step_post_hook(self.updated)
            RunStore._atomic_json(output / 'restore-audit.json', {'state': initial, 'from_run': resume_from,
                'verified': bool(selected), 'data_recipe': 'replay-v1', 'data_position_restored': bool(selected), 'bitwise_training_guarantee': False})

        def updated(self, *unused):
            self.updates += 1

        def on_train_batch_end(self, trainer, pl_module, outputs, batch, batch_idx):
            if wall_budget.after_batch(trainer.global_step):
                trainer.should_stop = True
                self.stop_reason = 'training_wall_time_budget'
                print('TRAINING_TIME_BUDGET_REACHED ' + str(trainer.global_step), flush=True)
            if args.probe_hold_step and trainer.global_step >= args.probe_hold_step:
                if not periodic.latest or periodic.latest['state']['global_step'] != trainer.global_step:
                    raise AssertionError('Probe interruption point must have a committed checkpoint')
                RunStore._atomic_json(output / 'probe-ready.json', {'step': trainer.global_step, 'actual_updates': self.updates})
                print('PERIODIC_CHECKPOINTS_READY_FOR_INTERRUPT', flush=True)
                time.sleep(120)
                raise RuntimeError('Probe controller failed to interrupt within 120 seconds')

        def teardown(self, trainer, pl_module, stage):
            if hasattr(self, 'handle'):
                self.handle.remove()

    class ManagedTrainer(pl.Trainer):
        def __init__(self, **kwargs):
            self.evidence = Evidence()
            callbacks = []
            for item in kwargs['callbacks']:
                if isinstance(item, module.RFDETREMACallback):
                    callbacks.append(ManagedEMA(decay=item._decay, tau=item._tau,
                        use_buffers=item._use_buffers, update_interval_steps=item._update_interval_steps))
                elif isinstance(item, module.MasterCocoEvalCallback):
                    callbacks.append(item)
            kwargs.update(callbacks=[replay] + callbacks + [TrainingProgressCallback(), periodic, self.evidence],
                enable_checkpointing=False, enable_progress_bar=False, logger=False,
                num_sanity_val_steps=0, max_steps=args.max_steps, use_distributed_sampler=False)
            if args.limit_train_batches is not None:
                kwargs.update(limit_train_batches=args.limit_train_batches, limit_val_batches=1)
            super().__init__(**kwargs)

        def fit(self, *positional, **kwargs):
            positional[0].strict_loading = True
            kwargs['ckpt_path'] = checkpoint
            result = super().fit(*positional, **kwargs)
            if selected and (self.global_step <= selected['state']['global_step'] or self.evidence.updates < 1):
                raise AssertionError('Restored run did not make actual optimizer progress')
            RunStore._atomic_json(output / 'managed-result.json', {'status': 'passed',
                'training_wall_seconds': wall_budget.elapsed, 'stop_reason': self.evidence.stop_reason,
                'from_run': resume_from, 'initial_step': self.evidence.initial['global_step'],
                'final_step': self.global_step, 'actual_optimizer_updates': self.evidence.updates,
                'latest_checkpoint': periodic.latest, 'data_recipe': 'replay-v1', 'data_position_restored': bool(selected), 'bitwise_training_guarantee': False})
            print('MANAGED_CHECKPOINT_RUN_PASSED', flush=True)
            return result

    module.Trainer = ManagedTrainer
    sys.argv = ['train_gcr.py', '--output_dir', str(output), '--epochs', str(args.epochs),
                '--batch_size', str(args.batch_size), '--grad_accum', str(args.grad_accum),
                '--num_workers', str(args.num_workers)]
    module.main()


if __name__ == '__main__':
    main()
