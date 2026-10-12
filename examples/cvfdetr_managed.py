"""Managed CVFDETR training without editing the original research files.

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


def main():
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
    parser.add_argument('--probe-hold-step', type=int)
    args = parser.parse_args()
    project = Path('/srv/research/cvfdetr')
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

    recipe = {key: getattr(args, key) for key in ('batch_size', 'grad_accum', 'num_workers', 'limit_train_batches')}
    sources = {str(path.relative_to(project)): sha256(path) for path in sorted((project / 'src').rglob('*.py'))
               if not path.is_symlink()}
    sources['train_gcr.py'] = sha256(project / 'train_gcr.py')
    # Detect source/recipe/runtime drift; not an immutable code or dataset snapshot.
    contract_data = {'schema_version': 1, 'sources': sources, 'recipe': recipe,
        'torch': torch.__version__, 'lightning': pl.__version__, 'python': sys.version.split()[0],
        'adapter_sha256': sha256(Path(__file__)), 'dataset': '/srv/datasets/example-coco'}
    contract = hashlib.sha256(json.dumps(contract_data, sort_keys=True).encode()).hexdigest()
    RunStore._atomic_json(output / 'experiment-contract.json', {'hash': contract, **contract_data})
    selected = None
    checkpoint = None
    if args.resume_from_run:
        source = base / args.resume_from_run / 'checkpoints'
        if not source.is_dir() or source.resolve() != source:
            raise ValueError('Managed source checkpoint directory missing or unsafe')
        selected = CheckpointStore(source).restore_copy(output / 'resume-source.ckpt', inspect_checkpoint, contract=contract)
        if selected['state']['contract'] != contract:
            raise ValueError('Serialized checkpoint contract mismatch')
        checkpoint = selected['path']
        RunStore._atomic_json(output / 'resume-selection.json', {'from_run': args.resume_from_run, **selected})
        print('CHECKPOINT_SELECTED ' + json.dumps({'step': selected['state']['global_step'], 'rejected': selected['rejected']}), flush=True)

    module_spec = importlib.util.spec_from_file_location('research_train_gcr', project / 'train_gcr.py')
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
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

        def on_train_start(self, trainer, pl_module):
            initial = live_state(trainer)
            if selected:
                for key in ('global_step', 'model', 'optimizers', 'schedulers', 'precision', 'callbacks', 'rng'):
                    if initial[key] != selected['state'][key]:
                        raise AssertionError('Restore mismatch: ' + key)
            self.initial = initial
            self.handle = trainer.optimizers[0].register_step_post_hook(self.updated)
            RunStore._atomic_json(output / 'restore-audit.json', {'state': initial, 'from_run': args.resume_from_run,
                'verified': bool(selected), 'exact_data_replay': False})

        def updated(self, *unused):
            self.updates += 1

        def on_train_batch_end(self, trainer, pl_module, outputs, batch, batch_idx):
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
            kwargs.update(callbacks=callbacks + [TrainingProgressCallback(), periodic, self.evidence],
                enable_checkpointing=False, enable_progress_bar=False, logger=False,
                num_sanity_val_steps=0, max_steps=args.max_steps)
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
                'from_run': args.resume_from_run, 'initial_step': self.evidence.initial['global_step'],
                'final_step': self.global_step, 'actual_optimizer_updates': self.evidence.updates,
                'latest_checkpoint': periodic.latest, 'exact_data_replay': False})
            print('MANAGED_CHECKPOINT_RUN_PASSED', flush=True)
            return result

    module.Trainer = ManagedTrainer
    sys.argv = ['train_gcr.py', '--output_dir', str(output), '--epochs', str(args.epochs),
                '--batch_size', str(args.batch_size), '--grad_accum', str(args.grad_accum),
                '--num_workers', str(args.num_workers)]
    module.main()


if __name__ == '__main__':
    main()
