"""Run RF-DETR through CCS without editing the research repository.

Every recovery creates a new Run.  Only atomically committed, hash-verified,
contract-compatible full Lightning checkpoints may be selected; failure to
select one aborts instead of silently starting from pretrained weights.  The
training DataLoader is adapted to acknowledged replay-v1 occurrences so a
resume starts at the last completed batch, not at a prefetched batch.
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


def dataset_aliases(root):
    """Map the single-view loader's aliases to the repository's COCO layout."""
    return {
        'annotations_oneview': root / 'annotations_oneview',
        'train': root / 'train2017',
        'val': root / 'val2017',
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description='CCS-managed single-view RF-DETR training')
    parser.add_argument('--project', type=Path, default=os.environ.get('CCS_RFDETR_PROJECT'),
                        help='Absolute source directory; or CCS_RFDETR_PROJECT')
    parser.add_argument('--dataset', type=Path, default=os.environ.get('CCS_RFDETR_DATASET'),
                        help='Absolute dataset directory; or CCS_RFDETR_DATASET')
    parser.add_argument('--pretrain', type=Path, default=os.environ.get('CCS_RFDETR_PRETRAIN'),
                        help='Absolute pretrained checkpoint; or CCS_RFDETR_PRETRAIN')
    parser.add_argument('--resume-from-run')
    parser.add_argument('--epochs', type=int, required=True)
    parser.add_argument('--batch-size', type=int, default=2)
    parser.add_argument('--grad-accum', type=int, default=4)
    parser.add_argument('--num-workers', type=int, default=4)
    parser.add_argument('--lr', type=float, default=1e-4)
    parser.add_argument('--lr-backbone', type=float, default=1e-5)
    parser.add_argument('--weight-decay', type=float, default=1e-4)
    parser.add_argument('--resolution', type=int, default=560)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--multi-scale', action='store_true')
    parser.add_argument('--fast-dev-run', action='store_true')
    parser.add_argument('--checkpoint-steps', type=int, default=1000)
    parser.add_argument('--checkpoint-seconds', type=float, default=1800)
    parser.add_argument('--checkpoint-keep', type=int, default=3)
    parser.add_argument('--limit-train-batches', type=int)
    parser.add_argument('--limit-val-batches', type=int)
    parser.add_argument('--max-steps', type=int, default=-1)
    parser.add_argument('--audit-data', action='store_true')
    parser.add_argument('--probe-hold-step', type=int, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def training_paths(args):
    """Require explicit locations before reading or importing training material."""
    selected = []
    for name in ('project', 'dataset', 'pretrain'):
        value = getattr(args, name)
        if value is None or not Path(value).is_absolute():
            raise ValueError(f'--{name} or CCS_RFDETR_{name.upper()} must be an absolute path')
        selected.append(Path(value).resolve())
    return tuple(selected)


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def recovery_recipe(args):
    """State-affecting inputs that must match for a controlled continuation."""
    return {key: getattr(args, key) for key in (
        'epochs', 'batch_size', 'grad_accum', 'num_workers', 'lr',
        'lr_backbone', 'weight_decay', 'resolution', 'seed', 'multi_scale',
        'fast_dev_run', 'limit_train_batches', 'limit_val_batches', 'max_steps',
    )}


def build_contract_data(*, project, dataset, pretrain, adapter, recipe, runtime,
                        recovery_modules):
    """Build the auditable experiment contract without importing training code."""
    source_root = project / 'src' / 'rfdetr'
    sources = {'train_single_view.py': sha256(project / 'train_single_view.py')}
    for path in sorted(source_root.rglob('*.py')):
        if path.is_file() and not path.is_symlink():
            sources[str(path.relative_to(project))] = sha256(path)
    annotations = {
        split: sha256(dataset / 'annotations_oneview' / f'{split}_cocoformat_master.json')
        for split in ('train', 'val')
    }
    return {
        'schema_version': 1,
        'sources': sources,
        'recipe': dict(recipe),
        'runtime': dict(runtime),
        'adapter_sha256': sha256(adapter),
        'recovery_modules': {
            name: sha256(path) for name, path in sorted(recovery_modules.items())
        },
        'pretrain_sha256': sha256(pretrain),
        'annotations_sha256': annotations,
        'dataset': str(dataset),
        'resume_semantics': {
            'training_state': 'full-lightning-state',
            'data_position': 'acknowledged-batch-replay-v1',
            'exact_data_replay': True,
            'scope': 'sample occurrence, order, and seeded CPU augmentation',
            'bitwise_training_guarantee': False,
        },
    }


def contract_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def optimizer_progressed(before, after):
    """Require a step advance plus changed model and optimizer state."""
    return (
        after['global_step'] > before['global_step'] and
        after['model'] != before['model'] and
        after['optimizers'] != before['optimizers']
    )


def resume_source_is_terminal(status):
    return status in {'CANCELLED', 'FAILED', 'SUCCEEDED'}


def install_replay_datamodule(module, replay_loader, *, identity, max_batches):
    """Adapt the research DataModule at its loader seam."""
    original = module.SingleViewRFDETRDataModule

    class ReplayDataModule(original):
        def train_dataloader(self):
            loader = super().train_dataloader()
            return replay_loader(
                loader, seed=self.train_config.seed,
                identity=identity, max_batches=max_batches,
            )

    module.SingleViewRFDETRDataModule = ReplayDataModule
    return ReplayDataModule


def main(argv=None):
    args = parse_args(argv)
    project, dataset, pretrain = training_paths(args)
    if args.epochs < 1 or args.batch_size < 1 or args.grad_accum < 1 or args.num_workers < 0:
        raise ValueError('epochs, batch size and gradient accumulation must be positive')
    if (args.checkpoint_steps < 1 or args.checkpoint_seconds <= 0 or args.checkpoint_keep < 2 or
            args.max_steps == 0 or args.max_steps < -1 or
            any(value is not None and value < 1 for value in (
                args.limit_train_batches, args.limit_val_batches, args.probe_hold_step))):
        raise ValueError('checkpoint intervals, limits and max steps are invalid')
    run_id = os.environ.get('TRAINCTL_RUN_ID', '')
    for value in (run_id, args.resume_from_run):
        if value is not None and not re.fullmatch(r'run_[a-f0-9]{32}', value):
            raise ValueError('run identifier is missing or invalid')
    if Path.cwd().resolve() != project:
        raise ValueError('unexpected project working directory')
    run_dir = Path(os.environ['TRAINCTL_RUN_DIR']).resolve()
    if run_dir.name != run_id or run_dir.parent.name != 'runs':
        raise ValueError('unexpected CCS run directory')
    if any(not path.is_file() or path.is_symlink() for path in (
            project / 'train_single_view.py', pretrain,
            dataset / 'annotations_oneview/train_cocoformat_master.json',
            dataset / 'annotations_oneview/val_cocoformat_master.json')):
        raise ValueError('required RF-DETR source, weights or annotations are unavailable')
    aliases = dataset_aliases(dataset)
    if any(not path.is_dir() or path.is_symlink() for path in aliases.values()):
        raise ValueError('required RF-DETR image directories are unavailable')

    output_root = project / 'orchestrator_runs'
    output_root.mkdir(exist_ok=True)
    if output_root.is_symlink():
        raise ValueError('RF-DETR output root must not be a symlink')
    output = output_root / run_id
    output.mkdir(exist_ok=False)
    dataset_view = output / 'dataset-view'
    dataset_view.mkdir()
    for alias, target in aliases.items():
        (dataset_view / alias).symlink_to(target, target_is_directory=True)
    for name, suffix in {
            'XDG_CACHE_HOME': 'cache', 'MPLCONFIGDIR': 'matplotlib',
            'TORCH_HOME': 'torch', 'HF_HOME': 'huggingface'}.items():
        directory = output / suffix
        directory.mkdir()
        os.environ[name] = str(directory)
    os.environ.setdefault('HF_HUB_OFFLINE', '1')
    os.environ.setdefault('WANDB_MODE', 'disabled')
    os.environ.setdefault('CUBLAS_WORKSPACE_CONFIG', ':4096:8')
    os.environ.setdefault('PYTORCH_CUDA_ALLOC_CONF', 'expandable_segments:True')

    import torch
    import pytorch_lightning as pl
    from pytorch_lightning.callbacks import LearningRateMonitor, ModelCheckpoint
    from trainctl.checkpoints import CheckpointStore
    from trainctl.lightning_checkpoints import PeriodicCheckpoint, live_state
    from trainctl.lightning_progress import TrainingProgressCallback
    from trainctl.lightning_replay import ReplayProgress, inspect_replay_checkpoint
    from trainctl.recovery_input import RecoveryInput
    from trainctl.replay_data import ReplayLoader
    from trainctl.run_store import RunStore

    if not torch.cuda.is_available():
        raise RuntimeError('CUDA is unavailable in the RFDETR environment')
    parameters = {
        'epochs': args.epochs, 'batch_size': args.batch_size,
        'grad_accum': args.grad_accum, 'num_workers': args.num_workers,
        'lr': args.lr, 'lr_backbone': args.lr_backbone,
        'weight_decay': args.weight_decay, 'resolution': args.resolution,
        'seed': args.seed, 'multi_scale': args.multi_scale,
        'fast_dev_run': args.fast_dev_run,
        'checkpoint_steps': args.checkpoint_steps,
        'checkpoint_seconds': args.checkpoint_seconds,
        'checkpoint_keep': args.checkpoint_keep,
        'limit_train_batches': args.limit_train_batches,
        'limit_val_batches': args.limit_val_batches,
        'max_steps': args.max_steps,
        'audit_data': args.audit_data,
    }
    contract_data = build_contract_data(
        project=project, dataset=dataset, pretrain=pretrain,
        adapter=Path(__file__), recipe=recovery_recipe(args),
        runtime={
            'python': sys.version.split()[0], 'torch': torch.__version__,
            'lightning': pl.__version__, 'cuda': torch.version.cuda,
        },
        recovery_modules={
            name: Path(__import__('trainctl').__file__).resolve().parent / name
            for name in (
                'checkpoints.py', 'lightning_checkpoints.py',
                'replay_data.py', 'lightning_replay.py',
            )
        },
    )
    contract = contract_hash(contract_data)
    RunStore._atomic_json(output / 'experiment-contract.json', {
        'hash': contract, **contract_data,
    })
    RunStore._atomic_json(output / 'ccs-run-metadata.json', {
        'schema_version': 1, 'run_id': run_id, 'project': str(project),
        'dataset': str(dataset), 'dataset_view': str(dataset_view),
        'pretrain_weights': str(pretrain),
        'parameters': parameters, 'python': sys.version.split()[0],
        'torch': torch.__version__, 'lightning': pl.__version__,
        'cuda': torch.version.cuda, 'gpu': torch.cuda.get_device_name(0),
        'source_sha256': sha256(project / 'train_single_view.py'),
        'adapter_sha256': sha256(Path(__file__)),
        'experiment_contract': contract,
        'resume_from_run': args.resume_from_run,
        'task_checkpoint_requested': bool(os.environ.get('TRAINCTL_TASK_CHECKPOINT')),
    })

    selected = None
    checkpoint = None
    local_store = None
    source_status = None
    if args.resume_from_run:
        source_run_dir = run_dir.parent / args.resume_from_run
        source_status_path = source_run_dir / 'status.json'
        if (source_run_dir.is_symlink() or source_status_path.is_symlink() or
                not source_status_path.is_file()):
            raise ValueError('managed source Run record is missing or unsafe')
        source_status = RunStore._read_json(source_status_path, {}).get('status')
        if not resume_source_is_terminal(source_status):
            raise ValueError('managed source Run must be terminal before recovery')
        source = output_root / args.resume_from_run / 'checkpoints'
        if not source.is_dir() or source.is_symlink() or source.resolve() != source:
            raise ValueError('managed source checkpoint directory is missing or unsafe')
        local_store = CheckpointStore(source)
    selected = RecoveryInput(inspect_replay_checkpoint).select(
        output / 'resume-source.ckpt', contract, local_store=local_store,
    )
    resume_from = args.resume_from_run
    if selected:
        resume_from = resume_from or selected.get('source', {}).get('source_run_id')
        if selected['state']['contract'] != contract:
            raise ValueError('serialized checkpoint contract mismatch')
        checkpoint = selected['path']
        RunStore._atomic_json(output / 'resume-selection.json', {
            'from_run': resume_from,
            'source_status': source_status,
            **selected,
        })
        print('CHECKPOINT_SELECTED ' + json.dumps({
            'step': selected['state']['global_step'],
            'checkpoint_id': selected['selected']['id'],
            'rejected': selected['rejected'],
        }), flush=True)

    module_spec = importlib.util.spec_from_file_location(
        'ccs_research_train_single_view', project / 'train_single_view.py')
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    install_replay_datamodule(
        module, ReplayLoader, identity=contract,
        max_batches=args.limit_train_batches,
    )
    replay = ReplayProgress(output / 'data-trace.jsonl' if args.audit_data else None)

    periodic = PeriodicCheckpoint(
        output / 'checkpoints', contract=contract,
        every_steps=args.checkpoint_steps,
        every_seconds=args.checkpoint_seconds,
        keep=args.checkpoint_keep,
        data_resume='acknowledged replay-v1 sample occurrences and seeded CPU augmentation',
    )

    class RecoveryEvidence(pl.Callback):
        def __init__(self):
            self.optimizer_step_events = 0
            self.initial = None

        def on_train_start(self, trainer, pl_module):
            initial = live_state(trainer)
            if selected:
                for key in (
                        'global_step', 'model', 'optimizers', 'schedulers',
                        'precision', 'callbacks', 'rng'):
                    if initial[key] != selected['state'][key]:
                        raise AssertionError('restore mismatch: ' + key)
            self.initial = initial
            RunStore._atomic_json(output / 'restore-audit.json', {
                'schema_version': 1,
                'from_run': resume_from,
                'verified': bool(selected),
                'state': initial,
                'data_recipe': 'replay-v1',
                'data_position_restored': bool(selected),
                'exact_data_replay': True,
                'replay_scope': 'sample occurrence, order, and seeded CPU augmentation',
                'bitwise_training_guarantee': False,
                'boundary': 'completed batch and optimizer step',
            })

        def on_before_optimizer_step(self, trainer, pl_module, optimizer):
            self.optimizer_step_events += 1

        def on_train_batch_end(self, trainer, pl_module, outputs, batch, batch_idx):
            if args.probe_hold_step and trainer.global_step >= args.probe_hold_step:
                if not periodic.latest or periodic.latest['state']['global_step'] != trainer.global_step:
                    raise AssertionError('probe point has no committed managed checkpoint')
                current = live_state(trainer)
                if not optimizer_progressed(self.initial, current):
                    print(
                        'RFDETR_CHECKPOINT_WAITING_FOR_MODEL_UPDATE '
                        f'step={trainer.global_step}',
                        flush=True,
                    )
                    return
                RunStore._atomic_json(output / 'probe-ready.json', {
                    'step': trainer.global_step,
                    'optimizer_step_events': self.optimizer_step_events,
                    'optimizer_progress_verified': True,
                    'checkpoint_id': periodic.latest['id'],
                })
                print('RFDETR_CHECKPOINT_READY_FOR_INTERRUPT', flush=True)
                time.sleep(120)
                raise RuntimeError('probe controller did not interrupt within 120 seconds')

    evidence = RecoveryEvidence()

    class FinalCheckpoint(pl.Callback):
        def on_train_end(self, trainer, pl_module):
            periodic.save_now(trainer, reason='train_end', pinned=True)

    class ManagedTrainer(pl.Trainer):
        def __init__(self, **kwargs):
            # The research ModelCheckpoint writes useful inference artifacts but
            # is not a CCS recovery point.  Remove it (and its logger-only LR
            # callback) so callback state and paths are stable across new Runs.
            callbacks = [item for item in (kwargs.get('callbacks') or [])
                         if not isinstance(item, (ModelCheckpoint, LearningRateMonitor))]
            callbacks = [replay] + callbacks + [
                TrainingProgressCallback(interval=15), periodic, evidence,
                FinalCheckpoint(),
            ]
            kwargs.update(
                callbacks=callbacks,
                enable_checkpointing=False,
                enable_progress_bar=False,
                logger=False,
                num_sanity_val_steps=0,
                max_steps=args.max_steps,
                use_distributed_sampler=False,
            )
            if args.limit_train_batches is not None:
                kwargs['limit_train_batches'] = args.limit_train_batches
            if args.limit_val_batches is not None:
                kwargs['limit_val_batches'] = args.limit_val_batches
            super().__init__(**kwargs)

        def fit(self, *positional, **kwargs):
            if not positional:
                raise ValueError('managed Trainer requires a model')
            positional[0].strict_loading = True
            if kwargs.get('ckpt_path') not in (None, checkpoint):
                raise ValueError('research entry attempted to override the managed checkpoint')
            kwargs['ckpt_path'] = checkpoint
            result = super().fit(*positional, **kwargs)
            final_state = live_state(self)
            progress_verified = optimizer_progressed(evidence.initial, final_state)
            if selected and not progress_verified:
                raise AssertionError('restored run made no optimizer progress')
            RunStore._atomic_json(output / 'managed-result.json', {
                'schema_version': 1,
                'run_id': run_id,
                'status': 'completed',
                'parameters': parameters,
                'from_run': resume_from,
                'initial_step': evidence.initial['global_step'],
                'final_step': self.global_step,
                'optimizer_step_events': evidence.optimizer_step_events,
                'optimizer_progress_verified': progress_verified,
                'final_state': final_state,
                'latest_checkpoint': periodic.latest,
                'data_recipe': 'replay-v1',
                'data_position_restored': bool(selected),
                'exact_data_replay': True,
                'replay_scope': 'sample occurrence, order, and seeded CPU augmentation',
                'bitwise_training_guarantee': False,
            })
            print('RFDETR_MANAGED_CHECKPOINT_RUN_PASSED', flush=True)
            return result

    module.Trainer = ManagedTrainer
    command = [
        'train_single_view.py', '--dataset_dir', str(dataset_view),
        '--output_dir', str(output), '--epochs', str(args.epochs),
        '--batch_size', str(args.batch_size), '--grad_accum', str(args.grad_accum),
        '--num_workers', str(args.num_workers), '--lr', str(args.lr),
        '--lr_backbone', str(args.lr_backbone), '--weight_decay', str(args.weight_decay),
        '--resolution', str(args.resolution), '--pretrain_weights', str(pretrain),
        '--seed', str(args.seed),
    ]
    if args.multi_scale:
        command.append('--multi_scale')
    if args.fast_dev_run:
        command.append('--fast_dev_run')
    previous_argv = sys.argv
    try:
        sys.argv = command
        module.main()
    finally:
        sys.argv = previous_argv


if __name__ == '__main__':
    main()
