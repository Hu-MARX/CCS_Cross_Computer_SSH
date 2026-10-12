"""Bounded checkpoint/cancel/resume acceptance using unchanged research modules.

Only checkpoints created by this probe inside orchestrator_runs are accepted.
This harness limits Trainer loops; it is not a production experiment recipe.
"""
import argparse
import hashlib
import importlib.util
import json
import os
import re
import sys
import time
from pathlib import Path


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


def write_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2), encoding='utf-8')
    os.replace(temporary, path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--resume-from-run')
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
    from pytorch_lightning import Callback, Trainer

    source = base / args.resume_from_run if args.resume_from_run else None
    checkpoint = source / 'resume.ckpt' if source else None
    previous = None
    if source:
        if source.is_symlink() or checkpoint.is_symlink() or not checkpoint.resolve().is_relative_to(base):
            raise ValueError('Checkpoint path escaped trusted output directory')
        previous = json.loads((source / 'checkpoint-ready.json').read_text())
        if previous['sha256'] != digest(checkpoint):
            raise ValueError('Checkpoint digest mismatch')

    module_spec = importlib.util.spec_from_file_location('research_train_gcr', project / 'train_gcr.py')
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)

    def state(trainer):
        optimizer = trainer.optimizers[0]
        steps = [float(item['step']) for item in optimizer.state.values() if 'step' in item]
        scaler = trainer.precision_plugin.scaler
        return {'global_step': trainer.global_step, 'epoch': trainer.current_epoch,
                'optimizer_entries': len(optimizer.state), 'optimizer_step_max': max(steps, default=0),
                'scheduler': trainer.lr_scheduler_configs[0].scheduler.state_dict(),
                'scaler': scaler.state_dict() if scaler else {}}

    class Evidence(Callback):
        updates = 0
        initial = None

        def on_train_start(self, trainer, pl_module):
            self.initial = state(trainer)
            if previous:
                for key in ('global_step', 'optimizer_entries', 'optimizer_step_max', 'scheduler', 'scaler'):
                    if self.initial[key] != previous['state'][key]:
                        raise AssertionError(f'Restore mismatch: {key}')
            self.handle = trainer.optimizers[0].register_step_post_hook(self.updated)
            write_json(output / 'restore-state.json', {'from_run': args.resume_from_run, 'state': self.initial})

        def updated(self, *unused):
            self.updates += 1

        def on_train_batch_end(self, trainer, pl_module, outputs, batch, batch_idx):
            record = {'batch': batch_idx, 'epoch': trainer.current_epoch, 'updates_in_process': self.updates,
                      'global_step': trainer.global_step, 'scale': trainer.precision_plugin.scaler.get_scale(),
                      'loss': float(outputs['loss'].detach().cpu())}
            with (output / 'steps.jsonl').open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(record) + '\n')

        def on_train_end(self, trainer, pl_module):
            if source:
                final = state(trainer)
                assert final['global_step'] > self.initial['global_step']
                assert final['optimizer_step_max'] > self.initial['optimizer_step_max']
                assert self.updates > 0
                assert digest(checkpoint) == previous['sha256'], 'Original checkpoint was modified'
                trainer.save_checkpoint(output / 'resumed-final.ckpt')
                write_json(output / 'resume-result.json', {'status': 'passed', 'from_run': args.resume_from_run,
                    'initial': self.initial, 'final': final, 'actual_optimizer_updates': self.updates})
                print('CHECKPOINT_RESUME_PASSED', flush=True)

    class BoundedTrainer(Trainer):
        def __init__(self, **kwargs):
            self.evidence = Evidence()
            # Retain research EMA/evaluation callbacks, omit unrelated checkpoint copies/loggers.
            kwargs['callbacks'] = [item for item in kwargs['callbacks']
                if isinstance(item, (module.RFDETREMACallback, module.MasterCocoEvalCallback))] + [self.evidence]
            kwargs.update(limit_train_batches=16, limit_val_batches=1, num_sanity_val_steps=0,
                          enable_checkpointing=False, enable_progress_bar=False, logger=False)
            super().__init__(**kwargs)

        def fit(self, *positional, **kwargs):
            kwargs['ckpt_path'] = str(checkpoint) if checkpoint else None
            result = super().fit(*positional, **kwargs)
            if not source:
                if self.evidence.updates < 1:
                    raise AssertionError('No real optimizer updates; checkpoint acceptance failed')
                # Save only after fit completes: epoch-start hooks can contain
                # mixed previous/next-epoch loop progress and skip work on restore.
                temporary = output / 'resume.tmp.ckpt'
                self.save_checkpoint(temporary)
                os.replace(temporary, output / 'resume.ckpt')
                write_json(output / 'checkpoint-ready.json', {'run_id': run_id,
                    'sha256': digest(output / 'resume.ckpt'), 'state': state(self), 'updates': self.evidence.updates})
                print('CHECKPOINT_READY_WAITING_FOR_CANCEL', flush=True)
                time.sleep(120)
                raise RuntimeError('Acceptance controller did not cancel within 120 seconds')
            return result

    module.Trainer = BoundedTrainer
    sys.argv = ['train_gcr.py', '--output_dir', str(output), '--epochs', '2' if source else '1', '--batch_size', '1',
                '--grad_accum', '1', '--num_workers', '0']
    module.main()


if __name__ == '__main__':
    main()
