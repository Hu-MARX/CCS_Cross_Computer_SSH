"""Optional single-device Lightning checkpoint adapter and state audit helpers."""
from __future__ import annotations

import hashlib
import math
import os
import random
import time
from pathlib import Path

import numpy as np
import torch
from pytorch_lightning import Callback

from controller.domain import utc_now
from .checkpoints import CheckpointStore
from .progress import ProgressReporter
from .run_store import RunStore


def fingerprint(value) -> str:
    """Device-independent recursive digest, including all tensor bytes."""
    digest = hashlib.sha256()
    def add(item):
        digest.update(type(item).__name__.encode() + b':')
        if isinstance(item, torch.Tensor):
            tensor = item.detach().cpu().contiguous()
            digest.update(str((tensor.dtype, tuple(tensor.shape))).encode())
            digest.update(tensor.reshape(-1).view(torch.uint8).numpy().tobytes())
        elif isinstance(item, dict):
            for key in sorted(item, key=repr):
                add(key)
                add(item[key])
        elif isinstance(item, (tuple, list)):
            for element in item:
                add(element)
        elif item is None or isinstance(item, (str, int, float, bool)):
            digest.update(repr(item).encode())
        else:
            raise ValueError(f'Unsupported state fingerprint type: {type(item)}')
        digest.update(b';')
    add(value)
    return digest.hexdigest()


def rng_state() -> dict:
    numpy_state = np.random.get_state()
    return {'python': random.getstate(),
            'numpy': (numpy_state[0], numpy_state[1].tolist(), *numpy_state[2:]),
            'torch': torch.get_rng_state(),
            'cuda': torch.cuda.get_rng_state_all() if torch.cuda.is_available() else []}


def restore_rng(state: dict) -> None:
    random.setstate(state['python'])
    ns = state['numpy']
    np.random.set_state((ns[0], np.asarray(ns[1], dtype=np.uint32), *ns[2:]))
    torch.set_rng_state(state['torch'].cpu())
    if state['cuda']:
        if len(state['cuda']) != torch.cuda.device_count():
            raise ValueError('CUDA RNG device count differs from checkpoint')
        torch.cuda.set_rng_state_all([item.cpu() for item in state['cuda']])


def inspect_checkpoint(path: Path) -> dict:
    # Only caller-owned files committed by CheckpointStore are permitted here.
    value = torch.load(path, map_location='cpu', weights_only=False)
    required = {'state_dict', 'optimizer_states', 'lr_schedulers', 'loops', 'epoch', 'global_step', 'trainctl_resume'}
    if not isinstance(value, dict) or not required.issubset(value):
        raise ValueError('Checkpoint lacks full training state')
    resume = value['trainctl_resume']
    if resume.get('schema_version') != 1 or not value['state_dict'] or not value['optimizer_states']:
        raise ValueError('Unsupported or incomplete managed checkpoint')
    precision_key = resume['precision_key']
    if resume['has_precision_state'] and precision_key not in value:
        raise ValueError('Checkpoint lacks AMP state')
    result = {'global_step': int(value['global_step']), 'epoch': int(value['epoch']),
            'contract': resume['contract'], 'model': fingerprint(value['state_dict']),
            'optimizers': fingerprint(value['optimizer_states']),
            'schedulers': fingerprint(value['lr_schedulers']),
            'precision': fingerprint(value.get(precision_key, {})),
            'callbacks': {key: fingerprint(state) for key, state in value.get('callbacks', {}).items()},
            'rng': fingerprint(resume['rng']), 'loop_state_present': bool(value['loops'])}
    if 'trainctl_replay' in value:
        from .replay_data import replay_checkpoint_state
        result['replay'] = replay_checkpoint_state(value)
    return result


def live_state(trainer) -> dict:
    return {'global_step': trainer.global_step,
            'model': fingerprint(trainer.lightning_module.state_dict()),
            'optimizers': fingerprint([o.state_dict() for o in trainer.optimizers]),
            'schedulers': fingerprint([c.scheduler.state_dict() for c in trainer.lr_scheduler_configs]),
            'precision': fingerprint(trainer.precision_plugin.state_dict()),
            'callbacks': {c.state_key: fingerprint(c.state_dict()) for c in trainer.callbacks if c.state_dict()},
            'rng': fingerprint(rng_state())}


class PeriodicCheckpoint(Callback):
    """Save at completed optimization boundaries, not during accumulation.

    Time intervals are lower bounds: a hung/long batch cannot reach a safe point.
    Intentionally supports single-device automatic optimization only.
    """
    def __init__(self, directory, *, contract, every_steps=1000, every_seconds=1800, keep=3,
                 data_resume='framework loop restoration; exact sampler/worker replay not guaranteed'):
        if type(every_steps) is not int or every_steps < 1:
            raise ValueError('every_steps must be a positive integer')
        if isinstance(every_seconds, bool) or not math.isfinite(every_seconds) or every_seconds <= 0:
            raise ValueError('every_seconds must be finite and positive')
        self.store = CheckpointStore(directory, keep=keep)
        self.contract = contract
        if not isinstance(data_resume, str) or not data_resume.strip():
            raise ValueError('data_resume must be a non-empty string')
        self.data_resume = data_resume
        self.every_steps, self.every_seconds = every_steps, every_seconds
        self.last_step, self.last_time = 0, 0.0
        self.pending_rng = None
        self.latest = None
        self.report_path = Path(os.environ['TRAINCTL_RUN_DIR']) / 'checkpoint-status.json' if os.environ.get('TRAINCTL_RUN_DIR') else None

    def report(self, state, **extra):
        if self.report_path:
            RunStore._atomic_json(self.report_path, {'state': state, 'updated_at': utc_now(),
                'interval_seconds': self.every_seconds, 'latest': self.latest, **extra})

    def on_train_start(self, trainer, pl_module):
        if trainer.world_size != 1 or not pl_module.automatic_optimization or len(trainer.optimizers) != 1:
            raise ValueError('Managed checkpoints require one device and one automatic optimizer')
        if self.pending_rng is not None:
            restore_rng(self.pending_rng)
            self.pending_rng = None
        self.last_step, self.last_time = trainer.global_step, time.monotonic()
        self.report('waiting', resumed_step=trainer.global_step)

    def on_save_checkpoint(self, trainer, pl_module, checkpoint):
        checkpoint['trainctl_resume'] = {'schema_version': 1, 'contract': self.contract,
            'rng': rng_state(), 'precision_key': type(trainer.precision_plugin).__qualname__,
            'has_precision_state': bool(trainer.precision_plugin.state_dict()),
            'data_resume': self.data_resume}

    def on_load_checkpoint(self, trainer, pl_module, checkpoint):
        value = checkpoint.get('trainctl_resume', {})
        if value.get('contract') != self.contract or value.get('schema_version') != 1:
            raise ValueError('Managed checkpoint contract mismatch')
        self.pending_rng = value['rng']

    def on_train_batch_end(self, trainer, pl_module, outputs, batch, batch_idx):
        step = trainer.global_step
        if step <= self.last_step:
            return
        # A changed global_step alone can occur before a new accumulation window;
        # check the actual boundary too, so unsaved partial gradients are avoided.
        boundary = (batch_idx + 1) % trainer.accumulate_grad_batches == 0 or batch_idx + 1 == trainer.num_training_batches
        if boundary and (step - self.last_step >= self.every_steps or time.monotonic() - self.last_time >= self.every_seconds):
            self.save_now(trainer, reason='periodic')

    def save_now(self, trainer, *, reason, pinned=False):
        self.report('saving', step=trainer.global_step)
        started = time.monotonic()
        try:
            result = self.store.commit(lambda path: trainer.save_checkpoint(path, weights_only=False),
                inspect_checkpoint, metadata={'contract': self.contract, 'reason': reason, 'pinned': pinned,
                                              'run_id': os.environ.get('TRAINCTL_RUN_ID')})
            self.latest = {key: result[key] for key in ('id', 'created_at', 'sha256', 'size', 'state', 'pruned_ids')}
            self.last_step, self.last_time = trainer.global_step, time.monotonic()
            self.report('committed', duration_seconds=self.last_time - started)
            print(f'CHECKPOINT_COMMITTED step={trainer.global_step} id={result["id"]}', flush=True)
            return result
        except Exception as exc:
            self.report('failed', error=str(exc))
            raise
