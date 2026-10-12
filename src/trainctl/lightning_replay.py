"""Lightning data acknowledgements and optional input trace for replay audits."""
from __future__ import annotations

import json
from pathlib import Path

from pytorch_lightning import Callback

from .lightning_checkpoints import fingerprint, inspect_checkpoint
from .replay_data import ReplayLoader, replay_checkpoint_state


def traceable_samples(samples):
    """Reduce RF-DETR single/multi-view containers to fingerprintable state."""
    if isinstance(samples, dict):
        return {key: traceable_samples(value) for key, value in samples.items()}
    if hasattr(samples, 'tensors') and hasattr(samples, 'mask'):
        return {'tensors': samples.tensors, 'mask': samples.mask}
    return samples


def inspect_replay_checkpoint(path):
    state = inspect_checkpoint(path)
    replay = state.get('replay')
    if not replay or not replay['safe_to_resume']:
        raise ValueError('UNSAFE_DATA_RESUME: ' + (replay or {}).get('reason', 'No acknowledged data cursor'))
    return state


class ReplayProgress(Callback):
    def __init__(self, trace_path=None):
        self.loader = None
        self.trace_path = Path(trace_path) if trace_path else None
        self.current_input = None
        self.expected_state = None

    def on_train_start(self, trainer, pl_module):
        if not isinstance(trainer.train_dataloader, ReplayLoader):
            raise ValueError('Expected a single ReplayLoader')
        self.loader = trainer.train_dataloader
        # DataModules may construct their loader after checkpoint hooks run.
        # Apply the callback-owned cursor explicitly at the first safe loader seam.
        if self.expected_state is not None:
            self.loader.load_state_dict(self.expected_state)
            if self.loader.state_dict() != self.expected_state:
                raise ValueError('Replay loader did not restore the acknowledged data cursor')

    def on_train_batch_start(self, trainer, pl_module, batch, batch_idx):
        if self.trace_path:
            samples, targets = batch
            self.current_input = fingerprint((traceable_samples(samples), targets))

    def on_train_batch_end(self, trainer, pl_module, outputs, batch, batch_idx):
        tokens = self.loader.acknowledge()
        if self.trace_path:
            loss = outputs.get('loss') if isinstance(outputs, dict) else outputs
            row = {'epoch': trainer.current_epoch, 'batch_idx': batch_idx, 'global_step': trainer.global_step,
                   'tokens': tokens, 'input_sha256': self.current_input,
                   'loss': float(loss.detach().cpu()) if loss is not None else None}
            with self.trace_path.open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(row, allow_nan=False) + '\n')

    def on_save_checkpoint(self, trainer, pl_module, checkpoint):
        if self.loader is None:
            raise ValueError('Cannot save replay checkpoint before train start')
        checkpoint['trainctl_replay'] = {'loader': self.loader.state_dict()}

    def on_load_checkpoint(self, trainer, pl_module, checkpoint):
        state = replay_checkpoint_state(checkpoint)
        if not state or not state['safe_to_resume']:
            raise ValueError('Checkpoint cannot safely restore the data position')
        self.expected_state = state['loader']
