"""Versioned deterministic data recipe with acknowledged (not prefetched) cursors.

CPU transforms using Python/NumPy/PyTorch RNG and reseedable augmentation objects
are isolated per occurrence. This
does not make arbitrary stateful datasets or CUDA augmentation reproducible.
"""
from __future__ import annotations

import hashlib
import random
from collections import deque
from contextlib import contextmanager

import numpy as np
import torch
from torch.utils.data import DataLoader, Dataset, Sampler, RandomSampler


def seed_for(seed, epoch, position, kind):
    raw = f'replay-v1:{seed}:{epoch}:{position}:{kind}'.encode()
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], 'big') % (2**63 - 1)


@contextmanager
def isolated_cpu_rng(seed):
    python_state, numpy_state, torch_state = random.getstate(), np.random.get_state(), torch.get_rng_state()
    try:
        random.seed(seed)
        np.random.seed(seed % 2**32)
        # Do not touch CUDA generators or initialize CUDA in DataLoader workers.
        torch.random.default_generator.manual_seed(seed)
        yield
    finally:
        random.setstate(python_state)
        np.random.set_state(numpy_state)
        torch.set_rng_state(torch_state)


def reseed_private_transforms(root, seed):
    """Follow the known RFDETR transform containers, not arbitrary object state.

    Albumentations Compose owns independent generators and propagates its public
    set_random_seed call to children. Global np.random.seed does not control it.
    """
    seen = set()
    def visit(value, path):
        if value is None or id(value) in seen:
            return
        seen.add(id(value))
        setter = getattr(value, 'set_random_seed', None)
        if callable(setter):
            setter(seed_for(seed, 0, 0, path) % 2**32)
            return
        if isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                visit(item, path + '/' + str(index))
            return
        for name in ('_dataset', 'dataset', '_transforms', 'transforms', 'master_transform', 'auxiliary_transform', 'transform'):
            child = getattr(value, name, None)
            if child is not None:
                visit(child, path + '/' + name)
    visit(root, 'root')


class ReplayDataset(Dataset):
    def __init__(self, dataset, seed):
        self.dataset, self.seed = dataset, seed

    def __len__(self):
        return len(self.dataset)

    def __getitem__(self, key):
        epoch, position, index = key
        seed = seed_for(self.seed, epoch, position, 'sample')
        with isolated_cpu_rng(seed):
            reseed_private_transforms(self.dataset, seed)
            value = self.dataset[index]
        return value, key


class ReplayCollate:
    def __init__(self, collate, seed):
        self.collate, self.seed = collate, seed

    def __call__(self, rows):
        values, tokens = zip(*rows)
        with isolated_cpu_rng(seed_for(self.seed, tokens[0][0], tokens[0][1], 'collate')):
            value = self.collate(list(values))
        return value, list(tokens)


class ReplaySampler(Sampler):
    def __init__(self, size, count, seed, replacement):
        self.size, self.count, self.seed, self.replacement = size, count, seed, replacement
        self.epoch, self.committed = 0, 0

    def __len__(self):
        # Lightning needs full epoch length even after mid-epoch restoration.
        return self.count

    def set_epoch(self, epoch):
        if epoch != self.epoch:
            self.epoch, self.committed = int(epoch), 0

    def __iter__(self):
        epoch = self.epoch
        generator = torch.Generator().manual_seed(seed_for(self.seed, epoch, 0, 'order'))
        indices = (torch.randint(self.size, (self.count,), generator=generator) if self.replacement
                   else torch.randperm(self.size, generator=generator)[:self.count]).tolist()
        for position in range(self.committed, self.count):
            yield epoch, position, indices[position]


class ReplayLoader(DataLoader):
    """Map-style ordered loader; single device, explicit completed-batch ack."""
    def __init__(self, original, *, seed, identity, max_batches=None):
        if not isinstance(original.sampler, RandomSampler) or original.batch_size is None:
            raise ValueError('ReplayLoader requires a batched RandomSampler loader')
        if getattr(original, 'in_order', True) is not True:
            raise ValueError('Out-of-order loaders are not supported')
        count = len(original.sampler)
        if max_batches is not None:
            if type(max_batches) is not int or max_batches < 1:
                raise ValueError('max_batches must be a positive integer')
            count = min(count, max_batches * original.batch_size)
        if original.drop_last:
            count -= count % original.batch_size
        sampler = ReplaySampler(len(original.dataset), count, seed, original.sampler.replacement)
        self.replay_identity = identity
        self.replay_seed = seed
        self.pending = deque()
        options = dict(batch_size=original.batch_size, sampler=sampler, drop_last=original.drop_last,
            collate_fn=ReplayCollate(original.collate_fn, seed), num_workers=original.num_workers,
            pin_memory=original.pin_memory, persistent_workers=False,
            generator=torch.Generator().manual_seed(seed_for(seed, 0, 0, 'worker')))
        if original.num_workers:
            options['prefetch_factor'] = original.prefetch_factor or 2
            options['multiprocessing_context'] = original.multiprocessing_context
        super().__init__(ReplayDataset(original.dataset, seed), **options)

    def __iter__(self):
        self.pending.clear()
        # Lazy initialization ensures Lightning's sampler.set_epoch happens first.
        for value, tokens in super().__iter__():
            self.pending.append(tokens)
            yield value

    def acknowledge(self):
        if not self.pending:
            raise RuntimeError('No yielded batch to acknowledge')
        tokens = self.pending.popleft()
        for epoch, position, _ in tokens:
            if epoch != self.sampler.epoch or position != self.sampler.committed:
                raise RuntimeError('Data acknowledgement is out of order')
            self.sampler.committed += 1
        return tokens

    def state_dict(self):
        return {'schema_version': 1, 'recipe': 'replay-v1', 'identity': self.replay_identity,
            'seed': self.replay_seed, 'epoch': self.sampler.epoch, 'committed': self.sampler.committed,
            'count': self.sampler.count, 'size': self.sampler.size, 'batch_size': self.batch_size,
            'replacement': self.sampler.replacement, 'drop_last': self.drop_last}

    def load_state_dict(self, state):
        expected = self.state_dict()
        for key in expected.keys() - {'epoch', 'committed'}:
            if state.get(key) != expected[key]:
                raise ValueError('Replay data contract mismatch: ' + key)
        cursor, epoch = state.get('committed'), state.get('epoch')
        if type(epoch) is not int or epoch < 0 or type(cursor) is not int or not 0 <= cursor <= self.sampler.count:
            raise ValueError('Invalid replay cursor')
        if cursor != self.sampler.count and cursor % self.batch_size:
            raise ValueError('Replay cursor is not a complete batch')
        self.sampler.epoch, self.sampler.committed = epoch, cursor
        self.pending.clear()


def replay_checkpoint_state(checkpoint):
    """Describe safe data continuation without rejecting an archival final save."""
    value = checkpoint.get('trainctl_replay')
    if value is None:
        return None
    state = value['loader']
    epoch = int(checkpoint['epoch'])
    # max_steps may end a partial epoch and Lightning increments current_epoch.
    # Such final files are archived, but select the last mid-epoch point instead.
    safe = epoch == state['epoch'] or (epoch == state['epoch'] + 1 and state['committed'] == state['count'])
    return {'recipe': 'replay-v1', 'loader': state, 'safe_to_resume': safe,
            'reason': None if safe else 'Trainer ended a partial epoch; use the preceding periodic point'}
