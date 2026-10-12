"""Bounded, framework-neutral projection of effective Trainer state.

Callers pass a constructed Trainer. The projection excludes tensors, model
weights and parameter names, and writing it must never be required for training.
"""
import json
import math
import os
from pathlib import Path
from uuid import uuid4


def _name(value):
    result = type(value).__module__ + '.' + type(value).__qualname__
    return result[:256]


def _scalar(value):
    if type(value) is bool or isinstance(value, str) and len(value) <= 256:
        return value
    if type(value) in (int, float):
        try:
            return value if math.isfinite(value) else None
        except OverflowError:
            return None
    if isinstance(value, (list, tuple)) and len(value) <= 8:
        items = [_scalar(item) for item in value]
        return items if all(item is not None for item in items) else None
    return None


def project(trainer, at):
    optimizers, total_groups, truncated = [], 0, False
    for optimizer_index, optimizer in enumerate(list(getattr(trainer, 'optimizers', []))[:8]):
        groups = []
        for group_index, group in enumerate(list(getattr(optimizer, 'param_groups', []))):
            if total_groups >= 64:
                truncated = True
                break
            values = {}
            for key in ('lr', 'initial_lr', 'weight_decay', 'betas', 'eps', 'momentum',
                        'dampening', 'nesterov', 'maximize', 'capturable', 'foreach', 'fused'):
                if key in group:
                    value = _scalar(group[key])
                    if value is not None:
                        values[key] = value
            if isinstance(group.get('name'), str) and len(group['name']) <= 128:
                values['name'] = group['name']
            groups.append({'index': group_index, **values})
            total_groups += 1
        optimizers.append({'index': optimizer_index, 'type': _name(optimizer), 'groups': groups})
    schedulers = []
    for index, config in enumerate(list(getattr(trainer, 'lr_scheduler_configs', []))[:16]):
        scheduler = getattr(config, 'scheduler', config)
        schedulers.append({'index': index, 'type': _name(scheduler),
                           'interval': str(getattr(config, 'interval', 'unknown'))[:32],
                           'frequency': _scalar(getattr(config, 'frequency', None))})
    datamodule = getattr(trainer, 'datamodule', None)
    train_config = getattr(datamodule, 'train_config', None)
    data = {}
    for key in ('batch_size', 'grad_accum_steps', 'num_workers', 'resolution'):
        value = _scalar(getattr(train_config, key, None))
        if value is not None:
            data[key] = value
    result = {'schema_version': 1, 'captured_at': at, 'source': 'constructed_trainer_state',
              'trainer': {'type': _name(trainer), 'precision': str(getattr(trainer, 'precision', 'unknown'))[:64],
                          'world_size': _scalar(getattr(trainer, 'world_size', None)),
                          'accumulate_grad_batches': _scalar(getattr(trainer, 'accumulate_grad_batches', None)),
                          'max_epochs': _scalar(getattr(trainer, 'max_epochs', None))},
              'model': {'type': _name(getattr(trainer, 'lightning_module', None))},
              'datamodule': {'type': _name(datamodule), **data},
              'optimizers': optimizers, 'schedulers': schedulers,
              'limits': {'optimizer_count': len(optimizers), 'parameter_group_count': total_groups,
                         'truncated': len(getattr(trainer, 'optimizers', [])) > 8 or truncated}}
    encoded = json.dumps(result, ensure_ascii=True, allow_nan=False)
    if len(encoded.encode()) > 64 * 1024:
        raise ValueError('Runtime configuration record too large')
    return result


def learning_rate_metrics(trainer):
    result, count = {}, 0
    for optimizer_index, optimizer in enumerate(list(getattr(trainer, 'optimizers', []))[:8]):
        for group_index, group in enumerate(list(getattr(optimizer, 'param_groups', []))[:64]):
            if count >= 64:
                return result
            value = _scalar(group.get('lr'))
            if type(value) in (int, float) and type(value) is not bool:
                result[f'optimizer/{optimizer_index}/group/{group_index}/lr'] = value
                count += 1
    return result


def write(directory, value):
    directory = Path(directory)
    temporary = directory / ('runtime-configuration.' + uuid4().hex + '.tmp')
    data = json.dumps(value, ensure_ascii=True, allow_nan=False)
    try:
        with temporary.open('w', encoding='utf-8') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, directory / 'runtime-configuration.json')
    finally:
        temporary.unlink(missing_ok=True)
