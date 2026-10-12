"""Read-only full-fit progress contract, with explicit resume/run bindings."""
from bisect import bisect_left, bisect_right
from itertools import accumulate
from pathlib import PurePosixPath
import math
import re


def bounds(binding):
    counts = binding.get('epoch_batch_counts')
    if (not isinstance(counts, list) or len(counts) != binding['epochs']
            or not counts or any(type(n) is not int or n <= 0 for n in counts)
            or sum(counts) != binding['max_steps']):
        raise ValueError('Invalid full-fit epoch schedule')
    return list(accumulate(counts))


def progress_point(item, binding, collected_at):
    ends = bounds(binding)
    step, loss, lr = item.get('step'), item.get('loss'), item.get('lr')
    updates = item.get('optimizer_updates')
    if (type(step) is not int or not 0 < step <= ends[-1]
            or type(loss) not in (int, float) or not math.isfinite(loss)
            or type(lr) not in (int, float) or not math.isfinite(lr) or lr < 0
            or type(updates) is not int or not 0 <= updates <= step):
        raise ValueError('Invalid full-fit progress values')
    epoch = bisect_left(ends, step)
    offset = step - (ends[epoch - 1] if epoch else 0)
    if (item.get('epoch') != epoch + 1 or item.get('epoch_batch') != offset
            or item.get('epoch_batches') != binding['epoch_batch_counts'][epoch]
            or item.get('total_steps') != ends[-1]):
        raise ValueError('Full-fit epoch/cursor mismatch')
    metrics = {'train/loss': loss, 'lr/min': lr, 'lr/max': lr,
               'progress/completed': step, 'progress/total': ends[-1],
               'train/optimizer_updates': updates, 'train/completed_epochs': bisect_right(ends, step),
               'train/epoch_batch': offset, 'train/epoch_batches': item['epoch_batches']}
    if 'loss_parts' in item:
        parts = item['loss_parts']
        if not isinstance(parts, dict):
            raise ValueError('Full-fit loss_parts must be an object')
        # Explicit scalar schema only; never infer metric names from arbitrary log keys.
        for name in ('vehicle', 'drone', 'cooperative', 'protection'):
            if name not in parts:
                continue
            value = parts[name]
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError('Invalid full-fit loss part: ' + name)
            metrics['train/loss_' + name] = value  # Keep a real 0.0 in the A control.
    for field in ['wall_seconds', 'gradient_norm', 'normal_matched_queries', 'dn_matched_queries',
                  'batch_size', 'epoch_unique_frames', 'total_train_frames',
                  'protection_eligible', 'protection_harmed']:
        value = item.get(field)
        if type(value) in (int, float) and math.isfinite(value) and value >= 0:
            metrics['train/' + ('elapsed_seconds' if field == 'wall_seconds' else field)] = value
    return dict(at=collected_at, step=step, loss=loss, epoch=epoch, updates=updates,
                phase='train', val_loss=None, map=None, metrics=metrics)


def report_snapshot(report, binding, collected_at):
    if (report.get('training_signature') != binding['config_sha256']
            or report.get('contract', {}).get('execution_key') != binding['execution_key']
            or report.get('total_batches') != binding['max_steps']
            or report.get('batches_per_epoch') != binding['epoch_batch_counts']):
        raise ValueError('Progress report run/signature/schedule mismatch')
    point = progress_point(report['last_progress'], binding, collected_at)
    if (report.get('global_batches') != point['step'] or report.get('optimizer_updates') != point['updates']
            or report.get('completed_epochs') != point['metrics']['train/completed_epochs']):
        raise ValueError('Progress report counters mismatch')
    result = dict(point=point, collected_at=collected_at)
    receipt = report.get('checkpoint')
    if receipt:
        target = PurePosixPath(receipt['path'])
        directory = (PurePosixPath(binding['snapshot']['project']) /
                     PurePosixPath(binding['snapshot']['path']).parent / 'checkpoints')
        step = receipt.get('global_batches')
        if (target.parent != directory or '..' in target.parts
                or not re.fullmatch(r'batch-\d{7}\.pth', target.name)
                or type(step) is not int or not 0 < step <= point['step']
                or target.name != 'batch-%07d.pth' % step
                or receipt.get('training_signature') != binding['config_sha256']
                or not re.fullmatch('[a-f0-9]{64}', str(receipt.get('sha256', '')))):
            raise ValueError('Checkpoint receipt binding mismatch')
        # Receipt is reported by the trainer; do not claim a fresh binary rehash/restore test.
        result['checkpoint'] = dict(state='reported_committed', step=step, path=str(target),
            id=target.name, sha256=receipt['sha256'], created_at=None, safe_to_resume=None,
            observed_at=collected_at, source='trainer atomic-checkpoint receipt')
    return result
