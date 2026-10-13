"""Conservative recovery facts; a trainer receipt is not a restore validation.

Only bounded JSON observations enter this module. It never opens checkpoint
files, imports a training framework, deserializes model state or starts a Run.
"""
import math
import hashlib
import json
from pathlib import PurePosixPath
import re


_DIGEST_KEYS = {'config_sha256', 'effective_config_sha256', 'training_signature', 'scientific_signature'}
_IDENTITY_KEYS = {'run_id', 'execution_key', 'experiment_id', 'source_run_id'}


def binding_digest(binding):
    """Cache scope includes paths/identity/contracts, not just model configuration."""
    return hashlib.sha256(json.dumps(binding, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=True, allow_nan=False).encode()).hexdigest()


def _step(value):
    # Worker projections sometimes serialize an integer step as a finite float.
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or int(value) != value:
        return None
    return int(value)


def ordinary_progress_point(item, max_steps, collected_at):
    step, loss, lr = item.get('step'), item.get('loss'), item.get('lr')
    if (type(step) is not int or not 0 < step <= max_steps
            or type(loss) not in (int, float) or not math.isfinite(loss)
            or not isinstance(lr, list) or not 1 <= len(lr) <= 1024
            or any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in lr)):
        raise ValueError('Invalid training progress event')
    metrics = {'train/loss': loss, 'lr/min': min(lr), 'lr/max': max(lr),
               'progress/completed': step, 'progress/total': max_steps}
    elapsed = item.get('wall_seconds')
    if type(elapsed) in (int, float) and math.isfinite(elapsed) and elapsed >= 0:
        metrics['train/elapsed_seconds'] = elapsed
    point = dict(at=collected_at, step=step, loss=loss, phase='train',
                 epoch=None, val_loss=None, map=None, metrics=metrics)
    updates = item.get('optimizer_updates')
    if type(updates) is int and 0 <= updates <= step:
        point['updates'] = updates
        metrics['train/optimizer_updates'] = updates
    return point


def _identity(report, binding):
    expected = binding.get('snapshot', {}).get('identity')
    if (not isinstance(expected, dict) or not expected or len(expected) > len(_IDENTITY_KEYS)
            or not set(expected).issubset(_IDENTITY_KEYS)
            or any(not isinstance(value, str) or not 0 < len(value) <= 256 for value in expected.values())
            or any(report.get(key) != value for key, value in expected.items())):
        raise ValueError('Progress report identity binding mismatch')


def reported_checkpoint(receipt, binding, collected_at, maximum_step, *, fullfit=False):
    """Validate declared JSON receipt identity, not checkpoint bytes or contents."""
    snapshot = binding.get('snapshot') or {}
    project = PurePosixPath(snapshot.get('project', ''))
    report_path = PurePosixPath(snapshot.get('path', ''))
    if (not project.is_absolute() or report_path.is_absolute() or report_path == PurePosixPath('.')
            or '..' in project.parts or '..' in report_path.parts):
        raise ValueError('Unsafe checkpoint report binding')
    directory = project / report_path.parent / 'checkpoints'
    target = PurePosixPath(receipt.get('path', ''))
    step_key = 'global_batches' if fullfit else binding.get('checkpoint_step_key')
    signature = (dict(key='training_signature', sha256=binding['config_sha256']) if fullfit
                 else binding.get('checkpoint_signature'))
    if (step_key not in ('global_batches', 'optimizer_updates') or not isinstance(signature, dict)
            or signature.get('key') not in _DIGEST_KEYS
            or not re.fullmatch('[a-f0-9]{64}', str(signature.get('sha256', '')))):
        raise ValueError('Invalid checkpoint receipt contract')
    step = receipt.get(step_key)
    if (not target.is_absolute() or target.parent != directory or '..' in target.parts
            or type(step) is not int or not (0 < step if fullfit else 0 <= step) or step > maximum_step
            or target.name != 'batch-%07d.pth' % step
            or receipt.get(signature['key']) != signature['sha256']
            or not re.fullmatch('[a-f0-9]{64}', str(receipt.get('sha256', '')))
            or (not fullfit and receipt.get('committed_optimizer_boundary') is not True)
            or receipt.get('committed_optimizer_boundary') is False):
        raise ValueError('Checkpoint receipt binding mismatch')
    result = dict(state='reported_committed', step=step, step_unit=step_key, path=str(target),
                  id=target.name, sha256=receipt['sha256'], created_at=None, safe_to_resume=None,
                  observed_at=collected_at, source='trainer atomic-checkpoint receipt')
    if type(receipt.get('bytes')) is int and receipt['bytes'] > 0:
        result['bytes'] = receipt['bytes']
    return result


def training_snapshot(report, binding, collected_at):
    """Adapter for declared ordinary training_progress reports with list LR."""
    key = binding.get('report_digest_key')
    if key not in _DIGEST_KEYS or report.get(key) != binding['config_sha256']:
        raise ValueError('Progress report configuration digest mismatch')
    _identity(report, binding)
    signature = binding.get('checkpoint_signature') or {}
    if (signature.get('key') not in _DIGEST_KEYS
            or report.get(signature['key']) != signature.get('sha256')):
        raise ValueError('Progress report checkpoint signature mismatch')
    if report.get('total_steps') != binding['max_steps']:
        raise ValueError('Progress report schedule mismatch')
    point = ordinary_progress_point(report['last_progress'], binding['max_steps'], collected_at)
    if report.get('global_batches') != point['step']:
        raise ValueError('Progress report counters mismatch')
    step_key = binding.get('checkpoint_step_key')
    if step_key == 'optimizer_updates':
        # Do not subtract an optimizer checkpoint from a microbatch counter.
        if report.get('optimizer_updates') != point['step'] or point.get('updates') != point['step']:
            raise ValueError('Progress/checkpoint step units differ')
    elif step_key != 'global_batches':
        raise ValueError('Invalid checkpoint step unit')
    result = dict(point=point, collected_at=collected_at, binding_sha256=binding_digest(binding))
    receipt = report.get('checkpoint')
    if receipt:
        result['checkpoint'] = reported_checkpoint(receipt, binding, collected_at, point['step'])
    return result


def checkpoint_log_event(item, binding, expected_config, point, collected_at):
    """Optional future adapter event; bare checkpoint filenames are ignored."""
    if not binding or binding.get('format') != 'training_progress_v1' or not binding.get('snapshot'):
        return None
    if item.get('config_sha256') != expected_config:
        raise ValueError('Checkpoint log configuration digest mismatch')
    _identity(item, binding)
    if not point or (binding.get('checkpoint_step_key') == 'optimizer_updates'
                     and point.get('updates') != point['step']):
        raise ValueError('Checkpoint log has no same-unit training progress')
    return reported_checkpoint(item, binding, collected_at, point['step'])


def recovery_projection(latest, *, logged_step=None, reported=None, reported_progress_step=None,
                        progress_source=None):
    """Keep progress, commit evidence and replay capability distinct and sourced."""
    native = latest.get('checkpoint') or {}
    native_step = _step(latest.get('step'))
    logged_step = _step(logged_step)
    native_checkpoint_step = _step(native.get('step'))
    native_selected = (native.get('safe_to_resume') is False or native.get('state') == 'failed'
                       or (native.get('state') == 'committed' and native_checkpoint_step is not None))
    checkpoint = native if native_selected else (reported or {})
    checkpoint_step = _step(checkpoint.get('step'))
    progress_step = native_step if native_selected else _step(reported_progress_step)
    if progress_step is None and not reported:
        progress_step = logged_step if logged_step is not None else native_step
    safe = checkpoint.get('safe_to_resume')
    if safe is False or checkpoint.get('state') == 'failed':
        capability, reason = 'not_resumable', 'Checkpoint monitor reports recovery unavailable'
    elif native_selected and safe is True and checkpoint.get('state') == 'committed':
        capability, reason = 'verified', 'Worker reports a committed replay-safe checkpoint; not a restore test'
    elif not native_selected and checkpoint.get('state') == 'reported_committed':
        capability, reason = 'reported_only', 'Trainer reports commit; checkpoint bytes and restoration are not verified here'
        safe = None
    else:
        capability, reason, safe = 'unknown', 'No verified replay capability was reported', None
    replay, basis = None, None
    if checkpoint_step is not None and progress_step is not None and checkpoint.get('state') in ('committed', 'reported_committed'):
        if checkpoint_step > progress_step:
            reason += '; checkpoint is ahead of the observed progress, so replay is unknown'
        else:
            replay = progress_step - checkpoint_step
            basis = ('native_step_minus_native_checkpoint' if native_selected
                     else 'reported_progress_minus_reported_checkpoint')
    return dict(schema_version=1, logged_step=logged_step, progress_step=progress_step,
                progress_source='CCS native progress' if native_selected else progress_source,
                checkpoint_step=checkpoint_step, checkpoint_state=checkpoint.get('state'),
                checkpoint_source=checkpoint.get('source') or ('CCS native checkpoint monitor' if native_selected else None),
                resume_capability=capability, safe_to_resume=safe, replay_steps=replay,
                replay_basis=basis, replay_steps_scope='observed_progress_only', reason=reason)
