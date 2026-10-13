"""Versioned, read-only task results shared by workflow callers and the UI.

This module never submits a job or treats cached success as authorization.
Metrics are observed scalars, not automatically a finalized scientific report.
"""
import copy
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

SAFE_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}')
TERMINAL = {'SUCCEEDED', 'FAILED', 'CANCELLED'}
TASK_KINDS = {'train', 'test', 'analysis', 'build', 'simulate', 'benchmark', 'data'}


def validate_task(task):
    from .domain import DomainError
    if not isinstance(task, dict) or set(task) - {'kind', 'source_run_id', 'checkpoint', 'inputs', 'pause'}:
        raise DomainError('Invalid task metadata')
    if task.get('kind') not in TASK_KINDS:
        raise DomainError('task.kind must be one of: ' + ', '.join(sorted(TASK_KINDS)))
    if 'pause' in task:
        pause = task['pause']
        if (task['kind'] != 'train' or not isinstance(pause, dict) or set(pause) != {'schema_version', 'protocol', 'contract'} or
                type(pause.get('schema_version')) is not int or pause['schema_version'] != 1 or
                pause.get('protocol') != 'managed-checkpoint-v1' or
                not isinstance(pause.get('contract'), str) or not re.fullmatch('[a-f0-9]{64}', pause['contract'])):
            raise DomainError('task.pause requires train kind, schema_version=1, protocol=managed-checkpoint-v1 and contract SHA-256')
    source = task.get('source_run_id')
    if source is not None and (not isinstance(source, str) or not SAFE_ID.fullmatch(source)):
        raise DomainError('Invalid task.source_run_id')
    refs = task.get('inputs', [])
    if not isinstance(refs, list) or len(refs) > 64:
        raise DomainError('task.inputs must be a bounded list')
    if task.get('checkpoint') is not None:
        refs = [*refs, task['checkpoint']]
    for ref in refs:
        if (not isinstance(ref, dict) or set(ref) != {'path', 'sha256'} or
                not isinstance(ref['path'], str) or not 1 <= len(ref['path']) <= 4096 or
                not isinstance(ref['sha256'], str) or not re.fullmatch('[a-f0-9]{64}', ref['sha256'])):
            raise DomainError('Each task input/checkpoint requires path and sha256')


def descriptor(run_id, spec, worker=None, *, created_at=None, started_at=None,
               recorded_status=None, updated_at=None):
    from .configuration import spec_digest
    if not isinstance(run_id, str) or not SAFE_ID.fullmatch(run_id):
        raise ValueError('Invalid run id')
    task = spec.get('task')
    if task is not None:
        validate_task(task)
    argv = spec.get('command', {}).get('argv', [])
    epochs = None
    try:
        epochs = int(argv[argv.index('--epochs') + 1])
    except (ValueError, IndexError, TypeError):
        pass
    resources = spec.get('resources') or {}
    return {'run_id': run_id, 'name': spec.get('name', run_id), 'worker': worker,
            'spec_sha256': spec_digest(spec),
            'kind': (task or {}).get('kind', 'unspecified'), 'epochs': epochs,
            'created_at': created_at, 'started_at': started_at,
            'recorded_status': recorded_status, 'updated_at': updated_at,
            'gpu_index': resources.get('gpu_index'),
            'source_run_id': (task or {}).get('source_run_id'),
            'checkpoint': copy.deepcopy((task or {}).get('checkpoint')),
            'pause': copy.deepcopy((task or {}).get('pause')),
            'inputs': copy.deepcopy((task or {}).get('inputs', [])),
            'artifact_pattern': spec.get('outputs', {}).get('checkpoint_glob') or spec.get('outputs', {}).get('artifact_glob'),
            'evidence': copy.deepcopy(spec.get('evidence'))}


def metric_values(metrics):
    if not isinstance(metrics, dict):
        return {}
    result, size = {}, 0
    for key, value in list(metrics.items())[:128]:
        if not isinstance(key, str) or len(key) > 160 or type(value) not in (int, float):
            continue
        try:
            if not math.isfinite(value):
                continue
        except OverflowError:
            continue
        cost = len(json.dumps({key: value}, ensure_ascii=True))
        if size + cost > 3072:
            continue
        result[key], size = value, size + cost
    return result


def evidence_status(evidence):
    """Project an evidence report to bounded status fields without file paths."""
    if not isinstance(evidence, dict):
        return {}
    result = {k: evidence.get(k) for k in ('schema_version', 'state', 'updated_at', 'drift_detected',
        'issue_count', 'rotations_completed', 'bytes_read', 'last_fast_check_at')}
    sections = {
        'execution_integrity': ('state', 'issue_count'),
        'deployment': ('state', 'issue_count'),
        'observer': ('state', 'health', 'coverage', 'warning_count'),
    }
    for name, keys in sections.items():
        value = evidence.get(name)
        if isinstance(value, dict):
            result[name] = {key: value.get(key) for key in keys}
    return {key: value for key, value in result.items() if value is not None}


def age(value):
    try:
        return max(0, (datetime.now(timezone.utc) - datetime.fromisoformat(value.replace('Z', '+00:00'))).total_seconds())
    except (ValueError, TypeError, AttributeError):
        return None


def observation_fresh(state):
    latest, monitor = state.get('latest') or {}, state.get('monitor') or {}
    stamps = [monitor.get('observed_at'), *[state.get('last_success', {}).get(k) for k in ('samples', 'monitor', 'alerts')]]
    if latest.get('status') not in TERMINAL:
        stamps.append(latest.get('at'))
    return (not state.get('catching_up', True) and not state.get('errors')
            and latest.get('observation_state') == 'fresh'
            and monitor.get('observation_state') != 'stale'
            and all(age(t) is not None and age(t) <= 90 for t in stamps))


def task_result(state):
    """Pure projection; preserves missing values and observation provenance."""
    latest = state.get('latest') or {}
    task = copy.deepcopy(state.get('task') or {'run_id': state['run_id'], 'name': state.get('name'), 'kind': 'unspecified'})
    values = metric_values(latest.get('metrics', {}))
    fresh = state.get('connection') != 'stale' and observation_fresh(state)
    return {'schema_version': 1, 'task': task,
            'execution': {'status': latest.get('status', 'UNKNOWN'), 'exit_code': latest.get('exit_code'),
                          'result': latest.get('execution_result')},
            'observation': {'state': 'fresh' if fresh else 'stale', 'sample_at': latest.get('at'),
                            'polled_at': state.get('polled_at'), 'cursor': state.get('cursor', 0),
                            'source': 'worker_samples_via_local_cache'},
            'progress': {'phase': latest.get('phase'), 'step': latest.get('step'),
                         'completed': values.get('progress/completed'), 'total': values.get('progress/total'),
                         'epoch': latest.get('epoch')},
            'metrics': {key: {'value': value, 'sample_at': latest.get('metrics_at', latest.get('at')),
                              'phase': latest.get('phase')} for key, value in values.items()},
            'artifacts': copy.deepcopy(state.get('artifacts') or {'state': 'not_collected', 'items': []}),
            'configuration': copy.deepcopy(state.get('configuration') or {'state': 'not_recorded', 'parameters': [], 'sources': []}),
            'runtime_configuration': copy.deepcopy(latest.get('runtime_configuration')),
            'checkpoint': copy.deepcopy(latest.get('checkpoint')),
            'pause': copy.deepcopy(latest.get('pause')),
            'errors': copy.deepcopy(state.get('errors', {})),
            'logs': copy.deepcopy(latest.get('logs', {})),
            'failure': ({'code': 'RUN_FAILED', 'message': 'Worker reported FAILED; inspect stderr/events for cause'}
                        if latest.get('status') == 'FAILED' else None),
            'scientific_conclusion': None}


def cached_result(directory, run_id):
    if not SAFE_ID.fullmatch(run_id):
        raise ValueError('Invalid run id')
    path = Path(directory) / (run_id + '.json')
    if path.stat().st_size > 32 * 1024**2:
        raise ValueError('Cache exceeds size limit')
    state = json.loads(path.read_text(encoding='utf-8'))
    if state.get('schema_version') != 1 or state.get('run_id') != run_id:
        raise ValueError('Cache identity/version mismatch')
    return task_result(state)
