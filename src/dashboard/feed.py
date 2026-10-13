"""Bounded local cache over existing monitor/samples/alerts read APIs.

Independent of controller reconciliation and its SQLite database. Browser reads
never invoke SSH; one collector polls at most three bounded remote queries/tick.
"""
import copy
import json
import math
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from controller.results import evidence_status, metric_values, observation_fresh, task_result


def now():
    return datetime.now(timezone.utc).isoformat()


def age(value):
    try:
        return max(0, (datetime.now(timezone.utc) - datetime.fromisoformat(value.replace('Z', '+00:00'))).total_seconds())
    except (ValueError, TypeError, AttributeError):
        return None


def finite(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid4().hex + '.tmp')
    try:
        with temporary.open('w', encoding='utf-8') as output:
            json.dump(value, output, ensure_ascii=True, allow_nan=False)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def pause_projection(value):
    """Bounded facts only; no request body, checkpoint path or action authority."""
    if not isinstance(value, dict):
        return None
    states = {'unsupported', 'not_requested', 'requested', 'expired',
              'checkpoint_committed', 'checkpoint_unverified', 'paused', 'invalid'}
    state = value.get('state') if value.get('state') in states else 'invalid'
    quiescent, safe = value.get('quiescent') is True, value.get('safe_to_resume') is True
    if state == 'paused' and not (quiescent and safe):
        state, safe = 'invalid', False
    if state != 'paused':
        safe = False
    request = value.get('request') if isinstance(value.get('request'), dict) else {}
    checkpoint = value.get('checkpoint') if isinstance(value.get('checkpoint'), dict) else {}
    verification = value.get('verification') if isinstance(value.get('verification'), dict) else {}
    text = lambda item: item[:128] if isinstance(item, str) else None
    return {'state':state, 'quiescent':quiescent, 'safe_to_resume':safe,
            'request_id':text(value.get('request_id')), 'requested_at':text(request.get('requested_at')),
            'expires_at':text(request.get('expires_at')), 'checkpoint_id':text(checkpoint.get('id')),
            'checkpoint_step':finite(checkpoint.get('global_step')),
            'bytes_verified_at':text(verification.get('bytes_verified_at')),
            'current_bytes_rehashed':verification.get('current_bytes_rehashed') is True}


def observation(payload, at):
    progress = payload.get('progress') or {}
    metrics = progress.get('metrics') or {}
    checkpoint = payload.get('checkpoint') or {}
    latest = checkpoint.get('latest') or {}
    execution = payload.get('execution') or {}
    evidence = payload.get('evidence_runtime') or {}
    return {'at': at, 'status': payload.get('lifecycle', 'UNKNOWN'),
            'observation_state': payload.get('observation_state'),
            'phase': progress.get('phase'), 'step': finite(progress.get('step')),
            'metrics': metric_values(metrics), 'metrics_at': progress.get('updated_at', at),
            'epoch': finite(metrics.get('epoch')), 'updates': finite(metrics.get('optimizer_updates_this_process')),
            'loss': finite(metrics.get('train/loss_step', metrics.get('train/loss'))),
            'val_loss': finite(metrics.get('val/loss')), 'map': finite(metrics.get('val/mAP_50_95')),
            'progress_state': payload.get('progress_state'),
            'runtime_configuration': payload.get('runtime_configuration'),
            'pause': pause_projection(payload.get('pause')),
            'memory_bytes': finite(execution.get('MemoryCurrent')),
            'execution_result': execution.get('Result'), 'exit_code': execution.get('ExecMainStatus'),
            'checkpoint': {'state': checkpoint.get('state'), 'updated_at': checkpoint.get('updated_at'),
                'id': latest.get('id'), 'created_at': latest.get('created_at'),
                'step': (latest.get('state') or {}).get('global_step'),
                'safe_to_resume': ((latest.get('state') or {}).get('replay') or {}).get('safe_to_resume')},
            'evidence': evidence_status(evidence),
            'logs': payload.get('logs') or {}}


def alert_projection(row):
    """Return a bounded browser-safe alert summary, including legacy evidence semantics."""
    payload = row.get('payload') if isinstance(row.get('payload'), dict) else {}
    issues = payload.get('issues') if isinstance(payload.get('issues'), list) else []
    warnings = (payload.get('observer') or {}).get('warnings') if isinstance(payload.get('observer'), dict) else []
    warnings = warnings if isinstance(warnings, list) else []
    first_detail = next((item.get('error') for item in [*issues, *warnings]
                         if isinstance(item, dict) and item.get('error')), '')
    result = {k: row.get(k) for k in ('key', 'run_id', 'kind', 'opened_at', 'resolved_at')}
    result['error'] = str(payload.get('error') or first_detail or '')[:1000]
    scopes = {
        'evidence_execution_integrity': 'execution',
        'evidence_deployment_changed': 'deployment',
        'evidence_observer_warning': 'observer',
        'evidence_monitor_failed': 'observer',
    }
    if row.get('kind') in scopes:
        result['evidence_scope'] = scopes[row['kind']]
    elif row.get('kind') == 'evidence_drift':
        binding = payload.get('evidence') if isinstance(payload.get('evidence'), dict) else {}
        race_errors = {'Evidence byte budget exceeded', 'Source changed while capturing evidence',
                       'Source changed while verifying evidence', 'File grew during verification'}
        if (binding.get('execution_mode') == 'snapshot' and issues and
                all(isinstance(item, dict) and item.get('error') in race_errors for item in issues)):
            result.update(evidence_scope='observer', compatibility='legacy_snapshot_verification_race')
        else:
            result['evidence_scope'] = 'execution' if binding.get('execution_mode') != 'snapshot' else 'unknown'
            result['compatibility'] = 'legacy_unclassified_drift'
    return result


class Feed:
    def __init__(self, remote, run_id, cache, *, name, epochs, interval=30, max_points=6000, task=None):
        if not 10 <= interval <= 3600 or max_points < 10:
            raise ValueError('Invalid dashboard limits')
        self.remote, self.run_id, self.cache = remote, run_id, Path(cache)
        self.interval, self.max_points = interval, max_points
        self.lock = threading.Lock()
        self.state = {'schema_version': 1, 'run_id': run_id, 'name': name, 'epochs': epochs,
                      'cursor': 0, 'points': [], 'downsampled': False, 'latest': None,
                      'monitor': None, 'alerts': [], 'errors': {}, 'last_success': {}, 'catching_up': True}
        if self.cache.exists():
            try:
                if self.cache.stat().st_size > 32 * 1024**2:
                    raise ValueError('Cache size exceeds limit')
                saved = json.loads(self.cache.read_text(encoding='utf-8'))
                if (saved['schema_version'] != 1 or saved['run_id'] != run_id or
                    not isinstance(saved['points'], list) or len(saved['points']) > max_points or
                    type(saved['cursor']) is not int or saved['cursor'] < 0):
                    raise ValueError('Invalid dashboard cache')
                self.state.update(saved, name=name, epochs=epochs)
            except (OSError, ValueError, KeyError, TypeError) as exc:
                self.state['errors']['cache'] = str(exc)
        self.current_ok = set()  # A prior process's successful query is not live connectivity.
        self.supplement = None
        if task is not None:
            self.state['task'] = copy.deepcopy(task)

    def poll(self):
        with self.lock:
            state = copy.deepcopy(self.state)
        current_ok = set()
        calls = [('monitor', self.remote.monitor),
                 ('samples', lambda: self.remote.samples(self.run_id, state['cursor'], 200)),
                 ('alerts', self.remote.alerts)]
        for kind, call in calls:
            try:
                value = call()
                if kind == 'samples':
                    self._samples(state, value)
                elif kind == 'monitor':
                    if not isinstance(value, dict):
                        raise ValueError('Invalid worker monitor response')
                    value = copy.deepcopy(value)
                    if isinstance(value.get('health'), dict):
                        # The browser receives the small negotiated projection from
                        # Catalog, not the full worker capability declaration.
                        value['health'].pop('ccs_interface', None)
                    state['monitor'] = value
                else:
                    if not isinstance(value, list):
                        raise ValueError('Invalid alert response')
                    state['alerts'] = [alert_projection(row) for row in value[:200]
                        if row.get('run_id') in (self.run_id, '_worker')]
                state['last_success'][kind] = now()
                state['errors'].pop(kind, None)
                current_ok.add(kind)
            except Exception as exc:
                state['errors'][kind] = str(exc)[:1000]
        state['polled_at'] = now()
        try:
            atomic_json(self.cache, state)
            state['errors'].pop('cache', None)
        except OSError as exc:
            state['errors']['cache'] = str(exc)
        with self.lock:
            self.state, self.current_ok = state, current_ok

    def _samples(self, state, rows):
        if not isinstance(rows, list) or len(rows) > 200:
            raise ValueError('Invalid sample page')
        # Validate a full page before advancing its durable cursor.
        parsed, cursor = [], state['cursor']
        for row in rows:
            if type(row.get('id')) is not int or row['id'] <= cursor or row.get('run_id') != self.run_id:
                raise ValueError('Invalid/nonmonotonic sample cursor')
            payload = row['payload']
            if not isinstance(payload, dict):
                raise ValueError('Invalid sample payload')
            parsed.append(observation(payload, row['created_at']))
            cursor = row['id']
        for sample in parsed:
            state['latest'] = sample
            if sample['step'] is not None or sample.get('metrics'):
                point = {k: sample[k] for k in ('at', 'step', 'epoch', 'phase', 'loss', 'val_loss', 'map', 'metrics')}
                if not state['points'] or point != state['points'][-1]:
                    state['points'].append(point)
        # Preserve full time span, progressively thin older points, keep recent half dense.
        while len(state['points']) > self.max_points:
            half = len(state['points']) // 2
            state['points'] = state['points'][:half:2] + state['points'][half:]
            state['downsampled'] = True
        state['cursor'], state['catching_up'] = cursor, len(rows) == 200

    def view(self):
        with self.lock:
            result = copy.deepcopy(self.state)
            current_ok = set(self.current_ok)
        latest = result.get('latest') or {}
        monitor = result.get('monitor') or {}
        old = age(latest.get('at'))
        worker_age = age(monitor.get('observed_at'))
        fresh = current_ok == {'samples', 'monitor', 'alerts'} and observation_fresh(result)
        result.update(connection='fresh' if fresh else 'stale', generated_at=now(),
                      sample_age_seconds=old, worker_age_seconds=worker_age, interval_seconds=self.interval)
        if self.supplement is not None:
            result = self.supplement(result)
        result['result'] = task_result(result)
        return result

    def run(self, stop):
        failures = 0
        while not stop.is_set():
            self.poll()
            with self.lock:
                failed = bool(self.state['errors'])
            failures = min(failures + 1, 3) if failed else 0
            stop.wait(min(120, self.interval * 2**failures))
