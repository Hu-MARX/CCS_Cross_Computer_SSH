"""Opt-in, read-only CCS JSON log bridge; no training-process injection.

Timestamps are local collection times, not reconstructed training timestamps.
Lifecycle and worker health remain owned by the standard monitor.
"""
import base64
import copy
import json
import math
import os
import subprocess
import sys
import threading
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from controller.configuration import read_configuration
from .feed import atomic_json, now, age
from .checkpoint_projection import (
    ordinary_progress_point, training_snapshot, checkpoint_log_event, recovery_projection, binding_digest,
)


def metric_freshness_seconds(binding):
    """Collection freshness accommodates declared log cadence, not liveness."""
    expected = binding.get('expected_metric_interval_seconds')
    if expected is None:
        return 90
    if type(expected) not in (int, float) or not math.isfinite(expected) or not 1 <= expected <= 86400:
        raise ValueError('Invalid expected metric interval; require finite 1..86400 seconds')
    # Two expected output periods plus this bridge's 30-second polling interval.
    return max(90, 2 * expected + 30)


def consume(state, page, run_id, expected_config, max_steps, collected_at, binding=None):
    state = copy.deepcopy(state)
    offset = state.get('offset', 0)
    if (page.get('run_id') != run_id or page.get('stream') != 'stdout'
            or page.get('offset') != offset):
        raise ValueError('Log page identity/cursor mismatch')
    raw = base64.b64decode(page['data_base64'], validate=True)
    if len(raw) > 262144 or page.get('next_offset') != offset + len(raw):
        raise ValueError('Log page byte extent mismatch')
    pending = base64.b64decode(state.get('pending', '')) + raw
    lines = pending.split(b'\n')
    if len(lines[-1]) > 262144:
        raise ValueError('Oversized incomplete log line')
    state.setdefault('points', [])
    for line in lines[:-1]:
        try:
            item = json.loads(line)
        except (ValueError, UnicodeDecodeError):
            continue  # Non-JSON training output is not a metric.
        if not isinstance(item, dict):
            continue
        if item.get('event') == 'training_launch':
            digest_key = (binding or {}).get('digest_key', 'config_sha256')
            if digest_key not in ('config_sha256', 'training_signature') or item.get(digest_key) != expected_config:
                raise ValueError('Training launch config digest mismatch')
            if binding and binding.get('format') == 'fullfit_v1':
                if item.get('total_batches') != max_steps or item.get('epochs') != binding['epochs']:
                    raise ValueError('Training launch schedule mismatch')
            state['verified'] = True
            state['launch'] = item
        if item.get('event') == 'checkpoint_committed' and state.get('verified'):
            point = state['points'][-1] if state['points'] else None
            checkpoint = checkpoint_log_event(item, binding, expected_config, point, collected_at)
            if checkpoint and checkpoint['step'] >= state.get('checkpoint', {}).get('step', 0):
                state['checkpoint'] = checkpoint
                state['checkpoint_binding_sha256'] = binding_digest(binding)
        if item.get('event') != 'training_progress' or not state.get('verified'):
            continue
        if binding and binding.get('format') == 'fullfit_v1':
            from .fullfit_metrics import progress_point
            point = progress_point(item, binding, collected_at)
            if not state['points'] or point['step'] > state['points'][-1]['step']:
                state['points'].append(point)
            continue
        point = ordinary_progress_point(item, max_steps, collected_at)
        step = point['step']
        if state['points'] and step <= state['points'][-1]['step']:
            continue
        state['points'].append(point)
    while len(state['points']) > 6000:
        mid = len(state['points'])//2
        state['points'] = state['points'][:mid:2]+state['points'][mid:]
        state['downsampled'] = True
    state.update(offset=offset+len(raw),pending=base64.b64encode(lines[-1]).decode(),
                 collected_at=collected_at,has_more=bool(page.get('has_more')))
    return state


class LogBridge:
    def __init__(self, root, task, fleet, known_hosts, identity_file):
        self.root, self.task = Path(root), task
        self.cache = self.root/'data/dashboard'/('log-'+task['run_id']+'.json')
        self.config_dir = self.root/'data/config-evidence'
        self.lock = threading.Lock()
        self.state = {}
        if self.cache.exists() and self.cache.stat().st_size < 16*1024**2:
            self.state = json.loads(self.cache.read_text(encoding='utf-8'))
        self.command = [sys.executable, '-m', 'controller.gateway_cli',
                        '--fleet',str(fleet),'--db',str(self.root/'data/orchestrator.db'),
                        '--audit-db',str(self.root/'data/ccs-audit.db'),
                        '--known-hosts',str(known_hosts),'--identity-file',str(identity_file),
                        'request','--file','-']

    def query(self, action, payload):
        request = dict(schema_version=1, request_id='dashboard-'+action+'-'+self.task['run_id'],
            actor={'id':'ccs-dashboard','kind':'agent'}, action=action, dry_run=False, payload=payload)
        env = dict(os.environ, PYTHONPATH=str(self.root/'src'))
        try:
            response = subprocess.run(self.command, input=json.dumps(request).encode(), capture_output=True,
                timeout=120, env=env, creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        except subprocess.TimeoutExpired:
            raise ValueError('CCS read-only query timed out after 120 seconds') from None
        envelope = json.loads(response.stdout)
        if response.returncode or not envelope.get('ok'):
            raise ValueError('CCS read-only query failed')
        return envelope['data']['result']

    def poll(self):
        config = read_configuration(self.config_dir, self.task)
        binding = config.get('log_bridge')
        if config.get('state') != 'available' or not binding:
            return
        if binding.get('worker_id') != self.task['worker']:
            raise ValueError('Log bridge worker binding mismatch')
        metric_freshness_seconds(binding)
        with self.lock:
            state = copy.deepcopy(self.state)
        fingerprint = binding_digest(binding)
        if (state.get('config_sha256') != binding['config_sha256']
                or state.get('binding_sha256') != fingerprint):
            state = {'config_sha256':binding['config_sha256'], 'binding_sha256':fingerprint}
        errors = []
        # Only two bounded read-only requests; current progress does not wait for log history.
        pool = ThreadPoolExecutor(max_workers=2)
        log_future = pool.submit(self.query, 'run.logs', dict(run_id=self.task['run_id'],
            worker_id=self.task['worker'], stream='stdout', after_offset=state.get('offset', 0)))
        if binding.get('format') in ('fullfit_v1', 'training_progress_v1') and binding.get('snapshot'):
            try:
                from .fullfit_metrics import report_snapshot
                target = binding['snapshot']
                result = self.query('workspace.execute', dict(worker_id=self.task['worker'], request=dict(
                    action='inspect', mode='read', project=target['project'], path=target['path'])))
                content = result['content'].encode()
                if (len(content) > 256*1024 or hashlib.sha256(content).hexdigest() != result['sha256']
                        or result.get('project') != target['project'] or result.get('path') != target['path']):
                    raise ValueError('Progress report readback mismatch')
                adapter = report_snapshot if binding.get('format') == 'fullfit_v1' else training_snapshot
                snapshot = adapter(json.loads(content), binding, now())
                if snapshot['point']['step'] < state.get('snapshot', {}).get('point', {}).get('step', 0):
                    raise ValueError('Progress snapshot moved backwards')
                state['snapshot'] = snapshot
                with self.lock:
                    self.state = copy.deepcopy(state)
            except Exception as exc:
                errors.append('snapshot: ' + str(exc)[:250])
        try:
            page = log_future.result()
            state = consume(state, page, self.task['run_id'], binding['config_sha256'],
                            binding['max_steps'], now(), binding=binding)
        except Exception as exc:
            errors.append('logs: ' + str(exc)[:250])
        pool.shutdown(wait=True)
        if errors: state['error'] = '; '.join(errors)
        else: state.pop('error', None)
        atomic_json(self.cache,state)
        with self.lock:
            self.state = state

    def run(self, stop):
        while not stop.is_set():
            started = time.monotonic()
            self.poll_safe()
            stop.wait(max(1, 30 - (time.monotonic() - started)))

    def poll_safe(self):
        try:
            self.poll()
        except Exception as exc:
            with self.lock:
                self.state['error'] = str(exc)[:300]

    def supplement(self, result):
        if result['run_id'] != self.task['run_id']:
            return result
        latest = result.get('latest') or {}
        result['recovery'] = recovery_projection(latest)
        config = read_configuration(self.config_dir,self.task)
        if config.get('state') != 'available':
            return result
        result['configuration'] = config
        binding = config.get('log_bridge') or {}
        if self.task.get('worker') and binding.get('worker_id') != self.task['worker']:
            return result
        with self.lock:
            s = copy.deepcopy(self.state)
        fingerprint = binding_digest(binding)
        if s.get('binding_sha256') is not None and s['binding_sha256'] != fingerprint:
            return result
        if (s.get('snapshot') or {}).get('binding_sha256') != fingerprint:
            s.pop('snapshot', None)
        if (((not s.get('verified') or not s.get('points')) and not s.get('snapshot'))
                or s.get('config_sha256') != config.get('log_bridge',{}).get('config_sha256')):
            return result
        points = list(s.get('points', [])) if s.get('verified') else []
        logged_step = points[-1]['step'] if points else None
        snapshot = s.get('snapshot')
        progress_source = 'CCS stdout training_progress'
        if snapshot and (not points or snapshot['point']['step'] > points[-1]['step']):
            points.append(snapshot['point'])
            progress_source = 'bound trainer progress report'
        p = points[-1]
        reported_candidates = [(snapshot or {}).get('checkpoint'),
            s.get('checkpoint') if s.get('checkpoint_binding_sha256') == fingerprint else None]
        reported = max((item for item in reported_candidates if item),
                       key=lambda item: item['step'], default=None)
        # Keep native metrics authoritative, but never discard independent
        # checkpoint facts merely because a native progress step is available.
        result['recovery'] = recovery_projection(latest, logged_step=logged_step,
            reported=reported, reported_progress_step=p['step'], progress_source=progress_source)
        checkpoint = latest.get('checkpoint') or {}
        if reported and checkpoint.get('state') != 'committed' and checkpoint.get('safe_to_resume') is not False and checkpoint.get('state') != 'failed':
            latest['checkpoint'] = reported
        native_progress = latest.get('step') is not None
        if not native_progress:
            result['points'] = points
            latest.update(step=p['step'],loss=p['loss'],phase='train',metrics=p['metrics'],
                          metrics_at=p['at'],updates=p.get('updates', p['step']))
        if binding.get('format') == 'fullfit_v1':
            if not native_progress:
                latest['epoch'] = p['epoch']
            result['epochs'] = binding['epochs']
        elif binding.get('steps_per_epoch'):
            if not native_progress:
                latest['epoch'] = p['step']//binding['steps_per_epoch']
            result['epochs'] = binding['epochs']
        # Do not set status, lifecycle freshness, training-event time or advancing.
        result['latest'] = latest
        if not native_progress:
            result['downsampled'] = s.get('downsampled',False)
        metric_error = s.get('error')
        try:
            freshness_seconds = metric_freshness_seconds(binding)
        except ValueError as exc:
            metric_error = '; '.join(value for value in (metric_error, str(exc)) if value)
            freshness_seconds = 90
        collected_age = age(p['at'])
        result['log_metrics'] = dict(source=progress_source,
            config_sha256=s['config_sha256'],collected_at=p['at'],
            timestamp_basis='local collection time; x-axis is logged global batch step',
            epoch_basis='reported_fullfit' if binding.get('format') == 'fullfit_v1' else 'nominal_steps',
            error=metric_error,has_more=s.get('has_more'),
            freshness_threshold_seconds=freshness_seconds,
            expected_metric_interval_seconds=binding.get('expected_metric_interval_seconds'),
            freshness_basis='local collection age; not evidence of training advancement',
            fresh=not metric_error and collected_age is not None and collected_age < freshness_seconds)
        return result
