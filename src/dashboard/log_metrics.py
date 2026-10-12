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
        if item.get('event') != 'training_progress' or not state.get('verified'):
            continue
        if binding and binding.get('format') == 'fullfit_v1':
            from .fullfit_metrics import progress_point
            point = progress_point(item, binding, collected_at)
            if not state['points'] or point['step'] > state['points'][-1]['step']:
                state['points'].append(point)
            continue
        step, loss, lr = item.get('step'), item.get('loss'), item.get('lr')
        if (type(step) is not int or not 0 < step <= max_steps
                or type(loss) not in (int, float) or not math.isfinite(loss)
                or not isinstance(lr, list) or not 1 <= len(lr) <= 1024
                or any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in lr)):
            raise ValueError('Invalid training progress event')
        if state['points'] and step <= state['points'][-1]['step']:
            continue
        metrics = {'train/loss': loss, 'lr/min': min(lr), 'lr/max': max(lr),
                   'progress/completed': step, 'progress/total': max_steps}
        elapsed = item.get('wall_seconds')
        if type(elapsed) in (int, float) and math.isfinite(elapsed) and elapsed >= 0:
            metrics['train/elapsed_seconds'] = elapsed
        state['points'].append(dict(at=collected_at,step=step,loss=loss,phase='train',
                                   epoch=None,val_loss=None,map=None,metrics=metrics))
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
        with self.lock:
            state = copy.deepcopy(self.state)
        if state.get('config_sha256') != binding['config_sha256']:
            state = {'config_sha256':binding['config_sha256']}
        errors = []
        # Only two bounded read-only requests; current progress does not wait for log history.
        pool = ThreadPoolExecutor(max_workers=2)
        log_future = pool.submit(self.query, 'run.logs', dict(run_id=self.task['run_id'],
            worker_id=self.task['worker'], stream='stdout', after_offset=state.get('offset', 0)))
        if binding.get('format') == 'fullfit_v1' and binding.get('snapshot'):
            try:
                from .fullfit_metrics import report_snapshot
                target = binding['snapshot']
                result = self.query('workspace.execute', dict(worker_id=self.task['worker'], request=dict(
                    action='inspect', mode='read', project=target['project'], path=target['path'])))
                content = result['content'].encode()
                if (len(content) > 256*1024 or hashlib.sha256(content).hexdigest() != result['sha256']
                        or result.get('project') != target['project'] or result.get('path') != target['path']):
                    raise ValueError('Progress report readback mismatch')
                snapshot = report_snapshot(json.loads(content), binding, now())
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
        config = read_configuration(self.config_dir,self.task)
        if config.get('state') != 'available':
            return result
        result['configuration'] = config
        with self.lock:
            s = copy.deepcopy(self.state)
        if (((not s.get('verified') or not s.get('points')) and not s.get('snapshot'))
                or s.get('config_sha256') != config.get('log_bridge',{}).get('config_sha256')):
            return result
        points = list(s.get('points', []))
        snapshot = s.get('snapshot')
        if snapshot and (not points or snapshot['point']['step'] > points[-1]['step']):
            points.append(snapshot['point'])
        p = points[-1]
        # Native samples win if they already report progress.
        latest = result.get('latest') or {}
        if latest.get('step') is not None:
            return result
        result['points'] = points
        latest.update(step=p['step'],loss=p['loss'],phase='train',metrics=p['metrics'],
                      metrics_at=p['at'],updates=p.get('updates', p['step']))
        binding = config['log_bridge']
        if binding.get('format') == 'fullfit_v1':
            latest['epoch'] = p['epoch']
            result['epochs'] = binding['epochs']
            if snapshot and snapshot.get('checkpoint') and not (latest.get('checkpoint') or {}).get('state'):
                latest['checkpoint'] = snapshot['checkpoint']
        elif binding.get('steps_per_epoch'):
            latest['epoch'] = p['step']//binding['steps_per_epoch']
            result['epochs'] = binding['epochs']
        # Do not set status, lifecycle freshness, training-event time or advancing.
        result['latest'] = latest
        result['downsampled'] = s.get('downsampled',False)
        result['log_metrics'] = dict(source='CCS stdout training_progress',
            config_sha256=s['config_sha256'],collected_at=p['at'],
            timestamp_basis='local collection time; x-axis is logged global batch step',
            epoch_basis='reported_fullfit' if binding.get('format') == 'fullfit_v1' else 'nominal_steps',
            error=s.get('error'),has_more=s.get('has_more'),
            fresh=not s.get('error') and (age(p['at']) or 0)<90)
        return result
