"""Bounded multi-run read model. One primary + one rotating history run per tick.

HTTP handlers only read memory. Catalog discovery is local/read-only. Worker
monitor and alert responses are shared within a tick rather than polled per run.
"""
import copy
import math
import threading
from pathlib import Path, PurePosixPath

from .feed import Feed, now, atomic_json
from controller.results import TERMINAL, age


class SharedRemote:
    def __init__(self, remote):
        self.remote, self.memo = remote, {}

    def query(self, key):
        if key not in self.memo:
            try:
                self.memo[key] = getattr(self.remote, key)()
            except Exception as exc:
                self.memo[key] = exc
        value = self.memo[key]
        if isinstance(value, Exception):
            raise value
        return copy.deepcopy(value)

    def monitor(self):
        return self.query('monitor')

    def alerts(self):
        return self.query('alerts')

    def samples(self, *args):
        return self.remote.samples(*args)


class Catalog:
    def __init__(self, remote, primary, directory, *, discover=lambda: [], workflows=None,
                 fleet=None, remote_resolver=None, interval=30, bridge_factory=None):
        self.remote = remote
        self.remote_resolver = remote_resolver
        self.shared_remotes = {id(remote): SharedRemote(remote)}
        self.shared = self.shared_remotes[id(remote)]
        self.primary = primary['run_id']
        self.directory, self.discover, self.workflows_provider, self.fleet_provider, self.interval = (
            Path(directory), discover, workflows, fleet, interval)
        self.feeds, self.descriptors, self.feed_remotes = {}, {}, {}
        self.bridge_factory, self.bridges = bridge_factory, {}
        self.lock = threading.RLock()
        self.turn, self.catalog_error = 0, None
        self.processes = {'state': 'not_collected', 'items': []}
        self.workflows = {'state': 'not_collected', 'studies': []}
        self.fleet = {'state': 'not_collected', 'selection_policy': 'upper_agent_explicit', 'workers': []}
        self.compatibility = {'state': 'not_collected', 'checked_at': None}
        self._add(primary)

    def _add(self, item):
        run_id = item['run_id']
        from controller.results import SAFE_ID
        if not isinstance(run_id, str) or not SAFE_ID.fullmatch(run_id):
            raise ValueError('Invalid catalog run id')
        if run_id in self.feeds:
            if self.descriptors[run_id].get('worker') != item.get('worker'):
                raise ValueError('Catalog run id belongs to multiple workers')
            return
        remote = self.remote_resolver(item) if self.remote_resolver is not None else self.remote
        if remote is None:
            raise ValueError('Catalog worker has no remote adapter')
        shared = self.shared_remotes.setdefault(id(remote), SharedRemote(remote))
        self.feeds[run_id] = Feed(shared, run_id, self.directory / (run_id + '.json'),
            name=item['name'], epochs=item.get('epochs'), task=item, interval=self.interval)
        self.descriptors[run_id] = copy.deepcopy(item)
        self.feed_remotes[run_id] = remote
        if self.bridge_factory is not None:
            bridge = self.bridge_factory(copy.deepcopy(item))
            self.bridges[run_id] = bridge
            self.feeds[run_id].supplement = bridge.supplement

    @staticmethod
    def _fleet_view(report):
        from controller.results import SAFE_ID
        if (not isinstance(report, dict) or report.get('schema_version') != 1 or
                report.get('selection_policy') != 'upper_agent_explicit' or
                not isinstance(report.get('workers'), list) or len(report['workers']) > 64):
            raise ValueError('Invalid fleet status')
        workers = []
        for raw in report['workers']:
            if (not isinstance(raw, dict) or not isinstance(raw.get('worker_id'), str) or
                    not SAFE_ID.fullmatch(raw['worker_id'])):
                raise ValueError('Invalid fleet worker')
            labels = raw.get('labels', [])
            gpu = raw.get('gpu', [])
            cpu = raw.get('cpu')
            memory = raw.get('memory')
            disk = raw.get('disk')
            compatibility = raw.get('compatibility')
            error = raw.get('error')
            connection = raw.get('connection')
            node = raw.get('node')
            disabled_reason = raw.get('disabled_reason')
            if (not isinstance(labels, list) or len(labels) > 64 or
                    any(not isinstance(label, str) or len(label) > 64 for label in labels) or
                    not isinstance(gpu, list) or len(gpu) > 16 or
                    cpu is not None and not isinstance(cpu, dict) or
                    memory is not None and not isinstance(memory, dict) or
                    disk is not None and not isinstance(disk, dict) or
                    compatibility is not None and not isinstance(compatibility, dict) or
                    connection is not None and not isinstance(connection, dict) or
                    node is not None and not isinstance(node, dict) or
                    disabled_reason is not None and (
                        not isinstance(disabled_reason, str) or len(disabled_reason) > 500) or
                    error is not None and not isinstance(error, dict)):
                raise ValueError('Invalid fleet worker details')
            if connection is not None and (
                    not isinstance(connection.get('host'), str) or len(connection['host']) > 255 or
                    not isinstance(connection.get('port'), int) or not 1 <= connection['port'] <= 65535 or
                    connection.get('user') is not None and (
                        not isinstance(connection.get('user'), str) or len(connection['user']) > 64)):
                raise ValueError('Invalid fleet connection details')
            if node is not None and any(
                    value is not None and (not isinstance(value, str) or len(value) > limit)
                    for key, limit in (('hostname', 128), ('platform', 256), ('python', 64))
                    for value in (node.get(key),)):
                raise ValueError('Invalid fleet node details')
            if cpu is not None:
                if (cpu.get('model') is not None and (
                        not isinstance(cpu['model'], str) or len(cpu['model']) > 256)):
                    raise ValueError('Invalid fleet CPU model')
                if any(value is not None and (
                        type(value) not in (int, float) or not math.isfinite(value))
                       for value in (cpu.get(key) for key in (
                           'logical_cores', 'utilization_percent', 'load_1m', 'load_5m', 'load_15m'))):
                    raise ValueError('Invalid fleet CPU metrics')
            if memory is not None and any(value is not None and (
                    type(value) not in (int, float) or not math.isfinite(value))
                    for value in (memory.get(key) for key in (
                        'total_bytes', 'available_bytes', 'used_bytes', 'utilization_percent',
                        'swap_total_bytes', 'swap_used_bytes'))):
                raise ValueError('Invalid fleet memory metrics')
            workers.append({
                'worker_id': raw['worker_id'],
                'labels': labels[:64],
                'disabled_reason': disabled_reason,
                'connection': ({key: connection.get(key) for key in ('host', 'port', 'user')}
                               if connection is not None else None),
                'node': ({key: node.get(key) for key in ('hostname', 'platform', 'python')}
                         if node is not None else None),
                'state': raw.get('state'),
                'compatibility': ({key: compatibility.get(key) for key in
                                   ('state', 'compatible', 'reason', 'protocol', 'software')}
                                  if compatibility is not None else None),
                'workload_count': raw.get('workload_count'),
                'managed_workload_count': raw.get('managed_workload_count'),
                'workload_state': raw.get('workload_state'),
                'cpu': ({key: cpu.get(key) for key in
                         ('model', 'logical_cores', 'utilization_percent',
                          'load_1m', 'load_5m', 'load_15m')}
                        if cpu is not None else None),
                'memory': ({key: memory.get(key) for key in
                            ('total_bytes', 'available_bytes', 'used_bytes',
                             'utilization_percent', 'swap_total_bytes', 'swap_used_bytes')}
                           if memory is not None else None),
                'gpu': [{key: item.get(key) for key in
                         ('index', 'name', 'utilization_percent', 'memory_used_mb',
                          'memory_total_mb', 'temperature_c')}
                        for item in gpu if isinstance(item, dict)],
                'disk': ({key: disk.get(key) for key in ('total_bytes', 'free_bytes', 'used_bytes')}
                         if disk is not None else None),
                'checked_at': raw.get('checked_at'),
                'error': ({'code': str(error.get('code') or '')[:128],
                           'message': str(error.get('message') or '')[:500],
                           'retryable': error.get('retryable') is True}
                          if error is not None else None),
            })
        return {'state': 'available', 'schema_version': 1,
                'selection_policy': 'upper_agent_explicit', 'workers': workers, 'error': None}

    def poll(self):
        checked_age = age(self.compatibility.get('checked_at'))
        if hasattr(self.remote, 'compatibility') and (checked_age is None or checked_age >= 300):
            checked_at = now()
            try:
                report = self.remote.compatibility({})
                if not isinstance(report, dict) or report.get('state') not in {
                        'compatible', 'legacy', 'incompatible'}:
                    raise ValueError('Invalid worker compatibility response')
                protocol = report.get('protocol')
                software = report.get('software')
                missing = report.get('missing_capabilities', [])
                if (protocol is not None and not isinstance(protocol, dict)) or (
                        software is not None and not isinstance(software, dict)) or not isinstance(missing, list):
                    raise ValueError('Invalid worker compatibility details')
                self.compatibility = {
                    'state': report['state'],
                    'compatible': report.get('compatible') is True,
                    'reason': str(report.get('reason') or '')[:500],
                    'protocol': ({'name': str(protocol.get('name') or '')[:64],
                                  'major': protocol.get('major'), 'minor': protocol.get('minor')}
                                 if isinstance(protocol, dict) else None),
                    'software': ({'name': str(software.get('name') or '')[:64],
                                  'version': str(software.get('version') or '')[:64]}
                                 if isinstance(software, dict) else None),
                    'missing_capabilities': [
                        {key: item.get(key) for key in ('name', 'required', 'available')}
                        for item in missing[:32] if isinstance(item, dict)
                    ],
                    'checked_at': checked_at,
                }
            except Exception as exc:
                self.compatibility = {'state': 'unavailable', 'compatible': False,
                                      'reason': str(exc)[:500], 'checked_at': checked_at}
        if self.fleet_provider is not None:
            try:
                projected = self._fleet_view(self.fleet_provider())
                with self.lock:
                    self.fleet = projected
            except Exception as exc:
                with self.lock:
                    self.fleet = {**self.fleet, 'state': 'unavailable', 'error': str(exc)[:1000]}
        if self.workflows_provider is not None:
            try:
                report = self.workflows_provider()
                if (not isinstance(report, dict) or report.get('schema_version') != 1 or
                        not isinstance(report.get('studies'), list) or len(report['studies']) > 20):
                    raise ValueError('Invalid workflow catalog')
                studies = []
                for item in report['studies']:
                    if (not isinstance(item, dict) or not isinstance(item.get('study'), dict) or
                            not isinstance(item.get('stages'), list) or len(item['stages']) > 64 or
                            not isinstance(item.get('recommendations', []), list) or
                            len(item.get('recommendations', [])) > 100):
                        raise ValueError('Invalid workflow study')
                    studies.append(copy.deepcopy(item))
                with self.lock:
                    self.workflows = {'state': 'available', 'studies': studies, 'error': None}
            except Exception as exc:
                with self.lock:
                    self.workflows = {**self.workflows, 'state': 'unavailable', 'error': str(exc)[:1000]}
        try:
            entries = list(self.discover())[:20]
            with self.lock:
                keep = {self.primary, *(x['run_id'] for x in entries)}
                for run_id in list(self.feeds):
                    if run_id not in keep:
                        del self.feeds[run_id]
                        del self.descriptors[run_id]
                        del self.feed_remotes[run_id]
                        self.bridges.pop(run_id, None)
                for item in entries:
                    self._add(item)
                self.catalog_error = None
        except Exception as exc:
            with self.lock:
                self.catalog_error = str(exc)[:1000]
        with self.lock:
            others = [key for key in self.feeds if key != self.primary]
            # Keep every active experiment in the bounded polling window, then
            # spend remaining capacity on rotating terminal history.
            selected = [self.feeds[self.primary]]
            active, history = [], []
            for key in others:
                feed_view = self.feeds[key].view()
                latest = feed_view.get('latest') or {}
                status = (latest.get('status') or
                          self.descriptors[key].get('recorded_status') or 'UNKNOWN')
                (history if status in TERMINAL else active).append(key)
            chosen = active[:7]
            remaining = 7 - len(chosen)
            if history and remaining:
                # Slow SSH history reads must not starve active-run freshness.
                width = min(1, remaining, len(history))
                chosen.extend(history[(self.turn + offset) % len(history)]
                              for offset in range(width))
                self.turn = (self.turn + width) % len(history)
            selected.extend(self.feeds[key] for key in chosen)
        for shared in self.shared_remotes.values():
            shared.memo.clear()
        if hasattr(self.remote, 'processes'):
            try:
                report = self.shared.query('processes')
                if (not isinstance(report, dict) or report.get('state') not in {'available', 'unsupported'}
                        or not isinstance(report.get('items'), list) or len(report['items']) > 128):
                    raise ValueError('Invalid workload inventory')
                items = []
                for item in report['items']:
                    if not isinstance(item, dict) or type(item.get('pid')) is not int:
                        raise ValueError('Invalid workload item')
                    items.append({key: item.get(key) for key in
                                  ('pid', 'launcher', 'script', 'project', 'state', 'memory_bytes', 'managed')})
                self.processes = {key: report.get(key) for key in ('state', 'observed_at', 'truncated')}
                self.processes['items'] = items
            except Exception as exc:
                self.processes = {'state': 'unavailable', 'items': [], 'error': str(exc)[:1000]}
        for feed in selected:
            from controller.configuration import read_configuration
            configuration = read_configuration(self.directory.parent / 'config-evidence', self.descriptors[feed.run_id])
            with feed.lock:
                feed.state['configuration'] = configuration
            feed.poll()
            # The bounded collector, never an HTTP handler, reads log pages.
            bridge = self.bridges.get(feed.run_id)
            if bridge is not None and not getattr(bridge, 'background_polling', False):
                bridge.poll_safe()
            # Inventory is collected once after a confirmed terminal observation.
            # Never load model files or traverse training/test datasets.
            view = feed.view()
            previous = view.get('artifacts', {})
            if (view['connection'] == 'fresh' and (view.get('latest') or {}).get('status') in TERMINAL
                    and previous.get('state') not in ('collected', 'truncated', 'requires_explicit_collection')
                    and (age(previous.get('attempted_at')) is None or age(previous['attempted_at']) >= 300)):
                try:
                    pattern = self.descriptors[feed.run_id].get('artifact_pattern')
                    parts = PurePosixPath(str(pattern)).parts
                    # Only the declared output directory's direct files, no recursive search.
                    if (not pattern or PurePosixPath(pattern).is_absolute() or '..' in parts or
                            any(any(c in part for c in '*?[') for part in parts[:-1]) or '**' in pattern):
                        with feed.lock:
                            feed.state['artifacts'] = {'state': 'requires_explicit_collection', 'items': []}
                            atomic_json(feed.cache, feed.state)
                        continue
                    rows = self.feed_remotes[feed.run_id].artifacts(feed.run_id)
                    if not isinstance(rows, list):
                        raise ValueError('Invalid artifact inventory')
                    inventory = {'state': 'truncated' if len(rows) > 200 else 'collected',
                        'collected_at': now(), 'items': [{k: row.get(k) for k in ('path', 'size', 'sha256')}
                                                       for row in rows[:200]]}
                except Exception as exc:
                    inventory = {'state': 'unavailable', 'items': [], 'attempted_at': now(), 'error': str(exc)[:1000]}
                with feed.lock:
                    feed.state['artifacts'] = inventory
                    try:
                        atomic_json(feed.cache, feed.state)
                    except OSError as exc:
                        feed.state['errors']['cache'] = str(exc)

    def view(self, run_id=None):
        with self.lock:
            run_id = run_id or self.primary
            if run_id not in self.feeds:
                raise KeyError(run_id)
            result = self.feeds[run_id].view()
            result['catalog'] = []
            for key, feed in self.feeds.items():
                feed_view = feed.view()
                latest = feed_view.get('latest') or {}
                projected = feed_view.get('result') or {}
                metrics = latest.get('metrics') if isinstance(latest.get('metrics'), dict) else {}
                live_lr = next((metrics.get(metric_key) for metric_key in
                                ('train/lr', 'learning_rate', 'lr', 'lr/max', 'optimizer/0/group/0/lr')
                                if type(metrics.get(metric_key)) in (int, float)), None)
                with feed.lock:
                    result['catalog'].append({**self.descriptors[key],
                        'last_status': latest.get('status') or self.descriptors[key].get('recorded_status') or 'UNKNOWN',
                        'connection': feed_view['connection'], 'sample_at': latest.get('at'),
                        'phase': latest.get('phase'), 'step': latest.get('step'),
                        'epoch': latest.get('epoch'), 'progress_state': latest.get('progress_state'),
                        'epochs': feed_view.get('epochs'),
                        'memory_bytes': latest.get('memory_bytes'), 'loss': latest.get('loss'),
                        'live_lr': live_lr,
                        'progress': projected.get('progress', {})})
            result['catalog_error'] = self.catalog_error
            result['workloads'] = copy.deepcopy(self.processes)
            result['workflows'] = copy.deepcopy(self.workflows)
            result['fleet'] = copy.deepcopy(self.fleet)
            result['worker_compatibility'] = copy.deepcopy(self.compatibility)
            return result

    def run(self, stop):
        while not stop.is_set():
            self.poll()
            with self.lock:
                failed = bool(self.feeds[self.primary].view()['errors'])
            stop.wait(min(120, self.interval * 2) if failed else self.interval)
