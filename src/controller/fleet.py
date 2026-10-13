"""Multi-worker directory that reports facts and obeys explicit Agent selection."""

from __future__ import annotations

import re
import math
import threading
import time
from concurrent.futures import Future, wait, FIRST_COMPLETED
from contextlib import nullcontext
from dataclasses import dataclass, replace
from typing import Any

from .capabilities import negotiate_worker
from .orchestrator import Orchestrator


_SAFE_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$')
_SAFE_LABEL = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$')


def _bounded_text(value, limit):
    if not isinstance(value, str):
        return None
    value = ''.join(character for character in value if character >= ' ' and character != '\x7f')
    return value[:limit] or None


def _error_message(exc):
    message = str(getattr(exc, 'message', str(exc)))
    return message[:2048] + ' [truncated]' if len(message) > 2048 else message


def _connection_facts(remote):
    host = _bounded_text(getattr(remote, 'host', None), 255)
    user = _bounded_text(getattr(remote, 'user', None), 64)
    port = getattr(remote, 'port', None)
    if port is None:
        port = 22
    if host is None or not isinstance(port, int) or not 1 <= port <= 65535:
        return None
    return {'host': host, 'port': port, 'user': user}


def _node_facts(health):
    facts = {
        'hostname': _bounded_text(health.get('hostname'), 128),
        'platform': _bounded_text(health.get('platform'), 256),
        'python': _bounded_text(health.get('python'), 64),
    }
    return facts if any(value is not None for value in facts.values()) else None


class WorkerDirectoryError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.code, self.message, self.retryable = code, message, retryable


@dataclass(frozen=True)
class WorkerBinding:
    worker_id: str
    remote: Any
    labels: frozenset[str] = frozenset()
    workspace_remote: Any | None = None
    enabled: bool = True
    disabled_reason: str | None = None

    def __post_init__(self):
        if not isinstance(self.worker_id, str) or not _SAFE_ID.fullmatch(self.worker_id):
            raise ValueError('worker_id contains unsupported characters')
        if (not isinstance(self.labels, frozenset) or len(self.labels) > 64 or
                any(not isinstance(label, str) or not _SAFE_LABEL.fullmatch(label) for label in self.labels)):
            raise ValueError('worker labels must be a bounded set of safe strings')
        if not isinstance(self.enabled, bool):
            raise ValueError('worker enabled must be boolean')
        if self.disabled_reason is not None and (
                not isinstance(self.disabled_reason, str) or not self.disabled_reason.strip() or
                len(self.disabled_reason) > 500):
            raise ValueError('worker disabled_reason must be a bounded non-empty string')
        if not self.enabled and self.disabled_reason is None:
            raise ValueError('disabled worker requires disabled_reason')


class WorkerDirectory:
    """Deep module for fleet observation and explicit worker resolution."""

    def __init__(self, bindings):
        items = list(bindings)
        if not items or len(items) > 64 or any(not isinstance(item, WorkerBinding) for item in items):
            raise ValueError('worker directory requires 1-64 WorkerBinding entries')
        if len({item.worker_id for item in items}) != len(items):
            raise ValueError('worker_id values must be unique')
        self._bindings = {item.worker_id: item for item in items}
        self._observation_lock = threading.Lock()
        self._status_lock = threading.Lock()
        self._observations = {}
        self._max_observers = 4

    @property
    def size(self) -> int:
        return len(self._bindings)

    @property
    def default_worker_id(self) -> str:
        return next(iter(self._bindings))

    @property
    def worker_ids(self) -> tuple[str, ...]:
        return tuple(self._bindings)

    def status(self, *, timeout_seconds=10, worker_timeout_seconds=5) -> dict[str, Any]:
        """Observe a bounded fleet without serial waits on unavailable workers.

        At most four observation threads exist per directory. A custom adapter
        that ignores cancellation retains its slot until it returns; subsequent
        polls report busy instead of creating more threads.
        """
        for value in (timeout_seconds, worker_timeout_seconds):
            if (type(value) not in {int, float} or not math.isfinite(value) or
                    not 0.01 <= value <= 60):
                raise ValueError('observation timeouts must be finite seconds in [0.01, 60]')
        if not self._status_lock.acquire(blocking=False):
            reports = {key: self._observation_error(binding, 'WORKER_OBSERVATION_BUSY',
                       'another fleet observation is in progress') for key, binding in self._bindings.items()}
        else:
            try:
                reports = self._bounded_status(timeout_seconds, worker_timeout_seconds)
            finally:
                self._status_lock.release()
        return {'schema_version': 1, 'selection_policy': 'upper_agent_explicit',
                'workers': [reports[key] for key in self._bindings],
                'observation_budget': {'max_concurrent': self._max_observers,
                                       'timeout_seconds': timeout_seconds,
                                       'worker_timeout_seconds': worker_timeout_seconds}}

    def _bounded_status(self, total_timeout, worker_timeout):
        deadline = time.monotonic() + total_timeout
        pending = list(self._bindings.values())
        reports, active = {}, {}
        while pending or active:
            with self._observation_lock:
                self._observations = {key: value for key, value in self._observations.items()
                                      if not value[0].done()}
                while pending:
                    binding = pending[0]
                    if binding.worker_id in self._observations:
                        pending.pop(0)
                        reports[binding.worker_id] = self._observation_error(
                            binding, 'WORKER_OBSERVATION_BUSY', 'previous observation is still stopping')
                        continue
                    if len(self._observations) >= self._max_observers:
                        break
                    pending.pop(0)
                    future, cancel = Future(), threading.Event()
                    started = time.monotonic()
                    self._observations[binding.worker_id] = (future, cancel)
                    active[future] = (binding, cancel, started + worker_timeout)
                    threading.Thread(target=self._observe_task,
                                     args=(binding, future, cancel, worker_timeout), daemon=True).start()
            now = time.monotonic()
            for future, (binding, cancel, worker_deadline) in list(active.items()):
                if future.done():
                    reports[binding.worker_id] = future.result()
                    del active[future]
                elif now >= min(deadline, worker_deadline):
                    cancel.set()
                    reports[binding.worker_id] = self._observation_error(
                        binding, 'WORKER_OBSERVATION_TIMEOUT', 'read-only worker observation exceeded its time budget')
                    del active[future]
            if pending and not active and now < deadline:
                with self._observation_lock:
                    occupied = sum(not item[0].done() for item in self._observations.values())
                if occupied < self._max_observers:
                    continue
            if now >= deadline or (pending and not active):
                for binding in pending:
                    reports[binding.worker_id] = self._observation_error(
                        binding, 'WORKER_OBSERVATION_BUSY', 'observation capacity or total time budget exhausted; retry a read later')
                break
            if active:
                soonest = min(deadline, *(value[2] for value in active.values()))
                wait(active, timeout=max(0, soonest - time.monotonic()), return_when=FIRST_COMPLETED)
        return reports

    def _observe_task(self, binding, future, cancel, timeout):
        try:
            budget = getattr(binding.remote, 'observation_budget', None)
            context = budget(timeout, cancel) if budget else nullcontext()
            with context:
                report = self._observe(binding, {})
        except Exception as exc:
            report = self._observation_error(binding, getattr(exc, 'code', type(exc).__name__),
                                             getattr(exc, 'message', str(exc))[:2048])
        future.set_result(report)

    @staticmethod
    def _observation_error(binding, code, message):
        return {'worker_id': binding.worker_id, 'labels': sorted(binding.labels),
                'connection': _connection_facts(binding.remote), 'node': None,
                'disabled_reason': binding.disabled_reason, 'state': 'unreachable',
                'compatibility': None, 'workload_count': None, 'managed_workload_count': None,
                'workload_state': 'unavailable', 'workload_error': None,
                'cpu': None, 'memory': None, 'gpu': [], 'disk': None, 'checked_at': None,
                'error': {'code': code, 'message': message, 'retryable': True}}

    def submission_plan(self, worker_id: str, required: dict[str, int]) -> dict[str, Any]:
        return self.operation_plan(worker_id, required, allow_legacy=False)

    def operation_plan(self, worker_id: str, required: dict[str, int], *, allow_legacy: bool,
                       workspace: bool = False) -> dict[str, Any]:
        binding = self._binding(worker_id)
        if workspace:
            # Negotiate the same identity/root/executable that workspace dispatch uses.
            binding = replace(binding, remote=self.remote_for(worker_id, workspace=True))
        report = self._observe(binding, required)
        if report['state'] == 'unreachable':
            error = report['error']
            raise WorkerDirectoryError(error['code'], error['message'], retryable=error['retryable'])
        if report['state'] == 'disabled':
            raise WorkerDirectoryError(
                'WORKER_DISABLED', report['disabled_reason'] or 'worker is disabled', retryable=False)
        accepted = {'compatible', 'legacy'} if allow_legacy else {'compatible'}
        if report['compatibility']['state'] not in accepted:
            state = report['compatibility']['state']
            code = 'WORKER_CAPABILITIES_UNDECLARED' if state == 'legacy' else 'WORKER_INCOMPATIBLE'
            raise WorkerDirectoryError(code, report['compatibility']['reason'])
        return report

    def remote_for(self, worker_id: str, *, workspace: bool = False):
        binding = self._binding(worker_id)
        if workspace and binding.workspace_remote is not None:
            return binding.workspace_remote
        return binding.remote

    def submit(self, worker_id, spec, idempotency_key, repository, *, request_id=None, actor=None, plan_sha256=None):
        binding = self._binding(worker_id)
        if not binding.enabled:
            raise WorkerDirectoryError('WORKER_DISABLED', binding.disabled_reason or 'worker is disabled')
        return Orchestrator(repository, binding.remote, worker=worker_id).submit(
            spec, idempotency_key=idempotency_key, request_id=request_id, actor=actor, plan_sha256=plan_sha256,
        )

    def _binding(self, worker_id):
        if not isinstance(worker_id, str) or not _SAFE_ID.fullmatch(worker_id):
            raise WorkerDirectoryError('INVALID_WORKER_SELECTION', 'worker_id is invalid')
        try:
            return self._bindings[worker_id]
        except KeyError as exc:
            raise WorkerDirectoryError('WORKER_NOT_FOUND', f'unknown worker_id: {worker_id}') from exc

    @staticmethod
    def _observe(binding, required):
        connection = _connection_facts(binding.remote)
        try:
            health = binding.remote.health()
            compatibility = negotiate_worker(health, required)
        except Exception as exc:
            return {
                'worker_id': binding.worker_id,
                'labels': sorted(binding.labels),
                'connection': connection,
                'node': None,
                'disabled_reason': binding.disabled_reason,
                'state': 'unreachable',
                'compatibility': None,
                'workload_count': None,
                'managed_workload_count': None,
                'workload_state': 'unavailable',
                'workload_error': None,
                'cpu': None,
                'memory': None,
                'gpu': [],
                'disk': None,
                'checked_at': None,
                'error': {
                    'code': getattr(exc, 'code', type(exc).__name__),
                    'message': _error_message(exc),
                    'retryable': bool(getattr(exc, 'retryable', True)),
                },
            }
        workload_error = None
        try:
            processes = binding.remote.processes()
            raw_items = processes.get('items') if isinstance(processes, dict) else None
            items = raw_items if isinstance(raw_items, list) else []
            workload_state = processes.get('state', 'available') if isinstance(processes, dict) else 'unavailable'
            workload_count = len(items)
            managed_count = sum(item.get('managed') is True for item in items if isinstance(item, dict))
        except Exception as exc:
            workload_state = 'unavailable'
            workload_count = managed_count = None
            workload_error = {
                'code': getattr(exc, 'code', type(exc).__name__),
                'message': _error_message(exc),
                'retryable': bool(getattr(exc, 'retryable', True)),
            }
        return {
            'worker_id': binding.worker_id,
            'labels': sorted(binding.labels),
            'connection': connection,
            'node': _node_facts(health),
            'disabled_reason': binding.disabled_reason,
            'state': ('disabled' if not binding.enabled else
                      'available' if compatibility['state'] in {'compatible', 'legacy'} else 'incompatible'),
            'compatibility': compatibility,
            'workload_count': workload_count,
            'managed_workload_count': managed_count,
            'workload_state': workload_state,
            'workload_error': workload_error,
            'cpu': health.get('cpu') if isinstance(health.get('cpu'), dict) else None,
            'memory': health.get('memory') if isinstance(health.get('memory'), dict) else None,
            'gpu': health.get('gpu') if isinstance(health.get('gpu'), list) else [],
            'disk': health.get('disk') if isinstance(health.get('disk'), dict) else None,
            'checked_at': health.get('checked_at'),
            'error': None,
        }


def directory_from_config(value, remote_factory, *, remote_root='/srv/ml-orchestrator',
                          user=None, port=None, known_hosts=None, identity_file=None,
                          workspace_user=None, workspace_root=None, workspace_executable=None):
    """Build the bounded worker Adapter directory shared by CLI and dashboard."""
    if (not isinstance(value, dict) or set(value) != {'schema_version', 'workers'} or
            value.get('schema_version') != 1 or not isinstance(value.get('workers'), list) or
            not 1 <= len(value['workers']) <= 64):
        raise ValueError('fleet configuration requires 1-64 workers')
    allowed = {
        'id', 'host', 'user', 'port', 'remote_root', 'trainctl_executable',
        'known_hosts', 'identity_file', 'labels', 'enabled', 'disabled_reason',
        'workspace_user', 'workspace_root', 'workspace_executable',
    }
    bindings = []
    for raw in value['workers']:
        if (not isinstance(raw, dict) or set(raw) - allowed or
                not isinstance(raw.get('id'), str) or not isinstance(raw.get('host'), str)):
            raise ValueError('each worker requires id and host')
        labels = raw.get('labels', [])
        if (not isinstance(labels, list) or len(labels) > 64 or
                any(not isinstance(label, str) for label in labels) or len(set(labels)) != len(labels)):
            raise ValueError('worker labels must be a bounded unique list')
        common = {
            'port': raw.get('port', port),
            'known_hosts': raw.get('known_hosts', known_hosts),
            'identity_file': raw.get('identity_file', identity_file),
        }
        remote = remote_factory(
            raw['host'], remote_root=raw.get('remote_root', remote_root),
            user=raw.get('user', user),
            trainctl_executable=raw.get('trainctl_executable', 'trainctl'), **common,
        )
        workspace_remote = remote_factory(
            raw['host'],
            remote_root=raw.get('workspace_root', workspace_root or raw.get('remote_root', remote_root)),
            user=raw.get('workspace_user', workspace_user or raw.get('user', user)),
            trainctl_executable=raw.get('workspace_executable', workspace_executable or 'trainctl'),
            **common,
        )
        bindings.append(WorkerBinding(
            raw['id'], remote, labels=frozenset(labels), workspace_remote=workspace_remote,
            enabled=raw.get('enabled', True), disabled_reason=raw.get('disabled_reason'),
        ))
    return WorkerDirectory(bindings)
