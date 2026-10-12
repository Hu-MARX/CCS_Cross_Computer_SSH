"""Small subprocess client for ccsctl; no dependency on worker internals."""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from .process import ProcessStopped, run_bounded


_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SHA256 = re.compile(r"[a-f0-9]{64}")
_MAX_REQUEST_BYTES = 20 * 1024 * 1024


class CCSError(RuntimeError):
    """Gateway or local transport failure with machine-readable fields."""

    def __init__(self, code: str, message: str, *, retryable: bool = False,
                 request_id: str | None = None, execution_state: str | None = None,
                 operation_id: str | None = None, worker_id: str | None = None,
                 run_id: str | None = None, idempotency_key: str | None = None,
                 field_errors: list[dict[str, str]] | None = None,
                 output_truncated: bool = False, continuation: str | None = None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.retryable = retryable
        self.request_id = request_id
        self.execution_state = execution_state
        self.operation_id = operation_id
        self.worker_id = worker_id
        self.run_id = run_id
        self.idempotency_key = idempotency_key
        self.field_errors = field_errors or []
        self.output_truncated, self.continuation = output_truncated, continuation

    def to_dict(self) -> dict[str, Any]:
        """The same metadata contract used by CLI and MCP errors."""
        return {key: getattr(self, key) for key in (
            'code', 'message', 'retryable', 'request_id', 'execution_state',
            'operation_id', 'worker_id', 'run_id', 'idempotency_key', 'field_errors',
            'output_truncated', 'continuation',
        )}


def _mutation(request):
    action = request.get('action')
    if request.get('dry_run'):
        return False
    if action == 'workspace.execute':
        payload = request.get('payload')
        payload = payload if isinstance(payload, dict) else {}
        nested = payload.get('request')
        return (nested.get('action') if isinstance(nested, dict) else None) not in {'inspect', 'diff', 'changes'}
    return action in {'run.submit', 'run.cancel', 'checkpoint.replicate', 'checkpoint.import'}


def _correlation(request):
    payload = request.get('payload')
    payload = payload if isinstance(payload, dict) else {}
    return {'worker_id': payload.get('worker_id', payload.get('target_worker_id')),
            'run_id': payload.get('run_id'), 'idempotency_key': payload.get('idempotency_key')}


def _json_snapshot(value: Mapping[str, Any], *, request_id: str | None = None) -> str:
    if not isinstance(value, Mapping):
        raise CCSError("INVALID_REQUEST", "Expected a JSON object", request_id=request_id)
    try:
        return json.dumps(dict(value), ensure_ascii=False, allow_nan=False,
                          separators=(",", ":"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise CCSError("INVALID_REQUEST", "Expected finite JSON values",
                       request_id=request_id) from exc


@dataclass(frozen=True, init=False)
class CCSPlan:
    """A dry-run snapshot. Exposed objects are copies, so approval cannot drift."""

    _request_json: str
    _data_json: str

    def __init__(self, request: Mapping[str, Any], data: Mapping[str, Any]):
        request_json = _json_snapshot(request)
        data_json = _json_snapshot(data)
        request_copy, data_copy = json.loads(request_json), json.loads(data_json)
        digest = data_copy.get("plan_sha256")
        if (request_copy.get("dry_run") is not True or
                data_copy.get("dry_run") is not True or
                data_copy.get("executed") is not False or
                data_copy.get("request_id") != request_copy.get("request_id") or
                data_copy.get("action") != request_copy.get("action") or
                not isinstance(digest, str) or not _SHA256.fullmatch(digest)):
            raise CCSError("INVALID_GATEWAY_RESPONSE", "A matching dry-run result is required",
                           request_id=request_copy.get("request_id"))
        object.__setattr__(self, "_request_json", request_json)
        object.__setattr__(self, "_data_json", data_json)

    @property
    def request(self) -> dict[str, Any]:
        return json.loads(self._request_json)

    @property
    def data(self) -> dict[str, Any]:
        return json.loads(self._data_json)

    @property
    def plan_sha256(self) -> str:
        return self.data["plan_sha256"]

    def save(self, path: str | os.PathLike[str]) -> None:
        """Create a UTF-8 plan file; existing approval files are never overwritten."""
        value = {'schema_version': 1, 'request': self.request, 'plan': self.data}
        serialized = _json_snapshot(value)
        if len(serialized.encode('utf-8')) > _MAX_REQUEST_BYTES:
            raise CCSError('PLAN_TOO_LARGE', 'Saved plan exceeds 20 MiB')
        with Path(path).open('x', encoding='utf-8') as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> 'CCSPlan':
        with Path(path).open('rb') as handle:
            raw = handle.read(_MAX_REQUEST_BYTES + 1)
        if len(raw) > _MAX_REQUEST_BYTES:
            raise CCSError('PLAN_TOO_LARGE', 'Saved plan exceeds 20 MiB')
        try:
            value = json.loads(raw.decode('utf-8'), parse_constant=lambda token: (_ for _ in ()).throw(ValueError(token)))
            if not isinstance(value, dict) or value.get('schema_version') != 1 or set(value) != {'schema_version', 'request', 'plan'}:
                raise ValueError('Invalid plan file')
            return cls(value['request'], value['plan'])
        except (ValueError, TypeError, KeyError, UnicodeDecodeError) as exc:
            raise CCSError('INVALID_PLAN', 'Expected a saved CCS v1 plan file') from exc


@dataclass
class CCSReadCursor:
    """Serializable delivered position, scoped to one run, worker and stream."""
    action: str
    run_id: str
    worker_id: str | None = None
    stream: str | None = None
    value: int = 0

    def __post_init__(self):
        if (self.action not in {'run.events', 'run.logs'} or type(self.value) is not int or self.value < 0 or
                not isinstance(self.run_id, str) or not _SAFE_ID.fullmatch(self.run_id) or
                (self.worker_id is not None and (not isinstance(self.worker_id, str) or not _SAFE_ID.fullmatch(self.worker_id))) or
                (self.action == 'run.logs' and self.stream not in {'stdout', 'stderr'}) or
                (self.action == 'run.events' and self.stream is not None)):
            raise ValueError('Invalid CCS read cursor')

    def to_dict(self):
        return {'action': self.action, 'run_id': self.run_id, 'worker_id': self.worker_id,
                'stream': self.stream, 'value': self.value}

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict) or set(value) != {'action', 'run_id', 'worker_id', 'stream', 'value'}:
            raise ValueError('Invalid CCS read cursor object')
        return cls(**value)


class CCSClient:
    """Call a configured ccsctl command through UTF-8 stdin and JSON stdout.

    ``command`` includes the executable and all target/configuration arguments,
    for example ``['ccsctl', '--fleet', 'workers.json', '--db', 'runs.db']``.
    The client appends ``request --file -``. Calls are never retried automatically.
    The default request is a dry run; use ``plan`` followed by ``confirm`` to keep
    the exact request identity and payload across a mutation's two calls.
    """

    def __init__(self, command: Sequence[str], *, cwd: str | os.PathLike[str] | None = None,
                 timeout: float = 120, actor_id: str = "python-sdk",
                 actor_kind: str = "service", max_response_bytes: int = 20 * 1024**2,
                 max_error_bytes: int = 64 * 1024, max_concurrent: int = 4, max_queued: int = 8):
        if (isinstance(command, (str, bytes)) or not isinstance(command, Sequence) or
                not command or any(not isinstance(arg, str) or not arg for arg in command)):
            raise ValueError("command must be a non-empty sequence of non-empty strings")
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or
                not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("timeout must be positive and finite")
        if not isinstance(actor_id, str) or not _SAFE_ID.fullmatch(actor_id):
            raise ValueError("actor_id must be a valid CCS identifier")
        if not isinstance(actor_kind, str) or actor_kind not in {"agent", "cli", "user", "service"}:
            raise ValueError("actor_kind must be agent, cli, user, or service")
        self._command = tuple(command)
        self.cwd = os.fspath(cwd) if cwd is not None else None
        self.timeout = timeout
        self._actor_id = actor_id
        self._actor_kind = actor_kind
        if any(type(value) is not int or value <= 0 for value in (max_response_bytes, max_error_bytes, max_concurrent)):
            raise ValueError('output budgets and max_concurrent must be positive integers')
        if type(max_queued) is not int or max_queued < 0:
            raise ValueError('max_queued must be a nonnegative integer')
        self.max_response_bytes, self.max_error_bytes = max_response_bytes, max_error_bytes
        self._max_concurrent, self._max_queued = max_concurrent, max_queued
        self._condition = threading.Condition()
        self._active, self._queued = 0, 0

    @property
    def command(self) -> tuple[str, ...]:
        return self._command

    @property
    def actor_id(self) -> str:
        return self._actor_id

    @property
    def actor_kind(self) -> str:
        return self._actor_kind

    @property
    def actor(self) -> dict[str, str]:
        return {"id": self.actor_id, "kind": self.actor_kind}

    def request(self, action: str, payload: Mapping[str, Any], *, dry_run: bool = True,
                request_id: str | None = None,
                confirm_plan_sha256: str | None = None, timeout: float | None = None,
                cancel_event: threading.Event | None = None) -> dict[str, Any]:
        """Build and execute one request. Read actions also default to a dry run."""
        request = self._request(action, payload, dry_run=dry_run, request_id=request_id)
        if confirm_plan_sha256 is not None:
            request["confirm_plan_sha256"] = confirm_plan_sha256
        return self.execute(request, timeout=timeout, cancel_event=cancel_event)

    def describe(self, action: str | None = None, *, workspace_action: str | None = None) -> dict[str, Any]:
        """Ask the configured gateway for installed contracts without contacting a worker."""
        payload = {}
        if action is not None:
            payload['action'] = action
        if workspace_action is not None:
            payload['workspace_action'] = workspace_action
        return self.request('gateway.describe', payload, dry_run=False)['result']

    def plan(self, action: str, payload: Mapping[str, Any], *,
             request_id: str | None = None, timeout: float | None = None,
             cancel_event: threading.Event | None = None) -> CCSPlan:
        """Preview a request and preserve its identity and payload for confirmation."""
        request = self._request(action, payload, dry_run=True, request_id=request_id)
        # Snapshot before starting the child, also isolating nested caller objects.
        request = json.loads(_json_snapshot(request, request_id=request["request_id"]))
        return CCSPlan(request, self.execute(request, timeout=timeout, cancel_event=cancel_event))

    def confirm(self, plan: CCSPlan, *, timeout: float | None = None,
                cancel_event: threading.Event | None = None) -> dict[str, Any]:
        """Execute exactly a saved plan; this method does not solicit human approval."""
        if not isinstance(plan, CCSPlan):
            raise TypeError("plan must be a CCSPlan returned by plan()")
        request = plan.request
        request["dry_run"] = False
        request["confirm_plan_sha256"] = plan.plan_sha256
        return self.execute(request, timeout=timeout, cancel_event=cancel_event)

    def _request(self, action, payload, *, dry_run, request_id):
        if not isinstance(payload, Mapping):
            raise CCSError("INVALID_REQUEST", "payload must be a JSON object", request_id=request_id)
        return {"schema_version": 1,
                "request_id": request_id if request_id is not None else "sdk_" + uuid4().hex,
                "actor": self.actor, "action": action, "dry_run": dry_run, "payload": dict(payload)}

    def execute(self, request: Mapping[str, Any], *, timeout: float | None = None,
                cancel_event: threading.Event | None = None) -> dict[str, Any]:
        """Execute a complete request, returning gateway data or raising CCSError.

        A timeout does not prove that a remote mutation failed. Inspect the run or
        query operation.query or operation.reconcile with its original identity.
        An unknown mutation must never be automatically resubmitted.
        """
        request_json = _json_snapshot(
            request, request_id=request.get("request_id") if isinstance(request, Mapping) else None,
        )
        snapshot = json.loads(request_json)
        request_id = snapshot.get("request_id")
        if not isinstance(request_id, str) or not _SAFE_ID.fullmatch(request_id):
            raise CCSError("INVALID_REQUEST", "A valid request_id is required")
        if (not isinstance(snapshot.get("action"), str) or
                type(snapshot.get("dry_run")) is not bool):
            raise CCSError("INVALID_REQUEST", "action and boolean dry_run are required",
                           request_id=request_id)
        request_bytes = request_json.encode("utf-8")
        if len(request_bytes) > _MAX_REQUEST_BYTES:
            raise CCSError("REQUEST_TOO_LARGE", "CCS request exceeds 20 MiB",
                           request_id=request_id)
        duration = self.timeout if timeout is None else timeout
        if isinstance(duration, bool) or not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration <= 0:
            raise ValueError('timeout must be positive and finite')
        deadline = time.monotonic() + duration
        admitted = False
        try:
            self._admit(deadline, cancel_event)
            admitted = True
            process = run_bounded(
                [*self.command, "request", "--file", "-"],
                input=request_bytes, cwd=self.cwd, timeout=max(0.000001, deadline - time.monotonic()),
                max_stdout_bytes=self.max_response_bytes, max_stderr_bytes=self.max_error_bytes,
                cancel_event=cancel_event,
            )
        except ProcessStopped as exc:
            codes = {'TIMEOUT': 'GATEWAY_TIMEOUT', 'CANCELLED': 'GATEWAY_CANCELLED',
                     'OUTPUT_LIMIT': 'GATEWAY_OUTPUT_LIMIT', 'BUSY': 'GATEWAY_BUSY',
                     'CONTAINMENT_FAILED': 'GATEWAY_UNAVAILABLE'}
            messages = {'TIMEOUT': 'The gateway timed out', 'CANCELLED': 'Local gateway waiting was cancelled',
                        'OUTPUT_LIMIT': 'Gateway output exceeded its bounded capture budget',
                        'BUSY': 'Gateway concurrency and waiting queue are full',
                        'CONTAINMENT_FAILED': 'The gateway transport could not safely complete local capture or cleanup'}
            raise CCSError(
                codes[exc.code], messages[exc.code] +
                ('; the remote action may already have executed. Inspect its state before repeating a mutation.'
                 if exc.started and _mutation(snapshot) else '.'),
                retryable=False, request_id=request_id,
                execution_state=('unknown' if exc.started else 'not_sent') if _mutation(snapshot) else None,
                output_truncated=exc.code == 'OUTPUT_LIMIT',
                continuation=('For paged reads reuse the last delivered event/log/artifact cursor with a smaller page. '
                              'For mutations query operation.query/reconcile using the original identity.')
                             if exc.code == 'OUTPUT_LIMIT' else None,
                **_correlation(snapshot),
            ) from exc
        except OSError as exc:
            raise CCSError("GATEWAY_UNAVAILABLE", str(exc), request_id=request_id,
                           execution_state='not_sent' if _mutation(snapshot) else None,
                           **_correlation(snapshot)) from exc
        finally:
            if admitted:
                with self._condition:
                    self._active -= 1
                    self._condition.notify_all()
        return self._response(process, snapshot, max_error_bytes=self.max_error_bytes)

    def _admit(self, deadline, cancel_event):
        with self._condition:
            if cancel_event is not None and cancel_event.is_set():
                raise ProcessStopped('CANCELLED')
            if self._active < self._max_concurrent:
                self._active += 1
                return
            if self._queued >= self._max_queued:
                raise ProcessStopped('BUSY')
            self._queued += 1
            try:
                while self._active >= self._max_concurrent:
                    if cancel_event is not None and cancel_event.is_set():
                        raise ProcessStopped('CANCELLED')
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ProcessStopped('TIMEOUT')
                    self._condition.wait(min(0.02, remaining))
                if cancel_event is not None and cancel_event.is_set():
                    raise ProcessStopped('CANCELLED')
                if deadline <= time.monotonic():
                    raise ProcessStopped('TIMEOUT')
                self._active += 1
            finally:
                self._queued -= 1

    @staticmethod
    def _deadline(total_timeout):
        if (isinstance(total_timeout, bool) or not isinstance(total_timeout, (int, float)) or
                not math.isfinite(total_timeout) or total_timeout <= 0):
            raise ValueError('total_timeout must be positive and finite')
        return time.monotonic() + total_timeout

    @staticmethod
    def _remaining(deadline, run_id, worker_id, cancel_event):
        if cancel_event is not None and cancel_event.is_set():
            raise CCSError('WAIT_CANCELLED', 'Local waiting was cancelled; the remote Run continues.',
                           run_id=run_id, worker_id=worker_id)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise CCSError('WAIT_TIMEOUT', 'The total waiting deadline expired; the remote Run continues.',
                           run_id=run_id, worker_id=worker_id)
        return remaining

    def _poll_read(self, action, payload, deadline, cancel_event):
        """Retry only transient reads, under one total deadline with bounded backoff."""
        delay = 0.1
        while True:
            remaining = self._remaining(deadline, payload['run_id'], payload.get('worker_id'), cancel_event)
            try:
                result = self.request(action, payload, dry_run=False, timeout=min(self.timeout, remaining),
                                      cancel_event=cancel_event)['result']
                self._remaining(deadline, payload['run_id'], payload.get('worker_id'), cancel_event)
                return result
            except CCSError as exc:
                self._remaining(deadline, payload['run_id'], payload.get('worker_id'), cancel_event)
                if not (exc.retryable or exc.code in {'GATEWAY_TIMEOUT', 'SSH_TIMEOUT', 'SSH_UNAVAILABLE'}):
                    raise
                self._pause(min(delay, deadline - time.monotonic()), cancel_event)
                delay = min(5, delay * 2)

    @staticmethod
    def _pause(duration, cancel_event):
        if cancel_event is None:
            time.sleep(max(0, duration))
        else:
            cancel_event.wait(max(0, duration))

    def wait_run(self, run_id: str, *, total_timeout: float, worker_id: str | None = None,
                 poll_interval: float = 0.5, max_poll_interval: float = 5,
                 cancel_event: threading.Event | None = None) -> dict[str, Any]:
        """Poll to a terminal snapshot. Deadline/cancellation never calls run.cancel."""
        deadline = self._deadline(total_timeout)
        if not 0 < poll_interval <= max_poll_interval < float('inf'):
            raise ValueError('Require 0 < poll_interval <= max_poll_interval < infinity')
        payload = {'run_id': run_id, **({'worker_id': worker_id} if worker_id is not None else {})}
        delay = poll_interval
        while True:
            snapshot = self._poll_read('run.snapshot', payload, deadline, cancel_event)
            if snapshot.get('status') in {'SUCCEEDED', 'FAILED', 'CANCELLED'}:
                return snapshot
            self._pause(min(delay, self._remaining(deadline, run_id, worker_id, cancel_event)), cancel_event)
            delay = min(max_poll_interval, delay * 1.5)

    @staticmethod
    def _cursor(action, run_id, worker_id, stream, cursor, start):
        if cursor is None:
            return CCSReadCursor(action, run_id, worker_id, stream, start)
        if not isinstance(cursor, CCSReadCursor) or (cursor.action, cursor.run_id, cursor.worker_id, cursor.stream) != (
                action, run_id, worker_id, stream):
            raise ValueError('Cursor belongs to another action, run, worker or stream')
        return cursor

    def iter_events(self, run_id: str, *, total_timeout: float, worker_id: str | None = None,
                    cursor: CCSReadCursor | None = None, after_seq: int = 0, limit: int = 100,
                    max_bytes: int = 1024**2, follow: bool = False, poll_interval: float = 0.5,
                    cancel_event: threading.Event | None = None):
        """Yield paged event dictionaries; persist cursor.to_dict() to reconnect.

        The caller's cursor advances for each delivered event, never for a fetched
        page's undispatched tail. ``follow`` drains again after a terminal snapshot.
        Explicit pagination requires a worker advertising run.events.page.
        """
        position = self._cursor('run.events', run_id, worker_id, None, cursor, after_seq)
        deadline = self._deadline(total_timeout)
        terminal_seen = False
        if poll_interval <= 0 or not math.isfinite(poll_interval):
            raise ValueError('poll_interval must be positive and finite')
        while True:
            payload = {'run_id': run_id, 'after_seq': position.value, 'limit': limit, 'max_bytes': max_bytes,
                       **({'worker_id': worker_id} if worker_id is not None else {})}
            page = self._poll_read('run.events', payload, deadline, cancel_event)
            events = page.get('events')
            if not isinstance(events, list) or type(page.get('has_more')) is not bool or type(page.get('next_cursor')) is not int:
                raise CCSError('INVALID_GATEWAY_RESPONSE', 'Expected a paged run.events result', run_id=run_id)
            previous = position.value
            for event in events:
                if not isinstance(event, dict) or type(event.get('seq')) is not int or event['seq'] <= previous:
                    raise CCSError('INVALID_GATEWAY_RESPONSE', 'Event sequence did not advance', run_id=run_id)
                previous = event['seq']
            if page['next_cursor'] != previous or (not events and page['has_more']):
                raise CCSError('INVALID_GATEWAY_RESPONSE', 'Event page cursor does not match delivered events', run_id=run_id)
            for event in events:
                self._remaining(deadline, run_id, worker_id, cancel_event)
                position.value = event['seq']
                yield event
            if page['has_more']:
                continue
            if not follow or (terminal_seen and not events):
                return
            snapshot = self._poll_read('run.snapshot', {'run_id': run_id, **({'worker_id': worker_id} if worker_id else {})},
                                       deadline, cancel_event)
            terminal_seen = snapshot.get('status') in {'SUCCEEDED', 'FAILED', 'CANCELLED'}
            if not terminal_seen:
                self._pause(min(poll_interval, self._remaining(deadline, run_id, worker_id, cancel_event)), cancel_event)

    def iter_logs(self, run_id: str, *, total_timeout: float, stream: str = 'stdout',
                  worker_id: str | None = None, cursor: CCSReadCursor | None = None,
                  after_offset: int = 0, follow: bool = False, poll_interval: float = 0.5,
                  cancel_event: threading.Event | None = None):
        """Yield existing byte-cursor log chunks; local cancellation leaves the Run running."""
        position = self._cursor('run.logs', run_id, worker_id, stream, cursor, after_offset)
        deadline = self._deadline(total_timeout)
        if poll_interval <= 0 or not math.isfinite(poll_interval):
            raise ValueError('poll_interval must be positive and finite')
        while True:
            payload = {'run_id': run_id, 'stream': stream, 'after_offset': position.value,
                       **({'worker_id': worker_id} if worker_id is not None else {})}
            page = self._poll_read('run.logs', payload, deadline, cancel_event)
            offset = page.get('next_offset')
            if (type(offset) is not int or offset < position.value or page.get('offset') != position.value or
                    type(page.get('has_more')) is not bool or type(page.get('eof')) is not bool or
                    not isinstance(page.get('text'), str)):
                raise CCSError('INVALID_GATEWAY_RESPONSE', 'Invalid byte-cursor log result', run_id=run_id)
            if offset > position.value:
                self._remaining(deadline, run_id, worker_id, cancel_event)
                position.value = offset
                yield page
            elif page['has_more']:
                raise CCSError('INVALID_GATEWAY_RESPONSE', 'Log cursor did not advance despite has_more', run_id=run_id)
            if page['eof'] or (not page['has_more'] and not follow):
                return
            if not page['has_more']:
                self._pause(min(poll_interval, self._remaining(deadline, run_id, worker_id, cancel_event)), cancel_event)

    @staticmethod
    def _response(process, request, *, max_error_bytes=64 * 1024) -> dict[str, Any]:
        request_id = request["request_id"]

        def invalid(message):
            return CCSError("INVALID_GATEWAY_RESPONSE", message, request_id=request_id,
                            execution_state='unknown' if _mutation(request) else None,
                            **_correlation(request))

        def nonfinite(value):
            raise ValueError("Non-finite JSON value: " + value)

        try:
            envelope = json.loads(process.stdout.decode("utf-8"), parse_constant=nonfinite)
        except (UnicodeDecodeError, ValueError) as exc:
            raise invalid(f"Gateway output is not UTF-8 JSON (exit code {process.returncode})") from exc
        if (not isinstance(envelope, dict) or
                not {"ok", "request_id", "data", "error"} <= envelope.keys() or
                type(envelope["ok"]) is not bool or envelope["request_id"] != request_id):
            raise invalid("Gateway returned an invalid envelope or mismatched request_id")
        if not envelope["ok"]:
            error = envelope["error"]
            if (process.returncode != 1 or envelope["data"] is not None or
                    not isinstance(error, dict) or not isinstance(error.get("code"), str) or
                    not error["code"] or not isinstance(error.get("message"), str) or
                    type(error.get("retryable")) is not bool):
                raise invalid("Gateway error envelope and exit code are inconsistent")
            metadata = _correlation(request)
            for key in ('worker_id', 'run_id', 'idempotency_key'):
                if error.get(key) is not None:
                    metadata[key] = error[key]
            state = error.get('execution_state')
            if _mutation(request) and state not in {'not_sent', 'rejected', 'confirmed', 'unknown'}:
                state = 'unknown'
            encoded_message = error['message'].encode('utf-8')
            truncated = len(encoded_message) > max_error_bytes
            message = (encoded_message[:max_error_bytes].decode('utf-8', errors='ignore') + ' [truncated]') if truncated else error['message']
            raise CCSError(error["code"], message,
                           retryable=False if _mutation(request) else error["retryable"],
                           request_id=request_id, execution_state=state,
                           operation_id=error.get('operation_id'), field_errors=error.get('field_errors'),
                           output_truncated=truncated or error.get('output_truncated') is True or error.get('truncated') is True,
                           continuation='Query gateway.audit using request_id for bounded audit detail.' if truncated else error.get('continuation'),
                           **metadata)
        data = envelope["data"]
        if process.returncode != 0 or envelope["error"] is not None or not isinstance(data, dict):
            raise invalid("Gateway success envelope and exit code are inconsistent")
        required = {"schema_version", "request_id", "action", "classification", "dry_run",
                    "executed", "executable", "plan_sha256", "required_capabilities",
                    "compatibility", "result"}
        if (not required <= data.keys() or type(data.get("schema_version")) is not int or
                data["schema_version"] != 1 or data["request_id"] != request_id or
                data["action"] != request["action"] or
                data["classification"] not in ("read", "mutation") or
                type(data["dry_run"]) is not bool or data["dry_run"] != request["dry_run"] or
                type(data["executed"]) is not bool or data["executed"] == request["dry_run"] or
                type(data["executable"]) is not bool or
                not isinstance(data["plan_sha256"], str) or not _SHA256.fullmatch(data["plan_sha256"]) or
                not isinstance(data["required_capabilities"], dict) or
                not isinstance(data["compatibility"], dict)):
            raise invalid("Gateway data does not match the CCS v1 request/result contract")
        return data
