"""Vendor-neutral JSON request interface over the CCS remote seam."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import Any

from .capabilities import requirements_for_spec
from .domain import DomainError, ExperimentSpec, Run, utc_now
from .fleet import WorkerBinding, WorkerDirectory
from .orchestrator import Orchestrator
from .repository import RunRepository, SubmissionError
from ccs.contracts import ContractError, describe, validate_payload, validate_request


_SAFE_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$')


class CCSRequestError(ValueError):
    def __init__(self, code: str, message: str, *, retryable: bool = False,
                 execution_state: str = 'not_sent', operation_id=None, worker_id=None,
                 run_id=None, idempotency_key=None, field_errors=None):
        super().__init__(message)
        self.code, self.message, self.retryable = code, message, retryable
        self.execution_state = execution_state
        self.operation_id, self.worker_id = operation_id, worker_id
        self.run_id, self.idempotency_key = run_id, idempotency_key
        self.field_errors = field_errors or []


class _AuditLog:
    """Private metadata-only audit store; request payloads never cross this seam."""

    def __init__(self, database: str):
        self.database = str(database)
        self._lock = RLock()
        self._memory_connection = None
        if self.database != ':memory:':
            Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('''CREATE TABLE IF NOT EXISTS gateway_audit (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                actor_kind TEXT NOT NULL,
                action TEXT NOT NULL,
                classification TEXT NOT NULL,
                dry_run INTEGER NOT NULL,
                plan_sha256 TEXT NOT NULL,
                outcome TEXT NOT NULL,
                error_code TEXT,
                created_at TEXT NOT NULL
            )''')
            columns = {row['name'] for row in db.execute('PRAGMA table_info(gateway_audit)')}
            for field in ('operation_id', 'worker_id', 'run_id', 'execution_state'):
                if field not in columns:
                    db.execute(f'ALTER TABLE gateway_audit ADD COLUMN {field} TEXT')

    def _connect(self):
        if self.database == ':memory:':
            if self._memory_connection is None:
                self._memory_connection = sqlite3.connect(':memory:', check_same_thread=False)
                self._memory_connection.row_factory = sqlite3.Row
            return self._memory_connection
        connection = sqlite3.connect(self.database, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def _db(self):
        with self._lock:
            connection = self._connect()
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            finally:
                if self.database != ':memory:':
                    connection.close()

    def record(self, request, classification, plan_sha256, outcome, error_code=None, *, operation=None):
        operation = operation or {}
        with self._db() as db:
            db.execute('''INSERT INTO gateway_audit
                (request_id, actor_id, actor_kind, action, classification, dry_run,
                 plan_sha256, outcome, error_code, created_at, operation_id, worker_id, run_id, execution_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
                    request['request_id'], request['actor']['id'], request['actor']['kind'],
                    request['action'], classification, int(request['dry_run']), plan_sha256,
                    outcome, error_code, utc_now(),
                    operation.get('operation_id'), operation.get('worker_id'),
                    operation.get('run_id'), operation.get('execution_state'),
                ))

    def query(self, filters):
        clauses, values = [], []
        for field in ('request_id', 'action', 'outcome', 'operation_id', 'worker_id', 'run_id'):
            if field in filters:
                clauses.append(f'{field} = ?')
                values.append(filters[field])
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ''
        values.append(filters.get('limit', 50))
        with self._db() as db:
            rows = db.execute(
                f'''SELECT seq, request_id, actor_id, actor_kind, action, classification,
                           dry_run, plan_sha256, outcome, error_code, created_at,
                           operation_id, worker_id, run_id, execution_state
                    FROM gateway_audit{where} ORDER BY seq DESC LIMIT ?''',
                values,
            ).fetchall()
        return [{
            'seq': row['seq'],
            'request_id': row['request_id'],
            'actor': {'id': row['actor_id'], 'kind': row['actor_kind']},
            'action': row['action'],
            'classification': row['classification'],
            'dry_run': bool(row['dry_run']),
            'plan_sha256': row['plan_sha256'],
            'outcome': row['outcome'],
            'error_code': row['error_code'],
            'created_at': row['created_at'],
            'operation_id': row['operation_id'], 'worker_id': row['worker_id'],
            'run_id': row['run_id'], 'execution_state': row['execution_state'],
        } for row in rows]


class _UnavailableAudit:
    """Keep operation queries usable while auditing is unavailable."""

    def record(self, *args, **kwargs):
        raise CCSRequestError('AUDIT_UNAVAILABLE', 'audit database is unavailable')

    def query(self, filters):
        raise CCSRequestError('AUDIT_UNAVAILABLE', 'audit database is unavailable')


class CCSGateway:
    """One deep request/result interface for CLI, Agent and script callers."""

    _WORKER_READS = {
        'worker.health': ('health', 'worker.health'),
        'worker.monitor': ('monitor', 'worker.monitor'),
        'worker.alerts': ('alerts', 'worker.telemetry'),
        'worker.storage': ('storage', 'worker.storage'),
        'worker.processes': ('processes', 'worker.processes'),
    }

    def __init__(self, remote, *, repository=None, worker='default', workspace_remote=None,
                 worker_directory=None, audit_path='data/ccs-audit.db', replica_store=None,
                 replica_root='data/checkpoint-replicas'):
        self.remote = remote
        self.workspace_remote = workspace_remote if workspace_remote is not None else remote
        self.repository = repository if repository is not None else RunRepository(':memory:')
        self.orchestrator = Orchestrator(self.repository, remote, worker=worker)
        self.worker_directory = worker_directory or WorkerDirectory([
            WorkerBinding(worker, remote, workspace_remote=self.workspace_remote),
        ])
        # Empty diagnostic requests historically target this exact adapter. Infer
        # its registered identity, without replacing it with the directory's first
        # binding (custom integrations can supply a different default adapter).
        matching_workers = [worker_id for worker_id in self.worker_directory.worker_ids
                            if self.worker_directory.remote_for(worker_id) is remote]
        self.default_read_worker_id = (worker if worker in matching_workers else
                                       matching_workers[0] if matching_workers else worker)
        self.audit_path = audit_path
        try:
            self.audit = _AuditLog(audit_path)
        except (sqlite3.Error, OSError):
            self.audit = _UnavailableAudit()
        self.replica_store = replica_store
        self.replica_root = replica_root

    def execute(self, request: dict[str, Any]) -> dict[str, Any]:
        normalized = self._request(request)
        action = normalized['action']
        payload = normalized['payload']
        self._validate_payload(action, payload)
        if action == 'gateway.describe':
            return self.describe_request(normalized)
        if action == 'run.snapshot':
            if (set(payload) - {'run_id', 'worker_id'} or 'run_id' not in payload or
                    not self._safe_id(payload['run_id'])):
                raise CCSRequestError('INVALID_REQUEST', 'run.snapshot requires a valid run_id')
            worker_id = self._run_worker(payload['run_id'], payload.get('worker_id'))
            classification = 'read'
            required = {'run.observe': 1}
            worker_plan = self.worker_directory.operation_plan(worker_id, required, allow_legacy=True)
            prepared = {'run_id': payload['run_id'], 'worker_id': worker_id, 'worker_plan': worker_plan}
        elif action == 'run.submit':
            if (set(payload) - {'spec', 'idempotency_key', 'worker_id'} or
                    not {'spec', 'idempotency_key'} <= set(payload) or
                    not self._safe_id(payload.get('idempotency_key'))):
                raise CCSRequestError(
                    'INVALID_REQUEST', 'run.submit requires spec, idempotency_key, and optional worker_id',
                )
            if self.worker_directory.size > 1 and 'worker_id' not in payload:
                raise CCSRequestError(
                    'WORKER_SELECTION_REQUIRED', 'multi-worker submission requires an upper-Agent worker_id selection',
                )
            try:
                spec = ExperimentSpec.from_dict(payload.get('spec'))
            except (DomainError, TypeError, ValueError) as exc:
                raise CCSRequestError('INVALID_REQUEST', f'invalid ExperimentSpec: {exc}') from exc
            worker_id = payload.get('worker_id', self.worker_directory.default_worker_id)
            classification = 'mutation'
            required = requirements_for_spec(spec)
            worker_plan = self.worker_directory.submission_plan(worker_id, required)
            prepared = {'spec': spec, 'worker_id': worker_id, 'worker_plan': worker_plan}
        elif action in self._WORKER_READS:
            if set(payload) - {'worker_id'}:
                raise CCSRequestError('INVALID_REQUEST', f'{action} accepts only optional worker_id')
            if 'worker_id' in payload:
                if not self._safe_id(payload['worker_id']):
                    raise CCSRequestError('INVALID_WORKER_SELECTION', 'worker_id is invalid')
                worker_id = payload['worker_id']
                # Resolution is independent of mutation admission: a disabled
                # worker remains available for diagnostic reads.
                target_remote = self.worker_directory.remote_for(worker_id)
            else:
                worker_id, target_remote = self.default_read_worker_id, self.remote
            prepared = {'worker_id': worker_id, 'remote': target_remote}
            classification = 'read'
            required = {self._WORKER_READS[action][1]: 1}
        elif action == 'run.events':
            prepared = self._run_cursor(payload, 'after_seq')
            classification = 'read'
            required = {'run.events.page': 1} if prepared.get('paged') else {'run.observe': 1}
            self._route_run(prepared, required, allow_legacy=not prepared.get('paged'))
        elif action == 'run.logs':
            prepared = self._log_request(payload)
            classification = 'read'
            required = {'run.logs': 1}
            self._route_run(prepared, required, allow_legacy=True)
        elif action == 'run.artifacts':
            prepared = self._artifact_request(payload)
            classification = 'read'
            required = {'run.artifacts.page': 1} if prepared.get('paged') else {'run.artifacts': 1}
            self._route_run(prepared, required, allow_legacy=not prepared.get('paged'))
        elif action == 'checkpoint.replicate':
            if (set(payload) - {'run_id', 'checkpoint_id', 'worker_id'} or
                    not {'run_id', 'checkpoint_id'} <= set(payload) or
                    not self._safe_id(payload.get('run_id')) or
                    not isinstance(payload.get('checkpoint_id'), str) or
                    not re.fullmatch(r'ckpt_[a-f0-9]{32}', payload['checkpoint_id'])):
                raise CCSRequestError(
                    'INVALID_REQUEST',
                    'checkpoint.replicate requires valid run_id, checkpoint_id, and optional worker_id',
                )
            prepared = dict(payload)
            classification = 'mutation'
            required = {'checkpoint.export': 1}
            self._route_run(prepared, required, allow_legacy=False)
        elif action == 'checkpoint.import':
            allowed = {'source_worker_id', 'source_run_id', 'checkpoint_id', 'target_worker_id'}
            if (set(payload) != allowed or
                    any(not self._safe_id(payload.get(key)) for key in (
                        'source_worker_id', 'source_run_id', 'target_worker_id')) or
                    not isinstance(payload.get('checkpoint_id'), str) or
                    not re.fullmatch(r'ckpt_[a-f0-9]{32}', payload['checkpoint_id'])):
                raise CCSRequestError(
                    'INVALID_REQUEST',
                    'checkpoint.import requires source worker/run/checkpoint and target worker ids',
                )
            worker_plan = self.worker_directory.operation_plan(
                payload['target_worker_id'], {'checkpoint.import': 1}, allow_legacy=False,
            )
            prepared = {**payload, 'worker_id': payload['target_worker_id'], 'worker_plan': worker_plan}
            classification = 'mutation'
            required = {'checkpoint.import': 1}
        elif action == 'run.cancel':
            if (set(payload) - {'run_id', 'reason', 'worker_id'} or
                    not {'run_id', 'reason'} <= set(payload) or not self._safe_id(payload.get('run_id')) or
                    not isinstance(payload.get('reason'), str) or not payload['reason'].strip() or
                    len(payload['reason']) > 500):
                raise CCSRequestError('INVALID_REQUEST', 'run.cancel requires a run_id and 1-500 character reason')
            prepared = {'run_id': payload['run_id'], 'reason': payload['reason'].strip(),
                        'worker_id': payload.get('worker_id')}
            classification = 'mutation'
            required = {'run.cancel': 1}
            self._route_run(prepared, required, allow_legacy=True)
        elif action == 'workspace.execute':
            workspace_request, classification = self._workspace_request(payload)
            required = {'workspace.control': 1}
            worker_id = self._selected_worker(payload.get('worker_id'))
            worker_plan = self.worker_directory.operation_plan(
                worker_id, required, allow_legacy=classification == 'read',
            )
            prepared = {'request': workspace_request, 'worker_id': worker_id, 'worker_plan': worker_plan}
        elif action == 'fleet.status':
            prepared = dict(payload)
            classification = 'read'
            required = {}
        elif action == 'gateway.audit':
            prepared = self._audit_filters(payload)
            classification = 'read'
            required = {}
        elif action in {'operation.query', 'operation.reconcile'}:
            if (len(payload) != 1 or set(payload) - {'operation_id', 'idempotency_key', 'request_id'} or
                    not all(self._safe_id(value) for value in payload.values())):
                raise CCSRequestError('INVALID_REQUEST', 'operation query requires exactly one valid identity')
            prepared = dict(payload)
            classification = 'read'
            required = {}
        else:
            raise CCSRequestError('UNSUPPORTED_ACTION', f'unsupported CCS action: {action}')

        if action in {'gateway.audit', 'fleet.status', 'operation.query', 'operation.reconcile'}:
            compatibility = {'state': 'not_required', 'reachable': None, 'compatible': True,
                             'reason': f'{action} does not target one worker'}
            executable = True
        elif action == 'run.submit':
            compatibility = prepared['worker_plan']['compatibility']
            executable = compatibility.get('state') == 'compatible'
        elif action == 'run.snapshot':
            compatibility = prepared['worker_plan']['compatibility']
            executable = compatibility.get('state') in {'compatible', 'legacy'}
        elif action in self._WORKER_READS:
            compatibility = prepared['remote'].compatibility(required)
            executable = compatibility.get('state') in {'compatible', 'legacy'}
        elif action in {
                'run.events', 'run.logs', 'run.artifacts', 'run.cancel',
                'checkpoint.replicate', 'checkpoint.import', 'workspace.execute'}:
            compatibility = prepared['worker_plan']['compatibility']
            executable = compatibility.get('state') == 'compatible' or (
                (classification == 'read' or action == 'run.cancel') and compatibility.get('state') == 'legacy'
            )
        else:
            capability_remote = self.workspace_remote if action == 'workspace.execute' else self.remote
            compatibility = capability_remote.compatibility(required)
            executable = compatibility.get('state') == 'compatible' or (
                (classification == 'read' or action == 'run.cancel') and compatibility.get('state') == 'legacy'
            )
        plan_sha256 = self._plan_sha256(normalized)
        if normalized['dry_run']:
            planned_result = ({'worker_id': prepared['worker_id'], 'worker': prepared['worker_plan']}
                              if action in {'run.submit', 'checkpoint.import'} else None)
            if action in self._WORKER_READS:
                planned_result = {'worker_id': prepared['worker_id']}
            response = self._result(
                normalized, classification, required, compatibility, executable, plan_sha256, planned_result, False,
            )
            try:
                self.audit.record(normalized, classification, plan_sha256, 'planned')
            except Exception:
                response['warnings'] = [{'code': 'AUDIT_UNAVAILABLE', 'message': 'plan prepared; audit write failed'}]
            return response
        if classification == 'mutation' and normalized.get('confirm_plan_sha256') != plan_sha256:
            self.audit.record(normalized, classification, plan_sha256, 'failed', 'CONFIRMATION_REQUIRED')
            raise CCSRequestError(
                'CONFIRMATION_REQUIRED', 'mutation requires its exact dry-run plan SHA-256 confirmation',
            )
        if not executable:
            code = 'WORKER_CAPABILITIES_UNDECLARED' if compatibility.get('state') == 'legacy' else 'WORKER_INCOMPATIBLE'
            self.audit.record(normalized, classification, plan_sha256, 'failed', code)
            raise CCSRequestError(code, str(compatibility.get('reason') or 'worker is incompatible'))

        operation = None
        if classification == 'mutation':
            try:
                if action == 'run.submit':
                    operation, _ = self.repository.reserve_submission(
                        prepared['spec'], prepared['worker_id'], payload['idempotency_key'],
                        request_id=normalized['request_id'], actor=normalized['actor'], plan_sha256=plan_sha256,
                    )
                else:
                    operation, dispatch = self.repository.start_operation(
                        action, normalized['request_id'], normalized['actor'],
                        prepared.get('worker_id', prepared.get('target_worker_id')), plan_sha256,
                    )
                    if not dispatch:
                        raise self._operation_error(operation, 'OPERATION_ALREADY_RECORDED',
                                                    'query the recorded operation before repeating a mutation')
            except Exception as exc:
                correlation = {field: getattr(exc, field, None) for field in
                               ('operation_id', 'worker_id', 'run_id', 'execution_state')}
                try:
                    self.audit.record(normalized, classification, plan_sha256, 'failed',
                                      getattr(exc, 'code', type(exc).__name__), operation=correlation)
                except Exception:
                    pass
                if isinstance(exc, (SubmissionError, CCSRequestError)):
                    raise
                raise CCSRequestError('OPERATION_STORE_UNAVAILABLE',
                                      'operation store unavailable; query original identity after recovery',
                                      execution_state='unknown',
                                      worker_id=prepared.get('worker_id', prepared.get('target_worker_id')),
                                      idempotency_key=payload.get('idempotency_key')) from exc
            try:
                self.audit.record(normalized, classification, plan_sha256, 'started', operation=operation)
            except Exception as exc:
                if action != 'run.submit':
                    try:
                        self.repository.mark_operation(operation['operation_id'], 'not_sent', error_code='AUDIT_UNAVAILABLE')
                    except Exception:
                        operation = dict(operation, state='not_sent', execution_state='not_sent')
                try:
                    operation = self.repository.get_operation(operation_id=operation['operation_id']) or operation
                except Exception:
                    if action == 'run.submit':
                        # A competing claimant may already have sent the reserved key.
                        operation = dict(operation, state='unknown', execution_state='unknown')
                raise self._operation_error(operation, 'AUDIT_UNAVAILABLE',
                                            'pre-dispatch audit failed; query operation state before retrying') from exc

        try:
            if action == 'run.snapshot':
                snapshot = self.worker_directory.remote_for(prepared['worker_id']).snapshot(prepared['run_id'])
                result = self._snapshot_result(snapshot)
                result['worker_id'] = prepared['worker_id']
            elif action == 'run.submit':
                result = self._run_result(self.worker_directory.submit(
                    prepared['worker_id'], prepared['spec'], payload['idempotency_key'], self.repository,
                    request_id=normalized['request_id'], actor=normalized['actor'], plan_sha256=plan_sha256,
                ))
            elif action in self._WORKER_READS:
                result = dict(getattr(prepared['remote'], self._WORKER_READS[action][0])())
                result['worker_id'] = prepared['worker_id']
            elif action == 'run.events':
                remote = self.worker_directory.remote_for(prepared['worker_id'])
                if prepared.get('paged'):
                    result = dict(remote.events_page(prepared['run_id'], prepared['after_seq'],
                                                     limit=prepared['limit'], max_bytes=prepared['max_bytes']))
                    result['events'] = [self._event_result(event) if not isinstance(event, dict) else event
                                        for event in result['events']]
                    result['worker_id'] = prepared['worker_id']
                else:
                    result = {'worker_id': prepared['worker_id'],
                              'events': [self._event_result(event) for event in
                                         remote.events(prepared['run_id'], prepared['after_seq'])]}
            elif action == 'run.logs':
                result = dict(self.worker_directory.remote_for(prepared['worker_id']).logs(
                    prepared['run_id'], prepared['stream'], prepared['after_offset'],
                ))
                result['worker_id'] = prepared['worker_id']
            elif action == 'run.artifacts':
                remote = self.worker_directory.remote_for(prepared['worker_id'])
                if prepared.get('paged'):
                    result = dict(remote.artifacts_page(prepared['run_id'], path=prepared.get('path'),
                                                       cursor=prepared.get('cursor'), limit=prepared['limit'],
                                                       max_bytes=prepared['max_bytes']))
                    result['worker_id'] = prepared['worker_id']
                else:
                    result = {'worker_id': prepared['worker_id'],
                              'artifacts': remote.artifacts(prepared['run_id'], path=prepared.get('path'))}
            elif action == 'run.cancel':
                result = self._snapshot_result(self.worker_directory.remote_for(prepared['worker_id']).cancel(
                    prepared['run_id'], prepared['reason'],
                ))
                result['worker_id'] = prepared['worker_id']
            elif action == 'checkpoint.replicate':
                if self.replica_store is None:
                    from .checkpoint_replication import CheckpointReplicaStore
                    self.replica_store = CheckpointReplicaStore(self.replica_root)
                result = self.replica_store.replicate(
                    self.worker_directory.remote_for(prepared['worker_id']),
                    worker_id=prepared['worker_id'], run_id=prepared['run_id'],
                    checkpoint_id=prepared['checkpoint_id'],
                )
            elif action == 'checkpoint.import':
                if self.replica_store is None:
                    from .checkpoint_replication import CheckpointReplicaStore
                    self.replica_store = CheckpointReplicaStore(self.replica_root)
                result = self.replica_store.import_to(
                    self.worker_directory.remote_for(prepared['target_worker_id']),
                    source_worker_id=prepared['source_worker_id'],
                    source_run_id=prepared['source_run_id'],
                    checkpoint_id=prepared['checkpoint_id'],
                    target_worker_id=prepared['target_worker_id'],
                )
            elif action == 'workspace.execute':
                result = dict(self.worker_directory.remote_for(
                    prepared['worker_id'], workspace=True,
                ).workspace(prepared['request']))
                result['worker_id'] = prepared['worker_id']
            elif action == 'fleet.status':
                result = self.worker_directory.status(**prepared)
            elif action in {'operation.query', 'operation.reconcile'}:
                result = self.repository.get_operation(**prepared)
                if result is None:
                    raise CCSRequestError('OPERATION_NOT_FOUND', 'operation identity is not recorded')
                if action == 'operation.reconcile':
                    if result['action'] != 'run.submit':
                        raise CCSRequestError('RECONCILIATION_UNSUPPORTED',
                                              'this action needs its documented action-specific recovery')
                    result = Orchestrator(
                        self.repository, self.worker_directory.remote_for(result['worker_id']),
                        worker=result['worker_id'],
                    ).reconcile_submission(operation_id=result['operation_id'])
            else:
                result = {'records': self.audit.query(prepared)}
        except Exception as exc:
            if operation is not None:
                if action != 'run.submit':
                    state = getattr(exc, 'execution_state', None)
                    if action not in {'run.cancel', 'workspace.execute'} or state not in {'not_sent', 'rejected'}:
                        state = 'unknown'
                    operation = dict(operation, state=state, execution_state=state,
                                     error_code=getattr(exc, 'code', type(exc).__name__))
                    try:
                        operation = self.repository.mark_operation(operation['operation_id'], state,
                                                                   error_code=operation['error_code'])
                    except Exception:
                        pass
                try:
                    operation = self.repository.get_operation(operation_id=operation['operation_id']) or operation
                except Exception:
                    if action == 'run.submit':
                        operation = dict(operation, execution_state=getattr(exc, 'execution_state', 'unknown'),
                                         run_id=getattr(exc, 'run_id', operation.get('run_id')))
            outcome = 'unknown' if operation and operation['execution_state'] == 'unknown' else 'failed'
            try:
                self.audit.record(normalized, classification, plan_sha256, outcome,
                                  getattr(exc, 'code', type(exc).__name__), operation=operation)
            except Exception:
                pass  # Durable operation facts remain authoritative when audit is unavailable.
            if operation is not None and not isinstance(exc, SubmissionError):
                raise self._operation_error(operation, getattr(exc, 'code', type(exc).__name__),
                                            'mutation result requires operation-state inspection') from exc
            raise
        if operation is not None:
            if action != 'run.submit':
                try:
                    self.repository.mark_operation(operation['operation_id'], 'confirmed', run_id=payload.get('run_id'))
                except Exception as exc:
                    raise CCSRequestError('OPERATION_RECORD_UNAVAILABLE', 'remote result confirmed; local record failed',
                                          execution_state='confirmed', operation_id=operation['operation_id'],
                                          worker_id=operation['worker_id'], run_id=payload.get('run_id')) from exc
            operation = dict(operation, state='confirmed', execution_state='confirmed',
                             run_id=result.get('id') if action == 'run.submit' else payload.get('run_id'))
            try:
                operation = self.repository.get_operation(operation_id=operation['operation_id']) or operation
            except Exception:
                pass  # The returned target result and committed marker already confirm execution.
        response = self._result(
            normalized, classification, required, compatibility, executable, plan_sha256, result, True,
        )
        if operation is not None:
            response['operation'] = operation
            response['execution_state'] = operation['execution_state']
        try:
            self.audit.record(normalized, classification, plan_sha256, 'succeeded', operation=operation)
        except Exception:
            response['warnings'] = [{'code': 'AUDIT_UNAVAILABLE', 'message': 'result confirmed; audit write failed'}]
        return response

    @staticmethod
    def _operation_error(operation, code, message):
        return CCSRequestError(code, message, execution_state=operation['execution_state'],
                               operation_id=operation['operation_id'], worker_id=operation['worker_id'],
                               run_id=operation.get('run_id'), idempotency_key=operation.get('idempotency_key'))

    @classmethod
    def describe_request(cls, request):
        """Offline gateway read: no remote adapter, persistence or audit is initialized."""
        normalized = cls._request(request)
        cls._validate_payload('gateway.describe', normalized['payload'])
        if normalized['action'] != 'gateway.describe':
            raise CCSRequestError('INVALID_REQUEST', 'offline contract query requires gateway.describe')
        payload = normalized['payload']
        result = describe(payload.get('action'), payload.get('workspace_action'))
        compatibility = {'state': 'not_required', 'reachable': None, 'compatible': True,
                         'reason': 'contract query uses installed package resources only'}
        return cls._result(normalized, 'read', {}, compatibility, True,
                           cls._plan_sha256(normalized), result, not normalized['dry_run'])

    @staticmethod
    def _validate_payload(action, payload):
        try:
            return validate_payload(action, payload)
        except ContractError as exc:
            raise CCSRequestError(exc.code, exc.message, field_errors=exc.field_errors) from exc

    @classmethod
    def _audit_filters(cls, payload: dict[str, Any]) -> dict[str, Any]:
        if set(payload) - {'request_id', 'action', 'outcome', 'limit', 'operation_id', 'worker_id', 'run_id'}:
            raise CCSRequestError('INVALID_REQUEST', 'gateway.audit contains unsupported filters')
        filters = dict(payload)
        for field in ('request_id', 'operation_id', 'worker_id', 'run_id'):
            if field in filters and not cls._safe_id(filters[field]):
                raise CCSRequestError('INVALID_REQUEST', f'gateway.audit {field} filter is invalid')
        if 'action' in filters and (not isinstance(filters['action'], str) or not filters['action']):
            raise CCSRequestError('INVALID_REQUEST', 'gateway.audit action filter is invalid')
        if 'outcome' in filters and filters['outcome'] not in {'planned', 'started', 'unknown', 'succeeded', 'failed'}:
            raise CCSRequestError('INVALID_REQUEST', 'gateway.audit outcome filter is invalid')
        limit = filters.get('limit', 50)
        if type(limit) is not int or not 1 <= limit <= 200:
            raise CCSRequestError('INVALID_REQUEST', 'gateway.audit limit must be between 1 and 200')
        filters['limit'] = limit
        return filters

    @staticmethod
    def _safe_id(value: Any) -> bool:
        return isinstance(value, str) and _SAFE_ID.fullmatch(value) is not None

    def _run_worker(self, run_id, explicit_worker):
        if explicit_worker is not None:
            if not self._safe_id(explicit_worker):
                raise CCSRequestError('INVALID_WORKER_SELECTION', 'worker_id is invalid')
            return explicit_worker
        try:
            recorded = self.repository.get_run(run_id).worker
        except KeyError:
            recorded = None
        if recorded is not None:
            return recorded
        if self.worker_directory.size > 1:
            raise CCSRequestError(
                'WORKER_SELECTION_REQUIRED', 'unregistered Run observation requires an explicit worker_id',
            )
        return self.worker_directory.default_worker_id

    def _selected_worker(self, explicit_worker):
        if explicit_worker is not None:
            if not self._safe_id(explicit_worker):
                raise CCSRequestError('INVALID_WORKER_SELECTION', 'worker_id is invalid')
            return explicit_worker
        if self.worker_directory.size > 1:
            raise CCSRequestError(
                'WORKER_SELECTION_REQUIRED', 'multi-worker operation requires an upper-Agent worker_id selection',
            )
        return self.worker_directory.default_worker_id

    def _route_run(self, prepared, required, *, allow_legacy):
        worker_id = self._run_worker(prepared['run_id'], prepared.get('worker_id'))
        prepared['worker_id'] = worker_id
        prepared['worker_plan'] = self.worker_directory.operation_plan(
            worker_id, required, allow_legacy=allow_legacy,
        )

    @classmethod
    def _run_cursor(cls, payload, cursor):
        if (set(payload) - {'run_id', cursor, 'worker_id', 'limit', 'max_bytes'} or
                not {'run_id', cursor} <= set(payload) or not cls._safe_id(payload.get('run_id'))):
            raise CCSRequestError('INVALID_REQUEST', f'run request requires run_id and {cursor}')
        value = payload[cursor]
        if type(value) is not int or value < 0:
            raise CCSRequestError('INVALID_REQUEST', f'{cursor} must be a nonnegative integer')
        return {'run_id': payload['run_id'], cursor: value, 'worker_id': payload.get('worker_id'),
                **cls._page_options(payload)}

    @staticmethod
    def _page_options(payload):
        paged = bool({'limit', 'max_bytes', 'cursor'} & set(payload))
        return {'paged': paged, 'limit': payload.get('limit', 100),
                'max_bytes': payload.get('max_bytes', 1024 * 1024)}

    @classmethod
    def _log_request(cls, payload):
        if (set(payload) - {'run_id', 'stream', 'after_offset', 'worker_id'} or
                not {'run_id', 'stream', 'after_offset'} <= set(payload) or
                not cls._safe_id(payload.get('run_id'))):
            raise CCSRequestError('INVALID_REQUEST', 'run.logs requires run_id, stream, and after_offset')
        if payload.get('stream') not in {'stdout', 'stderr'}:
            raise CCSRequestError('INVALID_REQUEST', 'run.logs stream must be stdout or stderr')
        if type(payload.get('after_offset')) is not int or payload['after_offset'] < 0:
            raise CCSRequestError('INVALID_REQUEST', 'run.logs after_offset must be a nonnegative integer')
        return dict(payload)

    @classmethod
    def _artifact_request(cls, payload):
        if set(payload) - {'run_id', 'path', 'worker_id', 'cursor', 'limit', 'max_bytes'} or not cls._safe_id(payload.get('run_id')):
            raise CCSRequestError('INVALID_REQUEST', 'run.artifacts requires run_id and optional path')
        path = payload.get('path')
        if path is not None and (not isinstance(path, str) or not path.startswith('/') or '\x00' in path):
            raise CCSRequestError('INVALID_REQUEST', 'artifact path must be an absolute POSIX path')
        return {**payload, **cls._page_options(payload)}

    @staticmethod
    def _workspace_request(payload):
        if (set(payload) - {'request', 'worker_id'} or 'request' not in payload or
                not isinstance(payload.get('request'), dict)):
            raise CCSRequestError('INVALID_REQUEST', 'workspace.execute requires one request object')
        request = dict(payload['request'])
        action = request.get('action')
        if action in {'inspect', 'diff', 'changes'}:
            return request, 'read'
        if action in {'stage', 'apply', 'restore', 'discard'}:
            return request, 'mutation'
        raise CCSRequestError('INVALID_REQUEST', 'unsupported workspace action')

    @staticmethod
    def _snapshot_result(snapshot) -> dict[str, Any]:
        result = dict(snapshot.__dict__)
        if hasattr(result.get('status'), 'value'):
            result['status'] = result['status'].value
        return result

    @staticmethod
    def _event_result(event) -> dict[str, Any]:
        return dict(event.__dict__)

    @staticmethod
    def _run_result(run: Run) -> dict[str, Any]:
        return {
            'id': run.id,
            'status': run.status.value,
            'spec': run.spec.to_dict(),
            'worker': run.worker,
            'parent_run_id': run.parent_run_id,
            'idempotency_key': run.idempotency_key,
            'attempt': run.attempt,
            'created_at': run.created_at,
            'updated_at': run.updated_at,
            'started_at': run.started_at,
            'finished_at': run.finished_at,
            'exit_code': run.exit_code,
        }

    @staticmethod
    def _request(request: Any) -> dict[str, Any]:
        try:
            validate_request(request)
        except ContractError as exc:
            raise CCSRequestError(exc.code, exc.message, field_errors=exc.field_errors) from exc
        return dict(request)

    @staticmethod
    def _plan_sha256(request: dict[str, Any]) -> str:
        plan = {key: request[key] for key in ('schema_version', 'request_id', 'actor', 'action', 'payload')}
        raw = json.dumps(plan, ensure_ascii=True, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
        return hashlib.sha256(raw).hexdigest()

    @staticmethod
    def _result(request, classification, required, compatibility, executable, plan_sha256, result, executed):
        return {
            'schema_version': 1,
            'request_id': request['request_id'],
            'action': request['action'],
            'classification': classification,
            'dry_run': request['dry_run'],
            'executed': executed,
            'executable': executable,
            'plan_sha256': plan_sha256,
            'required_capabilities': required,
            'compatibility': compatibility,
            'result': result,
        }
