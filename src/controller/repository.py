from __future__ import annotations

import json
import hashlib
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable
from uuid import uuid4

from .domain import ExperimentSpec, Run, RunEvent, RunStatus, utc_now


class SubmissionError(RuntimeError):
    """A mutation failure whose outcome and correlation survive adapter layers."""

    def __init__(self, code, message, *, execution_state='unknown', operation_id=None,
                 worker_id=None, run_id=None, idempotency_key=None):
        super().__init__(message)
        self.code, self.message, self.retryable = code, message, False
        self.execution_state = execution_state
        self.operation_id, self.worker_id, self.run_id = operation_id, worker_id, run_id
        self.idempotency_key = idempotency_key


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _digest(value):
    return hashlib.sha256(_canonical(value).encode('utf-8')).hexdigest()


_OPERATION_STATES = {'pending', 'started', 'unknown', 'confirmed', 'rejected', 'not_sent'}


class RunRepository:
    """Deep persistence module: callers see Runs and Events, not SQLite details."""

    @staticmethod
    def catalog(database, worker, limit=20):
        """Read a bounded local catalog without initializing/migrating/writing the DB."""
        from .results import descriptor
        if not 1 <= limit <= 100:
            raise ValueError('Invalid catalog limit')
        path = Path(database).resolve()
        if not path.exists():
            return []
        db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)
        try:
            rows = db.execute('''SELECT id, spec_json, worker, created_at, started_at, status, updated_at
                                 FROM runs WHERE worker=? ORDER BY created_at DESC LIMIT ?''',
                              (worker, limit)).fetchall()
            return [descriptor(run_id, json.loads(spec), host, created_at=created, started_at=started,
                               recorded_status=status, updated_at=updated)
                    for run_id, spec, host, created, started, status, updated in rows]
        finally:
            db.close()

    def __init__(self, database: str | Path):
        self.database = str(database)
        self._memory_connection: sqlite3.Connection | None = None
        if self.database != ":memory:":
            Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        if self.database == ":memory:":
            if self._memory_connection is None:
                self._memory_connection = sqlite3.connect(":memory:")
                self._memory_connection.row_factory = sqlite3.Row
            return self._memory_connection
        connection = sqlite3.connect(self.database)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def _db(self):
        connection = self._connect()
        try:
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            if self.database != ":memory:":
                connection.close()

    def _initialize(self) -> None:
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    parent_run_id TEXT,
                    worker TEXT,
                    status TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    idempotency_key TEXT UNIQUE,
                    attempt INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    exit_code INTEGER
                );
                CREATE TABLE IF NOT EXISTS events (
                    run_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    remote_seq INTEGER,
                    PRIMARY KEY (run_id, seq),
                    UNIQUE (run_id, remote_seq)
                );
                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    decision_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operations (
                    operation_id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    request_id TEXT,
                    actor_json TEXT,
                    worker_id TEXT,
                    idempotency_key TEXT UNIQUE,
                    spec_sha256 TEXT,
                    spec_json TEXT,
                    plan_sha256 TEXT,
                    intent_sha256 TEXT NOT NULL,
                    state TEXT NOT NULL,
                    run_id TEXT,
                    error_code TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operation_requests (
                    identity_sha256 TEXT PRIMARY KEY,
                    operation_id TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    actor_json TEXT,
                    intent_sha256 TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS operation_requests_request_id ON operation_requests(request_id);
                CREATE TABLE IF NOT EXISTS repository_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS remote_event_watermarks (
                    run_id TEXT PRIMARY KEY,
                    remote_seq INTEGER NOT NULL
                );
            """)
            # Additive migration: the original tables remain readable by older controllers.
            # A committed historical Run is evidence that its submission was accepted.
            if db.execute('SELECT 1 FROM repository_migrations WHERE version=1').fetchone() is None:
                db.execute('BEGIN IMMEDIATE')
                for row in db.execute('SELECT * FROM runs WHERE idempotency_key IS NOT NULL').fetchall():
                    spec = ExperimentSpec.from_dict(json.loads(row['spec_json']))
                    canonical = _canonical(spec.to_dict())
                    intent = _digest({'action': 'run.submit', 'worker_id': row['worker'],
                                      'idempotency_key': row['idempotency_key'], 'spec': spec.to_dict()})
                    db.execute('''INSERT OR IGNORE INTO operations VALUES
                               (?, 'run.submit', NULL, NULL, ?, ?, ?, ?, NULL, ?, 'confirmed', ?, NULL, ?, ?)''',
                               ('op_' + uuid4().hex, row['worker'], row['idempotency_key'],
                                _digest(spec.to_dict()), canonical, intent, row['id'],
                                row['created_at'], row['updated_at']))
                db.execute('INSERT OR IGNORE INTO repository_migrations VALUES (1, ?)', (utc_now(),))
            # One streaming SQL migration; subsequent polling reads one row.
            if db.execute('SELECT 1 FROM repository_migrations WHERE version=2').fetchone() is None:
                db.execute('''INSERT OR REPLACE INTO remote_event_watermarks
                              SELECT run_id, MAX(remote_seq) FROM events
                              WHERE remote_seq IS NOT NULL GROUP BY run_id''')
                db.execute('INSERT OR IGNORE INTO repository_migrations VALUES (2, ?)', (utc_now(),))

    @staticmethod
    def _operation(row):
        if row is None:
            return None
        result = {key: row[key] for key in row.keys() if key not in {'spec_json', 'actor_json', 'intent_sha256'}}
        result['actor'] = json.loads(row['actor_json']) if row['actor_json'] else None
        result['execution_state'] = ('not_sent' if row['state'] in {'pending', 'not_sent'} else
                                     'unknown' if row['state'] in {'started', 'unknown'} else row['state'])
        return result

    @staticmethod
    def _correlated_error(code, message, operation, *, execution_state='not_sent'):
        return SubmissionError(code, message, execution_state=execution_state,
                               operation_id=operation['operation_id'], worker_id=operation['worker_id'],
                               run_id=operation['run_id'], idempotency_key=operation['idempotency_key'])

    @staticmethod
    def _request_identity(request_id, actor):
        return _digest({'request_id': request_id, 'actor': actor}) if request_id is not None else None

    def _attach_request(self, db, operation, request_id, actor, intent):
        identity = self._request_identity(request_id, actor)
        if identity is None:
            return
        existing = db.execute('SELECT * FROM operation_requests WHERE identity_sha256=?', (identity,)).fetchone()
        if existing is not None:
            if existing['operation_id'] != operation['operation_id'] or existing['intent_sha256'] != intent:
                original = db.execute('SELECT * FROM operations WHERE operation_id=?',
                                      (existing['operation_id'],)).fetchone()
                raise self._correlated_error('OPERATION_CONFLICT', 'request identity already identifies a different mutation',
                                             original)
            return
        db.execute('INSERT INTO operation_requests VALUES (?, ?, ?, ?, ?)',
                   (identity, operation['operation_id'], request_id,
                    _canonical(actor) if actor is not None else None, intent))

    def reserve_submission(self, spec, worker_id, idempotency_key, *, request_id=None,
                           actor=None, plan_sha256=None):
        """Reserve a key before remote effects; availability is not a dispatch claim."""
        normalized = spec.to_dict()
        spec_sha256 = _digest(normalized)
        intent = _digest({'action': 'run.submit', 'worker_id': worker_id,
                          'idempotency_key': idempotency_key, 'spec': normalized})
        request_intent = _digest({'intent': intent, 'plan_sha256': plan_sha256})
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM operations WHERE idempotency_key=?', (idempotency_key,)).fetchone()
            if row is None:
                legacy = db.execute('SELECT * FROM runs WHERE idempotency_key=?', (idempotency_key,)).fetchone()
                if legacy is not None:
                    legacy_spec = ExperimentSpec.from_dict(json.loads(legacy['spec_json'])).to_dict()
                    if legacy['worker'] != worker_id or _digest(legacy_spec) != spec_sha256:
                        raise SubmissionError('IDEMPOTENCY_CONFLICT',
                                              'idempotency key identifies a different worker or ExperimentSpec',
                                              execution_state='not_sent', worker_id=worker_id, run_id=legacy['id'],
                                              idempotency_key=idempotency_key)
                now = utc_now()
                db.execute('INSERT INTO operations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)',
                           ('op_' + uuid4().hex, 'run.submit', request_id,
                            _canonical(actor) if actor is not None else None, worker_id, idempotency_key,
                            spec_sha256, _canonical(normalized), plan_sha256, intent,
                            'confirmed' if legacy is not None else 'pending',
                            legacy['id'] if legacy is not None else None, now, now))
                row = db.execute('SELECT * FROM operations WHERE idempotency_key=?', (idempotency_key,)).fetchone()
            if row['worker_id'] != worker_id or row['spec_sha256'] != spec_sha256:
                raise self._correlated_error('IDEMPOTENCY_CONFLICT',
                                             'idempotency key identifies a different worker or ExperimentSpec', row)
            self._attach_request(db, row, request_id, actor, request_intent)
            return self._operation(row), row['state'] in {'pending', 'not_sent'}

    def reserve_operation(self, action, request_id, actor, worker_id, plan_sha256):
        """Reserve generic mutation intent without granting permission to dispatch."""
        intent = _digest({'action': action, 'worker_id': worker_id, 'plan_sha256': plan_sha256})
        identity = self._request_identity(request_id, actor)
        if identity is None:
            raise ValueError('generic operations require request_id')
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            reference = db.execute('SELECT * FROM operation_requests WHERE identity_sha256=?', (identity,)).fetchone()
            if reference is not None:
                row = db.execute('SELECT * FROM operations WHERE operation_id=?', (reference['operation_id'],)).fetchone()
                if reference['intent_sha256'] != intent:
                    raise self._correlated_error('OPERATION_CONFLICT',
                                                 'request identity already identifies a different mutation', row)
                return self._operation(row), row['state'] in {'pending', 'not_sent'}
            now = utc_now()
            operation_id = 'op_' + uuid4().hex
            db.execute('''INSERT INTO operations VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?,
                       'pending', NULL, NULL, ?, ?)''',
                       (operation_id, action, request_id, _canonical(actor) if actor is not None else None,
                        worker_id, plan_sha256, intent, now, now))
            row = db.execute('SELECT * FROM operations WHERE operation_id=?', (operation_id,)).fetchone()
            self._attach_request(db, row, request_id, actor, intent)
            return self._operation(row), True

    def start_operation(self, action, request_id, actor, worker_id, plan_sha256):
        operation, _ = self.reserve_operation(action, request_id, actor, worker_id, plan_sha256)
        dispatch = self.begin_operation(operation['operation_id'])
        return self.get_operation(operation_id=operation['operation_id']), dispatch

    def begin_operation(self, operation_id):
        """Only the successful compare-and-swap caller may dispatch remote effects."""
        with self._db() as db:
            cursor = db.execute("UPDATE operations SET state='started', error_code=NULL, updated_at=? "
                                "WHERE operation_id=? AND state IN ('pending', 'not_sent')", (utc_now(), operation_id))
            return cursor.rowcount == 1

    begin_submission = begin_operation

    def get_operation(self, operation_id=None, idempotency_key=None, request_id=None, *, actor=None):
        selectors = [operation_id is not None, idempotency_key is not None, request_id is not None]
        if sum(selectors) != 1:
            raise ValueError('select exactly one operation_id, idempotency_key, or request_id')
        with self._db() as db:
            if request_id is not None:
                if actor is not None:
                    rows = db.execute('''SELECT o.* FROM operations o JOIN operation_requests r
                                      ON o.operation_id=r.operation_id WHERE r.identity_sha256=?''',
                                      (self._request_identity(request_id, actor),)).fetchall()
                else:
                    rows = db.execute('''SELECT DISTINCT o.* FROM operations o JOIN operation_requests r
                                      ON o.operation_id=r.operation_id WHERE r.request_id=?''', (request_id,)).fetchall()
                if len(rows) > 1:
                    raise SubmissionError('AMBIGUOUS_OPERATION', 'request_id matches multiple actor identities',
                                          execution_state='not_sent')
                row = rows[0] if rows else None
            else:
                column, value = ('operation_id', operation_id) if operation_id is not None else ('idempotency_key', idempotency_key)
                row = db.execute(f'SELECT * FROM operations WHERE {column}=?', (value,)).fetchone()
        return self._operation(row)

    def submission_spec(self, operation_id):
        with self._db() as db:
            row = db.execute('SELECT spec_json FROM operations WHERE operation_id=?', (operation_id,)).fetchone()
        if row is None or row['spec_json'] is None:
            raise KeyError(operation_id)
        return ExperimentSpec.from_dict(json.loads(row['spec_json']))

    def mark_operation(self, operation_id, state, *, run_id=None, error_code=None):
        if state not in _OPERATION_STATES:
            raise ValueError('unsupported operation state')
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM operations WHERE operation_id=?', (operation_id,)).fetchone()
            if row is None:
                raise KeyError(operation_id)
            if row['state'] == 'confirmed' and state != 'confirmed':
                return self._operation(row)
            if state == 'pending' and row['state'] not in {'pending', 'not_sent'}:
                raise ValueError('an already started operation cannot become pending')
            if row['run_id'] is not None and run_id is not None and row['run_id'] != run_id:
                raise self._correlated_error('OPERATION_CONFLICT', 'operation has a different confirmed run_id', row)
            db.execute('UPDATE operations SET state=?, run_id=COALESCE(?,run_id), error_code=?, updated_at=? '
                       'WHERE operation_id=?', (state, run_id, error_code, utc_now(), operation_id))
            return self._operation(db.execute('SELECT * FROM operations WHERE operation_id=?', (operation_id,)).fetchone())

    def confirm_submission(self, operation_id, run_id):
        """Commit Run, submitted event and accepted operation in one local transaction."""
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            operation = db.execute('SELECT * FROM operations WHERE operation_id=?', (operation_id,)).fetchone()
            if operation is None or operation['action'] != 'run.submit':
                raise KeyError(operation_id)
            if not isinstance(run_id, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}', run_id):
                raise self._correlated_error('INVALID_REMOTE_RESPONSE', 'worker returned an invalid run_id', operation,
                                             execution_state='unknown')
            if operation['run_id'] is not None and operation['run_id'] != run_id:
                raise self._correlated_error('OPERATION_CONFLICT', 'remote lookup contradicts recorded run_id', operation,
                                             execution_state='unknown')
            spec = ExperimentSpec.from_dict(json.loads(operation['spec_json']))
            existing = db.execute('SELECT * FROM runs WHERE id=? OR idempotency_key=?',
                                  (run_id, operation['idempotency_key'])).fetchall()
            if existing:
                if len(existing) != 1 or existing[0]['id'] != run_id or existing[0]['worker'] != operation['worker_id'] or \
                        _digest(ExperimentSpec.from_dict(json.loads(existing[0]['spec_json'])).to_dict()) != operation['spec_sha256']:
                    raise self._correlated_error('OPERATION_CONFLICT', 'local Run conflicts with accepted submission', operation,
                                                 execution_state='unknown')
            else:
                run = Run.new(spec, worker=operation['worker_id'], idempotency_key=operation['idempotency_key'])
                run.id = run_id
                db.execute('INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                           (run.id, run.parent_run_id, run.worker, run.status.value, _canonical(spec.to_dict()),
                            run.idempotency_key, run.attempt, run.created_at, run.updated_at, None, None, None))
                db.execute('INSERT INTO events VALUES (?, 1, ?, ?, ?, NULL)',
                           (run_id, 'submitted', _canonical({'worker': run.worker, 'operation_id': operation_id}), utc_now()))
            db.execute("UPDATE operations SET state='confirmed', run_id=?, error_code=NULL, updated_at=? "
                       'WHERE operation_id=?', (run_id, utc_now(), operation_id))
        return self.get_run(run_id)

    def create_run(self, run: Run) -> Run:
        with self._db() as db:
            if run.idempotency_key:
                existing = db.execute("SELECT id FROM runs WHERE idempotency_key = ?", (run.idempotency_key,)).fetchone()
                if existing:
                    return self.get_run(existing["id"])
            db.execute(
                "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (run.id, run.parent_run_id, run.worker, run.status.value, json.dumps(run.spec.to_dict()),
                 run.idempotency_key, run.attempt, run.created_at, run.updated_at, run.started_at,
                 run.finished_at, run.exit_code),
            )
        return run

    def get_run(self, run_id: str) -> Run:
        with self._db() as db:
            row = db.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(run_id)
        return Run(
            id=row["id"], spec=ExperimentSpec.from_dict(json.loads(row["spec_json"])),
            status=RunStatus(row["status"]), worker=row["worker"], parent_run_id=row["parent_run_id"],
            idempotency_key=row["idempotency_key"], attempt=row["attempt"], created_at=row["created_at"],
            updated_at=row["updated_at"], started_at=row["started_at"], finished_at=row["finished_at"],
            exit_code=row["exit_code"],
        )

    def find_by_idempotency_key(self, idempotency_key: str) -> Run | None:
        with self._db() as db:
            row = db.execute(
                "SELECT id FROM runs WHERE idempotency_key = ?", (idempotency_key,),
            ).fetchone()
        return self.get_run(row["id"]) if row is not None else None

    def list_active(self) -> list[Run]:
        with self._db() as db:
            ids = [row["id"] for row in db.execute(
                "SELECT id FROM runs WHERE status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED') ORDER BY created_at"
            )]
        return [self.get_run(run_id) for run_id in ids]

    def list_runs(self, worker: str | None = None) -> list[Run]:
        with self._db() as db:
            rows = db.execute("SELECT id FROM runs WHERE (? IS NULL OR worker=?) ORDER BY created_at",
                              (worker, worker)).fetchall()
        return [self.get_run(row["id"]) for row in rows]

    def update_run(self, run: Run) -> None:
        with self._db() as db:
            db.execute(
                """UPDATE runs SET status=?, updated_at=?, started_at=?, finished_at=?, exit_code=? WHERE id=?""",
                (run.status.value, run.updated_at, run.started_at, run.finished_at, run.exit_code, run.id),
            )

    def append_event(self, run_id: str, kind: str, payload: dict, *, remote_seq: int | None = None) -> RunEvent | None:
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            if remote_seq is not None:
                duplicate = db.execute("SELECT seq FROM events WHERE run_id=? AND remote_seq=?", (run_id, remote_seq)).fetchone()
                if duplicate:
                    return None
            row = db.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM events WHERE run_id=?", (run_id,)).fetchone()
            event = RunEvent(run_id=run_id, seq=row["next_seq"], kind=kind, payload=payload, remote_seq=remote_seq)
            db.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)",
                (event.run_id, event.seq, event.kind, json.dumps(event.payload), event.created_at, event.remote_seq),
            )
            if remote_seq is not None:
                db.execute('''INSERT INTO remote_event_watermarks VALUES (?,?)
                              ON CONFLICT(run_id) DO UPDATE SET remote_seq=MAX(remote_seq,excluded.remote_seq)''',
                           (run_id, remote_seq))
        return event

    def remote_event_watermark(self, run_id: str) -> int:
        with self._db() as db:
            row = db.execute('SELECT remote_seq FROM remote_event_watermarks WHERE run_id=?', (run_id,)).fetchone()
        return row['remote_seq'] if row else 0

    def events(self, run_id: str, after_seq: int = 0) -> list[RunEvent]:
        with self._db() as db:
            rows = db.execute("SELECT * FROM events WHERE run_id=? AND seq>? ORDER BY seq", (run_id, after_seq)).fetchall()
        return [RunEvent(run_id=row["run_id"], seq=row["seq"], kind=row["kind"], payload=json.loads(row["payload_json"]),
                         created_at=row["created_at"], remote_seq=row["remote_seq"]) for row in rows]

    def record_decision(self, run_id: str, decision: dict) -> None:
        with self._db() as db:
            db.execute("INSERT INTO decisions(run_id, decision_json, created_at) VALUES (?, ?, ?)",
                       (run_id, json.dumps(decision), utc_now()))
