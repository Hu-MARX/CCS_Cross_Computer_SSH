"""Indexed on-worker observations and a durable, deduplicated local alert inbox."""
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


class Telemetry:
    def __init__(self, root):
        self.path = Path(root) / 'telemetry.sqlite3'
        with self.db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS samples (
                    id INTEGER PRIMARY KEY, run_id TEXT NOT NULL,
                    created_at TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS sample_run ON samples(run_id,id);
                CREATE INDEX IF NOT EXISTS sample_age ON samples(created_at);
                CREATE TABLE IF NOT EXISTS sample_sequence (singleton INTEGER PRIMARY KEY CHECK(singleton=1), value INTEGER NOT NULL);
                INSERT OR IGNORE INTO sample_sequence VALUES(1,0);
                UPDATE sample_sequence SET value=MAX(value, (SELECT COALESCE(MAX(id),0) FROM samples)) WHERE singleton=1;
                CREATE TABLE IF NOT EXISTS alerts (
                    key TEXT PRIMARY KEY, run_id TEXT NOT NULL, kind TEXT NOT NULL,
                    opened_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    resolved_at TEXT, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS notification_outbox (
                    id INTEGER PRIMARY KEY, alert_key TEXT NOT NULL,
                    transition TEXT NOT NULL, created_at TEXT NOT NULL,
                    payload TEXT NOT NULL, delivered_at TEXT);
            ''')

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=10)
        try:
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA journal_mode=WAL')
            with db:
                yield db
        finally:
            db.close()

    def record(self, run_id, at, payload):
        with self.db() as db:
            # Durable high-water mark survives retention, including an empty table.
            db.execute('UPDATE sample_sequence SET value=value+1 WHERE singleton=1')
            db.execute('INSERT INTO samples(id,run_id,created_at,payload) SELECT value,?,?,? FROM sample_sequence WHERE singleton=1',
                       (run_id, at, json.dumps(payload, allow_nan=False)))

    def alert(self, run_id, kind, at, payload):
        key = f'{run_id}:{kind}'
        with self.db() as db:
            previous = db.execute('SELECT resolved_at FROM alerts WHERE key=?', (key,)).fetchone()
            if previous is None or previous['resolved_at'] is not None:
                db.execute('INSERT INTO notification_outbox(alert_key,transition,created_at,payload) VALUES(?,?,?,?)',
                           (key, 'opened', at, json.dumps(payload)))
            db.execute('''INSERT INTO alerts VALUES(?,?,?,?,?,NULL,?)
                ON CONFLICT(key) DO UPDATE SET updated_at=excluded.updated_at,
                    resolved_at=NULL,payload=excluded.payload''',
                       (key, run_id, kind, at, at, json.dumps(payload)))

    def resolve(self, run_id, kind, at):
        with self.db() as db:
            previous = db.execute('SELECT payload FROM alerts WHERE key=? AND resolved_at IS NULL',
                                  (f'{run_id}:{kind}',)).fetchone()
            if previous:
                db.execute('INSERT INTO notification_outbox(alert_key,transition,created_at,payload) VALUES(?,?,?,?)',
                           (f'{run_id}:{kind}', 'resolved', at, previous['payload']))
            db.execute('UPDATE alerts SET resolved_at=?,updated_at=? WHERE key=? AND resolved_at IS NULL',
                       (at, at, f'{run_id}:{kind}'))

    def samples(self, run_id, after=0, limit=200):
        if after < 0 or not 1 <= limit <= 1000:
            raise ValueError('Invalid sample cursor/limit')
        with self.db() as db:
            rows = db.execute('SELECT * FROM samples WHERE run_id=? AND id>? ORDER BY id LIMIT ?',
                              (run_id, after, limit)).fetchall()
        return [{**dict(row), 'payload': json.loads(row['payload'])} for row in rows]

    def alerts(self):
        with self.db() as db:
            rows = db.execute('SELECT * FROM alerts ORDER BY updated_at DESC LIMIT 200').fetchall()
        return [{**dict(row), 'payload': json.loads(row['payload'])} for row in rows]
