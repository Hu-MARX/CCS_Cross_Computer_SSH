"""Online telemetry backup and conservative, bounded retention maintenance.

Never touches logs, research files, artifacts, lifecycle events or alert outbox.
Only finalized-run/worker samples older than the retention window are eligible.
"""
from __future__ import annotations

import os
import re
import shutil
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Thread
from uuid import uuid4

from controller.domain import utc_now
from .locking import file_lock
from .telemetry import Telemetry


def storage_policy(config):
    raw = config.get('storage', {})
    policy = {'sample_retention_days': raw.get('sample_retention_days', 30),
              'backup_keep': raw.get('backup_keep', 7),
              'maintenance_interval_seconds': raw.get('maintenance_interval_seconds', 86400)}
    if any(type(n) is not int or n <= 0 for n in policy.values()):
        raise ValueError('storage policy values must be positive integers')
    return policy


def _backup(telemetry, directory, reserve):
    directory.mkdir(exist_ok=True)
    if directory.is_symlink():
        raise ValueError('Backup directory must not be a symlink')
    if shutil.disk_usage(directory).free < reserve + 2 * telemetry.path.stat().st_size:
        raise RuntimeError('BACKUP_DISK_LOW: preserving samples; insufficient backup space')
    name = datetime.now(timezone.utc).strftime('telemetry-%Y%m%dT%H%M%S-') + uuid4().hex + '.sqlite3'
    target = directory / name
    temporary = target.with_suffix('.partial')
    deadline = time.monotonic() + 30

    def progress(*_):
        if time.monotonic() > deadline:
            raise TimeoutError('Online backup exceeded 30 seconds')

    try:
        with telemetry.db() as source:
            destination = sqlite3.connect(temporary)
            try:
                source.backup(destination, pages=128, progress=progress, sleep=0.01)
                if destination.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                    raise RuntimeError('Backup integrity check failed')
                high_water = destination.execute('SELECT value FROM sample_sequence WHERE singleton=1').fetchone()[0]
            finally:
                destination.close()
        with temporary.open('r+b') as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        return target, high_water
    finally:
        temporary.unlink(missing_ok=True)


def maintain(store, *, force=False):
    """Return durable status. A failed backup prevents any sample deletion."""
    policy = storage_policy(store.config())
    state_path = store.root / 'storage-health.json'
    with file_lock(store.root / 'storage.lock', timeout=0):
        previous = store._read_json(state_path, {})
        at = datetime.now(timezone.utc)
        if not force and previous.get('completed_at'):
            elapsed = (at - datetime.fromisoformat(previous['completed_at'].replace('Z', '+00:00'))).total_seconds()
            if elapsed < policy['maintenance_interval_seconds']:
                return previous
        telemetry = Telemetry(store.root)
        try:
            eligible = ['_worker']
            for run in store.runs.iterdir():
                if not re.fullmatch(r'run_[a-f0-9]{32}', run.name) or run.is_symlink() or not run.is_dir():
                    continue
                # This marker is written only after terminal status + execution cleanup.
                if (run / 'monitor-finalized.json').is_file():
                    eligible.append(run.name)
            backup, high_water = _backup(telemetry, store.root / 'telemetry-backups', store.config().get('min_free_bytes', 5 * 1024**3))
            cutoff = (at - timedelta(days=policy['sample_retention_days'])).isoformat().replace('+00:00', 'Z')
            with telemetry.db() as db:
                db.execute('CREATE TEMP TABLE eligible_runs (run_id TEXT PRIMARY KEY)')
                db.executemany('INSERT INTO eligible_runs VALUES(?)', ((run,) for run in eligible))
                deleted = db.execute('''DELETE FROM samples WHERE id IN (
                    SELECT s.id FROM samples s JOIN eligible_runs e ON s.run_id=e.run_id
                    WHERE s.created_at < ? AND s.id <= ?
                    AND s.id < (SELECT MAX(last.id) FROM samples last WHERE last.run_id=s.run_id)
                    ORDER BY s.id LIMIT 50000)''', (cutoff, high_water)).rowcount
            # SQLite reuses freed pages. No blocking VACUUM against a live recorder.
            with telemetry.db() as db:
                db.execute('PRAGMA wal_checkpoint(PASSIVE)')
            backups = sorted((path for path in backup.parent.iterdir()
                if re.fullmatch(r'telemetry-\d{8}T\d{6}-[a-f0-9]{32}\.sqlite3', path.name)
                and path.is_file() and not path.is_symlink() and path != backup),
                key=lambda path: (path.stat().st_mtime_ns, path.name))
            # Only our strictly named snapshots are pruned, after a checked new backup.
            removed = []
            for path in backups[:max(0, len(backups) - policy['backup_keep'] + 1)]:
                path.unlink()
                removed.append(path.name)
            result = {'state': 'healthy', 'completed_at': utc_now(), 'backup_path': str(backup),
                      'backup_high_water_id': high_water, 'deleted_samples': deleted,
                      'cutoff': cutoff, 'removed_backups': removed, 'policy': policy}
            store._atomic_json(state_path, result)
            telemetry.resolve('_worker', 'storage_failed', utc_now())
            return result
        except Exception as exc:
            store._atomic_json(state_path, {**previous, 'state': 'failed', 'error_at': utc_now(), 'error': str(exc)})
            telemetry.alert('_worker', 'storage_failed', utc_now(), {'error': str(exc)})
            raise


def start_maintenance(store):
    """Caller holds at most one thread; worker heartbeats never wait for backups."""
    def run():
        try:
            maintain(store)
        except Exception as exc:
            # Failure is persisted by maintain; keep the observation loop alive.
            print(f'Storage maintenance failed: {exc}', flush=True)
    thread = Thread(target=run, daemon=True)
    thread.start()
    return thread
