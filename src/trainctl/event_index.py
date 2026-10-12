"""Disposable seek index for the authoritative append-only events.jsonl file."""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from uuid import uuid4


def synchronize(path: Path, *, repair_tail: bool = False):
    """Called under events.lock. Stream a legacy file once; index later appends only.

    A newline commits a JSONL record. Incomplete trailing bytes are never delivered;
    before a new append they are preserved in a fragment file and removed from the
    log. The log is fsynced before the disposable index transaction is committed.
    """
    db = sqlite3.connect(path.parent / 'events-index.sqlite3')
    try:
        db.executescript('''CREATE TABLE IF NOT EXISTS offsets
            (seq INTEGER PRIMARY KEY, offset INTEGER NOT NULL, length INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS metadata
            (id INTEGER PRIMARY KEY CHECK(id=1), indexed_bytes INTEGER NOT NULL,
             last_seq INTEGER NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL,
             observed_size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL);''')
        stat = path.stat() if path.exists() else None
        current = db.execute('SELECT indexed_bytes,last_seq,device,inode,observed_size,mtime_ns '
                             'FROM metadata WHERE id=1').fetchone()
        size = stat.st_size if stat else 0
        identity = (stat.st_dev, stat.st_ino) if stat else (0, 0)
        rebuild = (current is None or current[0] > size or current[2:4] != identity or
                   (current[4] == size and stat and current[5] != stat.st_mtime_ns))
        if rebuild:
            db.execute('DELETE FROM offsets')
            offset, last_seq = 0, 0
        else:
            offset, last_seq = current[:2]
        if path.exists():
            with path.open('rb') as handle:
                handle.seek(offset)
                while raw := handle.readline():
                    if not raw.endswith(b'\n'):
                        if repair_tail:
                            fragment = path.with_name('events-fragment-' + uuid4().hex + '.bin')
                            with fragment.open('xb') as discarded:
                                discarded.write(raw)
                                discarded.flush()
                                os.fsync(discarded.fileno())
                            with path.open('r+b') as recovered:
                                recovered.truncate(offset)
                                recovered.flush()
                                os.fsync(recovered.fileno())
                        break
                    event = json.loads(raw)
                    if (not isinstance(event, dict) or type(event.get('seq')) is not int or
                            event['seq'] != last_seq + 1):
                        raise ValueError('events.jsonl contains a non-contiguous event sequence')
                    last_seq = event['seq']
                    db.execute('INSERT INTO offsets VALUES (?,?,?)', (last_seq, offset, len(raw)))
                    offset += len(raw)
        stat = path.stat() if path.exists() else None
        db.execute('INSERT OR REPLACE INTO metadata VALUES (1,?,?,?,?,?,?)',
                   (offset, last_seq, stat.st_dev if stat else 0, stat.st_ino if stat else 0,
                    stat.st_size if stat else 0, stat.st_mtime_ns if stat else 0))
        db.commit()
        return db, offset, last_seq
    except BaseException:
        db.rollback()
        db.close()
        raise


def record_append(db, path: Path, seq: int, offset: int, length: int):
    stat = path.stat()
    db.execute('INSERT INTO offsets VALUES (?,?,?)', (seq, offset, length))
    db.execute('UPDATE metadata SET indexed_bytes=?,last_seq=?,device=?,inode=?,observed_size=?,mtime_ns=? '
               'WHERE id=1', (offset + length, seq, stat.st_dev, stat.st_ino,
                              stat.st_size, stat.st_mtime_ns))
    db.commit()
