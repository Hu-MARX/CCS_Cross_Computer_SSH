"""Atomic checkpoint bundles, checked retention and independent resume copies.

Checkpoint deserialization belongs to a caller-supplied validator in the trusted
training environment, never to the monitoring daemon. SHA256 detects corruption,
not malicious pickle payloads: only use self-produced, access-controlled bundles.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from pathlib import Path
from typing import Callable
from uuid import uuid4

from controller.domain import utc_now
from .locking import file_lock
from .run_store import RunStore


def sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def sync_directory(path: Path) -> None:
    if os.name == 'posix':
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


class CheckpointStore:
    """One writer per directory; metadata publication is the commit point."""
    def __init__(self, directory: str | Path, *, keep: int = 3, min_free_bytes: int = 5 * 1024**3):
        self.directory = Path(directory).absolute()
        if self.directory.resolve() != self.directory:
            raise ValueError('Checkpoint store must not traverse symlinks')
        if type(keep) is not int or keep < 2 or type(min_free_bytes) is not int or min_free_bytes < 0:
            raise ValueError('keep must be >=2 and min_free_bytes >=0')
        self.directory.mkdir(parents=True, exist_ok=True)
        self.keep, self.min_free_bytes = keep, min_free_bytes

    def records(self) -> list[dict]:
        result = []
        for path in self.directory.glob('ckpt_*.json'):
            if not re.fullmatch(r'ckpt_[a-f0-9]{32}\.json', path.name) or path.is_symlink():
                continue
            try:
                value = json.loads(path.read_text(encoding='utf-8'))
                if value['id'] != path.stem or value['file'] != path.stem + '.ckpt':
                    continue
                if not isinstance(value['created_at'], str) or not re.fullmatch('[a-f0-9]{64}', value['sha256']):
                    continue
                if type(value['size']) is not int or value['size'] <= 0:
                    continue
                result.append(value)
            except (ValueError, KeyError, TypeError):
                continue
        return sorted(result, key=lambda item: (item['created_at'], item['id']), reverse=True)

    def verify(self, record: dict, validator: Callable[[Path], dict] | None = None) -> dict:
        filename = record['file']
        if not re.fullmatch(r'ckpt_[a-f0-9]{32}\.ckpt', filename):
            raise ValueError('Invalid checkpoint filename')
        path = self.directory / filename
        if path.is_symlink() or not path.is_file() or path.stat().st_size != record['size']:
            raise ValueError('Checkpoint missing, unsafe, or size mismatch')
        if sha256(path) != record['sha256']:
            raise ValueError('Checkpoint SHA256 mismatch')
        return validator(path) if validator else record['state']

    def commit(self, save: Callable[[Path], None], validator: Callable[[Path], dict], *, metadata: dict) -> dict:
        with file_lock(self.directory / 'checkpoints.lock'):
            previous = self.records()
            estimate = max((item['size'] for item in previous), default=0)
            if shutil.disk_usage(self.directory).free < self.min_free_bytes + estimate:
                raise OSError('CHECKPOINT_DISK_LOW: previous recovery points preserved')
            identifier = 'ckpt_' + uuid4().hex
            temporary = self.directory / (identifier + '.partial')
            target = self.directory / (identifier + '.ckpt')
            try:
                save(temporary)
                if temporary.is_symlink() or not temporary.is_file() or temporary.stat().st_size == 0:
                    raise ValueError('Checkpoint save produced no safe data')
                state = validator(temporary)
                with temporary.open('r+b') as stream:
                    os.fsync(stream.fileno())
                record = {'id': identifier, 'file': target.name, 'created_at': utc_now(),
                          'size': temporary.stat().st_size, 'sha256': sha256(temporary),
                          'state': state, 'metadata': metadata}
                # Validate JSON before making the binary visible.
                json.dumps(record, allow_nan=False)
                os.replace(temporary, target)
                sync_directory(self.directory)
                RunStore._atomic_json(target.with_suffix('.json'), record)
                sync_directory(self.directory)
            finally:
                temporary.unlink(missing_ok=True)
            # Keep enough currently hash-valid points, plus explicitly pinned milestones.
            valid = []
            for candidate in self.records():
                try:
                    self.verify(candidate)
                    valid.append(candidate)
                except (OSError, ValueError):
                    continue
            removed = []
            for candidate in valid[self.keep:]:
                if candidate.get('metadata', {}).get('pinned'):
                    continue
                path = self.directory / candidate['file']
                # Only this module's exact validated pair, never another user's weights.
                path.with_suffix('.json').unlink()
                path.unlink()
                removed.append(candidate['id'])
            sync_directory(self.directory)
            return {**record, 'pruned_ids': removed}

    def restore_copy(self, destination: Path, validator: Callable[[Path], dict], *, contract: str) -> dict:
        """Select newest compatible valid point; copy under lock before returning.

        The resulting independent file is immune to source-run retention. Refuse
        an existing destination; never silently overwrite another run's state.
        """
        destination = Path(destination).absolute()
        if destination.resolve() != destination or destination.exists():
            raise ValueError('Resume destination must be a new non-symlink file')
        rejected = []
        with file_lock(self.directory / 'checkpoints.lock'):
            selected = None
            for record in self.records():
                try:
                    if record.get('metadata', {}).get('contract') != contract:
                        raise ValueError('Experiment contract mismatch')
                    state = self.verify(record, validator)
                except (OSError, ValueError, RuntimeError, EOFError) as exc:
                    rejected.append({'id': record['id'], 'reason': str(exc)})
                    continue
                selected = record
                break
            if selected is None:
                raise ValueError('NO_VALID_CHECKPOINT: ' + json.dumps(rejected))
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = destination.with_name(destination.name + '.' + uuid4().hex + '.partial')
            try:
                shutil.copyfile(self.directory / selected['file'], temporary)
                if sha256(temporary) != selected['sha256']:
                    raise ValueError('Resume copy digest mismatch')
                with temporary.open('r+b') as stream:
                    os.fsync(stream.fileno())
                # Exclusive publication, even if another reader chose this destination.
                os.link(temporary, destination)
                sync_directory(destination.parent)
            finally:
                temporary.unlink(missing_ok=True)
            return {'selected': selected, 'state': state, 'rejected': rejected, 'path': str(destination)}
