"""Restart-safe raw log download: the fsynced local file length is the cursor."""
import base64
import hashlib
import json
import os
import re
from pathlib import Path

from trainctl.locking import file_lock


def sync_logs(remote, run_id, stream, cache_root, *, max_pages=32):
    if not re.fullmatch(r'run_[a-f0-9]{32}', run_id) or stream not in {'stdout', 'stderr'}:
        raise ValueError('Invalid run or stream')
    if type(max_pages) is not int or not 1 <= max_pages <= 1000:
        raise ValueError('max_pages must be in 1..1000')
    identity = {'host': remote.host, 'user': remote.user, 'port': remote.port, 'root': remote.remote_root}
    namespace = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:20]
    directory = Path(cache_root).resolve() / namespace / run_id
    directory.mkdir(parents=True, exist_ok=True)
    if directory.is_symlink() or directory.parent.is_symlink():
        raise ValueError('Symlink cache rejected')
    path = directory / f'{stream}.log'
    if path.is_symlink():
        raise ValueError('Symlink log cache rejected')
    downloaded = 0
    eof = False
    with file_lock(directory / f'{stream}.lock'):
        with path.open('ab') as handle:
            offset = handle.tell()
            for _ in range(max_pages):
                result = remote.logs(run_id, stream, offset)
                if result.get('offset') != offset or result.get('run_id') != run_id or result.get('stream') != stream:
                    raise ValueError('Log response identity/cursor mismatch')
                if 'data_base64' not in result:
                    raise ValueError('Worker upgrade required for lossless log sync')
                data = base64.b64decode(result['data_base64'], validate=True)
                if len(data) > 256 * 1024 or result['next_offset'] != offset + len(data):
                    raise ValueError('Invalid log response length')
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
                downloaded += len(data)
                offset += len(data)
                eof = bool(result.get('eof'))
                if eof or not data:
                    break
    return {'run_id': run_id, 'stream': stream, 'path': str(path),
            'next_offset': offset, 'downloaded_bytes': downloaded, 'eof': eof}
