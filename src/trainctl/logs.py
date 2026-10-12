"""Bounded log capture with immutable segment names and absolute byte cursors.

Only the supervisor writes. Segments are never renamed, truncated or deleted.
The reader also accepts legacy single-file logs. A cursor is scoped to run+stream.
"""
from __future__ import annotations

import codecs
import base64
import os
from pathlib import Path
from threading import Thread
from .locking import file_lock


def log_limits(config):
    values = config.get('logs', {})
    segment = values.get('segment_bytes', 16 * 1024**2)
    total = values.get('max_stream_bytes', 1024**3)
    if any(type(n) is not int or n <= 0 for n in (segment, total)) or segment > total:
        raise ValueError('logs requires 0 < segment_bytes <= max_stream_bytes (integers)')
    return segment, total


def segments(run_dir, stream):
    if stream not in {'stdout', 'stderr'}:
        raise ValueError('Invalid log stream')
    directory = Path(run_dir) / 'logs' / stream
    if directory.parent.is_symlink():
        raise ValueError('Symlink logs directory rejected')
    if directory.exists():
        if directory.is_symlink():
            raise ValueError('Symlink log directory rejected')
        result = []
        for path in sorted(directory.glob('*.log')):
            if len(path.stem) != 20 or not path.stem.isascii() or not path.stem.isdigit() or path.is_symlink():
                raise ValueError('Invalid log segment')
            result.append((int(path.stem), path, path.stat().st_size))
        return result
    path = Path(run_dir) / (stream + '.log')
    if path.is_symlink():
        raise ValueError('Symlink log rejected')
    return [(0, path, path.stat().st_size)] if path.exists() else []


def log_size(run_dir, stream):
    if stream not in {'stdout', 'stderr'}:
        raise ValueError('Invalid log stream')
    with file_lock(Path(run_dir) / f'{stream}.log.lock'):
        return sum(size for _, _, size in segments(run_dir, stream))


def read_log(run_dir, stream, offset=0, *, terminal=False, limit=256 * 1024):
    if stream not in {'stdout', 'stderr'}:
        raise ValueError('Invalid log stream')
    with file_lock(Path(run_dir) / f'{stream}.log.lock'):
        return _read_log(run_dir, stream, offset, terminal=terminal, limit=limit)


def _read_log(run_dir, stream, offset, *, terminal, limit):
    if type(offset) is not int or offset < 0 or limit < 4:
        raise ValueError('Invalid log offset/limit')
    entries = segments(run_dir, stream)
    end = 0
    for start, _, size in entries:
        if start != end:
            raise ValueError('LOG_GAP: segment missing or truncated')
        end += size
    if offset > end:
        raise ValueError('LOG_CURSOR_AHEAD: offset exceeds available bytes')
    chunks = []
    cursor, remaining = offset, limit
    for start, path, size in entries:
        if cursor >= start + size:
            continue
        with path.open('rb') as handle:
            handle.seek(cursor - start)
            chunk = handle.read(min(remaining, start + size - cursor))
        chunks.append(chunk)
        cursor += len(chunk)
        remaining -= len(chunk)
        if not remaining:
            break
    data = b''.join(chunks)
    decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')
    text = decoder.decode(data, final=terminal and cursor == end)
    # Don't acknowledge an incomplete multibyte character; fetch it on the next call.
    pending, _ = decoder.getstate()
    next_offset = cursor - len(pending)
    return {'offset': offset, 'text': text, 'next_offset': next_offset,
            'data_base64': base64.b64encode(data[:next_offset - offset]).decode('ascii'),
            'total_bytes': end, 'has_more': next_offset < end,
            'eof': terminal and next_offset == end}


class SegmentedWriter:
    def __init__(self, run_dir, stream, *, segment_bytes, max_stream_bytes):
        if stream not in {'stdout', 'stderr'}:
            raise ValueError('Invalid log stream')
        self.lock_path = Path(run_dir) / f'{stream}.log.lock'
        log_limits({'logs': {'segment_bytes': segment_bytes, 'max_stream_bytes': max_stream_bytes}})
        self.directory = Path(run_dir) / 'logs' / stream
        self.directory.mkdir(parents=True, exist_ok=True)
        entries = segments(run_dir, stream)
        self.total = sum(size for _, _, size in entries)
        self.segment_bytes, self.maximum = segment_bytes, max_stream_bytes
        self.handle = None
        self.size = 0
        if entries and entries[-1][2] < segment_bytes:
            self.handle = entries[-1][1].open('ab', buffering=0)
            self.size = entries[-1][2]

    def write(self, data):
        with file_lock(self.lock_path):
            self._write(data)

    def _write(self, data):
        view = memoryview(data)
        while view:
            if self.total >= self.maximum:
                raise RuntimeError('LOG_LIMIT_EXCEEDED: per-stream byte budget reached')
            if self.handle is None or self.size >= self.segment_bytes:
                self.close()
                self.handle = (self.directory / f'{self.total:020d}.log').open('xb', buffering=0)
                self.size = 0
            count = min(len(view), self.segment_bytes - self.size, self.maximum - self.total)
            written = self.handle.write(view[:count])
            if not written:
                raise OSError('Log write made no progress')
            self.total += written
            self.size += written
            view = view[written:]

    def close(self):
        if self.handle is not None:
            try:
                os.fsync(self.handle.fileno())
            finally:
                self.handle.close()
                self.handle = None


class LogCapture:
    """Drain both pipes concurrently with fixed-size buffers; surface failures."""
    def __init__(self, child, run_dir, config):
        segment, total = log_limits(config)
        self.errors = []
        self.threads = []
        for stream in ('stdout', 'stderr'):
            pipe = getattr(child, stream)
            thread = Thread(target=self._drain, args=(pipe, run_dir, stream, segment, total), daemon=True)
            self.threads.append(thread)
            thread.start()

    def _drain(self, pipe, run_dir, stream, segment, total):
        writer = None
        try:
            writer = SegmentedWriter(run_dir, stream, segment_bytes=segment, max_stream_bytes=total)
            while chunk := pipe.read(64 * 1024):
                writer.write(chunk)
        except Exception as exc:
            self.errors.append(exc)
        finally:
            try:
                if writer:
                    writer.close()
            except Exception as exc:
                self.errors.append(exc)
            pipe.close()

    def check(self):
        if self.errors:
            raise RuntimeError(f'LOG_CAPTURE_FAILED: {self.errors[0]}')

    def finish(self):
        for thread in self.threads:
            thread.join(timeout=5)
        if any(thread.is_alive() for thread in self.threads):
            raise RuntimeError('LOG_DRAIN_TIMEOUT: descendant still holds output pipe')
        self.check()
