"""Cooperative GPU exclusion within one store; not a system-wide GPU sandbox."""
import os
import time
from contextlib import contextmanager

from controller.domain import RunStatus
from .locking import file_lock


class Interrupted(Exception):
    pass


@contextmanager
def gpu_slot(store, run_dir, resources, before_acquire=None):
    index = resources.get('gpu_index')
    if index is None:
        if before_acquire:
            before_acquire()
        yield
        return
    directory = store.root / 'resource-locks'
    directory.mkdir(exist_ok=True)
    deadline = time.monotonic() + resources.get('max_queue_seconds', 3600)
    heartbeat = 0.0
    store._append_event(run_dir, 'resource_wait', {'gpu_index': index})
    while True:
        if (run_dir / 'cancel.json').exists():
            raise Interrupted('CANCELLED')
        if time.monotonic() >= deadline:
            raise Interrupted('QUEUE_TIMEOUT')
        lock = file_lock(directory / f'gpu-{index}.lock', timeout=0)
        try:
            lock.__enter__()
        except TimeoutError:
            if time.monotonic() >= heartbeat:
                store._set_status(run_dir, RunStatus.PREPARING, pid=os.getpid())
                heartbeat = time.monotonic() + 2
            time.sleep(0.1)
            continue
        try:
            available = store.gpu_available(index, run_dir)
        except BaseException:
            lock.__exit__(None, None, None)
            raise
        if not available:
            lock.__exit__(None, None, None)
            time.sleep(0.2)
            continue
        break
    try:
        if before_acquire:
            before_acquire()
        if (run_dir / 'cancel.json').exists():
            raise Interrupted('CANCELLED')
        store._atomic_json(directory / f'gpu-{index}.lease.json', {'run_id': run_dir.name})
        store._append_event(run_dir, 'resource_acquired', {'gpu_index': index})
        yield
    finally:
        lock.__exit__(None, None, None)
