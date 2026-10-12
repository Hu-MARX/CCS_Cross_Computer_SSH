"""Lifecycle of the non-GPU observer; termination is confined to its own PID."""
import subprocess
from contextlib import contextmanager
from pathlib import Path

from controller.domain import utc_now


@contextmanager
def watching(store, run_dir, spec, env, workdir):
    if not spec.evidence:
        yield
        return
    store._atomic_json(run_dir / 'evidence-runtime.json', {'state': 'starting', 'updated_at': utc_now()})
    child = subprocess.Popen([spec.command['argv'][0], '-u', str(Path(__file__).with_name('evidence_watch.py')), str(run_dir)],
                             cwd=workdir, env=env, stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        yield
    finally:
        store._atomic_json(run_dir / 'evidence-watch-stop', {'at': utc_now()})
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.terminate()
            try:
                child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=2)
        if child.returncode != 0:
            prior = store._read_json(run_dir / 'evidence-runtime.json', {})
            store._atomic_json(run_dir / 'evidence-runtime.json', {**prior, 'state': 'error',
                'updated_at': utc_now(), 'error': 'Observer exited or failed to stop cleanly', 'exit_code': child.returncode})
