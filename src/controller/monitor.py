"""Foreground polling process; SQLite persists state across process restarts."""
from threading import Event
from .domain import utc_now


def watch(orchestrator, *, interval=5.0, max_interval=60.0, max_polls=None,
          until_idle=False, stop=None):
    if interval <= 0 or max_interval < interval or (max_polls is not None and max_polls < 1):
        raise ValueError("invalid polling interval or max_polls")
    stop = stop or Event()
    polls, failures = 0, 0
    while not stop.is_set():
        updated = orchestrator.reconcile()
        errors = orchestrator.last_errors
        failures = min(failures + 1, 16) if errors else 0
        delay = min(max_interval, interval * 2 ** failures)
        active = [run for run in orchestrator.repository.list_active() if run.worker == orchestrator.worker]
        polls += 1
        yield {"time": utc_now(), "poll": polls, "active_runs": len(active),
               "updated": [{"run_id": run.id, "status": run.status.value} for run in updated],
               "errors": errors, "next_poll_seconds": delay}
        if (until_idle and not active) or (max_polls is not None and polls >= max_polls):
            return
        stop.wait(delay)
