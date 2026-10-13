"""Always-on local observation; training units survive this process restarting."""
import argparse
import json
import os
import signal
import time
from datetime import datetime, timezone
from threading import Event

from controller.domain import RunStatus, utc_now
from .locking import file_lock
from .run_store import RunStore
from .telemetry import Telemetry
from .logs import log_size


def age(timestamp):
    return max(0, (datetime.now(timezone.utc) - datetime.fromisoformat(timestamp.replace('Z', '+00:00'))).total_seconds())


def observe(store, telemetry):
    at = utc_now()
    health = store.health()
    telemetry.record('_worker', at, health)
    config = store.config()
    if health['disk']['free_bytes'] < config.get('min_free_bytes', 5 * 1024**3):
        telemetry.alert('_worker', 'disk_low', at, health['disk'])
    else:
        telemetry.resolve('_worker', 'disk_low', at)
    count = 0
    for run_dir in store.runs.iterdir():
        if not run_dir.is_dir() or run_dir.is_symlink() or not (run_dir / 'status.json').exists():
            continue
        if (run_dir / 'monitor-finalized.json').exists():
            continue
        count += 1
        try:
            execution = store.reconcile_execution(run_dir)
            status = store._read_json(run_dir / 'status.json', {})
            previous = store._read_json(run_dir / 'observation.json', {})
            progress_path = run_dir / 'progress.json'
            if progress_path.exists() and progress_path.stat().st_size > 64 * 1024:
                raise ValueError('Oversized progress record')
            progress = store._read_json(progress_path, {})
            runtime_path = run_dir / 'runtime-configuration.json'
            runtime_configuration = None
            if runtime_path.exists():
                if runtime_path.stat().st_size > 64 * 1024:
                    runtime_configuration = {'state': 'invalid', 'error': 'Oversized runtime configuration'}
                else:
                    try:
                        runtime_configuration = store._read_json(runtime_path, None)
                        if not isinstance(runtime_configuration, dict) or runtime_configuration.get('schema_version') != 1:
                            raise ValueError('Invalid runtime configuration')
                    except (OSError, ValueError, TypeError) as exc:
                        runtime_configuration = {'state': 'invalid', 'error': str(exc)[:1000]}
            checkpoint = store._read_json(run_dir / 'checkpoint-status.json', None)
            if checkpoint and checkpoint.get('state') == 'failed':
                telemetry.alert(run_dir.name, 'checkpoint_failed', at, checkpoint)
            else:
                telemetry.resolve(run_dir.name, 'checkpoint_failed', at)
            if progress and not isinstance(progress, dict):
                raise ValueError('Invalid progress record')
            signature = [progress.get('phase'), progress.get('step'),
                         progress.get('metrics', {}).get('optimizer_updates_this_process')]
            last_progress = previous.get('last_progress_at')
            if progress and signature != previous.get('progress_signature'):
                last_progress = at
            terminal = RunStatus(status['status']).terminal
            runtime_evidence = store._read_json(run_dir / 'evidence-runtime.json', None)
            if runtime_evidence:
                if runtime_evidence.get('schema_version') == 2:
                    integrity = runtime_evidence.get('execution_integrity') or {}
                    deployment = runtime_evidence.get('deployment') or {}
                    observer = runtime_evidence.get('observer') or {}
                    evidence_alerts = {
                        'evidence_execution_integrity': integrity.get('state') == 'violation_observed',
                        'evidence_deployment_changed': deployment.get('state') == 'changed',
                        'evidence_observer_warning': observer.get('health') == 'degraded',
                    }
                    for kind, active in evidence_alerts.items():
                        if active:
                            telemetry.alert(run_dir.name, kind, at, runtime_evidence)
                        else:
                            telemetry.resolve(run_dir.name, kind, at)
                    telemetry.resolve(run_dir.name, 'evidence_drift', at)
                elif runtime_evidence.get('drift_detected'):
                    telemetry.alert(run_dir.name, 'evidence_drift', at, runtime_evidence)
                if (runtime_evidence.get('state') == 'error' or
                    (not terminal and age(runtime_evidence['updated_at']) > 180)):
                    telemetry.alert(run_dir.name, 'evidence_monitor_failed', at, runtime_evidence)
                else:
                    telemetry.resolve(run_dir.name, 'evidence_monitor_failed', at)
            if checkpoint and not terminal:
                checkpoint_at = (checkpoint.get('latest') or {}).get('created_at') or checkpoint.get('updated_at')
                maximum_age = max(300, 2 * float(checkpoint.get('interval_seconds', 1800)))
                if checkpoint_at and age(checkpoint_at) > maximum_age:
                    telemetry.alert(run_dir.name, 'checkpoint_overdue', at, {'last_checkpoint_at': checkpoint_at,
                        'threshold_seconds': maximum_age, 'checkpoint_state': checkpoint.get('state')})
                else:
                    telemetry.resolve(run_dir.name, 'checkpoint_overdue', at)
            else:
                telemetry.resolve(run_dir.name, 'checkpoint_overdue', at)
            stalled_after = config.get('stall_seconds', {}).get(progress.get('phase'), 1800)
            progress_state = 'unreported' if not progress else 'advancing'
            if progress and last_progress and age(last_progress) > stalled_after and not terminal:
                progress_state = 'suspected_stall'
                telemetry.alert(run_dir.name, 'suspected_stall', at, {'last_progress_at': last_progress, 'phase': progress.get('phase')})
            else:
                telemetry.resolve(run_dir.name, 'suspected_stall', at)
            if terminal:
                progress_state = 'finished'
            observation = {'observed_at': at, 'observation_state': 'fresh', 'execution': execution,
                'lifecycle': status['status'], 'progress_state': progress_state,
                'last_progress_at': last_progress, 'progress_signature': signature,
                'progress': progress, 'runtime_configuration': runtime_configuration,
                'checkpoint': checkpoint, 'evidence_runtime': runtime_evidence,
                'pause': store.pause_status(run_dir, status, execution),
                'logs': {stream: log_size(run_dir, stream) for stream in ('stdout', 'stderr')}}
            store._atomic_json(run_dir / 'observation.json', observation)
            telemetry.record(run_dir.name, at, observation)
            telemetry.resolve(run_dir.name, 'observation_failed', at)
            if terminal and (execution.get('quiescent') or execution['backend'] == 'process'):
                if status['status'] == 'FAILED':
                    telemetry.alert(run_dir.name, 'run_failed', at, status)
                store._atomic_json(run_dir / 'monitor-finalized.json', {'at': at})
        except Exception as exc:
            telemetry.alert(run_dir.name, 'observation_failed', at, {'error': str(exc)})
            # Preserve lifecycle; failed observation must not become training failure.
            store._atomic_json(run_dir / 'observation.json', {
                **store._read_json(run_dir / 'observation.json', {}),
                'observation_state': 'unknown', 'error_at': at, 'error': str(exc)})
    store._atomic_json(store.root / 'worker-health.json', {'observed_at': at, 'pid': os.getpid(),
        'observed_runs': count, 'interval_seconds': config.get('monitor_interval', 15), 'health': health})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    store = RunStore(args.root)
    telemetry = Telemetry(store.root)
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    maintenance = None
    next_maintenance_check = 0.0
    with file_lock(store.root / 'worker.lock', timeout=0):
        while not stop.is_set():
            observe(store, telemetry)
            if time.monotonic() >= next_maintenance_check and (maintenance is None or not maintenance.is_alive()):
                from .storage import start_maintenance
                maintenance = start_maintenance(store)
                next_maintenance_check = time.monotonic() + 300
            if args.once:
                if maintenance:
                    maintenance.join(timeout=35)
                break
            stop.wait(max(1, float(store.config().get('monitor_interval', 15))))


if __name__ == '__main__':
    main()
