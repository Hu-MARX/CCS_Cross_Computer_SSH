"""Independent supervisor; acknowledge cancellation only after child cleanup."""
from __future__ import annotations
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from contextlib import ExitStack
from controller.domain import ExperimentSpec, RunStatus
from .run_store import RunStore, TrainCtlError
from .locking import file_lock
from .resources import Interrupted, gpu_slot
from .logs import LogCapture, log_limits


def stop_children(process):
    if os.name == 'nt':
        if process.poll() is None:
            result = subprocess.run(['taskkill', '/PID', str(process.pid), '/T', '/F'],
                                    capture_output=True, timeout=10)
            if result.returncode and process.poll() is None:
                # Restricted Windows sessions may deny taskkill even for a
                # process this supervisor created. Killing the direct child is
                # a safe fallback; unrestricted workers still use /T above.
                process.kill()
    else:
        # Only signal the process group created by this supervisor.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        else:
            time.sleep(0.3)
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    process.wait(timeout=10)


def execute(argv, *, workdir, env, run_dir, deadline, store, phase, started_event='child_started'):
    if (run_dir / 'cancel.json').exists():
        raise Interrupted('CANCELLED')
    if time.monotonic() >= deadline:
        raise Interrupted('TIMEOUT')
    config = store.config()
    log_limits(config)
    child = subprocess.Popen(argv, cwd=workdir, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0,
                             stdin=subprocess.DEVNULL, shell=False, start_new_session=os.name != 'nt')
    capture = LogCapture(child, run_dir, config)
    try:
        store._append_event(run_dir, started_event, {'pid': child.pid, 'phase': phase.value})
        heartbeat = 0.0
        while child.poll() is None:
            capture.check()
            if (run_dir / 'cancel.json').exists():
                raise Interrupted('CANCELLED')
            now = time.monotonic()
            if now >= deadline:
                raise Interrupted('TIMEOUT')
            if now >= heartbeat:
                store._set_status(run_dir, phase)
                heartbeat = now + 2
            time.sleep(0.1)
        return child.returncode
    finally:
        try:
            stop_children(child)
        finally:
            capture.finish()


def supervise(run_dir):
    store = RunStore(run_dir.parent.parent)
    with file_lock(run_dir / 'supervisor.lock', timeout=0), ExitStack() as resources:
        if RunStatus(store._read_json(run_dir / 'status.json', {})['status']).terminal:
            return 0
        try:
            spec = ExperimentSpec.from_dict(json.loads((run_dir / 'spec.json').read_text(encoding='utf-8')))
            manifest = store._read_json(run_dir / 'manifest.json', {})
            base = Path(manifest.get('submission_cwd') or run_dir)
            workdir = Path(spec.runtime.get('workdir') or base)
            if not workdir.is_absolute():
                workdir = base / workdir
            workdir = workdir.resolve()
            env = os.environ.copy()
            env.update({str(k): str(v) for k, v in spec.runtime.get('environment_variables', {}).items()})
            env['TRAINCTL_RUN_DIR'] = str(run_dir)
            env['TRAINCTL_RUN_ID'] = run_dir.name
            from .evidence_gate import check
            if spec.evidence:
                env['TRAINCTL_EVIDENCE_ID'] = spec.evidence['bundle_id']
                env['TRAINCTL_EVIDENCE_SHA256'] = spec.evidence['manifest_sha256']
                env['TRAINCTL_EVIDENCE_REPORT'] = str(run_dir / 'evidence-launch.json')
            from .task_inputs import TaskInputGate
            TaskInputGate(store).prepare(run_dir, spec, env)
            check(store, run_dir, spec, workdir, env, execute, 'preflight')
            resources.enter_context(gpu_slot(store, run_dir, spec.resources,
                before_acquire=lambda: check(store, run_dir, spec, workdir, env, execute, 'launch')))
            from .source_snapshot import launch
            from .evidence_monitor import watching
            argv, training_env = launch(run_dir, spec, env)
            if spec.resources.get('gpu_index') is not None:
                env['CUDA_VISIBLE_DEVICES'] = str(spec.resources['gpu_index'])
                training_env['CUDA_VISIBLE_DEVICES'] = env['CUDA_VISIBLE_DEVICES']
            deadline = time.monotonic() + spec.resources.get('max_duration_seconds', float('inf'))
            store._set_status(run_dir, RunStatus.RUNNING, pid=os.getpid())
            store._append_event(run_dir, 'started', {'pid': os.getpid()})
            options = dict(workdir=workdir, env=training_env, run_dir=run_dir, deadline=deadline, store=store)
            with watching(store, run_dir, spec, env, workdir):
                code = execute(argv, phase=RunStatus.RUNNING, **options)
            if code == 0 and spec.evaluation.get('argv'):
                store._set_status(run_dir, RunStatus.EVALUATING)
                store._append_event(run_dir, 'evaluation_started', {})
                argv = [arg.replace('${RUN_ID}', run_dir.name) for arg in spec.evaluation['argv']]
                code = execute(argv, phase=RunStatus.EVALUATING, **options)
            if (run_dir / 'cancel.json').exists():
                raise Interrupted('CANCELLED')
            target = RunStatus.SUCCEEDED if code == 0 else RunStatus.FAILED
            error = None
        except Interrupted as exc:
            target = RunStatus.CANCELLED if str(exc) == 'CANCELLED' else RunStatus.FAILED
            code = 130 if target == RunStatus.CANCELLED else 124
            error = str(exc)
        except TrainCtlError as exc:
            target, code, error = RunStatus.FAILED, 1, exc.code + ': ' + exc.message
        except Exception as exc:
            target, code, error = RunStatus.FAILED, 1, str(exc)
        store.finish(run_dir, target, exit_code=code, error=error)
        return code


def main():
    if len(sys.argv) != 2:
        return 2
    return supervise(Path(sys.argv[1]).resolve())


if __name__ == '__main__':
    raise SystemExit(main())
