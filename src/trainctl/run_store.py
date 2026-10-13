from __future__ import annotations

import json
import hashlib
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
import re
import threading
import heapq
from pathlib import Path
from typing import Any
from uuid import uuid4

from controller.domain import ExperimentSpec, RunStatus, utc_now
from .locking import file_lock


class TrainCtlError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False,
                 execution_state: str | None = None):
        super().__init__(message)
        self.code, self.message, self.retryable = code, message, retryable
        self.execution_state = execution_state


class RunStore:
    """Filesystem implementation of the trainctl Interface with atomic state writes."""

    def __init__(self, root: str | Path = ".trainctl", *, initialize: bool = True):
        self.root = Path(root).resolve()
        self.runs = self.root / "runs"
        if initialize:
            self.runs.mkdir(parents=True, exist_ok=True)
        self.idempotency_file = self.root / "idempotency.json"

    def config(self):
        return self._read_json(self.root / 'worker.json', {})

    def monitor(self):
        from .worker import age
        value = self._read_json(self.root / 'worker-health.json', {})
        value['observation_state'] = 'unavailable'
        if value.get('observed_at'):
            value['age_seconds'] = age(value['observed_at'])
            value['observation_state'] = 'fresh' if value['age_seconds'] <= max(60, 3 * value.get('interval_seconds', 15)) else 'stale'
        return value

    def storage(self):
        from .storage import storage_policy
        return {'policy': storage_policy(self.config()),
                'maintenance': self._read_json(self.root / 'storage-health.json', {'state': 'not_run'})}

    def health(self) -> dict[str, Any]:
        disk = shutil.disk_usage(self.root)
        gpu = self._gpu_snapshot()
        cpu = self._cpu_snapshot()
        memory = self._memory_snapshot()
        result = {
            "hostname": socket.gethostname(), "platform": platform.platform(), "python": sys.version.split()[0],
            "disk": {"total_bytes": disk.total, "free_bytes": disk.free, "used_bytes": disk.used},
            "cpu": cpu, "memory": memory, "gpu": gpu,
            "checked_at": utc_now(),
        }
        try:
            from controller.capabilities import worker_interface
        except ImportError:
            # A health-only rolling update must remain usable on legacy workers.
            pass
        else:
            result['ccs_interface'] = worker_interface()
        return result

    def processes(self) -> dict[str, Any]:
        from .processes import discover
        return discover(self.runs)

    def workspace(self, request) -> dict[str, Any]:
        from .workspace import WorkspaceManager
        return WorkspaceManager(self.root, self.config()).execute(request)

    def submit(self, spec: ExperimentSpec, idempotency_key: str) -> dict[str, Any]:
        spec.validate()
        with file_lock(self.root / 'submit.lock'):
            return self._submit_locked(spec, idempotency_key)

    def lookup_submission(self, idempotency_key: str) -> dict[str, Any]:
        """Read the published submission identity without starting or modifying a run.

        The map is atomically replaced after the initial run receipt is written.
        An absent key is only a fact about this observation, not permission to
        replay a started submission: a concurrent or disconnected submit may
        still publish it later. No lock file is created by this query.
        """
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise TrainCtlError('INVALID_IDEMPOTENCY_KEY', 'idempotency key is required')
        try:
            mapping = self._read_json(self.idempotency_file, {})
        except (ValueError, UnicodeError) as exc:
            raise TrainCtlError(
                'SUBMISSION_LOOKUP_INCONSISTENT', 'submission idempotency map is malformed',
            ) from exc
        except OSError as exc:
            raise TrainCtlError(
                'SUBMISSION_LOOKUP_UNAVAILABLE', 'submission idempotency map cannot be read',
                retryable=True,
            ) from exc
        if not isinstance(mapping, dict):
            raise TrainCtlError(
                'SUBMISSION_LOOKUP_INCONSISTENT', 'submission idempotency map must be an object',
            )
        result = {'idempotency_key': idempotency_key, 'run_id': None,
                  'state': 'absent', 'observed_at': utc_now()}
        if idempotency_key not in mapping:
            return result

        run_id = mapping[idempotency_key]
        try:
            if not isinstance(run_id, str):
                raise ValueError('mapped run id is not a string')
            run_dir = self._require_run(run_id)
            manifest = self._read_json(run_dir / 'manifest.json', None)
            status = self._read_json(run_dir / 'status.json', None)
            # Only the first, immutable submission receipt is relevant. Reading
            # later event lines could race with a runner appending a new event.
            with (run_dir / 'events.jsonl').open(encoding='utf-8') as handle:
                submitted = json.loads(handle.readline())
            if (not isinstance(manifest, dict) or manifest.get('run_id') != run_id or
                    not isinstance(status, dict) or status.get('run_id') != run_id or
                    not isinstance(submitted, dict) or submitted.get('seq') != 1 or
                    submitted.get('kind') != 'submitted' or
                    not isinstance(submitted.get('payload'), dict) or
                    submitted['payload'].get('idempotency_key') != idempotency_key):
                raise ValueError('run receipt does not match the mapped submission identity')
        except (TrainCtlError, ValueError, UnicodeError, FileNotFoundError) as exc:
            raise TrainCtlError(
                'SUBMISSION_LOOKUP_INCONSISTENT',
                'mapped submission run receipt is missing, malformed, or inconsistent',
            ) from exc
        except OSError as exc:
            raise TrainCtlError(
                'SUBMISSION_LOOKUP_UNAVAILABLE', 'mapped submission run receipt cannot be read',
                retryable=True,
            ) from exc
        result.update(run_id=run_id, state='found')
        return result

    def _submit_locked(self, spec: ExperimentSpec, idempotency_key: str) -> dict[str, Any]:
        if not idempotency_key.strip():
            raise TrainCtlError("INVALID_IDEMPOTENCY_KEY", "idempotency key is required")
        mapping = self._read_json(self.idempotency_file, {})
        if idempotency_key in mapping:
            previous = self._read_json(self._require_run(mapping[idempotency_key]) / 'spec.json', {})
            if previous != spec.to_dict():
                raise TrainCtlError('IDEMPOTENCY_CONFLICT', 'key already belongs to a different specification')
            return self.snapshot(mapping[idempotency_key])
        if shutil.disk_usage(self.root).free < self.config().get('min_free_bytes', 0):
            raise TrainCtlError('DISK_LOW', 'Insufficient free disk space for a new run')
        from .logs import log_limits
        log_limits(self.config())
        run_id = f"run_{uuid4().hex}"
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True)
        execution = {'backend': self.config().get('backend', 'process')}
        if execution['backend'] == 'systemd':
            from .systemd_backend import unit_name, boot_id
            execution.update(unit=unit_name(self.root, run_id), boot_id=boot_id())
        elif execution['backend'] != 'process':
            raise TrainCtlError('INVALID_BACKEND', 'Unknown execution backend')
        self._atomic_json(run_dir / "spec.json", spec.to_dict())
        self._atomic_json(run_dir / "manifest.json", {
            "run_id": run_id, "hostname": socket.gethostname(), "created_at": utc_now(),
            "code": spec.project, "config": spec.to_dict(), "submission_cwd": str(Path.cwd()),
            "execution": execution,
            **({'evidence': spec.evidence} if spec.evidence else {}),
        })
        self._atomic_json(run_dir / "status.json", {
            "run_id": run_id, "status": RunStatus.QUEUED.value, "created_at": utc_now(),
            "updated_at": utc_now(), "pid": None, "exit_code": None,
        })
        self._append_event(run_dir, "submitted", {"idempotency_key": idempotency_key})
        mapping[idempotency_key] = run_id
        self._atomic_json(self.idempotency_file, mapping)
        try:
            self._set_status(run_dir, RunStatus.PREPARING)
            self._start_runner(run_dir)
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
            # The start may have reached systemd before an SSH/DBus timeout.
            # Do not claim FAILED or retry under a new ID; worker reconciles it.
            if execution['backend'] == 'systemd':
                self._append_event(run_dir, 'start_uncertain', {'error': str(exc)})
                raise TrainCtlError('START_UNCERTAIN', 'Start outcome unknown; query the same idempotency key', retryable=True) from exc
            self.finish(run_dir, RunStatus.FAILED, exit_code=1, error=str(exc))
            raise TrainCtlError("START_FAILED", str(exc), retryable=True) from exc
        # Runner alone publishes its PID. Parent must not overwrite newer state.
        return self.snapshot(run_id)

    def snapshot(self, run_id: str) -> dict[str, Any]:
        run_dir = self._require_run(run_id)
        status = self._read_json(run_dir / "status.json", {})
        status["health"] = self._gpu_snapshot()
        status['observation'] = self._read_json(run_dir / 'observation.json', None)
        binding = self._read_json(run_dir / 'manifest.json', {}).get('evidence')
        if binding:
            status['evidence'] = binding
            try:
                status['evidence_gate'] = self._read_json(run_dir / 'evidence-gate.json', None)
                status['evidence_runtime'] = self._read_json(run_dir / 'evidence-runtime.json', None)
                status['source_snapshot'] = self._read_json(run_dir / 'source-snapshot.json', None)
            except (OSError, ValueError):
                # Windows can transiently deny reads during atomic replacement.
                # A missing observation must not change the execution lifecycle.
                status['evidence_gate'] = {'state': 'unavailable'}
        try:
            status['progress'] = self._read_json(run_dir / 'progress.json', None)
        except (ValueError, OSError):
            status['progress'] = None
        try:
            status['runtime_configuration'] = self._read_json(run_dir / 'runtime-configuration.json', None)
        except (ValueError, OSError):
            status['runtime_configuration'] = {'state': 'unavailable'}
        if status['observation'] and not RunStatus(status['status']).terminal:
            from .worker import age
            observed = status['observation'].get('observed_at')
            if observed and age(observed) > max(60, 3 * self.config().get('monitor_interval', 15)):
                status['observation']['observation_state'] = 'stale'
        status['pause'] = self.pause_status(run_dir, status)
        return status

    def pause_status(self, run_dir: Path, lifecycle: dict, execution: dict | None = None):
        """Read pause facts separately from lifecycle and process liveness."""
        raw = self._read_json(run_dir / 'spec.json', {})
        declaration = (raw.get('task') or {}).get('pause')
        if declaration is None:
            return None
        try:
            from controller.configuration import spec_digest
            from .pause_control import status
            manifest = self._read_json(run_dir / 'manifest.json', {})
            base = Path(manifest.get('submission_cwd') or run_dir)
            workdir = Path(raw.get('runtime', {}).get('workdir') or base)
            if not workdir.is_absolute():
                workdir = base / workdir
            directory = workdir.resolve() / 'orchestrator_runs' / run_dir.name / 'checkpoints'
            # An ACK is never proof that training has stopped. Even a terminal
            # status needs independent backend cleanup evidence.
            paused_result = (lifecycle.get('status') == 'CANCELLED' and
                             lifecycle.get('error') == 'PAUSED' and lifecycle.get('exit_code') == 85)
            if paused_result and execution is None:
                execution = self.reconcile_execution(run_dir)
            quiescent = paused_result and bool((execution or {}).get('quiescent'))
            return status(run_dir, declaration, checkpoint_directory=directory,
                          spec_digest=spec_digest(raw), quiescent=quiescent)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            return {'schema_version': 1, 'state': 'invalid', 'safe_to_resume': False,
                    'error': str(exc)[:1000]}

    def logs(self, run_id: str, stream: str = "stdout", after_offset: int = 0) -> dict[str, Any]:
        if stream not in {"stdout", "stderr"}:
            raise TrainCtlError("INVALID_STREAM", "stream must be stdout or stderr")
        from .logs import read_log
        run_dir = self._require_run(run_id)
        terminal = RunStatus(self._read_json(run_dir / 'status.json', {})['status']).terminal
        return {'run_id': run_id, 'stream': stream,
                **read_log(run_dir, stream, after_offset, terminal=terminal)}

    def events(self, run_id: str, after_seq: int = 0) -> list[dict[str, Any]]:
        """Legacy list API; paged callers use events_page to bound memory/output."""
        path = self._require_run(run_id) / "events.jsonl"
        if not path.exists():
            return []
        events = []
        with file_lock(path.parent / 'events.lock'):
            from .event_index import synchronize
            db, _, _ = synchronize(path)
            try:
                with path.open('rb') as handle:
                    for offset, length in db.execute('SELECT offset,length FROM offsets WHERE seq>? ORDER BY seq',
                                                     (after_seq,)):
                        handle.seek(offset)
                        events.append(json.loads(handle.read(length)))
            finally:
                db.close()
        return events

    def events_page(self, run_id: str, after_seq: int = 0, *, limit: int = 100,
                    max_bytes: int = 1024 * 1024) -> dict[str, Any]:
        from controller.pagination import validate_page
        from .event_index import synchronize
        validate_page(limit, max_bytes)
        if type(after_seq) is not int or after_seq < 0:
            raise ValueError('after_seq must be a non-negative integer')
        path = self._require_run(run_id) / 'events.jsonl'
        with file_lock(path.parent / 'events.lock'):
            db, _, _ = synchronize(path)
            try:
                rows = db.execute('SELECT seq,offset,length FROM offsets WHERE seq>? ORDER BY seq LIMIT ?',
                                  (after_seq, limit + 1)).fetchall()
                selected, cursor, more, truncated, used = [], after_seq, False, False, 2
                if rows:
                    with path.open('rb') as handle:
                        for seq, offset, length in rows:
                            if len(selected) == limit:
                                more = True
                                break
                            if used + length > max_bytes:
                                if not selected:
                                    raise TrainCtlError('PAGE_ITEM_TOO_LARGE',
                                                        f'event requires max_bytes >= {length + 2}; after_seq unchanged')
                                more, truncated = True, True
                                break
                            handle.seek(offset)
                            selected.append(json.loads(handle.read(length)))
                            used += length
                            cursor = seq
            finally:
                db.close()
        return {'events': selected, 'next_cursor': cursor, 'has_more': more, 'truncated': truncated}

    def _artifact_candidates(self, run_id: str, path: str | None = None):
        run_dir = self._require_run(run_id)
        spec = ExperimentSpec.from_dict(self._read_json(run_dir / "spec.json", {}))
        pattern = spec.outputs.get("checkpoint_glob") or spec.outputs.get("artifact_glob")
        if not pattern and path is None:
            return Path.cwd(), [], None
        if pattern:
            pattern = str(pattern).replace('${RUN_ID}', run_id)
            pattern_path = Path(str(pattern))
            if pattern_path.is_absolute() or ".." in pattern_path.parts:
                raise TrainCtlError("INVALID_ARTIFACT_GLOB", "artifact glob must stay inside the workdir")
        manifest = self._read_json(run_dir / "manifest.json", {})
        base_dir = Path(manifest.get("submission_cwd") or Path.cwd())
        workdir_value = spec.runtime.get("workdir")
        workdir = Path(workdir_value) if workdir_value else base_dir
        if not workdir.is_absolute():
            workdir = base_dir / workdir
        workdir = workdir.resolve()
        matches = workdir.glob(pattern) if pattern else []
        reported_checkpoint = None
        if path is not None:
            if not isinstance(path, str) or "\x00" in path or not Path(path).is_absolute():
                raise TrainCtlError("INVALID_ARTIFACT_PATH", "artifact path must be absolute")
            requested = Path(path).resolve()
            if not requested.is_relative_to(workdir):
                raise TrainCtlError("INVALID_ARTIFACT_PATH", "artifact path must stay inside the workdir")
            matches = [candidate for candidate in matches if candidate.resolve() == requested]
            checkpoint = self._read_json(run_dir / 'checkpoint-status.json', {})
            latest = checkpoint.get('latest') if isinstance(checkpoint, dict) else None
            checkpoint_id = latest.get('id') if isinstance(latest, dict) else None
            if (checkpoint.get('state') == 'committed' and isinstance(checkpoint_id, str) and
                    re.fullmatch(r'ckpt_[a-f0-9]{32}', checkpoint_id)):
                managed = (workdir / 'orchestrator_runs' / run_id / 'checkpoints' /
                           (checkpoint_id + '.ckpt'))
                if managed.resolve() == requested:
                    matches = [managed]
                    reported_checkpoint = latest
        return workdir, matches, reported_checkpoint

    @staticmethod
    def _artifact_item(candidate, path, reported_checkpoint):
        item = {"path": str(candidate), "size": candidate.stat().st_size}
        if path is not None:
            digest = hashlib.sha256()
            with candidate.open('rb') as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(chunk)
            item['sha256'] = digest.hexdigest()
            if reported_checkpoint is not None and (
                    reported_checkpoint.get('size') != item['size'] or
                    reported_checkpoint.get('sha256') != item['sha256']):
                raise TrainCtlError('CHECKPOINT_MISMATCH',
                                    'managed checkpoint does not match its committed size and SHA256')
        return item

    def artifacts(self, run_id: str, path: str | None = None) -> list[dict[str, Any]]:
        workdir, matches, reported_checkpoint = self._artifact_candidates(run_id, path)
        artifacts = []
        for candidate in sorted(matches):
            if (candidate.is_symlink() or not candidate.is_file() or
                    not candidate.resolve().is_relative_to(workdir)):
                continue
            artifacts.append(self._artifact_item(candidate, path, reported_checkpoint))
        return artifacts

    def artifacts_page(self, run_id: str, path: str | None = None, *, cursor: str | None = None,
                       limit: int = 100, max_bytes: int = 1024 * 1024) -> dict[str, Any]:
        from controller.pagination import artifact_after, artifact_cursor, bounded_items, validate_page, PageItemTooLarge
        validate_page(limit, max_bytes)
        after = artifact_after(cursor, run_id, path)
        workdir, matches, reported_checkpoint = self._artifact_candidates(run_id, path)
        # Keyset pagination is stable for an unchanged inventory. New names below
        # the cursor require a fresh scan; this is a live listing, not a snapshot.
        candidates = (candidate for candidate in matches if str(candidate) > after and
                      not candidate.is_symlink() and candidate.is_file() and
                      candidate.resolve().is_relative_to(workdir))
        selected_paths = heapq.nsmallest(limit + 1, candidates, key=str)
        items = (self._artifact_item(candidate, path, reported_checkpoint) for candidate in selected_paths)
        try:
            selected, next_cursor, more, truncated = bounded_items(
                items, key=lambda item: item['path'], initial_cursor=cursor,
                encode_cursor=lambda value: artifact_cursor(run_id, path, value), limit=limit, max_bytes=max_bytes,
            )
        except PageItemTooLarge as exc:
            raise TrainCtlError(exc.code, exc.message) from exc
        return {'artifacts': selected, 'next_cursor': next_cursor, 'has_more': more, 'truncated': truncated}

    def pause(self, run_id: str, reason: str, request_id: str,
              timeout_seconds: int = 600) -> dict[str, Any]:
        """Persist an opt-in cooperative request; never signal or cancel a task."""
        try:
            run_dir = self._require_run(run_id)
        except TrainCtlError as exc:
            if exc.code in {'INVALID_RUN_ID', 'RUN_NOT_FOUND'}:
                raise TrainCtlError(exc.code, exc.message, retryable=exc.retryable,
                                    execution_state='rejected') from exc
            raise
        raw = self._read_json(run_dir / 'spec.json', {})
        task = raw.get('task') or {}
        declaration = task.get('pause') if isinstance(task, dict) else None
        if not isinstance(task, dict) or task.get('kind') != 'train' or declaration is None:
            raise TrainCtlError('PAUSE_NOT_SUPPORTED', 'Run did not declare cooperative pause support',
                                execution_state='rejected')
        if (not isinstance(reason, str) or not reason.strip() or len(reason) > 1024 or
                not isinstance(request_id, str) or
                not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}', request_id) or
                type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600):
            raise TrainCtlError('INVALID_PAUSE_REQUEST', 'Invalid reason, request identity, or timeout',
                                execution_state='rejected')
        from controller.configuration import spec_digest
        from .pause_control import create_request
        # Serialize with final publication: a new pause cannot be introduced
        # after a terminal result, while an existing immutable identity can be read.
        with file_lock(run_dir / 'finish.lock'):
            status = self._read_json(run_dir / 'status.json', {})
            if (not (run_dir / 'pause-request.json').exists() and
                    status.get('status') not in {'QUEUED', 'PREPARING', 'RUNNING'}):
                raise TrainCtlError('PAUSE_NOT_ACTIVE', 'Run is no longer in its training phase',
                                    execution_state='rejected')
            try:
                create_request(run_dir, declaration, request_id=request_id,
                               reason=reason, timeout_seconds=timeout_seconds,
                               spec_digest=spec_digest(raw))
            except ValueError as exc:
                code = ('PAUSE_REQUEST_CONFLICT' if str(exc).startswith('PAUSE_REQUEST_CONFLICT:')
                        else 'INVALID_PAUSE_REQUEST')
                # Only an explicit immutable-request conflict proves rejection.
                # Storage, publication, and later snapshot failures stay unknown.
                execution_state = 'rejected' if code == 'PAUSE_REQUEST_CONFLICT' else None
                raise TrainCtlError(code, str(exc), execution_state=execution_state) from exc
        return self.snapshot(run_id)

    def cancel(self, run_id: str, reason: str) -> dict[str, Any]:
        run_dir = self._require_run(run_id)
        status = self._read_json(run_dir / "status.json", {})
        if status.get("status") in {item.value for item in RunStatus if item.terminal}:
            return status
        if not reason.strip():
            raise TrainCtlError('INVALID_REASON', 'cancel reason is required')
        with file_lock(run_dir / 'cancel.lock'):
            if not (run_dir / 'cancel.json').exists():
                self._atomic_json(run_dir / 'cancel.json', {'reason': reason, 'requested_at': utc_now()})
                self._append_event(run_dir, 'cancel_requested', {'reason': reason})
        execution = self._read_json(run_dir / 'manifest.json', {}).get('execution', {})
        if execution.get('backend') == 'systemd':
            from .systemd_backend import stop
            stop(execution['unit'])
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            quiescent = True
            if execution.get('backend') == 'systemd':
                quiescent = self.reconcile_execution(run_dir).get('quiescent', False)
            status = self._read_json(run_dir / 'status.json', {})
            if RunStatus(status['status']).terminal and quiescent:
                return status
            time.sleep(0.1)
        raise TrainCtlError('CANCEL_PENDING', 'request persisted; runner has not confirmed cancellation', retryable=True)

    def _start_runner(self, run_dir: Path) -> int:
        execution = self._read_json(run_dir / 'manifest.json', {}).get('execution', {})
        if execution.get('backend') == 'systemd':
            from .systemd_backend import start
            start(run_dir, execution)
            return 0
        output = (run_dir / "runner.log").open("ab")
        source_root = str(Path(__file__).resolve().parents[1])
        child_env = os.environ.copy()
        existing_pythonpath = child_env.get("PYTHONPATH")
        child_env["PYTHONPATH"] = source_root if not existing_pythonpath else source_root + os.pathsep + existing_pythonpath
        child_env["TRAINCTL_SUBMIT_CWD"] = str(Path.cwd())
        kwargs: dict[str, Any] = {
            "stdout": output, "stderr": output, "stdin": subprocess.DEVNULL, "cwd": str(run_dir), "env": child_env,
        }
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | getattr(subprocess, "DETACHED_PROCESS", 0)
        else:
            kwargs["start_new_session"] = True
        try:
            process = subprocess.Popen([sys.executable, "-m", "trainctl.runner", str(run_dir)], **kwargs)
        finally:
            output.close()
        threading.Thread(target=process.wait, daemon=True).start()
        return process.pid

    def reconcile_execution(self, run_dir):
        from datetime import datetime, timezone
        from .systemd_backend import boot_id, inspect_unit, is_quiescent
        manifest = self._read_json(run_dir / 'manifest.json', {})
        execution = manifest.get('execution', {})
        status = self._read_json(run_dir / 'status.json', {})
        if execution.get('backend') != 'systemd':
            finalized = self._read_json(run_dir / 'runner-cleanup.json', {})
            group_verified = (RunStatus(status['status']).terminal and
                              finalized.get('schema_version') == 1 and
                              finalized.get('run_id') == run_dir.name and
                              finalized.get('supervisor_pid') == status.get('pid') and
                              finalized.get('scope') == 'process_group' and
                              finalized.get('process_group_quiescent') is True)
            # A legal setsid()/start_new_session helper escapes the launcher
            # group. Group cleanup is useful evidence, not proof of full-Run
            # quiescence; only the managed systemd cgroup has that scope.
            return {'backend': 'process', 'verified': False, 'quiescent': False,
                    'scope': 'process_group' if group_verified else 'unverified',
                    'process_group_quiescent': True if group_verified else None}
        if execution.get('boot_id') != boot_id():
            if not RunStatus(status['status']).terminal:
                self.finish(run_dir, RunStatus.FAILED, exit_code=1, error='WORKER_REBOOTED')
            return {'backend': 'systemd', 'quiescent': True, 'reason': 'previous_boot'}
        state = inspect_unit(execution['unit'])
        quiet = is_quiescent(state)
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(status['created_at'].replace('Z', '+00:00'))).total_seconds()
        if quiet and not RunStatus(status['status']).terminal and age > 30:
            cancelled = (run_dir / 'cancel.json').exists()
            target = RunStatus.CANCELLED if cancelled else RunStatus.FAILED
            self.finish(run_dir, target, exit_code=130 if cancelled else 1,
                        error='CANCELLED' if cancelled else 'SUPERVISOR_EXITED_WITHOUT_RESULT')
        # Cancellation need not wait for launch-grace once the unit has been seen.
        elif quiet and not RunStatus(status['status']).terminal and (run_dir / 'cancel.json').exists() and state.get('LoadState') != 'not-found':
            self.finish(run_dir, RunStatus.CANCELLED, exit_code=130, error='CANCELLED')
        return {'backend': 'systemd', 'quiescent': quiet, 'unit': execution['unit'], **state}

    def gpu_available(self, index, run_dir):
        lease = self._read_json(self.root / 'resource-locks' / f'gpu-{index}.lease.json', {})
        if not lease or lease.get('run_id') == run_dir.name:
            return True
        previous = self._require_run(lease['run_id'])
        execution = self._read_json(previous / 'manifest.json', {}).get('execution', {})
        if execution.get('backend') == 'systemd':
            return self.reconcile_execution(previous).get('quiescent', False)
        # Legacy process backend has no cgroup guarantee; don't mix live legacy runs.
        status = self._read_json(previous / 'status.json', {})
        return RunStatus(status['status']).terminal

    def finish(self, run_dir, status, *, exit_code, error=None):
        # The final event precedes the terminal snapshot, so clients can drain it.
        with file_lock(run_dir / 'finish.lock'):
            current = self._read_json(run_dir / 'status.json', {})
            if RunStatus(current['status']).terminal:
                return
            binding = self._read_json(run_dir / 'manifest.json', {}).get('evidence')
            result = {'status': status.value, 'exit_code': exit_code, 'error': error}
            if binding:
                result.update(evidence=binding, evidence_gate=self._read_json(run_dir / 'evidence-gate.json', None),
                              evidence_runtime=self._read_json(run_dir / 'evidence-runtime.json', None),
                              source_snapshot=self._read_json(run_dir / 'source-snapshot.json', None))
                self._atomic_json(run_dir / 'execution-result.json', result)
            self._append_event(run_dir, 'finished', result)
            self._set_status(run_dir, status, exit_code=exit_code, error=error)

    def _run_dir(self, run_id: str) -> Path:
        return self.runs / run_id

    def _require_run(self, run_id: str) -> Path:
        if not re.fullmatch(r'run_[a-f0-9]{32}', run_id):
            raise TrainCtlError("INVALID_RUN_ID", "invalid run id")
        path = self._run_dir(run_id)
        if not path.is_dir() or path.is_symlink() or not path.resolve().is_relative_to(self.runs):
            raise TrainCtlError("RUN_NOT_FOUND", "run was not found")
        return path

    @staticmethod
    def _read_json(path: Path, default: Any) -> Any:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return default

    @staticmethod
    def _atomic_json(path: Path, value: Any) -> None:
        temporary = path.with_name(path.name + '.' + uuid4().hex + '.tmp')
        try:
            with temporary.open('w', encoding='utf-8') as handle:
                json.dump(value, handle, ensure_ascii=False, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _append_event(run_dir: Path, kind: str, payload: dict) -> None:
        with file_lock(run_dir / 'events.lock'):
            RunStore._append_event_locked(run_dir, kind, payload)

    @staticmethod
    def _append_event_locked(run_dir: Path, kind: str, payload: dict) -> None:
        from .event_index import synchronize, record_append
        path = run_dir / "events.jsonl"
        db, offset, last_seq = synchronize(path, repair_tail=True)
        try:
            event = {'seq': last_seq + 1, 'kind': kind, 'payload': payload, 'created_at': utc_now()}
            raw = (json.dumps(event, ensure_ascii=False) + '\n').encode('utf-8')
            with path.open('ab') as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            record_append(db, path, last_seq + 1, offset, len(raw))
        finally:
            db.close()

    def _set_status(self, run_dir: Path, status: RunStatus, *, pid: int | None = None,
                    exit_code: int | None = None, error: str | None = None) -> None:
        with file_lock(run_dir / 'status.lock'):
            self._set_status_locked(run_dir, status, pid=pid, exit_code=exit_code, error=error)

    def _set_status_locked(self, run_dir: Path, status: RunStatus, *, pid: int | None = None,
                           exit_code: int | None = None, error: str | None = None) -> None:
        value = self._read_json(run_dir / "status.json", {})
        if value.get('status') and RunStatus(value['status']).terminal:
            return
        now = utc_now()
        value.update({"status": status.value, "updated_at": now})
        if pid is not None:
            value["pid"] = pid
        if exit_code is not None:
            value["exit_code"] = exit_code
        if error:
            value["error"] = error
        if status == RunStatus.RUNNING and not value.get("started_at"):
            value["started_at"] = now
        if status.terminal:
            value["finished_at"] = now
        self._atomic_json(run_dir / "status.json", value)

    @staticmethod
    def _parse_cpu_stat(value: str) -> tuple[int, int] | None:
        line = next((row for row in value.splitlines() if row.startswith('cpu ')), None)
        if line is None:
            return None
        try:
            fields = [int(item) for item in line.split()[1:]]
        except ValueError:
            return None
        if len(fields) < 4:
            return None
        # guest and guest_nice are already included in user and nice on Linux.
        total = sum(fields[:8])
        idle = fields[3] + (fields[4] if len(fields) > 4 else 0)
        return total, idle

    @staticmethod
    def _cpu_utilization(before: tuple[int, int] | None,
                         after: tuple[int, int] | None) -> float | None:
        if before is None or after is None:
            return None
        total_delta, idle_delta = after[0] - before[0], after[1] - before[1]
        if total_delta <= 0 or idle_delta < 0:
            return None
        return round(max(0.0, min(100.0, 100 * (total_delta - idle_delta) / total_delta)), 1)

    @staticmethod
    def _parse_meminfo(value: str) -> dict[str, Any]:
        fields = {}
        for line in value.splitlines():
            match = re.fullmatch(r'([^:]+):\s*(\d+)\s+kB', line.strip())
            if match:
                fields[match.group(1)] = int(match.group(2)) * 1024
        total = fields.get('MemTotal')
        available = fields.get('MemAvailable')
        if total is None or available is None or total <= 0:
            return {}
        used = max(0, total - available)
        result = {
            'total_bytes': total,
            'available_bytes': available,
            'used_bytes': used,
            'utilization_percent': round(100 * used / total, 1),
        }
        swap_total = fields.get('SwapTotal')
        swap_free = fields.get('SwapFree')
        if swap_total is not None and swap_free is not None:
            result.update(swap_total_bytes=swap_total,
                          swap_used_bytes=max(0, swap_total - swap_free))
        return result

    @classmethod
    def _cpu_snapshot(cls) -> dict[str, Any]:
        result: dict[str, Any] = {}
        logical = os.cpu_count()
        if isinstance(logical, int) and logical > 0:
            result['logical_cores'] = logical
        try:
            with Path('/proc/cpuinfo').open(encoding='utf-8', errors='replace') as handle:
                for line in handle:
                    if line.lower().startswith('model name') and ':' in line:
                        model = line.split(':', 1)[1].strip()
                        if model:
                            result['model'] = model[:256]
                        break
        except OSError:
            model = platform.processor().strip()
            if model:
                result['model'] = model[:256]
        try:
            load = os.getloadavg()
            result.update(load_1m=round(load[0], 2), load_5m=round(load[1], 2),
                          load_15m=round(load[2], 2))
        except (AttributeError, OSError):
            pass
        try:
            before = cls._parse_cpu_stat(Path('/proc/stat').read_text(encoding='ascii'))
            if before is not None:
                time.sleep(0.1)
                after = cls._parse_cpu_stat(Path('/proc/stat').read_text(encoding='ascii'))
                utilization = cls._cpu_utilization(before, after)
                if utilization is not None:
                    result['utilization_percent'] = utilization
        except OSError:
            pass
        return result

    @staticmethod
    def _memory_snapshot() -> dict[str, Any]:
        try:
            return RunStore._parse_meminfo(Path('/proc/meminfo').read_text(encoding='ascii'))
        except OSError:
            return {}

    @staticmethod
    def _gpu_snapshot() -> list[dict[str, Any]]:
        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu",
                 "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=3, check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return []
        if result.returncode != 0:
            return []
        rows = []
        for line in result.stdout.splitlines():
            fields = [field.strip() for field in line.split(",")]
            if len(fields) == 6:
                rows.append({"index": fields[0], "name": fields[1], "utilization_percent": fields[2],
                             "memory_used_mb": fields[3], "memory_total_mb": fields[4], "temperature_c": fields[5]})
        return rows
