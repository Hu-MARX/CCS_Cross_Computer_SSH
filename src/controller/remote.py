from __future__ import annotations

import json
import re
import shlex
import subprocess
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Protocol

from .domain import ExperimentSpec, RunStatus, utc_now
from .capabilities import negotiate_worker, requirements_for_spec


@dataclass(frozen=True)
class RemoteEvent:
    seq: int
    kind: str
    payload: dict


@dataclass(frozen=True)
class RemoteSnapshot:
    run_id: str
    status: RunStatus
    exit_code: int | None = None
    heartbeat_at: str | None = None
    observation: dict | None = None
    progress: dict | None = None
    evidence: dict | None = None
    evidence_gate: dict | None = None
    evidence_runtime: dict | None = None
    source_snapshot: dict | None = None
    runtime_configuration: dict | None = None
    pause: dict | None = None


class RemoteRunner(Protocol):
    def health(self) -> dict: ...
    def capabilities(self) -> dict: ...
    def compatibility(self, required: dict[str, int] | None = None) -> dict: ...
    def require_capabilities(self, required: dict[str, int]) -> dict: ...
    def submit(self, spec: ExperimentSpec, idempotency_key: str) -> str: ...
    def lookup_submission(self, idempotency_key: str) -> str | None: ...
    def snapshot(self, run_id: str) -> RemoteSnapshot: ...
    def events(self, run_id: str, after_seq: int) -> list[RemoteEvent]: ...
    def events_page(self, run_id: str, after_seq: int = 0, *, limit: int = 100,
                    max_bytes: int = 1024 * 1024) -> dict: ...
    def logs(self, run_id: str, stream: str = 'stdout', after_offset: int = 0) -> dict: ...
    def cancel(self, run_id: str, reason: str) -> RemoteSnapshot: ...
    def pause(self, run_id: str, reason: str, request_id: str, timeout_seconds: int = 600) -> dict: ...
    def artifacts(self, run_id: str, path: str | None = None) -> list[dict]: ...
    def artifacts_page(self, run_id: str, path: str | None = None, *, cursor: str | None = None,
                       limit: int = 100, max_bytes: int = 1024 * 1024) -> dict: ...
    def download_artifact(self, run_id: str, artifact: dict, destination: Path) -> dict: ...
    def import_checkpoint(self, replica: dict) -> dict: ...
    def monitor(self) -> dict: ...
    def storage(self) -> dict: ...
    def processes(self) -> dict: ...
    def alerts(self) -> dict: ...
    def workspace(self, request: dict) -> dict: ...


class RemoteRunnerError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = True,
                 execution_state: str | None = None):
        super().__init__(message)
        self.code, self.message, self.retryable = code, message, retryable
        self.execution_state = execution_state


class OpenSSHRunner:
    """OpenSSH Adapter behind the small RemoteRunner Interface."""

    _SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

    def __init__(self, host: str, *, remote_root: str = "/srv/ml-orchestrator", port: int | None = None,
                 user: str | None = None, known_hosts: str | None = None, connect_timeout: int = 10,
                 command_runner: Callable[..., Any] | None = None, identity_file: str | None = None,
                 trainctl_executable: str = 'trainctl', transfer_runner: Callable[..., Any] | None = None):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.:-]*", host):
            raise ValueError("SSH host must be a non-empty alias without whitespace")
        if user is not None and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", user):
            raise ValueError("invalid SSH user")
        if port is not None and not 1 <= port <= 65535:
            raise ValueError("SSH port must be between 1 and 65535")
        if not remote_root.startswith("/"):
            raise ValueError("remote_root must be an absolute POSIX path")
        if trainctl_executable != 'trainctl' and not trainctl_executable.startswith('/'):
            raise ValueError('trainctl_executable must be trainctl or an absolute POSIX path')
        self.host = host
        self.remote_root = remote_root.rstrip("/") or "/"
        self.port = port
        self.user = user
        self.known_hosts = known_hosts
        self.identity_file = identity_file
        self.connect_timeout = connect_timeout
        self.trainctl_executable = trainctl_executable
        self._command_runner = command_runner or subprocess.run
        self._bounded_commands = command_runner is None
        self._observation_local = threading.local()
        self._transfer_runner = transfer_runner or subprocess.run

    @contextmanager
    def observation_budget(self, timeout, cancel_event=None):
        """Bound the complete health/process observation, including nested calls."""
        previous = getattr(self._observation_local, 'budget', None)
        deadline = time.monotonic() + timeout
        if previous is not None:
            deadline = min(deadline, previous[0])
        self._observation_local.budget = (deadline, cancel_event)
        try:
            yield
        finally:
            self._observation_local.budget = previous

    def health(self) -> dict:
        return self._json_command(["health", "--json"])

    def capabilities(self) -> dict:
        return self._json_command(["capabilities", "--json"])

    def compatibility(self, required: dict[str, int] | None = None) -> dict:
        # Health is understood by pre-negotiation workers. An absent declaration
        # therefore means reachable legacy software, not a network outage.
        return negotiate_worker(self.health(), required)

    def require_capabilities(self, required: dict[str, int]) -> dict:
        result = self.compatibility(required)
        if result['compatible']:
            return result
        code = ('WORKER_CAPABILITIES_UNDECLARED' if result['state'] == 'legacy'
                else 'WORKER_INCOMPATIBLE')
        raise RemoteRunnerError(code, result['reason'], retryable=False, execution_state='not_sent')

    def submit(self, spec: ExperimentSpec, idempotency_key: str) -> str:
        self._validate_id(idempotency_key, "idempotency key")
        self.require_capabilities(requirements_for_spec(spec))
        # Old workers must reject rather than silently discard task provenance.
        required = ['--require-task-metadata'] if spec.task is not None else []
        data = self._json_command(
            ["submit", "--spec", "-", "--idempotency-key", idempotency_key, "--json", *required],
            stdin=json.dumps(spec.to_dict(), ensure_ascii=False),
        )
        try:
            run_id = data["run_id"]
            self._validate_id(run_id, 'run id')
            return run_id
        except (KeyError, TypeError, ValueError) as exc:
            raise RemoteRunnerError("INVALID_REMOTE_RESPONSE", "submit response has no valid run_id",
                                    retryable=False, execution_state='unknown') from exc

    def lookup_submission(self, idempotency_key: str) -> str | None:
        self._validate_id(idempotency_key, 'idempotency key')
        self.require_capabilities({'run.lookup': 1})
        data = self._json_command(['lookup-submission', '--idempotency-key', idempotency_key, '--json'])
        if not isinstance(data, dict) or data.get('idempotency_key') != idempotency_key:
            raise RemoteRunnerError('INVALID_REMOTE_RESPONSE', 'submission lookup identity mismatch', retryable=False)
        if data.get('state') == 'absent' and data.get('run_id') is None:
            return None
        if data.get('state') == 'found':
            try:
                self._validate_id(data.get('run_id'), 'run id')
                return data['run_id']
            except (TypeError, ValueError):
                pass
        raise RemoteRunnerError('INVALID_REMOTE_RESPONSE', 'submission lookup receipt is invalid', retryable=False)

    def snapshot(self, run_id: str) -> RemoteSnapshot:
        self._validate_id(run_id, "run id")
        data = self._json_command(["snapshot", run_id])
        try:
            return RemoteSnapshot(run_id, RunStatus(data["status"]), data.get("exit_code"), data.get("updated_at"),
                                  data.get('observation'), data.get('progress'), data.get('evidence'), data.get('evidence_gate'),
                                  data.get('evidence_runtime'), data.get('source_snapshot'), data.get('runtime_configuration'),
                                  data.get('pause'))
        except (KeyError, ValueError) as exc:
            raise RemoteRunnerError("INVALID_REMOTE_RESPONSE", "snapshot response has an invalid status", retryable=False) from exc

    def events(self, run_id: str, after_seq: int) -> list[RemoteEvent]:
        self._validate_id(run_id, "run id")
        result = self._run(["events", run_id, "--after-seq", str(max(0, after_seq)), "--jsonl"])
        events: list[RemoteEvent] = []
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
                events.append(RemoteEvent(int(item["seq"]), str(item["kind"]), dict(item.get("payload") or {})))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                raise RemoteRunnerError("INVALID_REMOTE_RESPONSE", "events response is not valid JSONL", retryable=False) from exc
        return events

    def events_page(self, run_id: str, after_seq: int = 0, *, limit: int = 100,
                    max_bytes: int = 1024 * 1024) -> dict:
        from .pagination import validate_page
        validate_page(limit, max_bytes)
        self._validate_id(run_id, 'run id')
        if type(after_seq) is not int or after_seq < 0:
            raise ValueError('after_seq must be a non-negative integer')
        self.require_capabilities({'run.events.page': 1})
        data = self._json_command(['events', run_id, '--after-seq', str(after_seq), '--page',
                                  '--limit', str(limit), '--max-bytes', str(max_bytes), '--json'])
        try:
            items = data['events']
            if not isinstance(items, list):
                raise ValueError('events array expected')
            previous = after_seq
            events = []
            for item in items:
                if (not isinstance(item, dict) or type(item.get('seq')) is not int or
                        item['seq'] <= previous or not isinstance(item.get('kind'), str) or
                        not isinstance(item.get('payload'), dict)):
                    raise ValueError('event page sequence/fields invalid')
                previous = item['seq']
                events.append(RemoteEvent(item['seq'], item['kind'], item['payload']))
            if (len(events) > limit or type(data.get('next_cursor')) is not int or
                    data['next_cursor'] != previous or type(data.get('has_more')) is not bool or
                    type(data.get('truncated')) is not bool or (data['has_more'] and not events)):
                raise ValueError('event page continuation invalid')
            return {**data, 'events': events}
        except (KeyError, TypeError, ValueError) as exc:
            raise RemoteRunnerError('INVALID_REMOTE_RESPONSE', 'invalid bounded event page', retryable=False) from exc

    def cancel(self, run_id: str, reason: str) -> RemoteSnapshot:
        self._validate_id(run_id, "run id")
        if not reason.strip() or len(reason) > 500:
            raise ValueError("cancel reason must contain 1-500 characters")
        data = self._json_command(["cancel", run_id, "--reason", reason, "--json"])
        try:
            return RemoteSnapshot(run_id, RunStatus(data["status"]), data.get("exit_code"), data.get("updated_at"))
        except (KeyError, TypeError, ValueError) as exc:
            raise RemoteRunnerError('INVALID_REMOTE_RESPONSE', 'cancel response has an invalid status',
                                    retryable=False, execution_state='unknown') from exc

    def pause(self, run_id: str, reason: str, request_id: str, timeout_seconds: int = 600) -> dict:
        self._validate_id(run_id, 'run id')
        self._validate_id(request_id, 'pause request id')
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 500:
            raise ValueError('pause reason must contain 1-500 characters')
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600:
            raise ValueError('pause timeout_seconds must be an integer between 1 and 3600')
        self.require_capabilities({'run.pause': 1})
        data = self._json_command(['pause', run_id, '--reason', reason, '--request-id', request_id,
                                   '--timeout-seconds', str(timeout_seconds), '--json'])
        try:
            if not isinstance(data, dict) or data.get('run_id') != run_id:
                raise ValueError('pause run identity mismatch')
            RunStatus(data['status'])
            receipt = data.get('pause')
            if (not isinstance(receipt, dict) or type(receipt.get('schema_version')) is not int or
                    receipt['schema_version'] != 1 or receipt.get('request_id') != request_id or
                    receipt.get('state') not in {'unsupported', 'not_requested', 'requested', 'expired',
                                                 'checkpoint_committed', 'checkpoint_unverified', 'paused', 'invalid'}):
                raise ValueError('pause request identity mismatch')
            return data
        except (KeyError, TypeError, ValueError) as exc:
            raise RemoteRunnerError('INVALID_REMOTE_RESPONSE', 'pause response has no matching request receipt',
                                    retryable=False, execution_state='unknown') from exc

    def logs(self, run_id: str, stream: str = "stdout", after_offset: int = 0) -> dict:
        self._validate_id(run_id, "run id")
        if stream not in {"stdout", "stderr"} or after_offset < 0:
            raise ValueError("invalid log stream or offset")
        return self._json_command(["logs", run_id, "--stream", stream, "--after-offset", str(after_offset), "--json"])

    def artifacts(self, run_id: str, path: str | None = None) -> list[dict]:
        self._validate_id(run_id, "run id")
        command = ["artifacts", run_id, "--json"]
        if path is not None:
            if not isinstance(path, str) or not path.startswith("/") or "\x00" in path:
                raise ValueError("artifact path must be an absolute POSIX path")
            command += ["--path", path]
        data = self._json_command(command)
        if not isinstance(data, list):
            raise RemoteRunnerError("INVALID_REMOTE_RESPONSE", "artifact list expected", retryable=False)
        return data

    def artifacts_page(self, run_id: str, path: str | None = None, *, cursor: str | None = None,
                       limit: int = 100, max_bytes: int = 1024 * 1024) -> dict:
        from .pagination import validate_page, artifact_after
        validate_page(limit, max_bytes)
        self._validate_id(run_id, 'run id')
        artifact_after(cursor, run_id, path)
        if path is not None and (not isinstance(path, str) or not path.startswith('/') or '\x00' in path):
            raise ValueError('artifact path must be an absolute POSIX path')
        self.require_capabilities({'run.artifacts.page': 1})
        command = ['artifacts', run_id, '--page', '--limit', str(limit), '--max-bytes', str(max_bytes), '--json']
        if path is not None:
            command += ['--path', path]
        if cursor is not None:
            command += ['--cursor', cursor]
        data = self._json_command(command)
        try:
            if (not isinstance(data, dict) or not isinstance(data.get('artifacts'), list) or
                    len(data['artifacts']) > limit or type(data.get('has_more')) is not bool or
                    type(data.get('truncated')) is not bool or
                    (data['has_more'] and not data['artifacts'])):
                raise ValueError('invalid artifact page fields')
            last = artifact_after(cursor, run_id, path)
            for item in data['artifacts']:
                if (not isinstance(item, dict) or not isinstance(item.get('path'), str) or
                        item['path'] <= last or type(item.get('size')) is not int or item['size'] < 0):
                    raise ValueError('invalid artifact page item')
                last = item['path']
            next_after = artifact_after(data.get('next_cursor'), run_id, path)
            if next_after != last:
                raise ValueError('invalid artifact page cursor')
            return data
        except (KeyError, TypeError, ValueError) as exc:
            raise RemoteRunnerError('INVALID_REMOTE_RESPONSE', 'invalid bounded artifact page', retryable=False) from exc

    def download_artifact(self, run_id: str, artifact: dict, destination: Path) -> dict:
        """SCP Adapter for an artifact descriptor already verified at the worker seam."""
        self._validate_id(run_id, 'run id')
        remote_path = artifact.get('path') if isinstance(artifact, dict) else None
        if (not isinstance(remote_path, str) or
                not re.fullmatch(r'/[A-Za-z0-9._/-]{1,4095}', remote_path) or
                '..' in PurePosixPath(remote_path).parts):
            raise ValueError('artifact path contains unsupported characters')
        destination = Path(destination).absolute()
        if destination.exists() or destination.is_symlink():
            raise ValueError('artifact destination must be a new file')
        target = f'{self.user}@{self.host}' if self.user else self.host
        command = ['scp', '-q', '-o', 'BatchMode=yes', '-o', f'ConnectTimeout={self.connect_timeout}']
        if self.known_hosts:
            command += ['-o', 'StrictHostKeyChecking=yes', '-o', f'UserKnownHostsFile={self.known_hosts}']
        else:
            command += ['-o', 'StrictHostKeyChecking=yes']
        if self.port:
            command += ['-P', str(self.port)]
        if self.identity_file:
            command += ['-i', self.identity_file, '-o', 'IdentitiesOnly=yes']
        command += ['--', target + ':' + remote_path, str(destination)]
        try:
            result = self._transfer_runner(
                command, capture_output=True, text=True, encoding='utf-8',
                timeout=24 * 60 * 60, check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise RemoteRunnerError('ARTIFACT_TRANSFER_TIMEOUT', 'artifact transfer timed out') from exc
        except OSError as exc:
            raise RemoteRunnerError('ARTIFACT_TRANSFER_UNAVAILABLE', str(exc)) from exc
        if result.returncode != 0:
            raise RemoteRunnerError(
                'ARTIFACT_TRANSFER_FAILED', result.stderr.strip() or 'artifact transfer failed',
            )
        return {'path': str(destination), 'size': destination.stat().st_size}

    def import_checkpoint(self, replica: dict) -> dict:
        """Prepare, upload and commit one locally verified replica on this worker."""
        if not isinstance(replica, dict):
            raise ValueError('checkpoint replica must be an object')
        source = {
            'worker_id': replica.get('worker_id'),
            'run_id': replica.get('run_id'),
            'checkpoint_id': replica.get('checkpoint_id'),
            'artifact': replica.get('artifact'),
            'replica_receipt_sha256': replica.get('receipt_sha256'),
        }
        prepared = self._json_command(
            ['checkpoint-import', 'prepare', '--request', '-'],
            stdin=json.dumps({'schema_version': 1, 'source': source}, ensure_ascii=False),
        )
        if not isinstance(prepared, dict):
            raise RemoteRunnerError(
                'INVALID_REMOTE_RESPONSE', 'checkpoint import preparation is invalid', retryable=False,
            )
        if prepared.get('status') == 'present':
            return prepared
        import_id = prepared.get('import_id')
        upload_path = prepared.get('upload_path')
        if not isinstance(import_id, str) or not re.fullmatch(r'import_[a-f0-9]{32}', import_id):
            raise RemoteRunnerError('INVALID_REMOTE_RESPONSE', 'worker returned an invalid import id', retryable=False)
        expected_upload = str(
            PurePosixPath(self.remote_root) / 'checkpoint-imports' / '.incoming' /
            import_id / 'checkpoint.partial'
        )
        if upload_path != expected_upload or prepared.get('sha256') != replica.get('sha256'):
            raise RemoteRunnerError(
                'INVALID_REMOTE_RESPONSE', 'worker returned an unexpected checkpoint upload target', retryable=False,
            )
        if type(prepared.get('size')) is not int or prepared['size'] != replica.get('size'):
            raise RemoteRunnerError(
                'INVALID_REMOTE_RESPONSE', 'worker returned an unexpected checkpoint upload size', retryable=False,
            )
        if prepared.get('upload_required') is True:
            self._upload_checkpoint(Path(replica.get('checkpoint_path', '')), upload_path)
        elif prepared.get('upload_required') is not False:
            raise RemoteRunnerError(
                'INVALID_REMOTE_RESPONSE', 'worker returned an invalid upload decision', retryable=False,
            )
        return self._json_command(
            ['checkpoint-import', 'commit', import_id], timeout=24 * 60 * 60,
        )

    def _upload_checkpoint(self, source: Path, remote_path: str) -> None:
        source = Path(source).absolute()
        if source.is_symlink() or not source.is_file():
            raise ValueError('checkpoint upload source must be a regular file')
        if (not isinstance(remote_path, str) or
                not re.fullmatch(r'/[A-Za-z0-9._/-]{1,4095}', remote_path) or
                '..' in PurePosixPath(remote_path).parts):
            raise ValueError('checkpoint upload path contains unsupported characters')
        target = f'{self.user}@{self.host}' if self.user else self.host
        command = ['scp', '-q', '-o', 'BatchMode=yes', '-o', f'ConnectTimeout={self.connect_timeout}']
        if self.known_hosts:
            command += ['-o', 'StrictHostKeyChecking=yes', '-o', f'UserKnownHostsFile={self.known_hosts}']
        else:
            command += ['-o', 'StrictHostKeyChecking=yes']
        if self.port:
            command += ['-P', str(self.port)]
        if self.identity_file:
            command += ['-i', self.identity_file, '-o', 'IdentitiesOnly=yes']
        command += ['--', str(source), target + ':' + remote_path]
        try:
            result = self._transfer_runner(
                command, capture_output=True, text=True, encoding='utf-8',
                timeout=24 * 60 * 60, check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise RemoteRunnerError('CHECKPOINT_IMPORT_TIMEOUT', 'checkpoint upload timed out') from exc
        except OSError as exc:
            raise RemoteRunnerError('CHECKPOINT_IMPORT_UNAVAILABLE', str(exc)) from exc
        if result.returncode != 0:
            raise RemoteRunnerError(
                'CHECKPOINT_IMPORT_TRANSFER_FAILED', result.stderr.strip() or 'checkpoint upload failed',
            )

    def monitor(self):
        return self._json_command(['monitor'])

    def storage(self):
        return self._json_command(['storage'])

    def processes(self):
        return self._json_command(['processes'])

    def workspace(self, request):
        if not isinstance(request, dict):
            raise ValueError('workspace request must be an object')
        return self._json_command(['workspace', '--request', '-'],
                                  stdin=json.dumps(request, ensure_ascii=False))

    def samples(self, run_id, after_id=0, limit=200):
        self._validate_id(run_id, 'run id')
        if after_id < 0 or not 1 <= limit <= 1000:
            raise ValueError('Invalid sample cursor/limit')
        return self._json_command(['samples', run_id, '--after-id', str(after_id), '--limit', str(limit)])

    def alerts(self):
        return self._json_command(['alerts'])

    def _json_command(self, command: list[str], *, stdin: str | None = None,
                      timeout: int | None = None) -> Any:
        mutation = self._mutation(command, stdin)
        result = self._run(command, stdin=stdin, timeout=timeout)
        try:
            envelope = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RemoteRunnerError("INVALID_REMOTE_RESPONSE", "remote command did not return JSON", retryable=False,
                                    execution_state='unknown' if mutation else None) from exc
        if not isinstance(envelope, dict) or type(envelope.get('ok')) is not bool:
            raise RemoteRunnerError("INVALID_REMOTE_RESPONSE", "JSON envelope expected", retryable=False,
                                    execution_state='unknown' if mutation else None)
        if not envelope.get("ok"):
            error = envelope.get("error") or {}
            if not isinstance(error, dict):
                raise RemoteRunnerError('INVALID_REMOTE_RESPONSE', 'remote error must be an object', retryable=False,
                                        execution_state='unknown' if mutation else None)
            raise RemoteRunnerError(str(error.get("code", "REMOTE_ERROR")), str(error.get("message", "remote command failed")),
                                     retryable=False if mutation else bool(error.get("retryable", False)),
                                     execution_state=error.get('execution_state', 'unknown') if mutation else None)
        return envelope.get("data")

    def _run(self, command: list[str], *, stdin: str | None = None,
             timeout: int | None = None):
        from ccs.process import ProcessStopped, run_bounded
        mutation = self._mutation(command, stdin)
        target = f"{self.user}@{self.host}" if self.user else self.host
        remote_argv = [self.trainctl_executable, "--root", self.remote_root, *command]
        ssh_args = ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={self.connect_timeout}"]
        if self.known_hosts:
            ssh_args += ["-o", "StrictHostKeyChecking=yes", "-o", f"UserKnownHostsFile={self.known_hosts}"]
        else:
            ssh_args += ["-o", "StrictHostKeyChecking=yes"]
        if self.port:
            ssh_args += ["-p", str(self.port)]
        if self.identity_file:
            ssh_args += ["-i", self.identity_file, "-o", "IdentitiesOnly=yes"]
        remote_command = " ".join(shlex.quote(str(token)) for token in remote_argv)
        command_timeout = timeout if timeout is not None else self.connect_timeout + 30
        budget = getattr(self._observation_local, 'budget', None)
        cancel_event = budget[1] if budget else None
        if budget:
            command_timeout = min(command_timeout, budget[0] - time.monotonic())
        if command_timeout <= 0 or (cancel_event is not None and cancel_event.is_set()):
            raise RemoteRunnerError('SSH_TIMEOUT', 'local observation budget expired before dispatch',
                                    retryable=not mutation, execution_state='not_sent' if mutation else None)
        try:
            if self._bounded_commands:
                captured = run_bounded([*ssh_args, target, remote_command],
                                       input=stdin.encode('utf-8') if stdin is not None else None,
                                       timeout=command_timeout, cancel_event=cancel_event,
                                       max_stdout_bytes=20 * 1024 * 1024, max_stderr_bytes=64 * 1024)
                result = subprocess.CompletedProcess(captured.args, captured.returncode,
                                                      captured.stdout.decode('utf-8', errors='replace'),
                                                      captured.stderr.decode('utf-8', errors='replace'))
            else:
                result = self._command_runner(
                    [*ssh_args, target, remote_command], input=stdin, capture_output=True,
                    text=True, encoding='utf-8', timeout=command_timeout, check=False,
                )
                if (len(result.stdout.encode('utf-8')) > 20 * 1024 * 1024 or
                        len(getattr(result, 'stderr', '').encode('utf-8')) > 64 * 1024):
                    raise ProcessStopped('OUTPUT_LIMIT', started=True)
        except ProcessStopped as exc:
            code = ('SSH_OUTPUT_LIMIT' if exc.code == 'OUTPUT_LIMIT' else
                    'SSH_TIMEOUT' if exc.code == 'TIMEOUT' else
                    'SSH_CANCELLED' if exc.code == 'CANCELLED' else 'SSH_CONTAINMENT_FAILED')
            message = ('SSH output truncated at the transport budget; use run.events/run.artifacts '
                       'limit/max_bytes or the run.logs cursor to continue; mutations must use operation reconciliation'
                       if exc.code == 'OUTPUT_LIMIT' else 'local SSH process stopped: ' + exc.code)
            error = RemoteRunnerError(code, message, retryable=not mutation and exc.code == 'TIMEOUT',
                                      execution_state=('unknown' if exc.started else 'not_sent') if mutation else None)
            if exc.code == 'OUTPUT_LIMIT':
                error.output_truncated = True
                error.continuation = ('Resume reads with the last delivered cursor and bounded page options; '
                                      'reconcile mutations using their original operation identity.')
            raise error from exc
        except subprocess.TimeoutExpired as exc:
            raise RemoteRunnerError("SSH_TIMEOUT", "SSH command timed out", retryable=not mutation,
                                    execution_state='unknown' if mutation else None) from exc
        except OSError as exc:
            raise RemoteRunnerError("SSH_UNAVAILABLE", str(exc), retryable=not mutation,
                                    execution_state='not_sent' if mutation else None) from exc
        if result.returncode != 0:
            try:
                envelope = json.loads(result.stdout)
            except (ValueError, TypeError):
                envelope = {}
            if (isinstance(envelope, dict) and envelope.get('ok') is False and
                    isinstance(envelope.get("error"), dict)):
                error = envelope["error"]
                raise RemoteRunnerError(error.get("code", "REMOTE_ERROR"), error.get("message", "remote error"),
                                        retryable=False if mutation else bool(error.get("retryable", False)),
                                        execution_state=error.get('execution_state', 'unknown') if mutation else None)
            raise RemoteRunnerError("SSH_COMMAND_FAILED", result.stderr.strip() or "SSH command failed",
                                    retryable=not mutation, execution_state='unknown' if mutation else None)
        return result

    @staticmethod
    def _mutation(command, stdin):
        if command[0] in {'submit', 'cancel', 'pause', 'checkpoint-import'}:
            return True
        if command[0] == 'workspace':
            try:
                return json.loads(stdin or '{}').get('action') not in {'inspect', 'diff', 'changes'}
            except (ValueError, TypeError):
                return True
        return False

    @classmethod
    def _validate_id(cls, value: str, label: str) -> None:
        if not cls._SAFE_ID.fullmatch(value):
            raise ValueError(f"{label} contains unsupported characters")


class FakeRemoteRunner:
    """Deterministic Adapter used before the OpenSSH Adapter exists."""

    def __init__(self):
        self.runs: dict[str, dict] = {}
        self.keys: dict[str, str] = {}

    def health(self) -> dict:
        from .capabilities import worker_interface
        return {"online": True, "adapter": "fake", "checked_at": utc_now(),
                "ccs_interface": worker_interface()}

    def capabilities(self) -> dict:
        return self.health()['ccs_interface']

    def compatibility(self, required: dict[str, int] | None = None) -> dict:
        return negotiate_worker(self.health(), required)

    def require_capabilities(self, required: dict[str, int]) -> dict:
        return self.compatibility(required)

    def submit(self, spec: ExperimentSpec, idempotency_key: str) -> str:
        if idempotency_key in self.keys:
            return self.keys[idempotency_key]
        run_id = f"remote_{len(self.runs) + 1:04d}"
        self.keys[idempotency_key] = run_id
        self.runs[run_id] = {
            "status": RunStatus.QUEUED,
            "exit_code": None,
            "spec": spec.to_dict(),
            "artifacts": [],
            "events": [RemoteEvent(1, "submitted", {"idempotency_key": idempotency_key})],
        }
        return run_id

    def lookup_submission(self, idempotency_key: str) -> str | None:
        return self.keys.get(idempotency_key)

    def snapshot(self, run_id: str) -> RemoteSnapshot:
        run = self.runs[run_id]
        return RemoteSnapshot(
            run_id, run["status"], run["exit_code"], utc_now(),
            observation=run.get('observation'),
            pause=run.get('pause'),
        )

    def events(self, run_id: str, after_seq: int) -> list[RemoteEvent]:
        return [event for event in self.runs[run_id]["events"] if event.seq > after_seq]

    def events_page(self, run_id: str, after_seq: int = 0, *, limit: int = 100,
                    max_bytes: int = 1024 * 1024) -> dict:
        from .pagination import bounded_items
        items = ({'seq': event.seq, 'kind': event.kind, 'payload': event.payload}
                 for event in self.events(run_id, after_seq))
        selected, cursor, more, truncated = bounded_items(
            items, key=lambda item: item['seq'], initial_cursor=after_seq, encode_cursor=lambda value: value,
            limit=limit, max_bytes=max_bytes,
        )
        return {'events': [RemoteEvent(item['seq'], item['kind'], item['payload']) for item in selected],
                'next_cursor': cursor, 'has_more': more, 'truncated': truncated}

    def logs(self, run_id: str, stream: str = 'stdout', after_offset: int = 0) -> dict:
        self.runs[run_id]
        return {'run_id': run_id, 'stream': stream, 'offset': after_offset,
                'next_offset': after_offset, 'text': '', 'content': '', 'eof': True, 'has_more': False}

    def cancel(self, run_id: str, reason: str) -> RemoteSnapshot:
        self.advance(run_id, RunStatus.CANCELLED, payload={"reason": reason})
        return self.snapshot(run_id)

    def pause(self, run_id: str, reason: str, request_id: str, timeout_seconds: int = 600) -> dict:
        # This fixture records a request only: no training adapter saves a checkpoint here.
        run = self.runs[run_id]
        run['pause'] = {'schema_version': 1, 'request_id': request_id, 'state': 'requested',
                        'reason': reason, 'timeout_seconds': timeout_seconds}
        return {'run_id': run_id, 'status': run['status'].value, 'pause': dict(run['pause'])}

    def artifacts(self, run_id: str, path: str | None = None) -> list[dict]:
        items = self.runs[run_id]["artifacts"]
        return [dict(item) for item in items if path is None or item.get("path") == path]

    def artifacts_page(self, run_id: str, path: str | None = None, *, cursor: str | None = None,
                       limit: int = 100, max_bytes: int = 1024 * 1024) -> dict:
        from .pagination import artifact_after, artifact_cursor, bounded_items
        after = artifact_after(cursor, run_id, path)
        items = (item for item in sorted(self.artifacts(run_id, path), key=lambda item: item['path'])
                 if item['path'] > after)
        selected, next_cursor, more, truncated = bounded_items(
            items, key=lambda item: item['path'], initial_cursor=cursor,
            encode_cursor=lambda value: artifact_cursor(run_id, path, value), limit=limit, max_bytes=max_bytes,
        )
        return {'artifacts': selected, 'next_cursor': next_cursor, 'has_more': more, 'truncated': truncated}

    def download_artifact(self, run_id: str, artifact: dict, destination: Path) -> dict:
        self.runs[run_id]
        path = artifact.get('path')
        try:
            data = self._artifact_content[(run_id, path)]
        except (AttributeError, KeyError) as exc:
            raise ValueError('fake artifact content is unavailable') from exc
        destination = Path(destination)
        destination.write_bytes(data)
        return {'path': str(destination), 'size': len(data)}

    def import_checkpoint(self, replica: dict) -> dict:
        path = Path(replica.get('checkpoint_path', ''))
        artifact = replica.get('artifact')
        if (not isinstance(artifact, dict) or not path.is_file() or path.is_symlink() or
                path.stat().st_size != artifact.get('size') or
                __import__('hashlib').sha256(path.read_bytes()).hexdigest() != artifact.get('sha256')):
            raise ValueError('fake checkpoint replica is invalid')
        source = {
            'worker_id': replica.get('worker_id'),
            'run_id': replica.get('run_id'),
            'checkpoint_id': replica.get('checkpoint_id'),
            'artifact': dict(artifact),
            'replica_receipt_sha256': replica.get('receipt_sha256'),
        }
        canonical = json.dumps(
            {'schema_version': 1, 'source': source},
            sort_keys=True, separators=(',', ':'),
        ).encode()
        import_id = 'import_' + __import__('hashlib').sha256(canonical).hexdigest()[:32]
        if not hasattr(self, 'checkpoint_imports'):
            self.checkpoint_imports = {}
        status = 'present' if import_id in self.checkpoint_imports else 'imported'
        self.checkpoint_imports.setdefault(import_id, source)
        return {
            'schema_version': 1,
            'status': status,
            'import_id': import_id,
            'checkpoint_path': '/fake/checkpoint-imports/' + import_id + '/checkpoint.ckpt',
            'manifest_path': '/fake/checkpoint-imports/' + import_id + '/manifest.json',
            'size': artifact['size'],
            'sha256': artifact['sha256'],
            'source': source,
            'import_receipt_sha256': 'f' * 64,
        }

    def monitor(self) -> dict:
        return {'online': True, 'runs': len(self.runs), 'checked_at': utc_now()}

    def storage(self) -> dict:
        return {'online': True, 'pressure': 'normal', 'checked_at': utc_now()}

    def processes(self) -> dict:
        return {'online': True, 'processes': [], 'checked_at': utc_now()}

    def alerts(self) -> dict:
        return {'online': True, 'alerts': [], 'checked_at': utc_now()}

    def workspace(self, request: dict) -> dict:
        return {'accepted': True, 'request': dict(request)}

    def set_artifacts(self, run_id: str, artifacts: list[dict]) -> None:
        self.runs[run_id]["artifacts"] = [dict(item) for item in artifacts]

    def set_artifact_content(self, run_id: str, path: str, data: bytes) -> None:
        if not hasattr(self, '_artifact_content'):
            self._artifact_content = {}
        digest = __import__('hashlib').sha256(data).hexdigest()
        self._artifact_content[(run_id, path)] = bytes(data)
        self.runs[run_id]['artifacts'] = [
            {'path': path, 'size': len(data), 'sha256': digest},
        ]

    def set_managed_checkpoint(self, run_id: str, checkpoint_id: str, path: str, data: bytes) -> None:
        self.set_artifact_content(run_id, path, data)
        artifact = self.runs[run_id]['artifacts'][0]
        self.runs[run_id]['observation'] = {'checkpoint': {
            'state': 'committed',
            'latest': {
                'id': checkpoint_id,
                'size': artifact['size'],
                'sha256': artifact['sha256'],
            },
        }}

    def advance(self, run_id: str, status: RunStatus, *, exit_code: int | None = None, payload: dict | None = None) -> None:
        run = self.runs[run_id]
        next_seq = len(run["events"]) + 1
        run["status"] = status
        run["exit_code"] = exit_code
        run["events"].append(RemoteEvent(next_seq, "status_changed", {
            "status": status.value, "exit_code": exit_code, **(payload or {})
        }))
