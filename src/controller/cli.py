from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from .domain import ExperimentSpec
from .orchestrator import Orchestrator
from .remote import OpenSSHRunner
from .repository import RunRepository
from .monitor import watch

MAX_WORKSPACE_REQUEST_BYTES = 20 * 1024 * 1024


def _workspace_request(source):
    if source == '-':
        raw = sys.stdin.buffer.read(MAX_WORKSPACE_REQUEST_BYTES + 1)
    else:
        path = Path(source)
        if path.stat().st_size > MAX_WORKSPACE_REQUEST_BYTES:
            raise ValueError('workspace request exceeds 20 MiB')
        raw = path.read_bytes()
    if len(raw) > MAX_WORKSPACE_REQUEST_BYTES:
        raise ValueError('workspace request exceeds 20 MiB')
    return json.loads(raw.decode('utf-8'))


def _capability_requirements(values: list[str]) -> dict[str, int]:
    required: dict[str, int] = {}
    for value in values:
        name, separator, raw_version = value.partition('=')
        if not separator or not name or name.strip() != name:
            raise ValueError('capability requirement must use NAME=VERSION')
        try:
            minimum = int(raw_version)
        except ValueError as exc:
            raise ValueError('capability version must be a positive integer') from exc
        if minimum < 1 or name in required:
            raise ValueError('capability requirements must be unique and use positive versions')
        required[name] = minimum
    return dict(sorted(required.items()))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orchestratorctl")
    parser.add_argument("--db", default="data/orchestrator.db")
    parser.add_argument("--host", required=True, help="SSH alias or hostname")
    parser.add_argument("--remote-root", default="/srv/ml-orchestrator")
    parser.add_argument("--user")
    parser.add_argument("--port", type=int)
    parser.add_argument("--known-hosts")
    parser.add_argument("--identity-file")
    parser.add_argument('--workspace-user')
    parser.add_argument('--workspace-root')
    parser.add_argument('--workspace-executable')
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("health")
    compatibility = sub.add_parser("compatibility")
    compatibility.add_argument('--require', action='append', default=[], metavar='NAME=VERSION')
    sub.add_parser('monitor')
    sub.add_parser('alerts')
    sub.add_parser('storage')
    workspace = sub.add_parser('workspace')
    workspace.add_argument('--request', required=True)
    workflow = sub.add_parser('workflow')
    workflow.add_argument('--request', required=True)
    samples = sub.add_parser('samples')
    samples.add_argument('run_id')
    samples.add_argument('--after-id', type=int, default=0)
    samples.add_argument('--limit', type=int, default=200)
    submit = sub.add_parser("submit")
    submit.add_argument("--spec", required=True)
    submit.add_argument("--idempotency-key", required=True)
    sub.add_parser("reconcile")
    sub.add_parser("list")
    sub.add_parser('catalog')
    result = sub.add_parser('result')
    result.add_argument('run_id')
    result.add_argument('--cache-directory', default='data/dashboard')
    monitor = sub.add_parser("watch")
    monitor.add_argument("--interval", type=float, default=5)
    monitor.add_argument("--max-interval", type=float, default=60)
    monitor.add_argument("--max-polls", type=int)
    monitor.add_argument("--until-idle", action="store_true")
    logs = sub.add_parser("logs")
    logs.add_argument("run_id")
    logs.add_argument("--stream", choices=["stdout", "stderr"], default="stdout")
    logs.add_argument("--after-offset", type=int, default=0)
    sync = sub.add_parser('sync-logs')
    sync.add_argument('run_id')
    sync.add_argument('--stream', choices=['stdout', 'stderr'], default='stdout')
    sync.add_argument('--max-pages', type=int, default=32)
    artifacts = sub.add_parser("artifacts")
    artifacts.add_argument("run_id")
    artifacts.add_argument("--path")
    snapshot = sub.add_parser("snapshot")
    snapshot.add_argument("run_id")
    events = sub.add_parser("events")
    events.add_argument("run_id")
    events.add_argument("--after-seq", type=int, default=0)
    cancel = sub.add_parser("cancel")
    cancel.add_argument("run_id")
    cancel.add_argument("--reason", required=True)
    return parser


def _response(data=None, error=None):
    return {"ok": error is None, "data": data, "error": error}


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command in ('catalog', 'result'):
            from .results import cached_result
            result = (RunRepository.catalog(args.db, args.host) if args.command == 'catalog'
                      else cached_result(args.cache_directory, args.run_id))
            if args.command == 'result' and result['task'].get('worker') != args.host:
                raise ValueError('Cached result belongs to a different or unknown worker')
            print(json.dumps(_response(result), ensure_ascii=True, allow_nan=False))
            return 0
        repository = RunRepository(args.db)
        remote = OpenSSHRunner(args.host, remote_root=args.remote_root, user=args.user, port=args.port,
                               known_hosts=args.known_hosts, identity_file=args.identity_file)
        orchestrator = Orchestrator(repository, remote, worker=args.host)
        if args.command == "health":
            result = remote.health()
        elif args.command == "compatibility":
            result = remote.compatibility(_capability_requirements(args.require))
        elif args.command == 'monitor':
            result = remote.monitor()
        elif args.command == 'storage':
            result = remote.storage()
        elif args.command == 'alerts':
            result = remote.alerts()
        elif args.command == 'workspace':
            workspace_remote = OpenSSHRunner(args.host, remote_root=args.workspace_root or args.remote_root,
                user=args.workspace_user or args.user, port=args.port,
                known_hosts=args.known_hosts, identity_file=args.identity_file,
                trainctl_executable=args.workspace_executable or 'trainctl')
            result = workspace_remote.workspace(_workspace_request(args.request))
        elif args.command == 'workflow':
            from .workflow import WorkflowEngine
            result = WorkflowEngine(args.db, orchestrator).execute(_workspace_request(args.request))
        elif args.command == 'samples':
            result = remote.samples(args.run_id, args.after_id, args.limit)
        elif args.command == "submit":
            raw = json.load(sys.stdin) if args.spec == "-" else json.loads(Path(args.spec).read_text(encoding="utf-8"))
            result = asdict(orchestrator.submit(ExperimentSpec.from_dict(raw), idempotency_key=args.idempotency_key))
        elif args.command == "reconcile":
            result = {"updated": [asdict(run) for run in orchestrator.reconcile()], "errors": orchestrator.last_errors}
        elif args.command == "watch":
            for report in watch(orchestrator, interval=args.interval, max_interval=args.max_interval,
                                max_polls=args.max_polls, until_idle=args.until_idle):
                print(json.dumps(report, ensure_ascii=True), flush=True)
            return 1 if orchestrator.last_errors else 0
        elif args.command == "list":
            result = [asdict(run) for run in repository.list_runs(args.host)]
        elif args.command == "logs":
            result = remote.logs(args.run_id, args.stream, args.after_offset)
        elif args.command == 'sync-logs':
            from .log_sync import sync_logs
            result = sync_logs(remote, args.run_id, args.stream, Path(args.db).resolve().parent / 'logs', max_pages=args.max_pages)
        elif args.command == "artifacts":
            result = remote.artifacts(args.run_id, path=args.path)
        elif args.command == "snapshot":
            result = orchestrator.remote.snapshot(args.run_id).__dict__
        elif args.command == "events":
            result = [event.__dict__ for event in orchestrator.remote.events(args.run_id, args.after_seq)]
        elif args.command == "cancel":
            result = asdict(orchestrator.cancel(args.run_id, args.reason))
        else:
            raise ValueError(f"unknown command: {args.command}")
        # JSON escapes preserve Unicode even on legacy Windows console encodings.
        print(json.dumps(_response(result), ensure_ascii=True, default=str))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(json.dumps(_response(error={"code": getattr(exc, "code", type(exc).__name__),
                                          "message": str(exc), "retryable": getattr(exc, "retryable", False)}), ensure_ascii=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
