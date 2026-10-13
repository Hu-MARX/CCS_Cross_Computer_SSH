from __future__ import annotations

import argparse
import json
import sys
import sqlite3
from pathlib import Path

from controller.domain import ExperimentSpec
from .run_store import RunStore, TrainCtlError

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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trainctl")
    parser.add_argument("--root", default=".trainctl", help="run store root")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("health").add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    sub.add_parser("capabilities").add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    sub.add_parser('monitor')
    sub.add_parser('processes')
    workspace = sub.add_parser('workspace')
    workspace.add_argument('--request', required=True)
    sub.add_parser('alerts')
    sub.add_parser('storage')
    evidence = sub.add_parser('evidence')
    evidence_sub = evidence.add_subparsers(dest='evidence_command', required=True)
    capture = evidence_sub.add_parser('capture')
    capture.add_argument('--plan', required=True)
    verify = evidence_sub.add_parser('verify')
    verify.add_argument('bundle_id')
    verify.add_argument('--expected-sha256', required=True)
    verify.add_argument('--check-sources', action='store_true')
    verify.add_argument('--check-environment', action='store_true')
    maintenance = sub.add_parser('maintain')
    maintenance.add_argument('--force', action='store_true')
    samples = sub.add_parser('samples')
    samples.add_argument('run_id')
    samples.add_argument('--after-id', type=int, default=0)
    samples.add_argument('--limit', type=int, default=200)
    submit = sub.add_parser("submit")
    submit.add_argument("--spec", required=True)
    submit.add_argument("--idempotency-key", required=True)
    submit.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    submit.add_argument('--require-task-metadata', action='store_true')
    lookup = sub.add_parser('lookup-submission')
    lookup.add_argument('--idempotency-key', required=True)
    lookup.add_argument('--json', action='store_true', help=argparse.SUPPRESS)
    snapshot = sub.add_parser("snapshot")
    snapshot.add_argument("run_id")
    snapshot.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    events = sub.add_parser("events")
    events.add_argument("run_id")
    events.add_argument("--after-seq", type=int, default=0)
    events.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    events.add_argument("--jsonl", action="store_true", help=argparse.SUPPRESS)
    events.add_argument('--page', action='store_true', help='return bounded cursor page')
    events.add_argument('--limit', type=int, default=100)
    events.add_argument('--max-bytes', type=int, default=1024 * 1024)
    logs = sub.add_parser("logs")
    logs.add_argument("run_id")
    logs.add_argument("--stream", choices=["stdout", "stderr"], default="stdout")
    logs.add_argument("--after-offset", type=int, default=0)
    logs.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    artifacts = sub.add_parser("artifacts")
    artifacts.add_argument("run_id")
    artifacts.add_argument("--path")
    artifacts.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    artifacts.add_argument('--page', action='store_true', help='return bounded cursor page')
    artifacts.add_argument('--cursor')
    artifacts.add_argument('--limit', type=int, default=100)
    artifacts.add_argument('--max-bytes', type=int, default=1024 * 1024)
    checkpoint_import = sub.add_parser('checkpoint-import')
    checkpoint_import_sub = checkpoint_import.add_subparsers(
        dest='checkpoint_import_command', required=True,
    )
    import_prepare = checkpoint_import_sub.add_parser('prepare')
    import_prepare.add_argument('--request', required=True)
    import_commit = checkpoint_import_sub.add_parser('commit')
    import_commit.add_argument('import_id')
    import_show = checkpoint_import_sub.add_parser('show')
    import_show.add_argument('import_id')
    cancel = sub.add_parser("cancel")
    cancel.add_argument("run_id")
    cancel.add_argument("--reason", required=True)
    cancel.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    pause = sub.add_parser('pause')
    pause.add_argument('run_id')
    pause.add_argument('--reason', required=True)
    pause.add_argument('--request-id', required=True)
    pause.add_argument('--timeout-seconds', type=int, default=600)
    pause.add_argument('--json', action='store_true', help=argparse.SUPPRESS)
    recover = sub.add_parser("recover")
    recover.add_argument("run_id")
    recover.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    evaluate = sub.add_parser("evaluate")
    evaluate.add_argument("run_id")
    evaluate.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    return parser


def _response(data=None, error=None) -> dict:
    return {"ok": error is None, "request_id": f"req_{id(data if data is not None else error):x}",
            "data": data, "error": error}


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    args = _parser().parse_args(argv)
    store = RunStore(args.root, initialize=args.command != 'lookup-submission')
    try:
        if args.command == "health":
            result = store.health()
        elif args.command == "capabilities":
            from controller.capabilities import worker_interface
            result = worker_interface()
        elif args.command == 'evidence':
            from .evidence import EvidenceStore
            evidence = EvidenceStore(store.root / 'evidence')
            if args.evidence_command == 'capture':
                plan = json.load(sys.stdin) if args.plan == '-' else json.loads(Path(args.plan).read_text(encoding='utf-8'))
                result = evidence.capture(plan)
            else:
                result = evidence.verify(args.bundle_id, args.expected_sha256, check_sources=args.check_sources,
                                         check_environment=args.check_environment)
                if not result['verified']:
                    print(json.dumps(_response(result), ensure_ascii=True))
                    return 1
        elif args.command == 'monitor':
            result = store.monitor()
        elif args.command == 'processes':
            result = store.processes()
        elif args.command == 'workspace':
            result = store.workspace(_workspace_request(args.request))
        elif args.command == 'storage':
            result = store.storage()
        elif args.command == 'maintain':
            from .storage import maintain
            result = maintain(store, force=args.force)
        elif args.command in {'samples', 'alerts'}:
            from .telemetry import Telemetry
            telemetry = Telemetry(store.root)
            result = telemetry.alerts() if args.command == 'alerts' else telemetry.samples(args.run_id, args.after_id, args.limit)
        elif args.command == "submit":
            raw = json.load(sys.stdin) if args.spec == "-" else json.loads(Path(args.spec).read_text(encoding="utf-8"))
            if args.require_task_metadata and raw.get('task') is None:
                raise TrainCtlError('TASK_METADATA_REQUIRED', 'task metadata is required')
            result = store.submit(ExperimentSpec.from_dict(raw), args.idempotency_key)
        elif args.command == 'lookup-submission':
            result = store.lookup_submission(args.idempotency_key)
        elif args.command == "snapshot":
            result = store.snapshot(args.run_id)
        elif args.command == "events":
            if args.page and args.jsonl:
                raise ValueError('--page cannot be combined with --jsonl')
            result = (store.events_page(args.run_id, args.after_seq, limit=args.limit, max_bytes=args.max_bytes)
                      if args.page else store.events(args.run_id, args.after_seq))
        elif args.command == "logs":
            result = store.logs(args.run_id, args.stream, args.after_offset)
        elif args.command == "artifacts":
            result = (store.artifacts_page(args.run_id, path=args.path, cursor=args.cursor,
                                          limit=args.limit, max_bytes=args.max_bytes)
                      if args.page else store.artifacts(args.run_id, path=args.path))
        elif args.command == 'checkpoint-import':
            from .checkpoint_imports import CheckpointImportStore
            imports = CheckpointImportStore(
                store.root / 'checkpoint-imports',
                min_free_bytes=store.config().get('min_free_bytes', 0),
            )
            if args.checkpoint_import_command == 'prepare':
                result = imports.prepare(_workspace_request(args.request))
            elif args.checkpoint_import_command == 'commit':
                result = imports.commit(args.import_id)
            else:
                result = imports.show(args.import_id)
        elif args.command == "cancel":
            result = store.cancel(args.run_id, args.reason)
        elif args.command == 'pause':
            result = store.pause(args.run_id, args.reason, args.request_id, args.timeout_seconds)
        elif args.command == "recover":
            result = store.snapshot(args.run_id)
        elif args.command == "evaluate":
            raise TrainCtlError("EVALUATION_NOT_IMPLEMENTED", "evaluation execution is the next implementation slice")
        else:
            raise TrainCtlError("UNKNOWN_COMMAND", args.command)
        if args.command == "events" and args.jsonl:
            for event in result:
                print(json.dumps(event, ensure_ascii=False))
        else:
            print(json.dumps(_response(result), ensure_ascii=False))
        return 0
    except (TrainCtlError, ValueError, OSError, KeyError, RuntimeError, sqlite3.Error) as exc:
        if isinstance(exc, TrainCtlError):
            error = {"code": exc.code, "message": exc.message, "retryable": exc.retryable}
            if exc.execution_state is not None:
                error['execution_state'] = exc.execution_state
        else:
            error = {"code": "INVALID_REQUEST", "message": str(exc), "retryable": False}
        print(json.dumps(_response(error=error), ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
