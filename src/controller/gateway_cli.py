"""Machine-oriented command line entry point for the CCS Agent Interface."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .fleet import directory_from_config
from .gateway import CCSGateway, CCSRequestError
from .remote import OpenSSHRunner
from .repository import RunRepository
from ccs.contracts import describe, request_schema, result_schema, experiment_spec_schema
from . import __version__


MAX_REQUEST_BYTES = 20 * 1024 * 1024


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='ccsctl')
    parser.add_argument('--version', action='version', version=__version__)
    target = parser.add_mutually_exclusive_group()
    target.add_argument('--host', help='SSH alias, host name, or Tailscale address')
    target.add_argument('--fleet', help='UTF-8 JSON worker-pool configuration')
    parser.add_argument('--db', default='data/orchestrator.db')
    parser.add_argument('--audit-db', default='data/ccs-audit.db')
    parser.add_argument('--replica-root', default='data/checkpoint-replicas')
    parser.add_argument('--remote-root', default='/srv/ml-orchestrator')
    parser.add_argument('--user')
    parser.add_argument('--port', type=int)
    parser.add_argument('--known-hosts')
    parser.add_argument('--identity-file')
    parser.add_argument('--workspace-user')
    parser.add_argument('--workspace-root')
    parser.add_argument('--workspace-executable')
    subcommands = parser.add_subparsers(dest='command', required=True)
    request = subcommands.add_parser('request', help='execute one CCS JSON request')
    request.add_argument('--file', required=True, help='UTF-8 JSON file, or - for stdin')
    contract = subcommands.add_parser('describe', help='query installed action contracts without a target or network')
    selection = contract.add_mutually_exclusive_group()
    selection.add_argument('--action', help='one CCS action; omission lists all actions')
    selection.add_argument('--schema', choices=('request', 'result', 'experiment-spec'),
                           help='export an installed JSON schema')
    contract.add_argument('--workspace-action', help='one workspace.execute sub-action')
    diagnostics = subcommands.add_parser('doctor', help='offline installation/configuration checks')
    diagnostics.add_argument('--python', help='absolute Python interpreter; default current interpreter')
    diagnostics.add_argument('--command-json', help='gateway argv JSON; never executed by offline checks')
    diagnostics.add_argument('--cwd', help='expected gateway working directory')
    diagnostics.add_argument('--mcp-config', help='existing MCP configuration to inspect')
    diagnostics.add_argument('--require-mcp', action='store_true')
    diagnostics.add_argument('--probe-remote', action='store_true', help='explicitly enable one read-only worker health probe')
    diagnostics.add_argument('--worker-id', help='worker selected for the explicit health probe')
    configuration = subcommands.add_parser('mcp-config', help='preview an absolute stdio MCP launcher configuration')
    configuration.add_argument('--python')
    configuration.add_argument('--command-json', required=True)
    configuration.add_argument('--cwd', required=True)
    configuration.add_argument('--output', help='new configuration path; never overwritten')
    configuration.add_argument('--name', default='ccs')
    configuration.add_argument('--allow-mutations', action='store_true', help='omit the default --read-only startup guard')
    configuration.add_argument('--write', action='store_true')
    configuration.add_argument('--confirm-sha256', help='digest returned by the reviewed configuration preview')
    return parser


def _read_request(source: str) -> dict[str, Any]:
    if source == '-':
        raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
    else:
        path = Path(source)
        if path.stat().st_size > MAX_REQUEST_BYTES:
            raise CCSRequestError('REQUEST_TOO_LARGE', 'CCS request exceeds 20 MiB')
        raw = path.read_bytes()
    if len(raw) > MAX_REQUEST_BYTES:
        raise CCSRequestError('REQUEST_TOO_LARGE', 'CCS request exceeds 20 MiB')
    try:
        value = json.loads(raw.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CCSRequestError('INVALID_JSON', 'request file must contain UTF-8 JSON') from exc
    if not isinstance(value, dict):
        raise CCSRequestError('INVALID_REQUEST', 'CCS request must be a JSON object')
    return value


def _fleet_config(source: str) -> dict[str, Any]:
    path = Path(source)
    if path.stat().st_size > 1024 * 1024:
        raise CCSRequestError('FLEET_CONFIG_TOO_LARGE', 'fleet configuration exceeds 1 MiB')
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CCSRequestError('INVALID_FLEET_CONFIG', 'fleet configuration must contain UTF-8 JSON') from exc
    if (not isinstance(value, dict) or set(value) != {'schema_version', 'workers'} or
            value.get('schema_version') != 1 or not isinstance(value.get('workers'), list) or
            not 1 <= len(value['workers']) <= 64):
        raise CCSRequestError('INVALID_FLEET_CONFIG', 'fleet configuration requires 1-64 workers')
    return value


def _worker_targets(args):
    if args.host is not None:
        config = {'schema_version': 1, 'workers': [{'id': args.host, 'host': args.host}]}
    else:
        config = _fleet_config(args.fleet)
    try:
        directory = directory_from_config(
            config, OpenSSHRunner, remote_root=args.remote_root, user=args.user, port=args.port,
            known_hosts=args.known_hosts, identity_file=args.identity_file,
            workspace_user=args.workspace_user, workspace_root=args.workspace_root,
            workspace_executable=args.workspace_executable,
        )
    except (TypeError, ValueError) as exc:
        raise CCSRequestError('INVALID_FLEET_CONFIG', str(exc)) from exc
    first_id = directory.default_worker_id
    return (directory.remote_for(first_id), directory.remote_for(first_id, workspace=True),
            directory, first_id)


def _envelope(*, request_id=None, data=None, error=None):
    return {'ok': error is None, 'request_id': request_id, 'data': data, 'error': error}


def _print(value) -> None:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    print(json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')))


def main(argv: list[str] | None = None) -> int:
    request = None
    try:
        args = _parser().parse_args(argv)
        if args.command == 'doctor':
            from ccs.diagnostics import doctor
            data = doctor(command=args.command_json, cwd=args.cwd, python=args.python,
                          mcp_config=args.mcp_config, require_mcp=args.require_mcp)
            if args.probe_remote:
                if not (args.host or args.fleet) or not args.worker_id:
                    raise CCSRequestError('TARGET_REQUIRED', 'remote doctor requires --host/--fleet and explicit --worker-id')
                _, _, directory, _ = _worker_targets(args)
                remote = directory.remote_for(args.worker_id)
                # Health only. No database, submission, process cleanup or worker mutation.
                data['remote_probe'] = {'worker_id': args.worker_id, 'health': remote.health()}
            _print(_envelope(data=data))
            return 0 if data['ok'] else 1
        if args.command == 'mcp-config':
            from ccs.diagnostics import configuration_plan, write_configuration
            data = configuration_plan(command=args.command_json, cwd=args.cwd, python=args.python,
                                      output=args.output, name=args.name, read_only=not args.allow_mutations)
            if args.write:
                data = write_configuration(data, args.confirm_sha256)
            _print(_envelope(data=data))
            return 0
        if args.command == 'describe':
            if args.schema is not None:
                if args.workspace_action is not None:
                    raise CCSRequestError('INVALID_REQUEST', '--workspace-action cannot be combined with --schema')
                schemas = {'request': request_schema, 'result': result_schema,
                           'experiment-spec': experiment_spec_schema}
                data = schemas[args.schema]()
            else:
                data = describe(args.action, args.workspace_action)
            _print(_envelope(data=data))
            return 0
        request = _read_request(args.file)
        request = CCSGateway._request(request)
        CCSGateway._validate_payload(request['action'], request['payload'])
        if request.get('action') == 'gateway.describe':
            result = CCSGateway.describe_request(request)
            _print(_envelope(request_id=result['request_id'], data=result))
            return 0
        if args.host is None and args.fleet is None:
            raise CCSRequestError('TARGET_REQUIRED', 'request requires --host or --fleet')
        remote, workspace_remote, worker_directory, default_worker = _worker_targets(args)
        gateway = CCSGateway(
            remote, repository=RunRepository(args.db), worker=default_worker,
            workspace_remote=workspace_remote, worker_directory=worker_directory,
            audit_path=args.audit_db,
            replica_root=args.replica_root,
        )
        result = gateway.execute(request)
        _print(_envelope(request_id=result['request_id'], data=result))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        request_id = request.get('request_id') if isinstance(request, dict) else None
        error = {
            'code': getattr(exc, 'code', type(exc).__name__),
            'message': getattr(exc, 'message', str(exc)),
            'retryable': bool(getattr(exc, 'retryable', False)),
        }
        for field in ('execution_state', 'operation_id', 'worker_id', 'run_id', 'idempotency_key',
                      'output_truncated', 'continuation'):
            value = getattr(exc, field, None)
            if value is not None:
                error[field] = value
        if getattr(exc, 'field_errors', None):
            error['field_errors'] = exc.field_errors
        if error['code'] == 'SSH_OUTPUT_LIMIT':
            error['output_truncated'] = True
            error.setdefault('continuation', 'Resume reads with the last delivered cursor and bounded page options; '
                             'reconcile mutations using their original operation identity.')
        if isinstance(request, dict) and not request.get('dry_run'):
            action = request.get('action')
            payload = request.get('payload')
            payload = payload if isinstance(payload, dict) else {}
            mutation = action in {'run.submit', 'run.cancel', 'run.pause', 'checkpoint.replicate', 'checkpoint.import'}
            if action == 'workspace.execute':
                nested = payload.get('request')
                mutation = (nested.get('action') if isinstance(nested, dict) else None) not in {'inspect', 'diff', 'changes'}
            if mutation:
                error['retryable'] = False
                error.setdefault('execution_state', 'not_sent' if isinstance(exc, CCSRequestError) else 'unknown')
        _print(_envelope(request_id=request_id, error=error))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
