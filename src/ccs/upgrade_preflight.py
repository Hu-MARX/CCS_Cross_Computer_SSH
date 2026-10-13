"""Offline upgrade preview from captured public CCS replies; never deploys."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys


_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}')
_SHA = re.compile(r'[a-f0-9]{64}')
_ROLES = ('runner_cli', 'observer_daemon', 'workspace')
_READS = {'fleet.status', 'worker.health', 'worker.monitor', 'worker.processes', 'workspace.execute'}
_MAX_BYTES = 4 * 1024 * 1024


def _digest(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False,
                     separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


def _text(value):
    return value if isinstance(value, str) and 0 < len(value) <= 256 and all(
        character >= ' ' and character != '\x7f' for character in value) else None


def _row(worker_id, role, target_version):
    return {'worker_id': worker_id, 'role': role, 'state': 'unknown', 'version': None,
            'matches_target': None, 'source': None, 'observed_at': None,
            'freshness': 'unknown', 'declared_capabilities': {}, 'python': None,
            'compatibility_state': None,
            'reason': 'no role-specific observation supplied', 'target_version': target_version}


def _object(value, field):
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(field + ' must be an object or null')
    return value


def _declaration(row, interface, source, observed_at=None, freshness='captured', compatibility_state=None):
    row.update(source=source, observed_at=_text(observed_at), freshness=freshness,
               compatibility_state=compatibility_state)
    software = interface.get('software', {}) if isinstance(interface, dict) else {}
    version = _text(software.get('version')) if isinstance(software, dict) else None
    if version is None:
        row.update(state='unknown', version=None, matches_target=None,
                   declared_capabilities={}, reason='software version not declared for this role')
        return
    capabilities = interface.get('capabilities', {})
    row.update(state='observed', version=version, matches_target=version == row['target_version'],
               declared_capabilities={key: value for key, value in capabilities.items()
                                      if _text(key) and type(value) is int and value > 0}
               if isinstance(capabilities, dict) else {},
               reason='captured declaration; not proof of deployment or action compatibility')


def upgrade_preview(evidence, *, worker_ids, target_version, artifact_sha256):
    """Return a deterministic, non-executable role matrix and side-by-side checklist.

    ``observations`` holds pairs of public request/reply JSON, in capture order.
    The latest observation of a role wins, including failed/unknown observations.
    No gateway, database, shell, network, installation or state write is invoked.
    """
    if (not isinstance(evidence, dict) or evidence.get('schema_version') != 1 or
            not isinstance(evidence.get('observations', []), list) or
            len(evidence.get('observations', [])) > 256):
        raise ValueError('expected schema_version=1 and at most 256 observations')
    if (not isinstance(worker_ids, (list, tuple)) or not 1 <= len(worker_ids) <= 64 or
            any(not isinstance(value, str) or not _ID.fullmatch(value) for value in worker_ids) or
            len(set(worker_ids)) != len(worker_ids)):
        raise ValueError('explicit, unique worker_ids are required')
    if not _text(target_version) or not isinstance(artifact_sha256, str) or not _SHA.fullmatch(artifact_sha256):
        raise ValueError('target_version and a lowercase SHA-256 artifact digest are required')
    evidence_digest = _digest(evidence)  # Reject non-finite/non-JSON input before producing a preview.
    controller = _row(None, 'controller', target_version)
    local = evidence.get('controller', {})
    if not isinstance(local, dict):
        raise ValueError('controller observation must be an object')
    _declaration(controller, {'software': {'version': local.get('version')}},
                 'controller.version', local.get('observed_at'))
    doctor = local.get('doctor', {})
    if isinstance(doctor, dict) and doctor.get('ok') is True:
        data = doctor.get('data', doctor)
        interpreter = data.get('interpreter') if isinstance(data, dict) else None
        controller['python'] = interpreter.get('python') if isinstance(interpreter, dict) else None
    rows = {(worker_id, role): _row(worker_id, role, target_version)
            for worker_id in worker_ids for role in _ROLES}
    workloads = {worker_id: {'state': 'unknown', 'count': None, 'source': None}
                 for worker_id in worker_ids}
    issues = []
    fleet_seen = set()
    for index, observation in enumerate(evidence.get('observations', [])):
        if not isinstance(observation, dict):
            raise ValueError('each observation requires public request and response objects')
        request, response = observation.get('request'), observation.get('response')
        if not isinstance(request, dict) or not isinstance(response, dict):
            raise ValueError('each observation requires public request and response objects')
        action, payload = request.get('action'), request.get('payload', {})
        if not isinstance(payload, dict) or action not in _READS:
            raise ValueError('only documented read actions are accepted as observations')
        if action == 'workspace.execute' and (not isinstance(payload.get('request'), dict) or
                payload['request'].get('action') not in {'inspect', 'diff', 'changes'}):
            raise ValueError('workspace observation must be inspect, diff or changes')
        worker_id = payload.get('worker_id')
        if action != 'fleet.status' and worker_id not in worker_ids:
            raise ValueError('each worker read requires an explicitly selected worker_id')
        source = f'observations[{index}]/{action}'
        data = response.get('data', response)
        if response.get('ok', True) is not True or not isinstance(data, dict):
            role = {'worker.health': 'runner_cli', 'worker.monitor': 'observer_daemon',
                    'workspace.execute': 'workspace'}.get(action)
            if role:
                rows[worker_id, role] = {**_row(worker_id, role, target_version),
                                        'source': source, 'reason': 'last role observation failed'}
            if action in {'fleet.status', 'worker.processes'}:
                for target in worker_ids if action == 'fleet.status' else [worker_id]:
                    workloads[target] = {'state': 'unknown', 'count': None, 'source': source}
                    if action == 'fleet.status':
                        rows[target, 'runner_cli'] = {**_row(target, 'runner_cli', target_version),
                                                     'source': source, 'reason': 'last fleet observation failed'}
            issues.append({'source': source, 'code': 'OBSERVATION_FAILED'})
            continue
        if (not _text(request.get('request_id')) or data.get('action') != action or
                data.get('request_id') != request.get('request_id') or
                data.get('classification') != 'read' or data.get('executed') is not True):
            raise ValueError('public reply action/request_id does not match its captured request')
        result = data.get('result')
        if not isinstance(result, dict):
            raise ValueError('successful public read must contain an object result')
        if action != 'fleet.status' and result.get('worker_id') != worker_id:
            raise ValueError('public reply worker_id does not match explicit selection')
        if action == 'fleet.status':
            workers = result.get('workers')
            if not isinstance(workers, list):
                raise ValueError('fleet.status result requires workers')
            for worker in workers:
                if not isinstance(worker, dict) or worker.get('worker_id') not in worker_ids:
                    continue
                selected = worker['worker_id']
                fleet_seen.add(selected)
                compatibility = _object(worker.get('compatibility'), 'fleet compatibility')
                _declaration(rows[selected, 'runner_cli'], {
                    'software': compatibility.get('software'),
                    'capabilities': compatibility.get('available_capabilities')},
                    source, worker.get('checked_at'), compatibility_state=compatibility.get('state'))
                node = _object(worker.get('node'), 'fleet node')
                rows[selected, 'runner_cli']['python'] = node.get('python')
                count = worker.get('workload_count')
                known = worker.get('workload_state') == 'available' and type(count) is int and count >= 0
                workloads[selected] = {'state': ('busy' if count else 'idle') if known else 'unknown',
                                       'count': count if known else None, 'source': source}
        elif action == 'worker.health':
            row = rows[worker_id, 'runner_cli']
            compatibility = _object(data.get('compatibility'), 'health compatibility')
            _declaration(row, result.get('ccs_interface'), source, result.get('checked_at'),
                         compatibility_state=compatibility.get('state'))
            row['python'] = result.get('python')
        elif action == 'worker.monitor':
            row = rows[worker_id, 'observer_daemon']
            health = _object(result.get('health'), 'monitor health')
            _declaration(row, health.get('ccs_interface'), source, result.get('observed_at'),
                         result.get('observation_state', 'unknown'))
            row['python'] = health.get('python')
            row['observer_pid'] = result.get('pid')
        elif action == 'workspace.execute':
            compatibility = _object(data.get('compatibility'), 'workspace compatibility')
            _declaration(rows[worker_id, 'workspace'], {
                'software': compatibility.get('software'),
                'capabilities': compatibility.get('available_capabilities')}, source,
                compatibility_state=compatibility.get('state'))
        elif action == 'worker.processes':
            items = result.get('items')
            known = (result.get('state') == 'available' and isinstance(items, list) and
                     result.get('truncated') is False)
            workloads[worker_id] = {'state': ('busy' if items else 'idle') if known else 'unknown',
                                    'count': len(items) if known else None, 'source': source}
    matrix = [controller, *(rows[worker_id, role] for worker_id in worker_ids for role in _ROLES)]
    missing = [{'worker_id': row['worker_id'], 'role': row['role'], 'code': 'ROLE_VERSION_UNKNOWN'}
               for row in matrix if row['state'] == 'unknown']
    missing.extend({'worker_id': worker_id, 'code': 'FLEET_OBSERVATION_REQUIRED'}
                   for worker_id in worker_ids if worker_id not in fleet_seen)
    preview = {
        'schema_version': 1, 'status': 'preview_only', 'executed': False,
        'execution_state': 'not_sent', 'authorization': 'not_granted',
        'artifact': {'version': target_version, 'sha256': artifact_sha256,
                     'verification': 'caller_declared_not_verified'},
        'evidence_sha256': evidence_digest, 'selected_worker_ids': list(worker_ids),
        'role_matrix': matrix, 'workloads': workloads, 'missing_observations': missing,
        'issues': issues,
        'environment': {'worker_venv_available': 'unknown', 'worker_conda_available': 'unknown',
                        'minimum_python': '3.11', 'core_third_party_dependencies': []},
        'deferred_checks': ['artifact bytes', 'actual role account/root/executable bindings',
                            'active Run captured runtime paths', 'environment creation prerequisites',
                            'state schema and rollback compatibility'],
        'steps': [
            {'phase': 'prepare', 'requirements': [
                'verify artifact bytes against declared digest',
                'prepare a new versioned installation under the actual role account',
                'retain old executable, source and environment for every active Run',
                'back up state using consistent database backup; never replace live state']},
            {'phase': 'validate', 'requirements': [
                'probe candidate executable and actual role identity with strict host keys',
                'negotiate declared capabilities for intended actions; version equality is not compatibility',
                'resolve unknown submissions and incomplete checkpoint transactions before incompatible downgrade']},
            {'phase': 'switch', 'requirements': [
                'requires a separately authorized mutation dry-run and exact plan_sha256',
                'switch only new-operation entrypoints and the observer; never restart training units',
                'refresh role-specific observations and verify active Run runtime is unchanged']},
            {'phase': 'rollback', 'requirements': [
                'stop new submissions and restore previous entrypoints, not previous database files',
                'preserve current operation/audit databases, checkpoints and transaction journals',
                'never automatically retry an unknown mutation or change its worker/idempotency identity']},
        ],
        'guards': {'always_preserve_active_runtime': True, 'overwrite_state': False,
                   'automatic_retry': False, 'automatic_worker_selection': False,
                   'preview_sha256_is_gateway_confirmation': False},
    }
    return {**preview, 'preview_sha256': _digest(preview)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', required=True, help='captured public request/reply JSON; - for stdin')
    parser.add_argument('--worker-id', action='append', required=True)
    parser.add_argument('--target-version', required=True)
    parser.add_argument('--artifact-sha256', required=True)
    args = parser.parse_args(argv)
    try:
        if args.evidence == '-':
            raw = sys.stdin.buffer.read(_MAX_BYTES + 1)
        else:
            path = Path(args.evidence)
            if path.stat().st_size > _MAX_BYTES:
                raise ValueError('evidence exceeds 4 MiB')
            raw = path.read_bytes()
        if len(raw) > _MAX_BYTES:
            raise ValueError('evidence exceeds 4 MiB')
        def reject_constant(value):
            raise ValueError('evidence requires finite JSON numbers')

        evidence = json.loads(raw.decode('utf-8-sig'), parse_constant=reject_constant)
        result = upgrade_preview(evidence, worker_ids=args.worker_id,
                                 target_version=args.target_version, artifact_sha256=args.artifact_sha256)
        output = {'ok': True, 'data': result, 'error': None}
    except (OSError, UnicodeDecodeError, ValueError, TypeError) as exc:
        output = {'ok': False, 'data': None, 'error': {'code': 'INVALID_UPGRADE_EVIDENCE',
                  'message': str(exc), 'retryable': False, 'execution_state': 'not_sent'}}
    print(json.dumps(output, ensure_ascii=False, allow_nan=False, separators=(',', ':')))
    return 0 if output['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
