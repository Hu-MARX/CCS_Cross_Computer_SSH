"""Opt-in, single-process pause at a caller-owned optimizer safe point.

The worker publishes an immutable request; the trusted trainer commits its full
state and acknowledges it before exiting with 85.  An ACK alone is not evidence
that any process has stopped.  The supervisor must separately prove cleanup.
This Module never sends signals, resumes training, or deserializes checkpoints.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .locking import file_lock


PAUSED_EXIT_CODE = 85
_DIGEST = re.compile(r'[a-f0-9]{64}')
_RUN = re.compile(r'run_[a-f0-9]{32}')
_REQUEST = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}')
_CHECKPOINT = re.compile(r'ckpt_[a-f0-9]{32}')
_FINGERPRINTS = {'model', 'optimizer', 'scheduler', 'precision', 'rng', 'sampler', 'loop'}


class TrainingPaused(SystemExit):
    """Exit only after a bound managed checkpoint and ACK have been committed."""

    def __init__(self):
        super().__init__(PAUSED_EXIT_CODE)


def request_digest(request):
    return hashlib.sha256(json.dumps(request, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=True, allow_nan=False).encode()).hexdigest()


def _clock(now=None):
    value = now if now is not None else datetime.now(timezone.utc)
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError('PAUSE_INVALID_TIME: an aware UTC timestamp is required')
    return value.astimezone(timezone.utc)


def _timestamp(value):
    return value.isoformat().replace('+00:00', 'Z')


def _safe_path(path):
    path = Path(path).absolute()
    if path.resolve() != path or path.is_symlink():
        raise ValueError('PAUSE_UNSAFE_PATH: symlinks are not permitted')
    return path


def _read(path, *, limit=64 * 1024):
    path = _safe_path(path)
    if not path.is_file() or path.stat().st_size > limit:
        raise ValueError('PAUSE_INVALID_RECORD: missing, unsafe, or oversized JSON record')
    value = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(value, dict):
        raise ValueError('PAUSE_INVALID_RECORD: JSON record must be an object')
    return value


def _write(path, value):
    # Lazy imports avoid a RunStore -> pause_control -> RunStore import cycle.
    from .checkpoints import sync_directory
    from .run_store import RunStore
    _safe_path(path)
    RunStore._atomic_json(path, value)
    sync_directory(path.parent)


def _context(run_dir, declaration, spec_digest, checkpoint_directory=None):
    run_dir = _safe_path(run_dir)
    if not run_dir.is_dir() or not _RUN.fullmatch(run_dir.name):
        raise ValueError('PAUSE_INVALID_RUN: a managed Run directory is required')
    if (not isinstance(declaration, dict) or set(declaration) != {'schema_version', 'protocol', 'contract'} or
            type(declaration.get('schema_version')) is not int or declaration['schema_version'] != 1 or
            declaration.get('protocol') != 'managed-checkpoint-v1' or
            not isinstance(declaration.get('contract'), str) or not _DIGEST.fullmatch(declaration['contract'])):
        raise ValueError('PAUSE_UNSUPPORTED: task has no supported pause declaration')
    if not isinstance(spec_digest, str) or not _DIGEST.fullmatch(spec_digest):
        raise ValueError('PAUSE_SPEC_MISMATCH: invalid experiment digest')
    spec = _read(run_dir / 'spec.json', limit=1024 * 1024)
    from controller.configuration import spec_digest as calculate_spec_digest
    if spec.get('task', {}).get('pause') != declaration or calculate_spec_digest(spec) != spec_digest:
        raise ValueError('PAUSE_SPEC_MISMATCH: declaration or experiment digest differs')
    if checkpoint_directory is not None:
        manifest = _read(run_dir / 'manifest.json', limit=1024 * 1024)
        base = Path(manifest.get('submission_cwd') or run_dir)
        workdir = Path(spec.get('runtime', {}).get('workdir') or base)
        if not workdir.is_absolute():
            workdir = base / workdir
        expected = _safe_path(workdir / 'orchestrator_runs' / run_dir.name / 'checkpoints')
        directory = _safe_path(checkpoint_directory)
        if directory != expected:
            raise ValueError('PAUSE_CHECKPOINT_PATH_MISMATCH: checkpoint is outside this managed Run')
        return run_dir, directory
    return run_dir, None


def _request(run_dir, declaration, spec_digest):
    request = _read(run_dir / 'pause-request.json')
    keys = {'schema_version', 'protocol', 'request_id', 'reason', 'requested_at', 'expires_at',
            'run_id', 'contract', 'spec_digest', 'timeout_seconds'}
    if (set(request) != keys or type(request.get('schema_version')) is not int or
            request['schema_version'] != 1 or request.get('protocol') != declaration['protocol'] or
            request.get('run_id') != run_dir.name or request.get('contract') != declaration['contract'] or
            request.get('spec_digest') != spec_digest):
        raise ValueError('PAUSE_REQUEST_MISMATCH: request identity or binding differs')
    _arguments(request['request_id'], request['reason'], request['timeout_seconds'])
    created, expires = _clock(request['requested_at']), _clock(request['expires_at'])
    if expires - created != timedelta(seconds=request['timeout_seconds']):
        raise ValueError('PAUSE_REQUEST_MISMATCH: request expiry differs from its bounded timeout')
    return request


def _arguments(request_id, reason, timeout_seconds):
    if not isinstance(request_id, str) or not _REQUEST.fullmatch(request_id):
        raise ValueError('PAUSE_INVALID_REQUEST_ID: request_id must contain 1-128 safe characters')
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 1024:
        raise ValueError('PAUSE_INVALID_REASON: reason must contain 1-1024 characters')
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600:
        raise ValueError('PAUSE_INVALID_TIMEOUT: timeout_seconds must be an integer in 1..3600')


def create_request(run_dir, declaration, *, request_id, reason, timeout_seconds, spec_digest, now=None):
    """Publish once; only the exact same identity and inputs are idempotent.

    Expiry never kills the trainer or permits replacing this Run's request.
    New attempts must be new Runs, not new request identities on the old Run.
    """
    run_dir, _ = _context(run_dir, declaration, spec_digest)
    _arguments(request_id, reason, timeout_seconds)
    lock = _safe_path(run_dir / 'pause.lock')
    with file_lock(lock):
        path = _safe_path(run_dir / 'pause-request.json')
        if path.exists():
            existing = _request(run_dir, declaration, spec_digest)
            if any(existing[key] != value for key, value in (
                    ('request_id', request_id), ('reason', reason), ('timeout_seconds', timeout_seconds))):
                raise ValueError('PAUSE_REQUEST_CONFLICT: this Run already has a different immutable request')
            return existing
        created = _clock(now)
        request = {'schema_version': 1, 'protocol': declaration['protocol'], 'request_id': request_id,
                   'reason': reason, 'requested_at': _timestamp(created),
                   'expires_at': _timestamp(created + timedelta(seconds=timeout_seconds)),
                   'run_id': run_dir.name, 'contract': declaration['contract'],
                   'spec_digest': spec_digest, 'timeout_seconds': timeout_seconds}
        _write(path, request)
        return request


def _signature(path):
    value = _safe_path(path).stat()
    return {'size': value.st_size, 'mtime_ns': value.st_mtime_ns, 'ctime_ns': value.st_ctime_ns,
            'device': value.st_dev, 'inode': value.st_ino}


def _checkpoint(directory, request, checkpoint, *, verify_bytes=True):
    from .checkpoints import sha256
    if not isinstance(checkpoint, dict) or not isinstance(checkpoint.get('id'), str) or not _CHECKPOINT.fullmatch(checkpoint['id']):
        raise ValueError('PAUSE_CHECKPOINT_INVALID: invalid managed checkpoint identity')
    record_path = _safe_path(directory / (checkpoint['id'] + '.json'))
    record = _read(record_path)
    path = _safe_path(directory / (checkpoint['id'] + '.ckpt'))
    state, metadata = record.get('state'), record.get('metadata')
    if (record.get('id') != checkpoint['id'] or record.get('file') != path.name or
            type(record.get('size')) is not int or record['size'] <= 0 or
            not isinstance(record.get('sha256'), str) or not _DIGEST.fullmatch(record['sha256']) or
            not path.is_file() or path.stat().st_size != record['size']):
        raise ValueError('PAUSE_CHECKPOINT_INVALID: managed checkpoint size or digest differs')
    if verify_bytes:
        before = _signature(path)
        if sha256(path) != record['sha256'] or before != _signature(path):
            raise ValueError('PAUSE_CHECKPOINT_INVALID: checkpoint digest differs or changed during verification')
    if (not isinstance(metadata, dict) or metadata.get('contract') != request['contract'] or
            metadata.get('run_id') != request['run_id'] or metadata.get('pinned') is not True or
            metadata.get('pause_request_sha256') != request_digest(request)):
        raise ValueError('PAUSE_CHECKPOINT_MISMATCH: checkpoint metadata is not pinned to this request')
    fingerprints = state.get('recovery_fingerprints') if isinstance(state, dict) else None
    replay = state.get('replay') if isinstance(state, dict) else None
    if (not isinstance(state, dict) or state.get('contract') != request['contract'] or
            type(state.get('global_step')) is not int or not 0 <= state['global_step'] <= 2**63 - 1 or
            type(state.get('optimizer_steps')) is not int or not 0 <= state['optimizer_steps'] <= state['global_step'] or
            not isinstance(fingerprints, dict) or set(fingerprints) != _FINGERPRINTS or
            any(not isinstance(fingerprints[key], str) or not _DIGEST.fullmatch(fingerprints[key]) for key in _FINGERPRINTS) or
            not isinstance(replay, dict) or replay.get('safe_to_resume') is not True):
        raise ValueError('PAUSE_CHECKPOINT_INCOMPLETE: full-state fingerprints and safe replay are required')
    result = {'id': record['id'], 'path': str(path), 'sha256': record['sha256'],
              'size': record['size'], 'global_step': state['global_step'],
              'optimizer_steps': state['optimizer_steps'], 'record_sha256': sha256(record_path)}
    return result, record


def _inspect_ack(run_dir, declaration, *, checkpoint_directory, spec_digest, verify_bytes):
    run_dir, directory = _context(run_dir, declaration, spec_digest, checkpoint_directory)
    request = _request(run_dir, declaration, spec_digest)
    ack = _read(run_dir / 'pause-ack.json')
    if (set(ack) != {'schema_version', 'protocol', 'request_id', 'request_sha256', 'run_id',
                     'contract', 'spec_digest', 'created_at', 'checkpoint'} or
            type(ack.get('schema_version')) is not int or ack['schema_version'] != 1 or
            ack.get('protocol') != declaration['protocol'] or ack.get('request_id') != request['request_id'] or
            ack.get('request_sha256') != request_digest(request) or ack.get('run_id') != run_dir.name or
            ack.get('contract') != declaration['contract'] or ack.get('spec_digest') != spec_digest):
        raise ValueError('PAUSE_ACK_MISMATCH: ACK does not bind this request, Run, and experiment')
    if not _clock(request['requested_at']) <= _clock(ack['created_at']) < _clock(request['expires_at']):
        raise ValueError('PAUSE_ACK_EXPIRED: ACK was not committed within the request lifetime')
    checkpoint, _ = _checkpoint(directory, request, ack.get('checkpoint'), verify_bytes=verify_bytes)
    if ack['checkpoint'] != checkpoint:
        raise ValueError('PAUSE_ACK_MISMATCH: ACK checkpoint differs from its committed manifest')
    return ack


def validate_ack(run_dir, declaration, *, checkpoint_directory, spec_digest):
    """Explicit full-byte verification, never deserialization or model semantics.

    Ordinary status polling does not call this potentially expensive operation.
    """
    return _inspect_ack(run_dir, declaration, checkpoint_directory=checkpoint_directory,
                        spec_digest=spec_digest, verify_bytes=True)


def _verification(run_dir, ack):
    from .checkpoints import sha256
    checkpoint = ack['checkpoint']
    path = Path(checkpoint['path'])
    return {'schema_version': 1, 'run_id': ack['run_id'], 'spec_digest': ack['spec_digest'],
            'request_sha256': ack['request_sha256'], 'ack_sha256': sha256(run_dir / 'pause-ack.json'),
            'checkpoint_sha256': checkpoint['sha256'], 'record_sha256': checkpoint['record_sha256'],
            'checkpoint_signature': _signature(path), 'record_signature': _signature(path.with_suffix('.json'))}


def status(run_dir, declaration, *, checkpoint_directory, spec_digest, quiescent=False, now=None):
    """Read-only projection: pause completion requires independent cleanup proof."""
    result = {'schema_version': 1, 'state': 'not_requested', 'quiescent': quiescent is True, 'safe_to_resume': False,
              'request_id': None, 'request': None, 'checkpoint': None, 'verification': None}
    if declaration is None:
        return {**result, 'state': 'unsupported'}
    try:
        run_dir, _ = _context(run_dir, declaration, spec_digest, checkpoint_directory)
        if not _safe_path(run_dir / 'pause-request.json').exists():
            return result
        request = _request(run_dir, declaration, spec_digest)
        result['request'] = request
        result['request_id'] = request['request_id']
        if _safe_path(run_dir / 'pause-ack.json').exists():
            ack = _inspect_ack(run_dir, declaration, checkpoint_directory=checkpoint_directory,
                               spec_digest=spec_digest, verify_bytes=False)
            observed = _verification(run_dir, ack)
            proof_path = _safe_path(run_dir / 'pause-verified.json')
            proof = _read(proof_path) if proof_path.exists() else None
            if (not isinstance(proof, dict) or set(proof) != set(observed) | {'verified_at'} or
                    any(proof.get(key) != value for key, value in observed.items())):
                return {**result, 'state': 'checkpoint_unverified', 'checkpoint': ack['checkpoint'], 'ack': ack}
            _clock(proof['verified_at'])
            return {**result, 'state': 'paused' if quiescent is True else 'checkpoint_committed',
                    'safe_to_resume': quiescent is True, 'checkpoint': ack['checkpoint'], 'ack': ack,
                    'verification': {'bytes_verified_at': proof['verified_at'],
                                     'scope': 'cached_byte_verification_and_current_file_signature',
                                     'current_bytes_rehashed': False}}
        return {**result, 'state': 'expired' if _clock(now) >= _clock(request['expires_at']) else 'requested'}
    except (OSError, ValueError, TypeError, KeyError) as exc:
        return {**result, 'state': 'invalid', 'error': str(exc)}


def checkpoint_and_pause(run_dir, declaration, *, checkpoint_directory, spec_digest,
                         global_step, save_checkpoint, now=None):
    """Called after a completed optimizer update, never mid-accumulation.

    ``save_checkpoint(request)`` must synchronously return CheckpointStore.commit
    with full-state inspection and metadata contract/run_id/pinned=True plus
    pause_request_sha256=request_digest(request).  Only the trusted training
    Adapter can establish full-state semantics.  Multi-process training is not
    supported by this protocol; it requires a separate all-rank safe-point gate.
    """
    run_dir, directory = _context(run_dir, declaration, spec_digest, checkpoint_directory)
    if not _safe_path(run_dir / 'pause-request.json').exists():
        return False
    if os.environ.get('WORLD_SIZE', '1') != '1':
        raise ValueError('PAUSE_UNSUPPORTED_WORLD_SIZE: managed-checkpoint-v1 supports one process')
    if type(global_step) is not int or global_step < 0:
        raise ValueError('PAUSE_INVALID_STEP: an optimizer safe-point step is required')
    request = _request(run_dir, declaration, spec_digest)
    if _clock(now) < _clock(request['requested_at']):
        raise ValueError('PAUSE_REQUEST_MISMATCH: request timestamp is in the future')
    if _clock(now) >= _clock(request['expires_at']):
        return False
    lock = _safe_path(run_dir / 'pause.lock')
    with file_lock(lock):
        if _safe_path(run_dir / 'pause-ack.json').exists():
            validate_ack(run_dir, declaration, checkpoint_directory=directory, spec_digest=spec_digest)
            raise TrainingPaused()
        saved = save_checkpoint(dict(request))
        checkpoint, record = _checkpoint(directory, request, saved)
        if any(saved.get(key) != record.get(key) for key in ('id', 'file', 'sha256', 'size', 'state', 'metadata')):
            raise ValueError('PAUSE_CHECKPOINT_MISMATCH: save callback differs from committed record')
        if checkpoint['global_step'] != global_step:
            raise ValueError('PAUSE_STEP_MISMATCH: checkpoint is not from this optimizer safe point')
        _write(run_dir / 'checkpoint-status.json', {'state': 'committed',
               'updated_at': _timestamp(_clock(now)), 'latest': record,
               'reason': 'cooperative_pause', 'pause_request_id': request['request_id']})
        completed = _clock(now)
        if completed >= _clock(request['expires_at']):
            return False  # Preserve the committed checkpoint but keep training.
        ack = {'schema_version': 1, 'protocol': declaration['protocol'], 'request_id': request['request_id'],
               'request_sha256': request_digest(request), 'run_id': run_dir.name,
               'contract': declaration['contract'], 'spec_digest': spec_digest,
               'created_at': _timestamp(completed), 'checkpoint': checkpoint}
        _write(run_dir / 'pause-ack.json', ack)
        verification = _verification(run_dir, ack)
        validate_ack(run_dir, declaration, checkpoint_directory=directory, spec_digest=spec_digest)
        if verification != _verification(run_dir, ack):
            raise ValueError('PAUSE_CHECKPOINT_CHANGED: files changed during ACK verification')
        _write(run_dir / 'pause-verified.json', {**verification,
                                              'verified_at': _timestamp(_clock(now))})
        raise TrainingPaused()
