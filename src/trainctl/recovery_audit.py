"""Trusted-adapter recovery comparison, separate from file integrity and job success.

RecoveryInput must select the checkpoint first. The adapter derives component
fingerprints from its actual serialized and loaded state; this Module compares
them without loading model weights. Summaries are not authentication against
another process with the same account, or a scientific efficacy conclusion.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
import re

from controller.domain import utc_now
from .locking import file_lock


COMPONENTS = frozenset({'model', 'optimizer', 'scheduler', 'precision', 'rng', 'sampler', 'loop'})
_SHA = re.compile('[a-f0-9]{64}')


def _summary(value, contract):
    if not isinstance(value, dict) or value.get('contract') != contract:
        raise ValueError('RECOVERY_AUDIT_CONTRACT_MISMATCH')
    counters = {key: value.get(key) for key in ('global_step', 'optimizer_steps')}
    if any(type(number) is not int or not 0 <= number <= 2**63-1 for number in counters.values()):
        raise ValueError('RECOVERY_AUDIT_COUNTER_INVALID')
    if counters['optimizer_steps'] > counters['global_step']:
        raise ValueError('RECOVERY_AUDIT_COUNTER_UNITS_INVALID')
    fingerprints = value.get('recovery_fingerprints')
    if (not isinstance(fingerprints, dict) or set(fingerprints) != COMPONENTS or
            any(not isinstance(digest, str) or not _SHA.fullmatch(digest) for digest in fingerprints.values())):
        raise ValueError('RECOVERY_AUDIT_COMPONENT_SUMMARY_REQUIRED')
    return {'contract': contract, **counters, 'recovery_fingerprints': dict(fingerprints)}


class RecoveryAudit:
    """One new Run's loaded → advanced audit through two explicit confirmations."""
    def __init__(self, selection, run_dir, contract):
        if not isinstance(contract, str) or not _SHA.fullmatch(contract):
            raise ValueError('RECOVERY_AUDIT_CONTRACT_INVALID')
        self.directory = Path(run_dir).absolute()
        if self.directory.resolve() != self.directory or not self.directory.is_dir():
            raise ValueError('RECOVERY_AUDIT_RUN_DIRECTORY_UNSAFE')
        if not isinstance(selection, dict) or not isinstance(selection.get('selected'), dict):
            raise ValueError('RECOVERY_AUDIT_SELECTION_REQUIRED')
        selected = selection['selected']
        metadata = selected.get('metadata')
        if (not isinstance(metadata, dict) or
                not isinstance(selected.get('sha256'), str) or not _SHA.fullmatch(selected['sha256']) or
                not isinstance(selected.get('id'), str) or not 1 <= len(selected['id']) <= 128 or
                metadata.get('contract') != contract):
            raise ValueError('RECOVERY_AUDIT_SELECTION_INVALID')
        source = Path(selection.get('path', '')).absolute()
        if not Path(selection.get('path', '')).is_absolute() or source.resolve() != source or not source.is_file():
            raise ValueError('RECOVERY_AUDIT_SELECTION_PATH_UNSAFE')
        expected = selection.get('state')
        if not isinstance(expected, dict) or (expected.get('replay') or {}).get('safe_to_resume') is not True:
            raise ValueError('RECOVERY_AUDIT_SAFE_POINT_REQUIRED')
        self.expected = _summary(expected, contract)
        self.source = {'checkpoint_id': selected['id'], 'checkpoint_sha256': selected['sha256'],
                       'source_run_id': (selection.get('source') or {}).get('source_run_id') or
                                        (selected.get('metadata') or {}).get('source_run_id')}
        self.path = self.directory / 'restore-audit.json'

    def _existing(self):
        if self.path.is_symlink():
            raise ValueError('RECOVERY_AUDIT_PATH_UNSAFE')
        if not self.path.exists():
            return None
        if self.path.stat().st_size > 16 * 1024:
            raise ValueError('RECOVERY_AUDIT_OVERSIZED')
        record = json.loads(self.path.read_text(encoding='utf-8'))
        if (not isinstance(record, dict) or record.get('schema_version') != 1 or
                record.get('source') != self.source or record.get('expected') != self.expected or
                record.get('loaded') != self.expected or record.get('loaded_state_verified') is not True or
                record.get('state') not in ('loaded', 'advanced')):
            raise ValueError('RECOVERY_AUDIT_EXISTING_CONFLICT')
        return record

    def _write(self, record):
        from .run_store import RunStore
        RunStore._atomic_json(self.path, record)
        from .checkpoints import sync_directory
        sync_directory(self.directory)
        return copy.deepcopy(record)

    def _lock(self):
        path = self.directory / 'restore-audit.lock'
        if path.is_symlink():
            raise ValueError('RECOVERY_AUDIT_LOCK_UNSAFE')
        return file_lock(path)

    def confirm_loaded(self, actual_state):
        """Compare real adapter-observed state against the inspected checkpoint."""
        actual = _summary(actual_state, self.expected['contract'])
        if actual != self.expected:
            raise ValueError('RECOVERY_AUDIT_LOADED_STATE_MISMATCH')
        with self._lock():
            existing = self._existing()
            if existing is not None:
                if existing.get('state') != 'loaded':
                    raise ValueError('RECOVERY_AUDIT_CANNOT_DOWNGRADE')
                return copy.deepcopy(existing)
            return self._write({'schema_version': 1, 'state': 'loaded', 'loaded_at': utc_now(),
                'source': self.source, 'expected': self.expected, 'loaded': actual,
                'loaded_state_verified': True, 'advancement_verified': False,
                'scope': 'trusted adapter component summaries; not authentication or model efficacy'})

    def confirm_advanced(self, actual_state):
        """Require optimizer updates and changes to both model and optimizer state."""
        actual = _summary(actual_state, self.expected['contract'])
        before = self.expected
        if (actual['global_step'] <= before['global_step'] or
                actual['optimizer_steps'] <= before['optimizer_steps'] or
                any(actual['recovery_fingerprints'][key] == before['recovery_fingerprints'][key]
                    for key in ('model', 'optimizer'))):
            raise ValueError('RECOVERY_AUDIT_NO_REAL_ADVANCEMENT')
        with self._lock():
            existing = self._existing()
            if existing is None or existing.get('loaded_state_verified') is not True:
                raise ValueError('RECOVERY_AUDIT_LOADED_CONFIRMATION_REQUIRED')
            if existing.get('state') == 'advanced':
                if existing.get('advanced') != actual:
                    raise ValueError('RECOVERY_AUDIT_ADVANCEMENT_ALREADY_RECORDED')
                return copy.deepcopy(existing)
            if existing.get('state') != 'loaded':
                raise ValueError('RECOVERY_AUDIT_INVALID_TRANSITION')
            return self._write({**existing, 'state': 'advanced', 'advanced_at': utc_now(),
                'advanced': actual, 'advancement_verified': True,
                'optimizer_updates_since_restore': actual['optimizer_steps'] - before['optimizer_steps']})
