"""Verified, restart-safe local replicas of worker-managed checkpoints."""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path, PurePosixPath
from uuid import uuid4

from trainctl.checkpoints import sha256, sync_directory
from trainctl.locking import file_lock
from trainctl.run_store import RunStore


_SAFE_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$')
_CHECKPOINT_ID = re.compile(r'^ckpt_[a-f0-9]{32}$')
_REMOTE_PATH = re.compile(r'^/[A-Za-z0-9._/-]{1,4095}$')


class CheckpointReplicaError(RuntimeError):
    def __init__(self, code, message, *, retryable=False):
        super().__init__(message)
        self.code, self.message, self.retryable = code, message, retryable


class CheckpointReplicaStore:
    """Deep Module for resolving, transferring and publishing one replica."""

    def __init__(self, root):
        self.root = Path(root).absolute()
        if self.root.exists() and self.root.is_symlink():
            raise ValueError('replica root must not be a symlink')
        self.root.mkdir(parents=True, exist_ok=True)
        if self.root.resolve() != self.root:
            raise ValueError('replica root must resolve to itself')

    def replicate(self, remote, *, worker_id, run_id, checkpoint_id):
        """Create or verify an immutable local checkpoint replica."""
        self._identifiers(worker_id, run_id, checkpoint_id)
        remote.require_capabilities({'checkpoint.export': 1})
        checkpoint = (remote.snapshot(run_id).observation or {}).get('checkpoint')
        latest = checkpoint.get('latest') if isinstance(checkpoint, dict) else None
        if (not isinstance(latest, dict) or checkpoint.get('state') != 'committed' or
                latest.get('id') != checkpoint_id):
            raise CheckpointReplicaError(
                'CHECKPOINT_NOT_COMMITTED_LATEST',
                'checkpoint is not the worker-confirmed latest committed checkpoint',
            )
        filename = checkpoint_id + '.ckpt'
        candidates = remote.artifacts(run_id)
        paths = [item.get('path') for item in candidates if isinstance(item, dict)]
        matches = [path for path in paths if self._remote_path(path, filename)]
        if len(matches) != 1:
            raise CheckpointReplicaError(
                'CHECKPOINT_NOT_FOUND',
                'worker did not expose exactly one matching managed checkpoint artifact',
            )
        exact = remote.artifacts(run_id, path=matches[0])
        if len(exact) != 1:
            raise CheckpointReplicaError(
                'CHECKPOINT_NOT_VERIFIED', 'worker did not verify the selected checkpoint',
            )
        artifact = self._artifact(exact[0], matches[0])
        if latest.get('size') != artifact['size'] or latest.get('sha256') != artifact['sha256']:
            raise CheckpointReplicaError(
                'CHECKPOINT_STATUS_MISMATCH',
                'checkpoint status and verified artifact identity do not match',
            )
        directory = self.root / worker_id / run_id / checkpoint_id
        directory.mkdir(parents=True, exist_ok=True)
        if directory.is_symlink() or not directory.resolve().is_relative_to(self.root):
            raise CheckpointReplicaError('UNSAFE_REPLICA_PATH', 'replica path escaped its store')
        target = directory / 'checkpoint.ckpt'
        manifest_path = directory / 'manifest.json'
        journal_path = directory / 'transaction.json'
        expected = self._manifest(worker_id, run_id, checkpoint_id, artifact, remote)
        expected['receipt_sha256'] = self._receipt(expected)

        with file_lock(directory / 'replica.lock'):
            if manifest_path.exists() or manifest_path.is_symlink():
                return self._existing(target, manifest_path, expected)
            if journal_path.exists() or journal_path.is_symlink():
                journal = self._journal(journal_path, expected)
            else:
                if target.exists() or target.is_symlink():
                    raise CheckpointReplicaError(
                        'INCOMPLETE_REPLICA',
                        'checkpoint has no source-bound transaction receipt; move this replica directory '
                        'outside the store for inspection, then retry replication',
                    )
                journal = {
                    'schema_version': 1, 'state': 'prepared',
                    'temporary': '.checkpoint.' + uuid4().hex + '.partial',
                    'manifest': expected,
                }
                self._write_journal(journal_path, journal, 'prepared')
            temporary = directory / journal['temporary']
            if target.exists() or target.is_symlink():
                # The journal was durable before publication. Never replace a
                # published file: verify it and publish only its missing receipt.
                self._verify_file(target, artifact)
            else:
                if not temporary.exists() and not temporary.is_symlink():
                    remote.download_artifact(run_id, artifact, temporary)
                self._verify_file(temporary, artifact)
                with temporary.open('r+b') as stream:
                    os.fsync(stream.fileno())
                self._write_journal(journal_path, journal, 'verified')
                os.replace(temporary, target)
                sync_directory(directory)
            self._write_journal(journal_path, journal, 'published')
            RunStore._atomic_json(manifest_path, journal['manifest'])
            sync_directory(directory)
            self._write_journal(journal_path, journal, 'committed')
            return self._result('replicated', target, manifest_path, journal['manifest'])

    def verified(self, *, worker_id, run_id, checkpoint_id):
        """Return one locally reverified replica without contacting a worker."""
        self._identifiers(worker_id, run_id, checkpoint_id)
        directory = self.root / worker_id / run_id / checkpoint_id
        if (not directory.is_dir() or directory.is_symlink() or
                not directory.resolve().is_relative_to(self.root)):
            raise CheckpointReplicaError('REPLICA_NOT_FOUND', 'verified checkpoint replica does not exist')
        target = directory / 'checkpoint.ckpt'
        manifest_path = directory / 'manifest.json'
        if not manifest_path.is_file() or manifest_path.is_symlink():
            raise CheckpointReplicaError('INCOMPLETE_REPLICA', 'existing replica is incomplete or unsafe')
        manifest = RunStore._read_json(manifest_path, {})
        if (manifest.get('schema_version') != 1 or manifest.get('worker_id') != worker_id or
                manifest.get('run_id') != run_id or manifest.get('checkpoint_id') != checkpoint_id):
            raise CheckpointReplicaError('REPLICA_IDENTITY_MISMATCH', 'replica has different provenance')
        manifest_artifact = manifest.get('artifact')
        if not isinstance(manifest_artifact, dict):
            raise CheckpointReplicaError('INVALID_ARTIFACT', 'replica artifact identity is invalid')
        artifact = self._artifact(manifest_artifact, manifest_artifact.get('path'))
        if not self._remote_path(artifact['path'], checkpoint_id + '.ckpt'):
            raise CheckpointReplicaError('INVALID_ARTIFACT', 'replica artifact path is invalid')
        receipt = manifest.get('receipt_sha256')
        body = {key: value for key, value in manifest.items() if key != 'receipt_sha256'}
        if receipt != self._receipt(body):
            raise CheckpointReplicaError('REPLICA_MANIFEST_MISMATCH', 'replica receipt is invalid')
        self._verify_file(target, artifact)
        return {
            'worker_id': worker_id,
            'run_id': run_id,
            'checkpoint_id': checkpoint_id,
            'artifact': artifact,
            'size': artifact['size'],
            'sha256': artifact['sha256'],
            'checkpoint_path': str(target),
            'manifest_path': str(manifest_path),
            'receipt_sha256': receipt,
        }

    def import_to(self, remote, *, source_worker_id, source_run_id, checkpoint_id,
                  target_worker_id):
        """Import one local replica to the exact upper-Agent-selected worker."""
        if not isinstance(target_worker_id, str) or not _SAFE_ID.fullmatch(target_worker_id):
            raise ValueError('target_worker_id is invalid')
        replica = self.verified(
            worker_id=source_worker_id, run_id=source_run_id,
            checkpoint_id=checkpoint_id,
        )
        remote.require_capabilities({'checkpoint.import': 1})
        result = remote.import_checkpoint(replica)
        if (not isinstance(result, dict) or result.get('status') not in {'imported', 'present'} or
                not isinstance(result.get('import_id'), str) or
                not re.fullmatch(r'import_[a-f0-9]{32}', result['import_id']) or
                result.get('size') != replica['size'] or result.get('sha256') != replica['sha256'] or
                not isinstance(result.get('checkpoint_path'), str) or
                not PurePosixPath(result['checkpoint_path']).is_absolute() or
                not isinstance(result.get('import_receipt_sha256'), str) or
                not re.fullmatch(r'[a-f0-9]{64}', result['import_receipt_sha256'])):
            raise CheckpointReplicaError(
                'INVALID_IMPORT_RECEIPT', 'target worker returned an invalid checkpoint import receipt',
            )
        return {
            **result,
            'source_worker_id': source_worker_id,
            'source_run_id': source_run_id,
            'checkpoint_id': checkpoint_id,
            'target_worker_id': target_worker_id,
            'replica_receipt_sha256': replica['receipt_sha256'],
        }

    @staticmethod
    def _identifiers(worker_id, run_id, checkpoint_id):
        if not isinstance(worker_id, str) or not _SAFE_ID.fullmatch(worker_id):
            raise ValueError('worker_id is invalid')
        if not isinstance(run_id, str) or not _SAFE_ID.fullmatch(run_id):
            raise ValueError('run_id is invalid')
        if not isinstance(checkpoint_id, str) or not _CHECKPOINT_ID.fullmatch(checkpoint_id):
            raise ValueError('checkpoint_id is invalid')

    @staticmethod
    def _remote_path(value, filename):
        if not isinstance(value, str) or not _REMOTE_PATH.fullmatch(value):
            return False
        path = PurePosixPath(value)
        return '..' not in path.parts and path.name == filename

    @staticmethod
    def _artifact(value, expected_path):
        if not isinstance(value, dict) or value.get('path') != expected_path:
            raise CheckpointReplicaError('INVALID_ARTIFACT', 'artifact identity changed during verification')
        size, digest = value.get('size'), value.get('sha256')
        if type(size) is not int or size <= 0 or not isinstance(digest, str) or not re.fullmatch(r'[a-f0-9]{64}', digest):
            raise CheckpointReplicaError('INVALID_ARTIFACT', 'artifact lacks a valid size or SHA256')
        return {'path': expected_path, 'size': size, 'sha256': digest}

    @staticmethod
    def _verify_file(path, artifact):
        if path.is_symlink() or not path.is_file() or path.stat().st_size != artifact['size']:
            raise CheckpointReplicaError(
                'REPLICA_SIZE_MISMATCH',
                'downloaded checkpoint size differs: ' + str(path) +
                '; preserve the file for inspection; no published checkpoint was replaced',
            )
        if sha256(path) != artifact['sha256']:
            raise CheckpointReplicaError(
                'REPLICA_SHA256_MISMATCH',
                'downloaded checkpoint SHA256 differs: ' + str(path) +
                '; preserve the file for inspection; no published checkpoint was replaced',
            )

    def _existing(self, target, manifest_path, expected):
        if not target.is_file() or target.is_symlink() or not manifest_path.is_file() or manifest_path.is_symlink():
            raise CheckpointReplicaError('INCOMPLETE_REPLICA', 'existing replica is incomplete or unsafe')
        manifest = RunStore._read_json(manifest_path, {})
        for key in ('schema_version', 'worker_id', 'run_id', 'checkpoint_id', 'artifact', 'source'):
            if manifest.get(key) != expected[key]:
                raise CheckpointReplicaError('REPLICA_IDENTITY_MISMATCH', 'existing replica has different provenance')
        receipt = manifest.get('receipt_sha256')
        if receipt != self._receipt(manifest):
            raise CheckpointReplicaError('REPLICA_MANIFEST_MISMATCH', 'replica receipt is invalid')
        self._verify_file(target, expected['artifact'])
        return self._result('present', target, manifest_path, manifest)

    @staticmethod
    def _receipt(body):
        body = {key: value for key, value in body.items() if key != 'receipt_sha256'}
        return hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
        ).hexdigest()

    @staticmethod
    def _journal_receipt(journal):
        body = {key: value for key, value in journal.items() if key != 'journal_sha256'}
        return hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
        ).hexdigest()

    @classmethod
    def _write_journal(cls, path, journal, state):
        journal['state'] = state
        journal['journal_sha256'] = cls._journal_receipt(journal)
        RunStore._atomic_json(path, journal)
        sync_directory(path.parent)

    @classmethod
    def _journal(cls, path, expected):
        try:
            if not path.is_file() or path.is_symlink():
                raise ValueError('unsafe journal')
            journal = RunStore._read_json(path, {})
            if (not isinstance(journal, dict) or set(journal) != {
                    'schema_version', 'state', 'temporary', 'manifest', 'journal_sha256'} or
                    journal.get('schema_version') != 1 or
                    journal.get('state') not in {'prepared', 'verified', 'published', 'committed'} or
                    not isinstance(journal.get('temporary'), str) or
                    not re.fullmatch(r'\.checkpoint\.[a-f0-9]{32}\.partial', journal['temporary']) or
                    journal.get('journal_sha256') != cls._journal_receipt(journal)):
                raise ValueError('invalid journal receipt')
        except (OSError, ValueError, TypeError) as exc:
            raise CheckpointReplicaError(
                'REPLICA_TRANSACTION_MISMATCH',
                'transaction receipt is missing, unsafe, or invalid; preserve this replica directory '
                'outside the store for inspection before retrying',
            ) from exc
        if journal.get('manifest') != expected:
            raise CheckpointReplicaError(
                'REPLICA_IDENTITY_MISMATCH',
                'transaction source or artifact differs from this request; preserve the directory '
                'and verify the selected worker identity before retrying',
            )
        return journal

    @staticmethod
    def _manifest(worker_id, run_id, checkpoint_id, artifact, remote):
        source = {
            'host': getattr(remote, 'host', None) if remote is not None else None,
            'user': getattr(remote, 'user', None) if remote is not None else None,
            'port': (getattr(remote, 'port', None) or 22) if remote is not None else None,
            'remote_root': getattr(remote, 'remote_root', None) if remote is not None else None,
        }
        return {
            'schema_version': 1,
            'worker_id': worker_id,
            'run_id': run_id,
            'checkpoint_id': checkpoint_id,
            'artifact': dict(artifact),
            'source': source,
        }

    @staticmethod
    def _result(status, target, manifest_path, manifest):
        return {
            'schema_version': 1,
            'status': status,
            'worker_id': manifest['worker_id'],
            'run_id': manifest['run_id'],
            'checkpoint_id': manifest['checkpoint_id'],
            'size': manifest['artifact']['size'],
            'sha256': manifest['artifact']['sha256'],
            'checkpoint_path': str(target),
            'manifest_path': str(manifest_path),
            'receipt_sha256': manifest['receipt_sha256'],
        }
