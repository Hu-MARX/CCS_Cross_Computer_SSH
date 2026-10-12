"""Verified, immutable checkpoint imports owned by one CCS worker."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
from pathlib import Path, PurePosixPath

from controller.domain import utc_now
from .checkpoints import sha256, sync_directory
from .locking import file_lock
from .run_store import RunStore, TrainCtlError


_SAFE_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$')
_CHECKPOINT_ID = re.compile(r'^ckpt_[a-f0-9]{32}$')
_IMPORT_ID = re.compile(r'^import_[a-f0-9]{32}$')
_DIGEST = re.compile(r'^[a-f0-9]{64}$')
_REMOTE_PATH = re.compile(r'^/[A-Za-z0-9._/-]{1,4095}$')


class CheckpointImportStore:
    """Deep Module for staging, verifying and publishing worker-side imports."""

    def __init__(self, root, *, min_free_bytes=0):
        self.root = Path(root).absolute()
        if type(min_free_bytes) is not int or min_free_bytes < 0:
            raise ValueError('min_free_bytes must be a nonnegative integer')
        if self.root.exists() and self.root.is_symlink():
            raise ValueError('checkpoint import root must not be a symlink')
        self.root.mkdir(parents=True, exist_ok=True)
        if self.root.resolve() != self.root:
            raise ValueError('checkpoint import root must resolve to itself')
        self.incoming = self.root / '.incoming'
        self.incoming.mkdir(exist_ok=True)
        if self.incoming.is_symlink() or not self.incoming.resolve().is_relative_to(self.root):
            raise ValueError('checkpoint import staging path is unsafe')
        self.min_free_bytes = min_free_bytes

    def prepare(self, request):
        expected = self._request(request)
        import_id = self._import_id(expected)
        final = self.root / import_id
        with file_lock(self.root / (import_id + '.lock')):
            if final.exists() or final.is_symlink():
                return self._recover_final(final, expected)
            if shutil.disk_usage(self.root).free < self.min_free_bytes + expected['source']['artifact']['size']:
                raise TrainCtlError(
                    'CHECKPOINT_IMPORT_DISK_LOW',
                    'worker does not have enough free space for this checkpoint import',
                )
            staging = self.incoming / import_id
            staging.mkdir(exist_ok=True)
            self._safe_directory(staging)
            expected_path = staging / 'expected.json'
            if expected_path.is_symlink():
                raise TrainCtlError('UNSAFE_CHECKPOINT_IMPORT', 'import source receipt is unsafe')
            if expected_path.exists() and RunStore._read_json(expected_path, {}) != expected:
                raise TrainCtlError('CHECKPOINT_IMPORT_CONFLICT', 'import id has different provenance')
            if not expected_path.exists():
                RunStore._atomic_json(expected_path, expected)
                sync_directory(staging)
            self._journal(staging, expected, create=True)
            partial = staging / 'checkpoint.partial'
            committed = staging / 'checkpoint.ckpt'
            if partial.is_symlink() or committed.is_symlink():
                raise TrainCtlError('UNSAFE_CHECKPOINT_IMPORT', 'import checkpoint path is unsafe')
            upload_required = not (
                self._matches(committed, expected['source']['artifact']) or
                self._matches(partial, expected['source']['artifact'])
            )
            return {
                'schema_version': 1,
                'status': 'ready',
                'import_id': import_id,
                'upload_path': str(partial),
                'upload_required': upload_required,
                'size': expected['source']['artifact']['size'],
                'sha256': expected['source']['artifact']['sha256'],
            }

    def commit(self, import_id):
        self._validate_import_id(import_id)
        final = self.root / import_id
        with file_lock(self.root / (import_id + '.lock')):
            if final.exists() or final.is_symlink():
                return self._recover_final(final)
            staging = self.incoming / import_id
            self._safe_directory(staging)
            expected_path = staging / 'expected.json'
            if not expected_path.is_file() or expected_path.is_symlink():
                raise TrainCtlError('UNSAFE_CHECKPOINT_IMPORT', 'import source receipt is missing or unsafe')
            expected = RunStore._read_json(expected_path, {})
            expected = self._request(expected)
            if self._import_id(expected) != import_id:
                raise TrainCtlError('CHECKPOINT_IMPORT_CONFLICT', 'import id does not match provenance')
            journal = self._journal(staging, expected, create=True)
            artifact = expected['source']['artifact']
            checkpoint = staging / 'checkpoint.ckpt'
            partial = staging / 'checkpoint.partial'
            candidate = checkpoint if checkpoint.exists() or checkpoint.is_symlink() else partial
            self._verify(candidate, artifact)
            self._write_journal(staging / 'transaction.json', journal, 'verified')
            if candidate == partial:
                with partial.open('r+b') as stream:
                    os.fsync(stream.fileno())
                os.replace(partial, checkpoint)
                sync_directory(staging)
            self._write_journal(staging / 'transaction.json', journal, 'published')
            body = journal['manifest']
            RunStore._atomic_json(staging / 'manifest.json', body)
            sync_directory(staging)
            self._write_journal(staging / 'transaction.json', journal, 'committed')
            if os.name == 'posix':
                os.chmod(checkpoint, 0o440)
                os.chmod(staging / 'expected.json', 0o440)
                os.chmod(staging / 'manifest.json', 0o440)
                os.chmod(staging / 'transaction.json', 0o440)
            sync_directory(staging)
            os.replace(staging, final)
            sync_directory(self.incoming)
            if os.name == 'posix':
                os.chmod(final, 0o550)
            sync_directory(self.root)
            return self._result('imported', final, body)

    def _recover_final(self, final, expected=None):
        """Mutation-only repair; show() deliberately never changes a receipt."""
        self._safe_directory(final)
        manifest_path = final / 'manifest.json'
        if manifest_path.exists() or manifest_path.is_symlink():
            manifest = RunStore._read_json(manifest_path, {})
            if expected is None:
                expected = {'schema_version': 1, 'source': manifest.get('source')}
            return self._existing(final, expected, status='present')
        expected_path = final / 'expected.json'
        if not expected_path.is_file() or expected_path.is_symlink():
            raise TrainCtlError(
                'INCOMPLETE_CHECKPOINT_IMPORT',
                'import has no source receipt; preserve this import directory outside the store '
                'for inspection before preparing it again',
            )
        recorded = self._request(RunStore._read_json(expected_path, {}))
        if self._import_id(recorded) != final.name or (expected is not None and recorded != expected):
            raise TrainCtlError('CHECKPOINT_IMPORT_CONFLICT', 'import source differs; no checkpoint was replaced')
        journal = self._journal(final, recorded, create=False)
        self._verify(final / 'checkpoint.ckpt', recorded['source']['artifact'])
        original_mode = stat.S_IMODE(final.stat().st_mode) if os.name == 'posix' else None
        changed_mode = original_mode is not None and not original_mode & stat.S_IWUSR
        try:
            # A published import is 0550. Its owner may temporarily enable only
            # owner-write for this verified receipt repair, restoring it even
            # when publication fails. The checkpoint stays read-only throughout.
            if changed_mode:
                os.chmod(final, original_mode | stat.S_IWUSR)
            RunStore._atomic_json(manifest_path, journal['manifest'])
            sync_directory(final)
            if os.name == 'posix':
                os.chmod(manifest_path, 0o440)
        except OSError as exc:
            raise TrainCtlError(
                'CHECKPOINT_IMPORT_RECOVERY_IO_FAILED',
                'verified import receipt could not be repaired; check owner permissions and free space '
                'for ' + str(final) + '; the checkpoint was not replaced',
            ) from exc
        finally:
            if changed_mode:
                try:
                    os.chmod(final, original_mode)
                except OSError as exc:
                    raise TrainCtlError(
                        'CHECKPOINT_IMPORT_RECOVERY_PERMISSION_RESTORE_FAILED',
                        'restore the original directory permissions for ' + str(final) +
                        ' before reusing this import; checkpoint contents were not replaced',
                    ) from exc
        return self._result('present', final, journal['manifest'])

    def show(self, import_id):
        self._validate_import_id(import_id)
        final = self.root / import_id
        manifest = RunStore._read_json(final / 'manifest.json', {})
        expected = {'schema_version': 1, 'source': manifest.get('source')}
        return self._existing(final, expected, status='present')

    @classmethod
    def _request(cls, value):
        if not isinstance(value, dict) or set(value) != {'schema_version', 'source'} or value.get('schema_version') != 1:
            raise TrainCtlError('INVALID_CHECKPOINT_IMPORT', 'import request shape is invalid')
        source = value.get('source')
        required = {'worker_id', 'run_id', 'checkpoint_id', 'artifact', 'replica_receipt_sha256'}
        if not isinstance(source, dict) or set(source) != required:
            raise TrainCtlError('INVALID_CHECKPOINT_IMPORT', 'import source shape is invalid')
        if (not isinstance(source.get('worker_id'), str) or not _SAFE_ID.fullmatch(source['worker_id']) or
                not isinstance(source.get('run_id'), str) or not _SAFE_ID.fullmatch(source['run_id']) or
                not isinstance(source.get('checkpoint_id'), str) or
                not _CHECKPOINT_ID.fullmatch(source['checkpoint_id']) or
                not isinstance(source.get('replica_receipt_sha256'), str) or
                not _DIGEST.fullmatch(source['replica_receipt_sha256'])):
            raise TrainCtlError('INVALID_CHECKPOINT_IMPORT', 'import source identity is invalid')
        artifact = source.get('artifact')
        if not isinstance(artifact, dict) or set(artifact) != {'path', 'size', 'sha256'}:
            raise TrainCtlError('INVALID_CHECKPOINT_IMPORT', 'import artifact shape is invalid')
        path = artifact.get('path')
        if (not isinstance(path, str) or not _REMOTE_PATH.fullmatch(path) or
                '..' in PurePosixPath(path).parts or type(artifact.get('size')) is not int or
                artifact['size'] <= 0 or not isinstance(artifact.get('sha256'), str) or
                not _DIGEST.fullmatch(artifact['sha256'])):
            raise TrainCtlError('INVALID_CHECKPOINT_IMPORT', 'import artifact identity is invalid')
        return {'schema_version': 1, 'source': {
            'worker_id': source['worker_id'],
            'run_id': source['run_id'],
            'checkpoint_id': source['checkpoint_id'],
            'artifact': dict(artifact),
            'replica_receipt_sha256': source['replica_receipt_sha256'],
        }}

    @staticmethod
    def _import_id(expected):
        canonical = json.dumps(expected, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
        return 'import_' + hashlib.sha256(canonical).hexdigest()[:32]

    @staticmethod
    def _receipt(body):
        value = {key: item for key, item in body.items() if key != 'import_receipt_sha256'}
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

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

    def _journal(self, directory, expected, *, create):
        path = directory / 'transaction.json'
        import_id = self._import_id(expected)
        if not path.exists() and not path.is_symlink() and create:
            manifest = {
                'schema_version': 1, 'import_id': import_id,
                'source': expected['source'], 'imported_at': utc_now(),
            }
            manifest['import_receipt_sha256'] = self._receipt(manifest)
            journal = {'schema_version': 1, 'state': 'prepared', 'manifest': manifest}
            self._write_journal(path, journal, 'prepared')
            return journal
        try:
            if not path.is_file() or path.is_symlink():
                raise ValueError('missing or unsafe transaction receipt')
            journal = RunStore._read_json(path, {})
            if (not isinstance(journal, dict) or set(journal) != {
                    'schema_version', 'state', 'manifest', 'journal_sha256'} or
                    journal.get('schema_version') != 1 or
                    journal.get('state') not in {'prepared', 'verified', 'published', 'committed'} or
                    journal.get('journal_sha256') != self._journal_receipt(journal)):
                raise ValueError('invalid transaction receipt')
            manifest = journal.get('manifest')
            if (not isinstance(manifest, dict) or set(manifest) != {
                    'schema_version', 'import_id', 'source', 'imported_at', 'import_receipt_sha256'} or
                    manifest.get('schema_version') != 1 or manifest.get('import_id') != import_id or
                    manifest.get('import_receipt_sha256') != self._receipt(manifest) or
                    not isinstance(manifest.get('imported_at'), str)):
                raise ValueError('invalid planned import receipt')
            if manifest.get('source') != expected['source']:
                raise TrainCtlError('CHECKPOINT_IMPORT_CONFLICT', 'transaction has different source provenance')
        except (OSError, ValueError, TypeError) as exc:
            raise TrainCtlError(
                'CHECKPOINT_IMPORT_TRANSACTION_MISMATCH',
                'transaction receipt is missing, unsafe, or invalid; preserve this import directory '
                'outside the store for inspection before preparing it again',
            ) from exc
        return journal

    def _safe_directory(self, path):
        if not path.is_dir() or path.is_symlink() or not path.resolve().is_relative_to(self.root):
            raise TrainCtlError('UNSAFE_CHECKPOINT_IMPORT', 'checkpoint import path is unsafe')

    @staticmethod
    def _matches(path, artifact):
        try:
            return (path.is_file() and not path.is_symlink() and
                    path.stat().st_size == artifact['size'] and sha256(path) == artifact['sha256'])
        except OSError:
            return False

    @classmethod
    def _verify(cls, path, artifact):
        if not path.is_file() or path.is_symlink() or path.stat().st_size != artifact['size']:
            raise TrainCtlError(
                'CHECKPOINT_IMPORT_SIZE_MISMATCH',
                'uploaded checkpoint size differs: ' + str(path) +
                '; preserve the file for inspection before uploading again; published imports are not overwritten',
            )
        if sha256(path) != artifact['sha256']:
            raise TrainCtlError(
                'CHECKPOINT_IMPORT_SHA256_MISMATCH',
                'uploaded checkpoint SHA256 differs: ' + str(path) +
                '; preserve the file for inspection before uploading again; published imports are not overwritten',
            )

    def _existing(self, final, expected, *, status):
        self._safe_directory(final)
        manifest_path = final / 'manifest.json'
        checkpoint = final / 'checkpoint.ckpt'
        if not manifest_path.is_file() or manifest_path.is_symlink():
            raise TrainCtlError('INCOMPLETE_CHECKPOINT_IMPORT', 'checkpoint import manifest is missing or unsafe')
        manifest = RunStore._read_json(manifest_path, {})
        try:
            normalized = self._request({'schema_version': 1, 'source': manifest.get('source')})
        except TrainCtlError as exc:
            raise TrainCtlError(
                'CHECKPOINT_IMPORT_MANIFEST_MISMATCH', 'checkpoint import source is invalid',
            ) from exc
        if (manifest.get('schema_version') != 1 or manifest.get('import_id') != final.name or
                self._import_id(normalized) != final.name or
                normalized['source'] != expected.get('source') or
                manifest.get('import_receipt_sha256') != self._receipt(manifest)):
            raise TrainCtlError('CHECKPOINT_IMPORT_MANIFEST_MISMATCH', 'checkpoint import receipt is invalid')
        self._verify(checkpoint, normalized['source']['artifact'])
        return self._result(status, final, manifest)

    @staticmethod
    def _result(status, final, manifest):
        artifact = manifest['source']['artifact']
        return {
            'schema_version': 1,
            'status': status,
            'import_id': manifest['import_id'],
            'checkpoint_path': str(final / 'checkpoint.ckpt'),
            'manifest_path': str(final / 'manifest.json'),
            'size': artifact['size'],
            'sha256': artifact['sha256'],
            'source': manifest['source'],
            'import_receipt_sha256': manifest['import_receipt_sha256'],
        }

    @staticmethod
    def _validate_import_id(import_id):
        if not isinstance(import_id, str) or not _IMPORT_ID.fullmatch(import_id):
            raise TrainCtlError('INVALID_CHECKPOINT_IMPORT', 'import id is invalid')
