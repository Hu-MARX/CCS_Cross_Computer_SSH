"""Trusted-training-side selection of one controlled recovery checkpoint."""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from uuid import uuid4

from .checkpoints import sync_directory


_DIGEST = re.compile(r'[a-f0-9]{64}')
_TASK_ENV = (
    'TRAINCTL_TASK_CHECKPOINT',
    'TRAINCTL_TASK_CHECKPOINT_SHA256',
    'TRAINCTL_TASK_INPUTS_REPORT',
)


class RecoveryInput:
    """Deep Module selecting either a gated task input or a local managed point.

    The validator runs only inside the caller's trusted Conda environment.  It
    must return a state mapping containing the serialized experiment contract.
    """

    def __init__(self, validator, environ=None):
        self.validator = validator
        self.environ = os.environ if environ is None else environ

    def select(self, destination, contract, *, local_store=None):
        destination = Path(destination).absolute()
        task_source = self._task_source()
        if task_source is not None and local_store is not None:
            raise ValueError('RECOVERY_INPUT_CONFLICT: task checkpoint and local source are mutually exclusive')
        if task_source is None:
            if local_store is None:
                return None
            return local_store.restore_copy(destination, self.validator, contract=contract)
        return self._copy_task_source(task_source, destination, contract)

    def _task_source(self):
        values = {key: self.environ.get(key) for key in _TASK_ENV}
        present = [key for key, value in values.items() if value]
        if not present:
            return None
        if len(present) != len(_TASK_ENV):
            raise ValueError('RECOVERY_INPUT_ENV_INCOMPLETE: gated checkpoint environment is incomplete')

        source = Path(values['TRAINCTL_TASK_CHECKPOINT'])
        report_path = Path(values['TRAINCTL_TASK_INPUTS_REPORT'])
        digest = values['TRAINCTL_TASK_CHECKPOINT_SHA256']
        if not source.is_absolute() or source.is_symlink() or not source.is_file():
            raise ValueError('RECOVERY_INPUT_UNSAFE: gated checkpoint is missing or unsafe')
        if not _DIGEST.fullmatch(digest):
            raise ValueError('RECOVERY_INPUT_DIGEST_INVALID: gated checkpoint digest is invalid')
        run_dir_value = self.environ.get('TRAINCTL_RUN_DIR')
        if not run_dir_value:
            raise ValueError('RECOVERY_INPUT_REPORT_INVALID: TRAINCTL_RUN_DIR is missing')
        run_dir = Path(run_dir_value).absolute()
        expected_report = run_dir / 'task-inputs.json'
        if (not report_path.is_absolute() or report_path.absolute() != expected_report or
                report_path.is_symlink() or not report_path.is_file()):
            raise ValueError('RECOVERY_INPUT_REPORT_INVALID: task input report is missing or unexpected')
        try:
            report = json.loads(report_path.read_text(encoding='utf-8'))
            checkpoint = report['checkpoint']
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise ValueError('RECOVERY_INPUT_REPORT_INVALID: task input report cannot be read') from exc
        if (not isinstance(report, dict) or not isinstance(checkpoint, dict) or
                report.get('schema_version') != 1 or checkpoint.get('path') != str(source.absolute()) or
                checkpoint.get('sha256') != digest):
            raise ValueError('RECOVERY_INPUT_REPORT_MISMATCH: task input report differs from environment')
        return {'path': source.absolute(), 'sha256': digest, 'report': report}

    def _copy_task_source(self, source, destination, contract):
        path = source['path']
        if destination.exists() or destination.is_symlink():
            raise ValueError('RECOVERY_INPUT_DESTINATION_EXISTS: recovery destination must be new')
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.resolve() != destination:
            raise ValueError('RECOVERY_INPUT_DESTINATION_UNSAFE: recovery destination traverses a symlink')
        before = self._signature(path)
        temporary = destination.with_name(destination.name + '.' + uuid4().hex + '.partial')
        digest = hashlib.sha256()
        try:
            with path.open('rb') as reader, temporary.open('xb') as writer:
                while chunk := reader.read(1024 * 1024):
                    digest.update(chunk)
                    writer.write(chunk)
                writer.flush()
                os.fsync(writer.fileno())
            if before != self._signature(path):
                raise ValueError('RECOVERY_INPUT_CHANGED: checkpoint changed while being copied')
            if digest.hexdigest() != source['sha256']:
                raise ValueError('RECOVERY_INPUT_DIGEST_MISMATCH: checkpoint differs from launch binding')
            state = self.validator(temporary)
            if not isinstance(state, dict) or state.get('contract') != contract:
                raise ValueError('RECOVERY_INPUT_CONTRACT_MISMATCH: serialized experiment contract differs')
            os.link(temporary, destination)
            try:
                sync_directory(destination.parent)
            except BaseException:
                destination.unlink(missing_ok=True)
                raise
        finally:
            temporary.unlink(missing_ok=True)

        report_checkpoint = source['report']['checkpoint']
        selected = {
            'id': report_checkpoint.get('import_id') or 'task_' + source['sha256'][:32],
            'file': destination.name,
            'size': destination.stat().st_size,
            'sha256': source['sha256'],
            'state': state,
            'metadata': {
                'contract': contract,
                'source_kind': report_checkpoint.get('kind', 'external'),
                'source_run_id': source['report'].get('source_run_id'),
                'import_receipt_sha256': report_checkpoint.get('import_receipt_sha256'),
            },
        }
        return {
            'selected': selected,
            'state': state,
            'rejected': [],
            'path': str(destination),
            'source': {
                'kind': 'task-checkpoint',
                'source_run_id': source['report'].get('source_run_id'),
                'import_id': report_checkpoint.get('import_id'),
                'import_receipt_sha256': report_checkpoint.get('import_receipt_sha256'),
            },
        }

    @staticmethod
    def _signature(path):
        stat = path.stat()
        return stat.st_size, stat.st_mtime_ns, getattr(stat, 'st_ino', None)
