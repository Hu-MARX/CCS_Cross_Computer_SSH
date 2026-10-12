"""Launch-time integrity gate for checkpoint inputs declared by a Run."""
from __future__ import annotations

import os
from pathlib import Path

from controller.domain import utc_now
from .checkpoints import sha256
from .run_store import RunStore, TrainCtlError


class TaskInputGate:
    """Deep Module that binds one declared checkpoint before GPU acquisition."""

    def __init__(self, store):
        self.store = store

    def prepare(self, run_dir, spec, env):
        task = spec.task
        reference = task.get('checkpoint') if isinstance(task, dict) else None
        if reference is None:
            return None
        path = Path(reference['path']).absolute()
        if not Path(reference['path']).is_absolute():
            raise TrainCtlError('TASK_CHECKPOINT_PATH_INVALID', 'task checkpoint path must be absolute')
        if path.is_symlink() or not path.is_file():
            raise TrainCtlError('TASK_CHECKPOINT_MISSING', 'task checkpoint is missing or unsafe')
        before = self._signature(path)
        digest = sha256(path)
        after = self._signature(path)
        if before != after:
            raise TrainCtlError('TASK_CHECKPOINT_CHANGED', 'task checkpoint changed during launch verification')
        if digest != reference['sha256']:
            raise TrainCtlError('TASK_CHECKPOINT_SHA256_MISMATCH', 'task checkpoint SHA256 differs')

        checkpoint = {
            'path': str(path),
            'sha256': digest,
            'size': after[0],
            'kind': 'external',
        }
        imports_root = (self.store.root / 'checkpoint-imports').absolute()
        if path.resolve().is_relative_to(imports_root):
            if not imports_root.is_dir() or imports_root.is_symlink():
                raise TrainCtlError('TASK_CHECKPOINT_IMPORT_MISSING', 'checkpoint import store is unavailable')
            relative = path.resolve().relative_to(imports_root.resolve())
            if len(relative.parts) != 2 or relative.parts[1] != 'checkpoint.ckpt':
                raise TrainCtlError('TASK_CHECKPOINT_IMPORT_INVALID', 'checkpoint import path is not published')
            from .checkpoint_imports import CheckpointImportStore
            imported = CheckpointImportStore(imports_root).show(relative.parts[0])
            if imported['checkpoint_path'] != str(path) or imported['sha256'] != digest:
                raise TrainCtlError('TASK_CHECKPOINT_IMPORT_MISMATCH', 'checkpoint import receipt differs')
            checkpoint.update({
                'kind': 'imported',
                'import_id': imported['import_id'],
                'source': imported['source'],
                'import_receipt_sha256': imported['import_receipt_sha256'],
            })

        record = {
            'schema_version': 1,
            'checked_at': utc_now(),
            'source_run_id': task.get('source_run_id'),
            'checkpoint': checkpoint,
            'scope': 'verified before GPU acquisition; training code must validate serialized state compatibility',
        }
        report = Path(run_dir) / 'task-inputs.json'
        RunStore._atomic_json(report, record)
        env['TRAINCTL_TASK_CHECKPOINT'] = str(path)
        env['TRAINCTL_TASK_CHECKPOINT_SHA256'] = digest
        env['TRAINCTL_TASK_INPUTS_REPORT'] = str(report)
        return record

    @staticmethod
    def _signature(path):
        stat = path.stat()
        return stat.st_size, stat.st_mtime_ns, getattr(stat, 'st_ino', None)
