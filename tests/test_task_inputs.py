import hashlib
import os
import tempfile
import unittest
from pathlib import Path

from controller.domain import ExperimentSpec
from trainctl.checkpoint_imports import CheckpointImportStore
from trainctl.run_store import RunStore, TrainCtlError
from trainctl.task_inputs import TaskInputGate


class TaskInputGateTests(unittest.TestCase):
    payload = b'task checkpoint state'

    @classmethod
    def spec(cls, path, digest):
        return ExperimentSpec.from_dict({
            'schema_version': 1,
            'name': 'checkpoint-input',
            'project': {},
            'runtime': {},
            'command': {'argv': ['python', 'resume.py']},
            'task': {
                'kind': 'train',
                'inputs': [],
                'checkpoint': {'path': str(path), 'sha256': digest},
            },
        })

    @classmethod
    def import_request(cls):
        return {
            'schema_version': 1,
            'source': {
                'worker_id': 'source-worker',
                'run_id': 'run_' + 'a' * 32,
                'checkpoint_id': 'ckpt_' + 'b' * 32,
                'artifact': {
                    'path': '/source/checkpoints/ckpt_' + 'b' * 32 + '.ckpt',
                    'size': len(cls.payload),
                    'sha256': hashlib.sha256(cls.payload).hexdigest(),
                },
                'replica_receipt_sha256': 'c' * 64,
            },
        }

    def test_external_checkpoint_is_bound_to_environment_and_run_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = RunStore(root / 'worker')
            run_dir = root / 'run'
            run_dir.mkdir()
            checkpoint = root / 'checkpoint.ckpt'
            checkpoint.write_bytes(self.payload)
            digest = hashlib.sha256(self.payload).hexdigest()
            env = {}

            record = TaskInputGate(store).prepare(
                run_dir, self.spec(checkpoint, digest), env,
            )

            self.assertEqual(record['checkpoint']['kind'], 'external')
            self.assertEqual(env['TRAINCTL_TASK_CHECKPOINT'], str(checkpoint.absolute()))
            self.assertEqual(env['TRAINCTL_TASK_CHECKPOINT_SHA256'], digest)
            self.assertTrue((run_dir / 'task-inputs.json').is_file())

    def test_digest_mismatch_fails_without_publishing_a_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = RunStore(root / 'worker')
            run_dir = root / 'run'
            run_dir.mkdir()
            checkpoint = root / 'checkpoint.ckpt'
            checkpoint.write_bytes(self.payload)
            env = {}

            with self.assertRaises(TrainCtlError) as raised:
                TaskInputGate(store).prepare(
                    run_dir, self.spec(checkpoint, '0' * 64), env,
                )

            self.assertEqual(raised.exception.code, 'TASK_CHECKPOINT_SHA256_MISMATCH')
            self.assertFalse((run_dir / 'task-inputs.json').exists())
            self.assertNotIn('TRAINCTL_TASK_CHECKPOINT', env)

    def test_imported_checkpoint_requires_and_carries_target_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = RunStore(root / 'worker')
            imports = CheckpointImportStore(store.root / 'checkpoint-imports')
            prepared = imports.prepare(self.import_request())
            Path(prepared['upload_path']).write_bytes(self.payload)
            imported = imports.commit(prepared['import_id'])
            run_dir = root / 'run'
            run_dir.mkdir()

            record = TaskInputGate(store).prepare(
                run_dir,
                self.spec(imported['checkpoint_path'], imported['sha256']),
                {},
            )

            self.assertEqual(record['checkpoint']['kind'], 'imported')
            self.assertEqual(record['checkpoint']['import_id'], imported['import_id'])
            self.assertEqual(
                record['checkpoint']['import_receipt_sha256'],
                imported['import_receipt_sha256'],
            )

    def test_tampered_import_is_rejected_before_a_run_binding_exists(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = RunStore(root / 'worker')
            imports = CheckpointImportStore(store.root / 'checkpoint-imports')
            prepared = imports.prepare(self.import_request())
            Path(prepared['upload_path']).write_bytes(self.payload)
            imported = imports.commit(prepared['import_id'])
            checkpoint = Path(imported['checkpoint_path'])
            os.chmod(checkpoint, 0o660)
            checkpoint.write_bytes(b'tampered')
            run_dir = root / 'run'
            run_dir.mkdir()

            with self.assertRaises(TrainCtlError):
                TaskInputGate(store).prepare(
                    run_dir,
                    self.spec(checkpoint, imported['sha256']),
                    {},
                )
            self.assertFalse((run_dir / 'task-inputs.json').exists())


if __name__ == '__main__':
    unittest.main()
