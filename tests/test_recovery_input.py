import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from trainctl.recovery_input import RecoveryInput


class RecoveryInputTests(unittest.TestCase):
    payload = b'controlled recovery checkpoint'
    contract = 'c' * 64

    def gated(self, root, *, payload=None, report_digest=None):
        payload = self.payload if payload is None else payload
        source = root / 'imported.ckpt'
        source.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        run_dir = root / 'run'
        run_dir.mkdir()
        report = run_dir / 'task-inputs.json'
        report.write_text(json.dumps({
            'schema_version': 1,
            'source_run_id': 'run_' + 'a' * 32,
            'checkpoint': {
                'path': str(source.absolute()),
                'sha256': report_digest or digest,
                'kind': 'imported',
                'import_id': 'import_' + 'b' * 32,
                'import_receipt_sha256': 'd' * 64,
            },
        }), encoding='utf-8')
        env = {
            'TRAINCTL_RUN_DIR': str(run_dir),
            'TRAINCTL_TASK_CHECKPOINT': str(source.absolute()),
            'TRAINCTL_TASK_CHECKPOINT_SHA256': digest,
            'TRAINCTL_TASK_INPUTS_REPORT': str(report.absolute()),
        }
        return source, digest, env

    def validator(self, path):
        self.assertEqual(path.read_bytes(), self.payload)
        return {'contract': self.contract, 'global_step': 7}

    def test_no_declared_source_returns_none(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / 'resume.ckpt'
            self.assertIsNone(RecoveryInput(self.validator, {}).select(destination, self.contract))
            self.assertFalse(destination.exists())

    def test_local_managed_store_remains_a_supported_adapter(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / 'resume.ckpt'
            calls = []

            class Store:
                def restore_copy(inner, path, validator, *, contract):
                    calls.append((path, validator, contract))
                    return {'path': str(path), 'state': {'contract': contract}}

            result = RecoveryInput(self.validator, {}).select(
                destination, self.contract, local_store=Store(),
            )

            self.assertEqual(result['path'], str(destination.absolute()))
            self.assertEqual(calls, [(destination.absolute(), self.validator, self.contract)])

    def test_gated_task_checkpoint_is_copied_and_semantically_validated(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, digest, env = self.gated(root)
            destination = root / 'output/resume.ckpt'

            result = RecoveryInput(self.validator, env).select(destination, self.contract)

            self.assertEqual(destination.read_bytes(), source.read_bytes())
            self.assertEqual(result['path'], str(destination.absolute()))
            self.assertEqual(result['selected']['sha256'], digest)
            self.assertEqual(result['state']['global_step'], 7)
            self.assertEqual(result['source']['kind'], 'task-checkpoint')
            self.assertEqual(result['source']['import_id'], 'import_' + 'b' * 32)

    def test_report_mismatch_is_rejected_before_deserialization(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, _, env = self.gated(root, report_digest='0' * 64)
            called = []

            with self.assertRaisesRegex(ValueError, 'RECOVERY_INPUT_REPORT_MISMATCH'):
                RecoveryInput(lambda path: called.append(path), env).select(
                    root / 'resume.ckpt', self.contract,
                )

            self.assertEqual(called, [])

    def test_serialized_contract_mismatch_is_not_published(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, _, env = self.gated(root)
            destination = root / 'resume.ckpt'

            with self.assertRaisesRegex(ValueError, 'RECOVERY_INPUT_CONTRACT_MISMATCH'):
                RecoveryInput(lambda path: {'contract': 'wrong'}, env).select(
                    destination, self.contract,
                )

            self.assertFalse(destination.exists())
            self.assertEqual(list(root.glob('*.partial')), [])

    def test_checkpoint_changed_after_launch_binding_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, _, env = self.gated(root)
            source.write_bytes(b'replaced after task input gate')
            destination = root / 'resume.ckpt'

            with self.assertRaisesRegex(ValueError, 'RECOVERY_INPUT_DIGEST_MISMATCH'):
                RecoveryInput(self.validator, env).select(destination, self.contract)

            self.assertFalse(destination.exists())

    def test_partial_environment_and_dual_sources_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, _, env = self.gated(root)
            partial = {'TRAINCTL_TASK_CHECKPOINT': env['TRAINCTL_TASK_CHECKPOINT']}
            with self.assertRaisesRegex(ValueError, 'RECOVERY_INPUT_ENV_INCOMPLETE'):
                RecoveryInput(self.validator, partial).select(root / 'one.ckpt', self.contract)

            with self.assertRaisesRegex(ValueError, 'RECOVERY_INPUT_CONFLICT'):
                RecoveryInput(self.validator, env).select(
                    root / 'two.ckpt', self.contract, local_store=object(),
                )


if __name__ == '__main__':
    unittest.main()
