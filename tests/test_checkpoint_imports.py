import hashlib
import io
import json
import os
import stat
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from trainctl.checkpoint_imports import CheckpointImportStore
from trainctl.cli import main as trainctl_main
from trainctl.run_store import RunStore, TrainCtlError


class CheckpointImportTests(unittest.TestCase):
    payload = b'verified checkpoint state'

    @classmethod
    def request(cls):
        return {
            'schema_version': 1,
            'source': {
                'worker_id': 'worker-a',
                'run_id': 'run_' + 'a' * 32,
                'checkpoint_id': 'ckpt_' + 'b' * 32,
                'artifact': {
                    'path': '/research/checkpoints/ckpt_' + 'b' * 32 + '.ckpt',
                    'size': len(cls.payload),
                    'sha256': hashlib.sha256(cls.payload).hexdigest(),
                },
                'replica_receipt_sha256': 'c' * 64,
            },
        }

    def test_prepare_commit_show_and_repeat_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointImportStore(Path(directory) / 'imports')
            prepared = store.prepare(self.request())
            self.assertEqual(prepared['status'], 'ready')
            self.assertTrue(prepared['upload_required'])
            Path(prepared['upload_path']).write_bytes(self.payload)

            committed = store.commit(prepared['import_id'])
            repeated = store.prepare(self.request())
            shown = store.show(prepared['import_id'])

            self.assertEqual(committed['status'], 'imported')
            self.assertEqual(repeated['status'], 'present')
            self.assertEqual(shown['sha256'], hashlib.sha256(self.payload).hexdigest())
            self.assertEqual(Path(shown['checkpoint_path']).read_bytes(), self.payload)

    def test_bad_upload_never_publishes_an_import(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointImportStore(Path(directory) / 'imports')
            prepared = store.prepare(self.request())
            Path(prepared['upload_path']).write_bytes(b'wrong')

            with self.assertRaises(TrainCtlError) as raised:
                store.commit(prepared['import_id'])

            self.assertEqual(raised.exception.code, 'CHECKPOINT_IMPORT_SIZE_MISMATCH')
            self.assertFalse((store.root / prepared['import_id']).exists())

    def test_published_checkpoint_or_receipt_corruption_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointImportStore(Path(directory) / 'imports')
            prepared = store.prepare(self.request())
            Path(prepared['upload_path']).write_bytes(self.payload)
            committed = store.commit(prepared['import_id'])
            Path(committed['checkpoint_path']).write_bytes(b'corrupt')

            with self.assertRaises(TrainCtlError):
                store.show(prepared['import_id'])

    def test_trainctl_exposes_prepare_and_commit_without_deserializing_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            request_path = Path(directory) / 'request.json'
            request_path.write_text(json.dumps(self.request()), encoding='utf-8')
            output = io.StringIO()
            with redirect_stdout(output):
                code = trainctl_main([
                    '--root', directory, 'checkpoint-import', 'prepare',
                    '--request', str(request_path),
                ])
            prepared = json.loads(output.getvalue())['data']
            Path(prepared['upload_path']).write_bytes(self.payload)
            output = io.StringIO()
            with redirect_stdout(output):
                committed_code = trainctl_main([
                    '--root', directory, 'checkpoint-import', 'commit', prepared['import_id'],
                ])

            self.assertEqual(code, 0)
            self.assertEqual(committed_code, 0)
            self.assertEqual(json.loads(output.getvalue())['data']['status'], 'imported')

    def test_commit_restart_recovers_file_manifest_and_directory_publication(self):
        for boundary in ('before_file', 'after_file', 'before_manifest', 'after_manifest',
                         'before_directory', 'after_directory'):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / 'imports'
                store = CheckpointImportStore(root)
                prepared = store.prepare(self.request())
                partial = Path(prepared['upload_path'])
                partial.write_bytes(self.payload)
                original_stat = partial.stat()
                journal_path = partial.parent / 'transaction.json'
                receipt = json.loads(journal_path.read_text())['manifest']['import_receipt_sha256']
                replace, atomic_json = os.replace, RunStore._atomic_json

                def interrupted_replace(source, destination):
                    kind = 'file' if Path(source).name == 'checkpoint.partial' else (
                        'directory' if Path(destination).parent == root and
                        Path(destination).name == prepared['import_id'] else None)
                    if kind and boundary == 'before_' + kind:
                        raise OSError('injected interruption before ' + kind)
                    result = replace(source, destination)
                    if kind and boundary == 'after_' + kind:
                        raise OSError('injected interruption after ' + kind)
                    return result

                def interrupted_manifest(path, body):
                    is_manifest = Path(path).name == 'manifest.json'
                    if is_manifest and boundary == 'before_manifest':
                        raise OSError('injected interruption before manifest')
                    atomic_json(path, body)
                    if is_manifest and boundary == 'after_manifest':
                        raise OSError('injected interruption after manifest')

                with patch('trainctl.checkpoint_imports.os.replace', interrupted_replace), \
                        patch.object(RunStore, '_atomic_json', interrupted_manifest):
                    with self.assertRaises(OSError):
                        store.commit(prepared['import_id'])
                restarted = CheckpointImportStore(root)
                recovered = restarted.commit(prepared['import_id'])
                repeated = restarted.prepare(self.request())
                self.assertEqual(recovered['import_receipt_sha256'], receipt)
                self.assertEqual(repeated['status'], 'present')
                self.assertEqual(Path(recovered['checkpoint_path']).stat().st_mtime_ns, original_stat.st_mtime_ns)
                self.assertEqual(restarted.show(prepared['import_id'])['sha256'],
                                 hashlib.sha256(self.payload).hexdigest())

    def test_missing_final_manifest_is_repaired_only_by_a_mutation_with_verified_journal(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointImportStore(Path(directory) / 'imports')
            prepared = store.prepare(self.request())
            Path(prepared['upload_path']).write_bytes(self.payload)
            committed = store.commit(prepared['import_id'])
            final = Path(committed['checkpoint_path']).parent
            if os.name == 'posix':
                os.chmod(final, 0o750)
            manifest_path = Path(committed['manifest_path'])
            manifest_path.unlink()
            if os.name == 'posix':
                os.chmod(final, 0o550)
            original_stat = Path(committed['checkpoint_path']).stat()
            with self.assertRaises(TrainCtlError) as raised:
                store.show(prepared['import_id'])
            self.assertEqual(raised.exception.code, 'INCOMPLETE_CHECKPOINT_IMPORT')
            self.assertFalse(manifest_path.exists())
            repaired = store.prepare(self.request())
            self.assertEqual(repaired['import_receipt_sha256'], committed['import_receipt_sha256'])
            self.assertEqual(Path(repaired['checkpoint_path']).stat().st_mtime_ns, original_stat.st_mtime_ns)
            if os.name == 'posix':
                self.assertEqual(stat.S_IMODE(final.stat().st_mode), 0o550)

    def test_recovery_rejects_bad_journal_source_and_published_file(self):
        for violation in ('source', 'file', 'journal', 'orphan'):
            with self.subTest(violation=violation), tempfile.TemporaryDirectory() as directory:
                store = CheckpointImportStore(Path(directory) / 'imports')
                prepared = store.prepare(self.request())
                Path(prepared['upload_path']).write_bytes(self.payload)
                atomic_json = RunStore._atomic_json

                def interrupt(path, body):
                    if Path(path).name == 'manifest.json':
                        raise OSError('injected interruption')
                    atomic_json(path, body)

                with patch.object(RunStore, '_atomic_json', interrupt):
                    with self.assertRaises(OSError):
                        store.commit(prepared['import_id'])
                staging = Path(prepared['upload_path']).parent
                target = staging / 'checkpoint.ckpt'
                if violation == 'file':
                    target.write_bytes(b'corrupt')
                elif violation == 'source':
                    expected_path = staging / 'expected.json'
                    body = json.loads(expected_path.read_text())
                    body['source']['worker_id'] = 'worker-different'
                    expected_path.write_text(json.dumps(body))
                elif violation == 'journal':
                    journal_path = staging / 'transaction.json'
                    body = json.loads(journal_path.read_text())
                    body['manifest']['source']['worker_id'] = 'worker-different'
                    journal_path.write_text(json.dumps(body))
                else:
                    # Moving a lone binary into final does not supply provenance.
                    (staging / 'transaction.json').unlink()
                    os.replace(staging, store.root / prepared['import_id'])
                    target = store.root / prepared['import_id'] / 'checkpoint.ckpt'
                preserved = target.read_bytes()
                with self.assertRaises(TrainCtlError):
                    store.commit(prepared['import_id'])
                self.assertEqual(target.read_bytes(), preserved)
                self.assertFalse((target.parent / 'manifest.json').exists())

    def test_complete_legacy_import_does_not_require_or_create_a_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointImportStore(Path(directory) / 'imports')
            prepared = store.prepare(self.request())
            Path(prepared['upload_path']).write_bytes(self.payload)
            committed = store.commit(prepared['import_id'])
            final = Path(committed['checkpoint_path']).parent
            if os.name == 'posix':
                os.chmod(final, 0o750)
            transaction = final / 'transaction.json'
            transaction.unlink()
            self.assertEqual(store.prepare(self.request())['status'], 'present')
            self.assertEqual(store.commit(prepared['import_id'])['status'], 'present')
            self.assertEqual(store.show(prepared['import_id'])['import_receipt_sha256'],
                             committed['import_receipt_sha256'])
            self.assertFalse(transaction.exists())


if __name__ == '__main__':
    unittest.main()
