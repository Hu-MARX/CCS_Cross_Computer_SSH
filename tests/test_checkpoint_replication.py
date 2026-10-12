import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from controller.checkpoint_replication import CheckpointReplicaError, CheckpointReplicaStore
from controller.domain import ExperimentSpec
from controller.remote import FakeRemoteRunner
from trainctl.run_store import RunStore


class CheckpointReplicationTests(unittest.TestCase):
    def setUp(self):
        self.remote = FakeRemoteRunner()
        self.remote.host = '192.0.2.8'
        self.remote.user = 'runner'
        self.remote.port = 22
        self.remote.remote_root = '/srv/ccs'
        spec = ExperimentSpec.from_dict({
            'schema_version': 1, 'name': 'replica-source', 'project': {}, 'runtime': {},
            'command': {'argv': ['python', 'train.py']},
        })
        self.run_id = self.remote.submit(spec, 'replica-source-001')
        self.checkpoint_id = 'ckpt_' + 'a' * 32
        self.path = '/research/orchestrator_runs/' + self.run_id + '/checkpoints/' + self.checkpoint_id + '.ckpt'
        self.payload = b'full managed checkpoint state'
        self.remote.set_managed_checkpoint(
            self.run_id, self.checkpoint_id, self.path, self.payload,
        )

    def test_replication_publishes_verified_checkpoint_and_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointReplicaStore(directory)

            result = store.replicate(
                self.remote, worker_id='worker-a', run_id=self.run_id,
                checkpoint_id=self.checkpoint_id,
            )

            self.assertEqual(result['status'], 'replicated')
            self.assertEqual(Path(result['checkpoint_path']).read_bytes(), self.payload)
            manifest = json.loads(Path(result['manifest_path']).read_text())
            self.assertEqual(manifest['artifact']['sha256'], hashlib.sha256(self.payload).hexdigest())
            self.assertEqual(manifest['source']['host'], '192.0.2.8')

    def test_same_replica_is_idempotent_but_local_corruption_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointReplicaStore(directory)
            first = store.replicate(
                self.remote, worker_id='worker-a', run_id=self.run_id,
                checkpoint_id=self.checkpoint_id,
            )
            second = store.replicate(
                self.remote, worker_id='worker-a', run_id=self.run_id,
                checkpoint_id=self.checkpoint_id,
            )
            self.assertEqual(second['status'], 'present')

            Path(first['checkpoint_path']).write_bytes(b'corrupt')
            with self.assertRaisesRegex(CheckpointReplicaError, 'size differs'):
                store.replicate(
                    self.remote, worker_id='worker-a', run_id=self.run_id,
                    checkpoint_id=self.checkpoint_id,
                )

    def test_download_mismatch_never_publishes_a_manifest(self):
        class CorruptTransfer(FakeRemoteRunner):
            def download_artifact(self, run_id, artifact, destination):
                Path(destination).write_bytes(b'wrong')
                return {'path': str(destination), 'size': 5}

        remote = CorruptTransfer()
        remote.host, remote.user, remote.port, remote.remote_root = 'worker', 'runner', 22, '/srv/ccs'
        remote.runs = self.remote.runs
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointReplicaStore(directory)
            with self.assertRaisesRegex(CheckpointReplicaError, 'size differs'):
                store.replicate(
                    remote, worker_id='worker-a', run_id=self.run_id,
                    checkpoint_id=self.checkpoint_id,
                )
            self.assertEqual(list(Path(directory).rglob('manifest.json')), [])

    def test_only_exact_managed_checkpoint_identifier_is_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointReplicaStore(directory)
            with self.assertRaises(ValueError):
                store.replicate(
                    self.remote, worker_id='worker-a', run_id=self.run_id,
                    checkpoint_id='../checkpoint',
                )

    def test_checkpoint_must_be_current_committed_latest(self):
        stale_id = 'ckpt_' + 'b' * 32
        stale_path = '/research/' + self.run_id + '/checkpoints/' + stale_id + '.ckpt'
        self.remote.set_artifact_content(self.run_id, stale_path, b'stale checkpoint')
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(CheckpointReplicaError, 'latest committed') as raised:
                CheckpointReplicaStore(directory).replicate(
                    self.remote, worker_id='worker-a', run_id=self.run_id,
                    checkpoint_id=stale_id,
                )
        self.assertEqual(raised.exception.code, 'CHECKPOINT_NOT_COMMITTED_LATEST')

    def test_verified_replica_imports_only_to_the_selected_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointReplicaStore(directory)
            store.replicate(
                self.remote, worker_id='worker-a', run_id=self.run_id,
                checkpoint_id=self.checkpoint_id,
            )
            target = FakeRemoteRunner()

            first = store.import_to(
                target, source_worker_id='worker-a', source_run_id=self.run_id,
                checkpoint_id=self.checkpoint_id, target_worker_id='worker-b',
            )
            second = store.import_to(
                target, source_worker_id='worker-a', source_run_id=self.run_id,
                checkpoint_id=self.checkpoint_id, target_worker_id='worker-b',
            )

            self.assertEqual(first['status'], 'imported')
            self.assertEqual(second['status'], 'present')
            self.assertEqual(first['target_worker_id'], 'worker-b')
            self.assertEqual(len(target.checkpoint_imports), 1)

    def test_local_corruption_blocks_import_before_target_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointReplicaStore(directory)
            replica = store.replicate(
                self.remote, worker_id='worker-a', run_id=self.run_id,
                checkpoint_id=self.checkpoint_id,
            )
            Path(replica['checkpoint_path']).write_bytes(b'corrupt')
            target = FakeRemoteRunner()

            with self.assertRaises(CheckpointReplicaError):
                store.import_to(
                    target, source_worker_id='worker-a', source_run_id=self.run_id,
                    checkpoint_id=self.checkpoint_id, target_worker_id='worker-b',
                )
            self.assertFalse(hasattr(target, 'checkpoint_imports'))

    def test_restart_recovers_each_publication_boundary_without_redownload_or_overwrite(self):
        for boundary in ('before_file', 'after_file', 'before_manifest', 'after_manifest'):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as directory:
                store = CheckpointReplicaStore(directory)
                replace, atomic_json = os.replace, RunStore._atomic_json

                def interrupted_replace(source, destination):
                    publishes_file = Path(destination).name == 'checkpoint.ckpt'
                    if publishes_file and boundary == 'before_file':
                        raise OSError('injected interruption before file publication')
                    result = replace(source, destination)
                    if publishes_file and boundary == 'after_file':
                        raise OSError('injected interruption after file publication')
                    return result

                def interrupted_manifest(path, body):
                    is_manifest = Path(path).name == 'manifest.json'
                    if is_manifest and boundary == 'before_manifest':
                        raise OSError('injected interruption before manifest publication')
                    atomic_json(path, body)
                    if is_manifest and boundary == 'after_manifest':
                        raise OSError('injected interruption after manifest publication')

                with patch.object(self.remote, 'download_artifact', wraps=self.remote.download_artifact) as download:
                    with patch('controller.checkpoint_replication.os.replace', interrupted_replace), \
                            patch.object(RunStore, '_atomic_json', interrupted_manifest):
                        with self.assertRaises(OSError):
                            store.replicate(
                                self.remote, worker_id='worker-a', run_id=self.run_id,
                                checkpoint_id=self.checkpoint_id,
                            )
                    replica_dir = Path(directory) / 'worker-a' / self.run_id / self.checkpoint_id
                    journal = json.loads((replica_dir / 'transaction.json').read_text())
                    preserved = replica_dir / 'checkpoint.ckpt'
                    if not preserved.exists():
                        preserved = replica_dir / journal['temporary']
                    original_stat = preserved.stat()
                    restarted = CheckpointReplicaStore(directory)
                    repaired = restarted.replicate(
                        self.remote, worker_id='worker-a', run_id=self.run_id,
                        checkpoint_id=self.checkpoint_id,
                    )
                    second = restarted.replicate(
                        self.remote, worker_id='worker-a', run_id=self.run_id,
                        checkpoint_id=self.checkpoint_id,
                    )
                    self.assertEqual(download.call_count, 1)
                    self.assertEqual(second['status'], 'present')
                    self.assertEqual(Path(repaired['checkpoint_path']).stat().st_mtime_ns, original_stat.st_mtime_ns)
                    self.assertEqual(restarted.verified(
                        worker_id='worker-a', run_id=self.run_id, checkpoint_id=self.checkpoint_id,
                    )['sha256'], hashlib.sha256(self.payload).hexdigest())

    def test_recovery_refuses_changed_source_corruption_and_untrusted_orphans(self):
        for violation in ('source', 'file', 'journal', 'orphan'):
            with self.subTest(violation=violation), tempfile.TemporaryDirectory() as directory:
                store = CheckpointReplicaStore(directory)
                atomic_json = RunStore._atomic_json

                def interrupt(path, body):
                    if Path(path).name == 'manifest.json':
                        raise OSError('injected interruption')
                    atomic_json(path, body)

                with patch.object(RunStore, '_atomic_json', interrupt):
                    with self.assertRaises(OSError):
                        store.replicate(
                            self.remote, worker_id='worker-a', run_id=self.run_id,
                            checkpoint_id=self.checkpoint_id,
                        )
                replica_dir = Path(directory) / 'worker-a' / self.run_id / self.checkpoint_id
                target = replica_dir / 'checkpoint.ckpt'
                journal_path = replica_dir / 'transaction.json'
                original_host = self.remote.host
                if violation == 'source':
                    self.remote.host = '192.0.2.99'
                elif violation == 'file':
                    target.write_bytes(b'corrupt')
                elif violation == 'journal':
                    journal = json.loads(journal_path.read_text())
                    journal['manifest']['source']['host'] = '192.0.2.99'
                    journal_path.write_text(json.dumps(journal))
                else:
                    journal_path.unlink()
                preserved = target.read_bytes()
                try:
                    with self.assertRaises(CheckpointReplicaError):
                        CheckpointReplicaStore(directory).replicate(
                            self.remote, worker_id='worker-a', run_id=self.run_id,
                            checkpoint_id=self.checkpoint_id,
                        )
                    self.assertEqual(target.read_bytes(), preserved)
                    self.assertFalse((replica_dir / 'manifest.json').exists())
                finally:
                    self.remote.host = original_host

    def test_completed_replica_binds_the_worker_connection_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointReplicaStore(directory)
            result = store.replicate(
                self.remote, worker_id='worker-a', run_id=self.run_id,
                checkpoint_id=self.checkpoint_id,
            )
            self.remote.remote_root = '/another/ccs'
            with self.assertRaises(CheckpointReplicaError) as raised:
                store.replicate(
                    self.remote, worker_id='worker-a', run_id=self.run_id,
                    checkpoint_id=self.checkpoint_id,
                )
            self.assertEqual(raised.exception.code, 'REPLICA_IDENTITY_MISMATCH')
            self.assertEqual(Path(result['checkpoint_path']).read_bytes(), self.payload)

    def test_complete_legacy_replica_does_not_require_or_create_a_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointReplicaStore(directory)
            result = store.replicate(
                self.remote, worker_id='worker-a', run_id=self.run_id,
                checkpoint_id=self.checkpoint_id,
            )
            transaction = Path(result['manifest_path']).with_name('transaction.json')
            transaction.unlink()
            repeated = store.replicate(
                self.remote, worker_id='worker-a', run_id=self.run_id,
                checkpoint_id=self.checkpoint_id,
            )
            self.assertEqual(repeated['status'], 'present')
            self.assertEqual(store.verified(
                worker_id='worker-a', run_id=self.run_id, checkpoint_id=self.checkpoint_id,
            )['receipt_sha256'], result['receipt_sha256'])
            self.assertFalse(transaction.exists())


if __name__ == '__main__':
    unittest.main()
