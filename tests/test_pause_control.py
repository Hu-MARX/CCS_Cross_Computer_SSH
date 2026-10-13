import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from controller.configuration import spec_digest
from trainctl.checkpoints import CheckpointStore
from trainctl.pause_control import (
    PAUSED_EXIT_CODE, TrainingPaused, checkpoint_and_pause, create_request,
    request_digest, status, validate_ack,
)


class PauseControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).absolute()
        self.run_dir = self.root / 'runs' / ('run_' + 'a' * 32)
        self.run_dir.mkdir(parents=True)
        self.workdir = self.root / 'project'
        self.workdir.mkdir()
        self.declaration = {'schema_version': 1, 'protocol': 'managed-checkpoint-v1', 'contract': 'c' * 64}
        self.spec = {'schema_version': 1, 'name': 'cpu counter fixture',
                     'project': {'local_root': str(self.workdir)},
                     'runtime': {'workdir': str(self.workdir)}, 'command': {'argv': ['python', 'counter.py']},
                     'task': {'kind': 'train', 'pause': self.declaration}}
        self.write(self.run_dir / 'spec.json', self.spec)
        self.write(self.run_dir / 'manifest.json', {'run_id': self.run_dir.name,
                                                  'submission_cwd': str(self.workdir)})
        self.digest = spec_digest(self.spec)
        self.directory = self.workdir / 'orchestrator_runs' / self.run_dir.name / 'checkpoints'
        self.store = CheckpointStore(self.directory, keep=2, min_free_bytes=0)
        self.now = datetime.now(timezone.utc)
        self.saved = []
        self.patch_env = patch.dict(os.environ, {'WORLD_SIZE': '1'})
        self.patch_env.start()

    def tearDown(self):
        self.patch_env.stop()
        self.temp.cleanup()

    @staticmethod
    def write(path, value):
        path.write_text(json.dumps(value), encoding='utf-8')

    def request(self, **changes):
        args = dict(request_id='pause-1', reason='planned maintenance', timeout_seconds=30,
                    spec_digest=self.digest, now=self.now)
        args.update(changes)
        return create_request(self.run_dir, self.declaration, **args)

    def state(self, step=7):
        return {'contract': self.declaration['contract'], 'global_step': step, 'optimizer_steps': step,
                'recovery_fingerprints': {key: hashlib.sha256(key.encode()).hexdigest()
                    for key in ('model', 'optimizer', 'scheduler', 'precision', 'rng', 'sampler', 'loop')},
                'replay': {'safe_to_resume': True}}

    def save(self, request, state=None):
        state = self.state() if state is None else state
        result = self.store.commit(lambda path: self.write(path, state),
                                  lambda path: json.loads(path.read_text(encoding='utf-8')),
                                  metadata={'contract': self.declaration['contract'], 'run_id': self.run_dir.name,
                                            'pinned': True, 'pause_request_sha256': request_digest(request)})
        self.saved.append(result)
        return result

    def pause(self, **changes):
        args = dict(checkpoint_directory=self.directory, spec_digest=self.digest,
                    global_step=7, save_checkpoint=self.save, now=self.now + timedelta(seconds=1))
        args.update(changes)
        return checkpoint_and_pause(self.run_dir, self.declaration, **args)

    def observe(self, **changes):
        args = dict(checkpoint_directory=self.directory, spec_digest=self.digest, now=self.now)
        args.update(changes)
        return status(self.run_dir, self.declaration, **args)

    def acknowledge(self):
        self.request()
        with self.assertRaises(TrainingPaused) as stopped:
            self.pause()
        self.assertEqual(stopped.exception.code, PAUSED_EXIT_CODE)

    def test_request_is_immutable_and_exact_retry_does_not_extend_expiry(self):
        original = self.request()
        self.assertEqual(self.request(now=self.now + timedelta(seconds=10)), original)
        for changes in ({'request_id': 'pause-2'}, {'reason': 'other'}, {'timeout_seconds': 31}):
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, 'PAUSE_REQUEST_CONFLICT'):
                self.request(**changes)
        self.assertEqual(self.observe()['state'], 'requested')

    def test_no_request_and_expired_request_do_not_save_exit_or_change_files(self):
        self.assertFalse(self.pause())
        request = self.request(timeout_seconds=1)
        self.assertFalse(self.pause(now=self.now + timedelta(seconds=2)))
        self.assertEqual(self.saved, [])
        self.assertFalse((self.run_dir / 'pause-ack.json').exists())
        self.assertEqual(self.observe(now=self.now + timedelta(seconds=2))['state'], 'expired')
        self.assertEqual(json.loads((self.run_dir / 'pause-request.json').read_text()), request)

    def test_success_requires_full_checkpoint_then_ack_then_separate_cleanup(self):
        self.acknowledge()
        ack = validate_ack(self.run_dir, self.declaration, checkpoint_directory=self.directory, spec_digest=self.digest)
        self.assertEqual(ack['checkpoint']['global_step'], 7)
        live = self.observe()
        self.assertEqual(live['state'], 'checkpoint_committed')
        self.assertFalse(live['safe_to_resume'])
        stopped = self.observe(quiescent=True)
        self.assertEqual(stopped['state'], 'paused')
        self.assertTrue(stopped['safe_to_resume'])
        self.assertFalse(stopped['verification']['current_bytes_rehashed'])

    def test_polling_does_not_hash_checkpoint_bytes(self):
        self.acknowledge()
        from trainctl.checkpoints import sha256
        calls = []
        def traced(path):
            calls.append(Path(path))
            return sha256(path)
        with patch('trainctl.checkpoints.sha256', side_effect=traced):
            self.assertEqual(self.observe(quiescent=True)['state'], 'paused')
        self.assertTrue(calls)
        self.assertFalse(any(path.suffix == '.ckpt' for path in calls))

    def test_missing_verification_or_changed_file_signature_is_not_resume_ready(self):
        self.acknowledge()
        proof = self.run_dir / 'pause-verified.json'
        original = proof.read_bytes()
        proof.unlink()
        self.assertEqual(self.observe(quiescent=True)['state'], 'checkpoint_unverified')
        proof.write_bytes(original)
        path = self.directory / self.saved[-1]['file']
        path.write_bytes(path.read_bytes())
        self.assertFalse(self.observe(quiescent=True)['safe_to_resume'])

    def test_corruption_is_rejected_by_explicit_byte_verification(self):
        self.acknowledge()
        path = self.directory / self.saved[-1]['file']
        payload = path.read_bytes()
        path.write_bytes(b'!' + payload[1:])
        with self.assertRaisesRegex(ValueError, 'PAUSE_CHECKPOINT_INVALID'):
            validate_ack(self.run_dir, self.declaration, checkpoint_directory=self.directory, spec_digest=self.digest)
        self.assertFalse(self.observe(quiescent=True)['safe_to_resume'])

    def test_wrong_step_incomplete_state_and_unbound_checkpoint_never_ack(self):
        self.request()
        cases = [self.state(step=6), self.state()]
        cases[-1]['replay']['safe_to_resume'] = False
        missing = self.state()
        del missing['recovery_fingerprints']['optimizer']
        cases.append(missing)
        for state in cases:
            with self.subTest(state=state), self.assertRaises(ValueError):
                self.pause(save_checkpoint=lambda request: self.save(request, state))
            self.assertFalse((self.run_dir / 'pause-ack.json').exists())
        old = self.save({**self.request(), 'request_id': 'different'})
        with self.assertRaisesRegex(ValueError, 'PAUSE_CHECKPOINT_MISMATCH'):
            self.pause(save_checkpoint=lambda request: old)

    def test_save_error_keeps_request_and_never_ack_or_exit_as_paused(self):
        self.request()
        def failed(request):
            raise OSError('disk unavailable')
        with self.assertRaisesRegex(OSError, 'disk unavailable'):
            self.pause(save_checkpoint=failed)
        self.assertEqual(self.observe()['state'], 'requested')
        self.assertFalse((self.run_dir / 'pause-ack.json').exists())

    def test_expiry_during_save_preserves_checkpoint_but_does_not_stop_training(self):
        self.request(timeout_seconds=1, now=None)
        def slow_save(request):
            record = self.save(request)
            time.sleep(1.05)
            return record
        self.assertFalse(self.pause(now=None, save_checkpoint=slow_save))
        self.assertEqual(len(self.saved), 1)
        self.assertFalse((self.run_dir / 'pause-ack.json').exists())

    def test_request_and_ack_identity_tampering_is_invalid(self):
        self.acknowledge()
        path = self.run_dir / 'pause-ack.json'
        ack = json.loads(path.read_text())
        for key, value in (('run_id', 'run_' + 'b' * 32), ('request_sha256', 'd' * 64),
                           ('created_at', (self.now + timedelta(seconds=40)).isoformat())):
            changed = copy.deepcopy(ack)
            changed[key] = value
            self.write(path, changed)
            self.assertEqual(self.observe(quiescent=True)['state'], 'invalid')

    def test_spec_and_checkpoint_directory_mismatch_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'PAUSE_SPEC_MISMATCH'):
            self.request(spec_digest='f' * 64)
        self.request()
        with self.assertRaisesRegex(ValueError, 'PAUSE_CHECKPOINT_PATH_MISMATCH'):
            self.pause(checkpoint_directory=self.root / 'other')

    def test_bounded_request_and_json_records(self):
        for changes in ({'timeout_seconds': 3601}, {'timeout_seconds': True}, {'reason': 'x' * 1025},
                        {'request_id': '../escape'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.request(**changes)
        (self.run_dir / 'pause-request.json').write_text(' ' * (64 * 1024 + 1))
        self.assertEqual(self.observe()['state'], 'invalid')

    def test_symlink_request_and_managed_checkpoint_are_rejected(self):
        outside = self.root / 'outside.json'
        self.write(outside, {'preserve': True})
        request_path = self.run_dir / 'pause-request.json'
        try:
            request_path.symlink_to(outside)
        except OSError:
            self.skipTest('symlinks unavailable')
        with self.assertRaisesRegex(ValueError, 'PAUSE_UNSAFE_PATH'):
            self.request()
        self.assertEqual(self.observe()['state'], 'invalid')
        self.assertEqual(json.loads(outside.read_text()), {'preserve': True})
        request_path.unlink()
        self.request()
        def unsafe(request):
            record = self.save(request)
            path = self.directory / record['file']
            path.unlink()
            path.symlink_to(outside)
            return record
        with self.assertRaisesRegex(ValueError, 'PAUSE_UNSAFE_PATH'):
            self.pause(save_checkpoint=unsafe)

    def test_multi_process_training_is_explicitly_not_supported(self):
        self.request()
        with patch.dict(os.environ, {'WORLD_SIZE': '2'}), self.assertRaisesRegex(ValueError, 'PAUSE_UNSUPPORTED_WORLD_SIZE'):
            self.pause()
        self.assertFalse(self.saved)

    def test_new_cpu_fixture_pauses_then_restores_full_state_and_advances_in_new_run(self):
        fixture = Path(__file__).resolve().parents[1] / 'examples' / 'cooperative_pause_smoke.py'
        self.request()
        env = os.environ.copy()
        for key in ('TRAINCTL_TASK_CHECKPOINT', 'TRAINCTL_TASK_CHECKPOINT_SHA256', 'TRAINCTL_TASK_INPUTS_REPORT'):
            env.pop(key, None)
        env.update(TRAINCTL_RUN_DIR=str(self.run_dir), TRAINCTL_RUN_ID=self.run_dir.name,
                   TRAINCTL_SPEC_SHA256=self.digest)
        first = subprocess.run([sys.executable, str(fixture), '--steps', '4', '--delay', '0'],
                               cwd=self.workdir, env=env, capture_output=True, text=True, timeout=15)
        self.assertEqual(first.returncode, 85, first.stdout + first.stderr)
        ack = validate_ack(self.run_dir, self.declaration, checkpoint_directory=self.directory, spec_digest=self.digest)
        self.assertEqual(ack['checkpoint']['global_step'], 1)
        next_dir = self.run_dir.parent / ('run_' + 'b' * 32)
        next_dir.mkdir()
        next_spec = copy.deepcopy(self.spec)
        next_spec['task']['source_run_id'] = self.run_dir.name
        self.write(next_dir / 'spec.json', next_spec)
        self.write(next_dir / 'manifest.json', {'run_id': next_dir.name, 'submission_cwd': str(self.workdir)})
        env.update(TRAINCTL_RUN_DIR=str(next_dir), TRAINCTL_RUN_ID=next_dir.name,
                   TRAINCTL_SPEC_SHA256=spec_digest(next_spec))
        second = subprocess.run([sys.executable, str(fixture), '--steps', '4', '--delay', '0',
                                 '--resume-from-run', self.run_dir.name],
                                cwd=self.workdir, env=env, capture_output=True, text=True, timeout=15)
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        audit = json.loads((next_dir / 'restore-audit.json').read_text())
        self.assertEqual(audit['source']['source_run_id'], self.run_dir.name)
        self.assertEqual(audit['state'], 'advanced')
        self.assertTrue(audit['loaded_state_verified'])
        self.assertTrue(audit['advancement_verified'])
        self.assertEqual(audit['expected']['global_step'], 1)
        self.assertEqual(audit['advanced']['global_step'], 2)
        final = CheckpointStore(self.workdir / 'orchestrator_runs' / next_dir.name / 'checkpoints',
                                min_free_bytes=0).records()[0]
        payload = json.loads((self.workdir / 'orchestrator_runs' / next_dir.name / 'checkpoints' / final['file']).read_text())
        self.assertEqual(payload['global_step'], 4)
        self.assertEqual(payload['optimizer']['accumulator'], 30)
        self.assertEqual(payload['model']['counter'], 10)


if __name__ == '__main__':
    unittest.main()
