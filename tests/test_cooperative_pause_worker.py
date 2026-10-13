"""Worker seam acceptance for opt-in checkpoint-and-pause, without a GPU."""
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch
from threading import Event, Thread

from controller.domain import ExperimentSpec, RunStatus
from trainctl.cli import main
from trainctl.run_store import RunStore, TrainCtlError


CONTRACT = 'a' * 64


class CooperativePauseWorkerTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.store = RunStore(self.root / 'worker')
        self.gpu = patch.object(RunStore, '_gpu_snapshot', return_value=[])
        self.gpu.start()
        self.started = []
        self.supervisors = []
        self.supervisor_runs = {}

    def tearDown(self):
        try:
            for run_id in self.started:
                if not RunStatus(self.store.snapshot(run_id)['status']).terminal:
                    self.store.cancel(run_id, 'local test cleanup')
        finally:
            try:
                # A terminal status is published before the supervisor exits.
                # Join the actual process, not a fixed sleep or ignored cleanup
                # error, before removing runner.log on Windows.
                for process in self.supervisors:
                    process.wait(timeout=20)
            finally:
                self.gpu.stop()
            self.folder.cleanup()

    def submit(self, *, pause=False, code='pass', evaluation=None, start=False):
        task = {'kind': 'train'}
        if pause:
            task['pause'] = {'schema_version': 1, 'protocol': 'managed-checkpoint-v1',
                             'contract': CONTRACT}
        spec = ExperimentSpec.from_dict({
            'schema_version': 1, 'name': 'cooperative-pause-test',
            'runtime': {'workdir': str(self.root)},
            'command': {'argv': [sys.executable, '-c', code]},
            'resources': {'max_duration_seconds': 15}, 'task': task,
            'evaluation': {'argv': [sys.executable, '-c', evaluation]} if evaluation else {},
        })
        if start:
            real_popen = subprocess.Popen
            before = len(self.supervisors)

            def launch(*args, **kwargs):
                process = real_popen(*args, **kwargs)
                self.supervisors.append(process)
                return process
            with patch('trainctl.run_store.subprocess.Popen', side_effect=launch):
                value = self.store.submit(spec, str(len(self.started)))
            self.started.append(value['run_id'])
            self.supervisor_runs[value['run_id']] = self.supervisors[before:]
        else:
            with patch.object(self.store, '_start_runner', return_value=0):
                value = self.store.submit(spec, str(len(list(self.store.runs.iterdir()))))
        return value['run_id']

    def wait(self, run_id):
        for _ in range(150):
            result = self.store.snapshot(run_id)
            if RunStatus(result['status']).terminal:
                for process in self.supervisor_runs.get(run_id, []):
                    process.wait(timeout=20)
                # The public terminal record remains authoritative, but fixture
                # consumers now observe it after all supervisor handles close.
                return self.store.snapshot(run_id)
            time.sleep(0.1)
        self.fail('local supervisor did not finish')

    def test_snapshot_serializes_with_status_publication(self):
        from trainctl.locking import file_lock
        run_id = self.submit()
        run_dir = self.store.runs / run_id
        attempted, completed = Event(), Event()
        outcome = []
        def read_snapshot():
            attempted.set()
            try:
                outcome.append(self.store.snapshot(run_id))
            finally:
                completed.set()
        reader = Thread(target=read_snapshot)
        try:
            with file_lock(run_dir / 'status.lock'):
                reader.start()
                self.assertTrue(attempted.wait(2))
                self.assertFalse(completed.wait(0.2), 'snapshot bypassed a pending status publication')
                status = self.store._read_json(run_dir / 'status.json', {})
                status['status'] = 'RUNNING'
                self.store._atomic_json(run_dir / 'status.json', status)
        finally:
            # The context releases the exact writer lock before joining.
            if reader.ident is not None:
                reader.join(timeout=2)
        self.assertFalse(reader.is_alive())
        self.assertEqual(outcome[0]['status'], 'RUNNING')

    def test_undeclared_pause_is_rejected_without_request_or_cancel(self):
        run_id = self.submit()
        with self.assertRaises(TrainCtlError) as raised:
            self.store.pause(run_id, 'planned maintenance', 'pause-test', 600)
        self.assertEqual(raised.exception.code, 'PAUSE_NOT_SUPPORTED')
        run_dir = self.store.runs / run_id
        self.assertFalse((run_dir / 'pause-request.json').exists())
        self.assertFalse((run_dir / 'cancel.json').exists())
        self.assertEqual(self.store.snapshot(run_id)['status'], 'PREPARING')

    def test_pause_request_is_immutable_and_duplicate_returns_same_identity(self):
        run_id = self.submit(pause=True)
        first = self.store.pause(run_id, 'planned maintenance', 'pause-test', 600)
        run_dir = self.store.runs / run_id
        original = (run_dir / 'pause-request.json').read_bytes()
        duplicate = self.store.pause(run_id, 'planned maintenance', 'pause-test', 600)
        self.assertEqual(first['pause']['request_id'], 'pause-test')
        self.assertEqual(duplicate['pause']['state'], 'requested')
        self.assertEqual((run_dir / 'pause-request.json').read_bytes(), original)
        self.assertFalse((run_dir / 'cancel.json').exists())
        self.assertEqual(duplicate['status'], 'PREPARING')
        with self.assertRaises(TrainCtlError) as raised:
            self.store.pause(run_id, 'different content', 'pause-test', 600)
        self.assertEqual(raised.exception.code, 'PAUSE_REQUEST_CONFLICT')
        self.assertEqual((run_dir / 'pause-request.json').read_bytes(), original)

    def test_cli_pause_reports_requested_without_stopping_training(self):
        run_id = self.submit(pause=True)
        output = io.StringIO()
        with redirect_stdout(output):
            code = main(['--root', str(self.store.root), 'pause', run_id,
                         '--reason', 'planned maintenance', '--request-id', 'cli-pause'])
        reply = json.loads(output.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(reply['data']['pause']['schema_version'], 1)
        self.assertEqual(reply['data']['pause']['request_id'], 'cli-pause')
        self.assertEqual(reply['data']['pause']['state'], 'requested')
        self.assertEqual(reply['data']['status'], 'PREPARING')

    def test_cli_pause_explicit_prepublication_rejection_is_identified(self):
        run_id = self.submit()
        output = io.StringIO()
        with redirect_stdout(output):
            code = main(['--root', str(self.store.root), 'pause', run_id,
                         '--reason', 'planned maintenance', '--request-id', 'rejected-pause'])
        reply = json.loads(output.getvalue())
        self.assertEqual(code, 1)
        self.assertEqual(reply['error']['execution_state'], 'rejected')
        self.assertFalse((self.store.runs / run_id / 'pause-request.json').exists())

    def test_cli_pause_failure_after_publication_keeps_outcome_unknown(self):
        run_id = self.submit(pause=True)
        output = io.StringIO()
        with patch.object(RunStore, 'snapshot', side_effect=OSError('reader unavailable')):
            with redirect_stdout(output):
                code = main(['--root', str(self.store.root), 'pause', run_id,
                             '--reason', 'planned maintenance', '--request-id', 'published-pause'])
        reply = json.loads(output.getvalue())
        self.assertEqual(code, 1)
        self.assertNotIn('execution_state', reply['error'])
        self.assertTrue((self.store.runs / run_id / 'pause-request.json').exists())

    def test_worker_observation_includes_pause_without_changing_lifecycle(self):
        from trainctl.telemetry import Telemetry
        from trainctl.worker import observe
        run_id = self.submit(pause=True)
        self.store.pause(run_id, 'planned maintenance', 'monitor-pause')
        observe(self.store, Telemetry(self.store.root))
        result = self.store.snapshot(run_id)
        self.assertEqual(result['observation']['pause']['state'], 'requested')
        self.assertEqual(result['status'], 'PREPARING')

    def test_invalid_timeout_never_publishes_a_request(self):
        run_id = self.submit(pause=True)
        for timeout in (0, 3601, True, 0.5):
            with self.subTest(timeout=timeout), self.assertRaises(TrainCtlError):
                self.store.pause(run_id, 'planned maintenance', 'invalid-timeout', timeout)
        self.assertFalse((self.store.runs / run_id / 'pause-request.json').exists())

    def test_finished_training_rejects_a_new_pause_identity(self):
        run_id = self.submit(pause=True, start=True)
        result = self.wait(run_id)
        self.assertEqual(result['status'], 'SUCCEEDED', (
            result, self.store.logs(run_id, 'stderr')['text'],
            (self.store.runs / run_id / 'runner.log').read_text(encoding='utf-8')))
        with self.assertRaises(TrainCtlError) as raised:
            self.store.pause(run_id, 'too late', 'late-pause')
        self.assertEqual(raised.exception.code, 'PAUSE_NOT_ACTIVE')
        self.assertFalse((self.store.runs / run_id / 'pause-request.json').exists())

    def test_force_cancel_does_not_masquerade_as_checkpoint_pause(self):
        run_id = self.submit(pause=True, code='import time; time.sleep(30)', start=True)
        self.store.pause(run_id, 'planned maintenance', 'cancel-pending-pause')
        result = self.store.cancel(run_id, 'force stop explicitly requested')
        self.assertEqual((result['status'], result['error']), ('CANCELLED', 'CANCELLED'))
        self.assertFalse(self.store.snapshot(run_id)['pause']['safe_to_resume'])

    @staticmethod
    def trainer(mode='pause'):
        return '''
import json, os, sys, time
from pathlib import Path
from trainctl.checkpoints import CheckpointStore
from trainctl.pause_control import checkpoint_and_pause, request_digest, TrainingPaused
directory = Path(os.environ['TRAINCTL_RUN_DIR'])
declaration = json.loads((directory / 'spec.json').read_text())['task']['pause']
checkpoints = Path(os.environ['TRAINCTL_CHECKPOINT_DIR'])
while not (directory / 'pause-request.json').exists():
    time.sleep(0.02)
storage = CheckpointStore(checkpoints, keep=2, min_free_bytes=0)
def save(request):
    state = {'contract': declaration['contract'], 'global_step': 17, 'optimizer_steps': 17,
             'recovery_fingerprints': {key: '1' * 64 for key in
                 ('model', 'optimizer', 'scheduler', 'precision', 'rng', 'sampler', 'loop')},
             'replay': {'safe_to_resume': True}}
    return storage.commit(lambda path: path.write_text(json.dumps(state)),
                          lambda path: json.loads(path.read_text()), metadata={
        'contract': declaration['contract'], 'run_id': directory.name, 'pinned': True,
        'pause_request_sha256': request_digest(request)})
try:
    checkpoint_and_pause(directory, declaration, checkpoint_directory=checkpoints,
                         spec_digest=os.environ['TRAINCTL_SPEC_SHA256'], global_step=17,
                         save_checkpoint=save)
except TrainingPaused:
    mode = MODE
    if mode == 'crash':
        raise SystemExit(7)
    if mode == 'corrupt':
        ack = json.loads((directory / 'pause-ack.json').read_text())
        path = Path(ack['checkpoint']['path'])
        raw = path.read_bytes()
        path.write_bytes(bytes((raw[0] ^ 1,)) + raw[1:])
    raise
'''.replace('MODE', repr(mode))

    def test_valid_checkpoint_pause_skips_evaluation_and_exports_managed_artifact(self):
        run_id = self.submit(pause=True, code=self.trainer(),
                             evaluation="print('EVALUATION_MUST_NOT_RUN')", start=True)
        self.store.pause(run_id, 'planned maintenance', 'runner-pause')
        result = self.wait(run_id)
        self.assertEqual((result['status'], result['error'], result['exit_code']),
                         ('CANCELLED', 'PAUSED', 85))
        self.assertNotIn('EVALUATION_MUST_NOT_RUN', self.store.logs(run_id)['text'])
        self.assertEqual(result['pause']['state'], 'checkpoint_committed')
        self.assertFalse(result['pause']['safe_to_resume'])
        artifact = self.store.artifacts(run_id, path=result['pause']['checkpoint']['path'])
        self.assertEqual(artifact[0]['sha256'], result['pause']['checkpoint']['sha256'])

    def test_empty_process_group_is_not_complete_run_quiescence(self):
        run_id = self.submit(pause=True, code=self.trainer(), start=True)
        self.store.pause(run_id, 'planned maintenance', 'process-group-only')
        result = self.wait(run_id)
        run_dir = self.store.runs / run_id
        # Boundary fixture: POSIX can prove the launched group empty while an
        # independently detached helper is not covered by that group at all.
        self.store._atomic_json(run_dir / 'runner-cleanup.json', {
            'schema_version': 1, 'run_id': run_id, 'supervisor_pid': result['pid'],
            'scope': 'process_group', 'process_group_quiescent': True,
        })
        observed = self.store.reconcile_execution(run_dir)
        self.assertTrue(observed['process_group_quiescent'])
        self.assertFalse(observed['quiescent'])
        projected = self.store.snapshot(run_id)['pause']
        self.assertEqual(projected['state'], 'checkpoint_committed')
        self.assertFalse(projected['safe_to_resume'])

    def test_exit_85_without_ack_is_failed_not_paused(self):
        run_id = self.submit(pause=True, code='raise SystemExit(85)', start=True)
        result = self.wait(run_id)
        self.assertEqual((result['status'], result['exit_code']), ('FAILED', 85))
        self.assertTrue(result['error'].startswith('PAUSE_ACK_INVALID'))
        self.assertFalse(result['pause']['safe_to_resume'])

    def test_crash_after_valid_ack_is_failed_not_masked_by_checkpoint(self):
        run_id = self.submit(pause=True, code=self.trainer('crash'), start=True)
        self.store.pause(run_id, 'planned maintenance', 'crash-pause')
        result = self.wait(run_id)
        self.assertEqual((result['status'], result['exit_code']), ('FAILED', 7),
                         (result, self.store.logs(run_id, 'stderr')['text']))
        self.assertEqual(result['pause']['state'], 'checkpoint_committed')
        self.assertFalse(result['pause']['safe_to_resume'])

    def test_corrupt_checkpoint_with_ack_and_exit_85_is_failed(self):
        run_id = self.submit(pause=True, code=self.trainer('corrupt'), start=True)
        self.store.pause(run_id, 'planned maintenance', 'corrupt-pause')
        result = self.wait(run_id)
        self.assertEqual(result['status'], 'FAILED')
        self.assertTrue(result['error'].startswith('PAUSE_ACK_INVALID'))
        self.assertFalse(result['pause']['safe_to_resume'])


if __name__ == '__main__':
    unittest.main()
