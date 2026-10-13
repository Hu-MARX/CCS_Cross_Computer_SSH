"""Read-only recovery facts across the real log bridge supplement seam."""
import base64
import hashlib
import json
import threading
import unittest
from unittest.mock import patch

from dashboard import log_metrics
from dashboard.fullfit_metrics import report_snapshot


class CheckpointProjectionTests(unittest.TestCase):
    def fullfit_binding(self):
        return dict(format='fullfit_v1', digest_key='training_signature', config_sha256='a'*64,
                    max_steps=11, epochs=3, epoch_batch_counts=[3, 4, 4],
                    worker_id='worker-test', execution_key='resume-v1',
                    snapshot=dict(project='/project', path='training/resume/progress.json'))

    def fullfit_report(self):
        event = dict(event='training_progress', step=7, epoch=2, epoch_batch=4,
                     epoch_batches=4, total_steps=11, loss=2.5, lr=.0001, optimizer_updates=6)
        return dict(training_signature='a'*64, contract=dict(execution_key='resume-v1'),
                    total_batches=11, batches_per_epoch=[3, 4, 4], global_batches=7,
                    optimizer_updates=6, completed_epochs=2, last_progress=event,
                    checkpoint=dict(path='/project/training/resume/checkpoints/batch-0000005.pth',
                                    sha256='b'*64, global_batches=5, training_signature='a'*64))

    def bridge(self, state):
        bridge = object.__new__(log_metrics.LogBridge)
        bridge.task = dict(run_id='run_test', worker='worker-test')
        bridge.config_dir = 'unused'
        bridge.lock = threading.Lock()
        bridge.state = state
        return bridge

    def supplement(self, state, latest=None, binding=None):
        binding = dict(binding or self.fullfit_binding())
        binding.setdefault('worker_id', 'worker-test')
        config = dict(state='available', log_bridge=binding)
        result = dict(run_id='run_test', latest=latest or dict(status='FAILED'), connection='stale')
        with patch.object(log_metrics, 'read_configuration', return_value=config):
            return self.bridge(state).supplement(result)

    def test_reported_receipt_is_not_a_verified_restore(self):
        state = dict(config_sha256='a'*64,
                     snapshot=report_snapshot(self.fullfit_report(), self.fullfit_binding(), 'now'))
        result = self.supplement(state)
        recovery = result['recovery']
        self.assertEqual(recovery['checkpoint_step'], 5)
        self.assertEqual(recovery['progress_step'], 7)
        self.assertIsNone(recovery['logged_step'])
        self.assertEqual(recovery['progress_source'], 'bound trainer progress report')
        self.assertEqual(recovery['replay_steps'], 2)
        self.assertEqual(recovery['resume_capability'], 'reported_only')
        self.assertIsNone(recovery['safe_to_resume'])
        self.assertEqual(result['latest']['status'], 'FAILED')
        self.assertEqual(result['connection'], 'stale')

    def test_native_metrics_do_not_hide_bound_checkpoint_receipt(self):
        state = dict(config_sha256='a'*64,
                     snapshot=report_snapshot(self.fullfit_report(), self.fullfit_binding(), 'now'))
        result = self.supplement(state, dict(status='RUNNING', step=9, loss=1.2,
                                             checkpoint=dict(state=None, step=None)))
        self.assertEqual(result['latest']['step'], 9)
        self.assertEqual(result['latest']['loss'], 1.2)
        self.assertEqual(result['latest']['checkpoint']['step'], 5)
        # The bridge's batch step may differ from a native framework's global step.
        self.assertEqual(result['recovery']['progress_step'], 7)
        self.assertEqual(result['recovery']['replay_steps'], 2)

    def test_log_progress_without_receipt_does_not_invent_a_checkpoint(self):
        state = dict(verified=True, config_sha256='a'*64, points=[
            dict(at='now', step=20, loss=1.2, metrics={'train/loss': 1.2})])
        result = self.supplement(state, binding=dict(config_sha256='a'*64, max_steps=100))
        self.assertEqual(result['recovery']['logged_step'], 20)
        self.assertIsNone(result['recovery']['checkpoint_step'])
        self.assertIsNone(result['recovery']['replay_steps'])
        self.assertEqual(result['recovery']['resume_capability'], 'unknown')

    def test_native_verified_checkpoint_wins_without_mixing_step_units(self):
        state = dict(config_sha256='a'*64,
                     snapshot=report_snapshot(self.fullfit_report(), self.fullfit_binding(), 'now'))
        result = self.supplement(state, dict(status='RUNNING', step=9, checkpoint=dict(
            state='committed', step=8, id='native-checkpoint', safe_to_resume=True)))
        self.assertEqual(result['latest']['checkpoint']['id'], 'native-checkpoint')
        self.assertEqual(result['recovery']['resume_capability'], 'verified')
        self.assertEqual(result['recovery']['progress_step'], 9)
        self.assertEqual(result['recovery']['replay_steps'], 1)
        self.assertEqual(result['recovery']['replay_basis'], 'native_step_minus_native_checkpoint')

    def test_checkpoint_ahead_of_progress_is_unknown_not_negative_replay(self):
        result = self.supplement({}, dict(status='RUNNING', step=3, checkpoint=dict(
            state='committed', step=8, safe_to_resume=True)))
        self.assertIsNone(result['recovery']['replay_steps'])
        self.assertIn('ahead', result['recovery']['reason'])

    def test_foreign_bridge_config_never_contributes_recovery(self):
        state = dict(config_sha256='b'*64,
                     snapshot=report_snapshot(self.fullfit_report(), self.fullfit_binding(), 'now'))
        result = self.supplement(state)
        self.assertIsNone(result['recovery']['checkpoint_step'])
        self.assertEqual(result['recovery']['resume_capability'], 'unknown')

    def test_snapshot_from_another_output_binding_is_not_reused(self):
        binding = self.fullfit_binding()
        state = dict(config_sha256='a'*64,
                     snapshot=report_snapshot(self.fullfit_report(), binding, 'now'))
        binding['snapshot']['path'] = 'training/another/progress.json'
        result = self.supplement(state, binding=binding)
        self.assertIsNone(result['recovery']['checkpoint_step'])

    def test_native_explicit_failure_is_not_overwritten_by_trainer_receipt(self):
        state = dict(config_sha256='a'*64,
                     snapshot=report_snapshot(self.fullfit_report(), self.fullfit_binding(), 'now'))
        result = self.supplement(state, dict(status='FAILED', step=9, checkpoint=dict(
            state='failed', step=8, safe_to_resume=False)))
        self.assertEqual(result['latest']['checkpoint']['state'], 'failed')
        self.assertFalse(result['recovery']['safe_to_resume'])
        self.assertEqual(result['recovery']['resume_capability'], 'not_resumable')

    def test_declared_log_cadence_changes_collection_freshness_not_lifecycle(self):
        state = dict(verified=True, config_sha256='a'*64, points=[
            dict(at='2026-10-13T00:00:00Z', step=20, loss=1.2, metrics={'train/loss': 1.2})])
        binding = dict(config_sha256='a'*64, max_steps=100, expected_metric_interval_seconds=120)
        with patch.object(log_metrics, 'age', return_value=106):
            result = self.supplement(state, binding=binding)
        self.assertTrue(result['log_metrics']['fresh'])
        self.assertEqual(result['log_metrics']['freshness_threshold_seconds'], 270)
        self.assertEqual(result['latest']['status'], 'FAILED')
        self.assertEqual(result['connection'], 'stale')
        self.assertNotIn('advancing', result['latest'])
        with patch.object(log_metrics, 'age', return_value=106):
            binding.pop('expected_metric_interval_seconds')
            result = self.supplement(state, binding=binding)
        self.assertFalse(result['log_metrics']['fresh'])

    def test_bad_cadence_and_unknown_timestamp_do_not_claim_freshness(self):
        for value in (True, 0, -1, float('nan'), float('inf'), '120', 86401):
            with self.subTest(value=value), self.assertRaises(ValueError):
                log_metrics.metric_freshness_seconds(dict(expected_metric_interval_seconds=value))
        state = dict(verified=True, config_sha256='a'*64, points=[
            dict(at='not-a-timestamp', step=20, loss=1.2, metrics={'train/loss': 1.2})])
        result = self.supplement(state, binding=dict(config_sha256='a'*64, max_steps=100))
        self.assertFalse(result['log_metrics']['fresh'])


class OrdinarySnapshotTests(unittest.TestCase):
    def binding(self):
        return dict(format='training_progress_v1', config_sha256='a'*64,
                    report_digest_key='effective_config_sha256', max_steps=100,
                    snapshot=dict(project='/project', path='training/current/progress.json',
                                  identity=dict(experiment_id='experiment-test')),
                    checkpoint_step_key='optimizer_updates',
                    checkpoint_signature=dict(key='scientific_signature', sha256='c'*64))

    def report(self):
        return dict(effective_config_sha256='a'*64, scientific_signature='c'*64,
                    experiment_id='experiment-test', total_steps=100, global_batches=25,
                    optimizer_updates=25, last_progress=dict(event='training_progress',
                        step=25, loss=1.2, lr=[.0001], optimizer_updates=25),
                    checkpoint=dict(path='/project/training/current/checkpoints/batch-0000020.pth',
                        sha256='b'*64, bytes=5000, optimizer_updates=20,
                        scientific_signature='c'*64, committed_optimizer_boundary=True))

    def test_plain_progress_report_is_bound_and_checkpoint_remains_reported(self):
        from dashboard.checkpoint_projection import training_snapshot
        snapshot = training_snapshot(self.report(), self.binding(), 'now')
        self.assertEqual(snapshot['point']['step'], 25)
        self.assertEqual(snapshot['checkpoint']['step'], 20)
        self.assertIsNone(snapshot['checkpoint']['safe_to_resume'])
        self.assertEqual(snapshot['checkpoint']['step_unit'], 'optimizer_updates')

    def test_report_rejects_foreign_identity_config_and_uncommitted_checkpoint(self):
        from dashboard.checkpoint_projection import training_snapshot
        for field in ('identity', 'config', 'signature', 'commit', 'future', 'path', 'bool', 'digest'):
            report = self.report()
            if field == 'identity': report['experiment_id'] = 'foreign'
            if field == 'config': report['effective_config_sha256'] = 'f'*64
            if field == 'signature': report['checkpoint']['scientific_signature'] = 'f'*64
            if field == 'commit': report['checkpoint']['committed_optimizer_boundary'] = False
            if field == 'future': report['checkpoint']['optimizer_updates'] = 30
            if field == 'path': report['checkpoint']['path'] = '/project/foreign/batch-0000020.pth'
            if field == 'bool': report['checkpoint']['optimizer_updates'] = True
            if field == 'digest': report['checkpoint']['sha256'] = 'invalid'
            with self.subTest(field=field), self.assertRaises(ValueError):
                training_snapshot(report, self.binding(), 'now')

    def test_checkpoint_log_requires_launch_and_explicit_snapshot_binding(self):
        # Existing trainers do not emit this event. A future adapter may emit the
        # same bound receipt, but a filename/step or an unbound event is not enough.
        binding = self.binding()
        event = dict(event='checkpoint_committed', config_sha256='a'*64,
                     experiment_id='experiment-test', **self.report()['checkpoint'])
        raw = (json.dumps(dict(event='training_launch', config_sha256='a'*64))+'\n'+
               json.dumps(dict(event='training_progress', step=25, loss=1.2, lr=[.0001], optimizer_updates=25))+'\n'+
               json.dumps(event)+'\n').encode()
        page = dict(run_id='run_test', stream='stdout', offset=0, next_offset=len(raw),
                    data_base64=base64.b64encode(raw).decode())
        state = log_metrics.consume({}, page, 'run_test', 'a'*64, 100, 'now', binding)
        self.assertEqual(state['checkpoint']['step'], 20)
        self.assertIsNone(state['checkpoint']['safe_to_resume'])
        unbound = log_metrics.consume({}, page, 'run_test', 'a'*64, 100, 'now')
        self.assertNotIn('checkpoint', unbound)

    def test_checkpoint_event_cannot_mix_optimizer_and_microbatch_units(self):
        binding = self.binding()
        event = dict(event='checkpoint_committed', config_sha256='a'*64,
                     experiment_id='experiment-test', **self.report()['checkpoint'])
        raw = (json.dumps(dict(event='training_launch', config_sha256='a'*64))+'\n'+
               json.dumps(dict(event='training_progress', step=25, loss=1.2, lr=[.0001], optimizer_updates=5))+'\n'+
               json.dumps(event)+'\n').encode()
        page = dict(run_id='run_test', stream='stdout', offset=0, next_offset=len(raw),
                    data_base64=base64.b64encode(raw).decode())
        with self.assertRaisesRegex(ValueError, 'same-unit'):
            log_metrics.consume({}, page, 'run_test', 'a'*64, 100, 'now', binding)

    def test_declared_snapshot_uses_existing_read_seam_and_no_checkpoint_load(self):
        binding = self.binding()
        binding['worker_id'] = 'worker-test'
        config = dict(state='available', log_bridge=binding)
        bridge = CheckpointProjectionTests().bridge({})
        bridge.cache = 'unused'
        report_content = json.dumps(self.report())
        calls = []
        def query(action, payload):
            calls.append((action, payload))
            if action == 'workspace.execute':
                return dict(content=report_content, sha256=hashlib.sha256(report_content.encode()).hexdigest(),
                            project=binding['snapshot']['project'], path=binding['snapshot']['path'])
            self.assertEqual(action, 'run.logs')
            raw = (json.dumps(dict(event='training_launch', config_sha256='a'*64))+'\n'+
                   json.dumps(dict(event='training_progress', step=20, loss=1.3, lr=[.0001]))+'\n').encode()
            return dict(run_id='run_test', stream='stdout', offset=0, next_offset=len(raw),
                        data_base64=base64.b64encode(raw).decode())
        bridge.query = query
        with patch.object(log_metrics, 'read_configuration', return_value=config), \
                patch.object(log_metrics, 'atomic_json'), patch.object(log_metrics, 'now', return_value='now'):
            bridge.poll()
            result = bridge.supplement(dict(run_id='run_test', latest=dict(status='RUNNING')))
        self.assertEqual({call[0] for call in calls}, {'workspace.execute', 'run.logs'})
        self.assertEqual(len(calls), 2)
        request = next(call[1]['request'] for call in calls if call[0] == 'workspace.execute')
        self.assertEqual(request['action'], 'inspect')
        self.assertEqual(request['mode'], 'read')
        self.assertEqual(result['recovery']['logged_step'], 20)
        self.assertEqual(result['recovery']['progress_step'], 25)
        self.assertEqual(result['recovery']['checkpoint_step'], 20)
        self.assertEqual(result['recovery']['replay_steps'], 5)

    def test_receipt_only_json_poll_never_accepts_readback_digest_mismatch(self):
        binding = self.binding()
        binding['worker_id'] = 'worker-test'
        config = dict(state='available', log_bridge=binding)
        bridge = CheckpointProjectionTests().bridge({})
        bridge.cache = 'unused'
        def query(action, payload):
            if action == 'workspace.execute':
                return dict(content=json.dumps(self.report()), sha256='f'*64,
                            project=binding['snapshot']['project'], path=binding['snapshot']['path'])
            return dict(run_id='run_test', stream='stdout', offset=0, next_offset=0, data_base64='')
        bridge.query = query
        with patch.object(log_metrics, 'read_configuration', return_value=config), \
                patch.object(log_metrics, 'atomic_json'):
            bridge.poll()
        self.assertNotIn('snapshot', bridge.state)
        self.assertIn('readback mismatch', bridge.state['error'])


if __name__ == '__main__':
    unittest.main()
