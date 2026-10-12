import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from controller.domain import ExperimentSpec, RunStatus, utc_now
from trainctl.progress import ProgressReporter
from trainctl.run_store import RunStore
from trainctl.telemetry import Telemetry
from trainctl.worker import observe
from trainctl.systemd_backend import unit_name, is_quiescent


class WorkerMonitorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = RunStore(self.directory.name)
        self.telemetry = Telemetry(self.directory.name)
        with patch.object(self.store, '_start_runner', return_value=0), patch.object(self.store, '_gpu_snapshot', return_value=[]):
            result = self.store.submit(ExperimentSpec.from_dict({'schema_version': 1, 'name': 'observed',
                'command': {'argv': ['unused']}}), 'test')
        self.run = result['run_id']
        self.run_dir = self.store.runs / self.run
        self.store._set_status(self.run_dir, RunStatus.RUNNING)

    def tearDown(self):
        self.directory.cleanup()

    def observe(self):
        with patch.object(self.store, '_gpu_snapshot', return_value=[]):
            observe(self.store, self.telemetry)

    def test_monitor_restart_preserves_history_and_progress(self):
        ProgressReporter(self.run_dir).emit('train', step=3, force=True)
        self.observe()
        samples = self.telemetry.samples(self.run)
        self.assertEqual(samples[0]['payload']['progress']['step'], 3)
        self.telemetry = Telemetry(self.directory.name)
        ProgressReporter(self.run_dir).emit('train', step=4, force=True)
        self.observe()
        newer = self.telemetry.samples(self.run, samples[-1]['id'])
        self.assertEqual(len(newer), 1)
        self.assertEqual(newer[0]['payload']['progress']['step'], 4)

    def test_observation_failure_does_not_fail_run(self):
        with patch.object(self.store, 'reconcile_execution', side_effect=RuntimeError('bus unavailable')):
            self.observe()
        self.assertEqual(self.store._read_json(self.run_dir / 'status.json', {})['status'], 'RUNNING')
        self.assertEqual(self.telemetry.alerts()[0]['kind'], 'observation_failed')
        self.observe()
        self.assertIsNotNone(self.telemetry.alerts()[0]['resolved_at'])

    def test_stall_alert_is_deduplicated_and_recovers(self):
        ProgressReporter(self.run_dir).emit('train', step=1, force=True)
        self.observe()
        observation = self.store._read_json(self.run_dir / 'observation.json', {})
        observation['last_progress_at'] = '2000-01-01T00:00:00Z'
        self.store._atomic_json(self.run_dir / 'observation.json', observation)
        self.observe()
        self.observe()
        alerts = [item for item in self.telemetry.alerts() if item['kind'] == 'suspected_stall']
        self.assertEqual(len(alerts), 1)
        self.assertIsNone(alerts[0]['resolved_at'])
        ProgressReporter(self.run_dir).emit('train', step=2, force=True)
        self.observe()
        self.assertIsNotNone(self.telemetry.alerts()[0]['resolved_at'])

    def test_previous_boot_is_interrupted_not_restarted(self):
        manifest = self.store._read_json(self.run_dir / 'manifest.json', {})
        manifest['execution'] = {'backend': 'systemd', 'unit': unit_name(self.store.root, self.run), 'boot_id': 'old'}
        self.store._atomic_json(self.run_dir / 'manifest.json', manifest)
        with patch('trainctl.systemd_backend.boot_id', return_value='new'):
            state = self.store.reconcile_execution(self.run_dir)
        self.assertTrue(state['quiescent'])
        self.assertEqual(self.store._read_json(self.run_dir / 'status.json', {})['error'], 'WORKER_REBOOTED')

    def test_gpu_does_not_reuse_lease_before_cgroup_is_empty(self):
        directory = self.store.root / 'resource-locks'
        directory.mkdir()
        self.store._atomic_json(directory / 'gpu-0.lease.json', {'run_id': self.run})
        manifest = self.store._read_json(self.run_dir / 'manifest.json', {})
        manifest['execution'] = {'backend': 'systemd'}
        self.store._atomic_json(self.run_dir / 'manifest.json', manifest)
        with patch.object(self.store, 'reconcile_execution', return_value={'quiescent': False}):
            self.assertFalse(self.store.gpu_available(0, self.store.runs / ('run_' + 'f'*32)))

    def test_terminal_event_is_available_before_terminal_observation(self):
        self.store.finish(self.run_dir, RunStatus.SUCCEEDED, exit_code=0)
        self.assertEqual(self.store.events(self.run)[-1]['kind'], 'finished')
        self.observe()
        count = len(self.telemetry.samples(self.run))
        self.observe()
        self.assertEqual(len(self.telemetry.samples(self.run)), count)

    def test_cgroup_active_is_not_quiescent(self):
        self.assertFalse(is_quiescent({'ActiveState': 'deactivating'}))
        self.assertFalse(is_quiescent({'ActiveState': 'active'}))

    def test_progress_nan_rejected(self):
        with self.assertRaises(ValueError):
            ProgressReporter(self.run_dir).emit('train', metrics={'loss': float('nan')}, force=True)

    def test_monitor_staleness_does_not_change_lifecycle(self):
        self.store._atomic_json(self.store.root / 'worker-health.json', {'observed_at': '2000-01-01T00:00:00Z'})
        self.assertEqual(self.store.monitor()['observation_state'], 'stale')
        self.assertEqual(self.store._read_json(self.run_dir / 'status.json', {})['status'], 'RUNNING')

    def test_checkpoint_failure_and_overdue_are_observed_without_stopping_run(self):
        self.store._atomic_json(self.run_dir / 'checkpoint-status.json', {
            'state': 'failed', 'updated_at': '2000-01-01T00:00:00Z', 'interval_seconds': 1800,
            'error': 'disk full', 'latest': None})
        self.observe()
        kinds = {alert['kind'] for alert in self.telemetry.alerts() if alert['resolved_at'] is None}
        self.assertIn('checkpoint_failed', kinds)
        self.assertIn('checkpoint_overdue', kinds)
        self.assertEqual(self.store._read_json(self.run_dir / 'status.json', {})['status'], 'RUNNING')
        from controller.domain import utc_now
        self.store._atomic_json(self.run_dir / 'checkpoint-status.json', {
            'state': 'committed', 'updated_at': utc_now(), 'latest': {'created_at': utc_now()}})
        self.observe()
        self.assertTrue(all(a['resolved_at'] for a in self.telemetry.alerts() if a['kind'].startswith('checkpoint_')))

    def test_alert_outbox_only_records_transitions(self):
        for _ in range(3):
            self.telemetry.alert(self.run, 'test', 'now', {})
        self.telemetry.resolve(self.run, 'test', 'later')
        self.telemetry.resolve(self.run, 'test', 'later')
        with self.telemetry.db() as db:
            transitions = [row['transition'] for row in db.execute('SELECT transition FROM notification_outbox')]
        self.assertEqual(transitions, ['opened', 'resolved'])

    def test_evidence_monitor_stale_then_recovers_without_stopping_training(self):
        self.store._atomic_json(self.run_dir / 'evidence-runtime.json', {
            'state': 'observing', 'updated_at': '2000-01-01T00:00:00Z', 'drift_detected': False})
        self.observe()
        self.assertTrue(any(a['kind'] == 'evidence_monitor_failed' and not a['resolved_at']
                            for a in self.telemetry.alerts()))
        from controller.domain import utc_now
        self.store._atomic_json(self.run_dir / 'evidence-runtime.json', {
            'state': 'observing', 'updated_at': utc_now(), 'drift_detected': False})
        self.observe()
        self.assertTrue(all(a['resolved_at'] for a in self.telemetry.alerts()
                            if a['kind'] == 'evidence_monitor_failed'))
        self.assertEqual(self.store.snapshot(self.run)['status'], 'RUNNING')

    def test_evidence_v2_separates_deployment_change_and_observer_warning(self):
        report = {
            'schema_version': 2, 'state': 'observing', 'updated_at': utc_now(),
            'execution_integrity': {'state': 'no_violation_observed', 'issue_count': 0, 'issues': []},
            'deployment': {'state': 'changed', 'issue_count': 1, 'issues': [{'path': '/app/code.py'}]},
            'observer': {'state': 'observing', 'health': 'degraded', 'coverage': 'partial',
                         'warning_count': 1, 'warnings': [{'code': 'verification_race'}]},
            'drift_detected': False,
        }
        self.store._atomic_json(self.run_dir / 'evidence-runtime.json', report)
        self.observe()
        active = {item['kind'] for item in self.telemetry.alerts() if item['resolved_at'] is None}
        self.assertIn('evidence_deployment_changed', active)
        self.assertIn('evidence_observer_warning', active)
        self.assertNotIn('evidence_execution_integrity', active)
        self.assertNotIn('evidence_drift', active)
        self.assertEqual(self.store.snapshot(self.run)['status'], 'RUNNING')
