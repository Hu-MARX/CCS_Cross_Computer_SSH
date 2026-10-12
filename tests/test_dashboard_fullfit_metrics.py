import base64
import json
import unittest
import threading
from unittest.mock import patch
from dashboard.log_metrics import consume


class FullFitMetricsTests(unittest.TestCase):
    def test_live_snapshot_supplements_before_history_without_changing_lifecycle(self):
        from dashboard.log_metrics import LogBridge
        from dashboard.fullfit_metrics import report_snapshot
        bridge = object.__new__(LogBridge)
        bridge.task = {'run_id': 'run_test'}
        bridge.config_dir = 'unused'
        bridge.lock = threading.Lock()
        bridge.state = dict(config_sha256='a'*64, snapshot=report_snapshot(self.report(), self.binding(), 'now'))
        config = dict(state='available', log_bridge=self.binding())
        with patch('dashboard.log_metrics.read_configuration', return_value=config):
            result = bridge.supplement(dict(run_id='run_test', latest=dict(status='FAILED'), connection='stale'))
        self.assertEqual(result['latest']['status'], 'FAILED')
        self.assertEqual(result['connection'], 'stale')
        self.assertEqual(result['epochs'], 3)
        self.assertEqual(result['latest']['updates'], 6)
        self.assertEqual(result['latest']['step'], 7)
        self.assertEqual(result['latest']['checkpoint']['state'], 'reported_committed')
        self.assertEqual(result['log_metrics']['epoch_basis'], 'reported_fullfit')

    def binding(self):
        return dict(format='fullfit_v1', digest_key='training_signature', config_sha256='a'*64,
                    max_steps=11, epochs=3, epoch_batch_counts=[3, 4, 4],
                    worker_id='ubuntu-second', execution_key='resume-v1',
                    snapshot=dict(project='/project', path='training/resume/progress.json'))

    def event(self, **changes):
        event = dict(event='training_progress', step=7, epoch=2, epoch_batch=4,
                     epoch_batches=4, total_steps=11, loss=2.5, lr=0.0001,
                     optimizer_updates=6, wall_seconds=30)
        event.update(changes)
        return event

    def page(self, events):
        raw = ''.join(json.dumps(e)+'\n' for e in events).encode()
        return dict(run_id='run_test', stream='stdout', offset=0, next_offset=len(raw),
                    data_base64=base64.b64encode(raw).decode())

    def test_scalar_lr_resume_and_variable_epoch(self):
        page = self.page([dict(event='training_launch', training_signature='a'*64,
                              total_batches=11, epochs=3), self.event()])
        state = consume({}, page, 'run_test', 'a'*64, 11, 'now', binding=self.binding())
        p = state['points'][0]
        self.assertEqual((p['epoch'], p['updates']), (1, 6))
        self.assertEqual(p['metrics']['train/completed_epochs'], 2)
        self.assertEqual(p['metrics']['lr/max'], 0.0001)

    def test_signature_and_epoch_mismatch_rejected(self):
        for launch, event in [('b'*64, self.event()), ('a'*64, self.event(epoch=3)),
                              ('a'*64, self.event(epoch_batch=3))]:
            with self.subTest(launch=launch, event=event), self.assertRaises(ValueError):
                consume({}, self.page([dict(event='training_launch', training_signature=launch,
                    total_batches=11, epochs=3), event]), 'run_test', 'a'*64, 11, 'now', binding=self.binding())

    def report(self):
        return dict(training_signature='a'*64, contract=dict(execution_key='resume-v1'),
                    total_batches=11, batches_per_epoch=[3,4,4], global_batches=7,
                    optimizer_updates=6, completed_epochs=2, last_progress=self.event(),
                    checkpoint=dict(path='/project/training/resume/checkpoints/batch-0000007.pth',
                        sha256='b'*64, global_batches=7, training_signature='a'*64))

    def test_snapshot_binding_and_reported_checkpoint(self):
        from dashboard.fullfit_metrics import report_snapshot
        result = report_snapshot(self.report(), self.binding(), 'now')
        self.assertEqual(result['point']['step'], 7)
        self.assertEqual(result['checkpoint']['state'], 'reported_committed')
        self.assertIsNone(result['checkpoint']['safe_to_resume'])
        self.assertIsNone(result['checkpoint']['created_at'])

    def test_snapshot_rejects_foreign_run_and_future_checkpoint(self):
        from dashboard.fullfit_metrics import report_snapshot
        for field in ['identity', 'path', 'step']:
            report = self.report()
            if field == 'identity': report['contract']['execution_key'] = 'another-run'
            if field == 'path': report['checkpoint']['path'] = '/project/other/checkpoint.pth'
            if field == 'step': report['checkpoint']['global_batches'] = 8
            with self.subTest(field=field), self.assertRaises(ValueError):
                report_snapshot(report, self.binding(), 'now')

    def test_joint_loss_parts_include_zero_and_ignore_unbound_names(self):
        from dashboard.fullfit_metrics import progress_point
        point = progress_point(self.event(loss_parts=dict(vehicle=1.5, drone=1.0,
            protection=0.0, arbitrary_field=999)), self.binding(), 'now')
        self.assertEqual(point['metrics']['train/loss_vehicle'], 1.5)
        self.assertEqual(point['metrics']['train/loss_drone'], 1.0)
        self.assertEqual(point['metrics']['train/loss_protection'], 0.0)
        self.assertNotIn('train/loss_arbitrary_field', point['metrics'])

    def test_joint_loss_parts_reject_invalid_shapes_and_values(self):
        from dashboard.fullfit_metrics import progress_point
        for parts in [[], None, {'vehicle': float('nan')}, {'drone': True},
                      {'protection': -0.1}, {'vehicle': '1.0'}, {'drone': float('inf')},
                      {'cooperative': float('nan')}]:
            with self.subTest(parts=parts), self.assertRaises(ValueError):
                progress_point(self.event(loss_parts=parts), self.binding(), 'now')

    def test_joint_loss_parts_survive_both_snapshot_and_log_collection(self):
        from dashboard.fullfit_metrics import report_snapshot
        report = self.report()
        report['last_progress']['loss_parts'] = dict(vehicle=1.5, drone=1., protection=0.)
        observed = report_snapshot(report, self.binding(), 'now')
        events = [dict(event='training_launch', training_signature='a'*64,
                       total_batches=11, epochs=3), report['last_progress']]
        state = consume({}, self.page(events), 'run_test', 'a'*64, 11, 'now', binding=self.binding())
        self.assertEqual(observed['point']['metrics'], state['points'][0]['metrics'])

    def test_staged_cooperative_loss_and_protection_counts_survive_both_sources(self):
        from dashboard.fullfit_metrics import report_snapshot
        report = self.report()
        report['last_progress'].update(loss_parts=dict(cooperative=2.25, protection=.25),
                                       protection_eligible=7, protection_harmed=3)
        observed = report_snapshot(report, self.binding(), 'now')
        events = [dict(event='training_launch', training_signature='a'*64,
                       total_batches=11, epochs=3), report['last_progress']]
        state = consume({}, self.page(events), 'run_test', 'a'*64, 11, 'now', binding=self.binding())
        metrics = observed['point']['metrics']
        self.assertEqual(metrics, state['points'][0]['metrics'])
        self.assertEqual(metrics['train/loss_cooperative'], 2.25)
        self.assertEqual(metrics['train/loss_protection'], .25)
        self.assertEqual(metrics['train/protection_eligible'], 7)
        self.assertEqual(metrics['train/protection_harmed'], 3)
        self.assertNotIn('train/loss_vehicle', metrics)
        self.assertNotIn('train/loss_drone', metrics)

    def test_protection_counts_keep_zero_and_ignore_invalid_optional_values(self):
        from dashboard.fullfit_metrics import progress_point
        for value in [0, -1, float('nan'), float('inf'), True, '1']:
            with self.subTest(value=value):
                point = progress_point(self.event(protection_eligible=value,
                    protection_harmed=value), self.binding(), 'now')
                for field in ['protection_eligible', 'protection_harmed']:
                    if type(value) is int and value == 0:
                        self.assertEqual(point['metrics']['train/' + field], 0)
                    else:
                        self.assertNotIn('train/' + field, point['metrics'])
