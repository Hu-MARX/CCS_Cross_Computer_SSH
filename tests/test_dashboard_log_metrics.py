import base64
import json
import unittest
from unittest.mock import patch
import threading
from dashboard import log_metrics


class LogMetricsTests(unittest.TestCase):
    def page(self, raw, offset=0, run='run_test'):
        return dict(run_id=run, stream='stdout', offset=offset,
                    next_offset=offset+len(raw), data_base64=base64.b64encode(raw).decode())

    def test_partial_page_restart_and_idempotence(self):
        launch=json.dumps(dict(event='training_launch',config_sha256='a'*64)).encode()+b'\n'
        event=json.dumps(dict(event='training_progress',step=20,loss=4.2,lr=[0.0001,0.0002],wall_seconds=10)).encode()+b'\n'
        s=log_metrics.consume({},self.page(launch+event[:20]),'run_test','a'*64,100,'2026-09-09T00:00:00Z')
        self.assertEqual(len(s['points']),0)
        s=log_metrics.consume(s,self.page(event[20:],s['offset']),'run_test','a'*64,100,'2026-09-09T00:00:01Z')
        self.assertEqual(s['points'][0]['loss'],4.2)
        self.assertEqual(s['points'][0]['metrics']['lr/max'],0.0002)
        again=log_metrics.consume(s,self.page(b'',s['offset']),'run_test','a'*64,100,'2026-09-09T00:00:02Z')
        self.assertEqual(again['points'],s['points'])

    def test_wrong_run_and_config_fail_closed(self):
        with self.assertRaises(ValueError):
            log_metrics.consume({},self.page(b'',run='other'),'run_test','a'*64,100,'now')
        raw=json.dumps(dict(event='training_launch',config_sha256='b'*64)).encode()+b'\n'
        with self.assertRaises(ValueError):
            log_metrics.consume({},self.page(raw),'run_test','a'*64,100,'now')

    def test_nonfinite_loss_rejected(self):
        raw=b'{"event":"training_progress","step":1,"loss":NaN,"lr":[0.1]}\n'
        with self.assertRaises(ValueError):
            log_metrics.consume({'verified':True},self.page(raw),'run_test','a'*64,100,'now')

    def test_supplement_preserves_lifecycle_and_isolates_runs(self):
        bridge=object.__new__(log_metrics.LogBridge)
        bridge.task={'run_id':'run_test'}
        bridge.config_dir='unused'
        bridge.lock=threading.Lock()
        bridge.state=dict(verified=True,config_sha256='a'*64,points=[
            dict(at='2026-09-09T00:00:00Z',step=20,loss=4.2,metrics={'train/loss':4.2})])
        config=dict(state='available',log_bridge=dict(config_sha256='a'*64,steps_per_epoch=10,epochs=10))
        state=dict(run_id='run_test',latest=dict(status='FAILED',step=None),connection='stale')
        with patch.object(log_metrics,'read_configuration',return_value=config):
            result=bridge.supplement(state)
        self.assertEqual(result['latest']['status'],'FAILED')
        self.assertEqual(result['connection'],'stale')
        self.assertEqual(result['latest']['loss'],4.2)
        self.assertEqual(result['latest']['epoch'],2)
        self.assertEqual(bridge.supplement({'run_id':'other'}),{'run_id':'other'})
