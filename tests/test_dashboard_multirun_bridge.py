import tempfile
import unittest
from dashboard.catalog import Catalog


class Remote:
    def monitor(self): return {}
    def alerts(self): return []
    def samples(self, *args): return []


class Bridge:
    def __init__(self, task): self.task, self.polls = task, 0
    def poll_safe(self): self.polls += 1
    def supplement(self, result):
        result['bridge_worker'] = self.task['worker']
        result['bridge_polls'] = self.polls
        return result


class MultiRunBridgeTests(unittest.TestCase):
    def test_background_primary_is_not_polled_twice_by_catalog(self):
        primary=dict(run_id='run_live',name='live',worker='ubuntu-second',recorded_status='RUNNING')
        with tempfile.TemporaryDirectory() as folder:
            c=Catalog(Remote(),primary,folder,bridge_factory=Bridge)
            c.bridges['run_live'].background_polling=True
            c.poll()
            self.assertEqual(c.bridges['run_live'].polls,0)

    def test_only_one_history_bridge_per_tick(self):
        primary=dict(run_id='run_live',name='live',worker='ubuntu-second',recorded_status='RUNNING')
        history=[dict(run_id=f'run_history{i}',name='history',worker='ubuntu-second',recorded_status='SUCCEEDED') for i in range(10)]
        with tempfile.TemporaryDirectory() as folder:
            c=Catalog(Remote(),primary,folder,discover=lambda:history,bridge_factory=Bridge)
            c.poll()
            self.assertEqual(c.bridges['run_live'].polls,1)
            self.assertEqual(sum(c.bridges[x['run_id']].polls for x in history),1)

    def test_newly_discovered_run_gets_own_bridge_and_views_do_not_poll(self):
        old=dict(run_id='run_old',name='old',worker='lab118',recorded_status='RUNNING')
        new=dict(run_id='run_new',name='new',worker='ubuntu-second',recorded_status='RUNNING')
        entries=[]
        with tempfile.TemporaryDirectory() as folder:
            c=Catalog(Remote(),old,folder,discover=lambda:entries,bridge_factory=Bridge)
            c.poll()
            entries.append(new)
            c.poll()
            self.assertEqual(c.view('run_new')['bridge_worker'],'ubuntu-second')
            self.assertEqual(c.view('run_old')['bridge_worker'],'lab118')
            self.assertEqual(c.view('run_new')['bridge_polls'],1)
            self.assertEqual(c.view('run_new')['bridge_polls'],1)
            entries.clear()
            c.poll()
            self.assertNotIn('run_new',c.bridges)

    def test_bridge_poll_error_is_contained(self):
        from dashboard.log_metrics import LogBridge
        import threading
        from unittest.mock import Mock
        bridge=object.__new__(LogBridge)
        bridge.lock=threading.Lock(); bridge.state={}
        bridge.poll=Mock(side_effect=ValueError('disconnected'))
        bridge.poll_safe()
        self.assertEqual(bridge.state['error'],'disconnected')
