"""Local collector preferences never issue remote mutations."""
import json
import tempfile
import threading
import unittest
from http.client import HTTPConnection
from pathlib import Path
from unittest.mock import patch

from dashboard.catalog import Catalog
from dashboard.server import server


class Remote:
    def __init__(self):
        self.calls = []

    def monitor(self):
        self.calls.append('monitor')
        return {}

    def alerts(self):
        self.calls.append('alerts')
        return []

    def samples(self, *args):
        self.calls.append(args)
        return []


class Bridge:
    def __init__(self, task):
        self.task, self.polls = task, 0

    def poll_safe(self):
        self.polls += 1

    def supplement(self, result):
        return result


class DashboardFocusTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.old = dict(run_id='run_old', name='old', worker='first', recorded_status='SUCCEEDED')
        self.live = dict(run_id='run_live', name='live', worker='second', recorded_status='RUNNING')
        self.remotes = {'first': Remote(), 'second': Remote()}
        self.catalog = Catalog(self.remotes['first'], self.old, self.folder.name,
            discover=lambda: [self.live], remote_resolver=lambda task: self.remotes[task['worker']],
            bridge_factory=Bridge)
        self.catalog.poll()
        for remote in self.remotes.values():
            remote.calls.clear()

    def tearDown(self):
        self.folder.cleanup()

    def test_local_http_selection_changes_collector_not_worker_or_run(self):
        http = server(self.catalog, 0, 'private-test-token')
        worker = threading.Thread(target=http.serve_forever, daemon=True)
        worker.start()
        def request(path, method='GET', body=None, extra=None):
            client = HTTPConnection('127.0.0.1', http.server_port, timeout=2)
            try:
                client.request(method, path, body=body, headers={
                    'Authorization': 'Bearer private-test-token',
                    'Origin': f'http://127.0.0.1:{http.server_port}',
                    'Content-Type': 'application/json', **(extra or {})})
                result = client.getresponse()
                return result.status, json.loads(result.read())
            finally:
                client.close()
        try:
            body = json.dumps({'run_id': 'run_live'})
            status, focus = request('/api/focus', 'POST', body)
            self.assertEqual(status, 200)
            self.assertEqual(focus['run_id'], 'run_live')
            self.assertEqual(focus['worker'], 'second')
            self.assertEqual(request('/api/state?run_id=run_old')[1]['collector_focus']['run_id'], 'run_live')
            self.assertEqual(request('/api/focus', 'POST', body,
                {'Sec-Fetch-Site': 'cross-site'})[0], 403)
            self.assertEqual(request('/api/focus', 'POST', body,
                {'Authorization': 'Bearer wrong'})[0], 401)
            self.assertEqual(request('/api/focus', 'POST', '{"run_id":"unknown"}')[0], 404)
            self.assertEqual(request('/api/focus', 'POST', '{"run_id":"run_old","worker":"second"}')[0], 400)
            self.assertEqual(request('/api/cancel', 'POST', '{}')[0], 405)
            self.assertEqual(sum(len(item.calls) for item in self.remotes.values()), 0)
        finally:
            http.shutdown()
            http.server_close()
            worker.join(2)

    def test_focused_native_and_logs_continue_while_history_is_blocked(self):
        entered, release, complete = threading.Event(), threading.Event(), threading.Event()
        self.catalog.enable_focus_collectors()
        self.catalog.set_focus('run_live')
        with patch.object(self.remotes['first'], 'monitor',
                          side_effect=lambda: (entered.set(), release.wait(2), {})[-1]):
            slow = threading.Thread(target=self.catalog.poll)
            slow.start()
            try:
                self.assertTrue(entered.wait(1))
                def focused():
                    self.catalog.poll_focused()
                    self.catalog.poll_focused(logs=True)
                    complete.set()
                quick = threading.Thread(target=focused)
                quick.start()
                self.assertTrue(complete.wait(1), 'History must not block focused collection')
                self.assertIn(('run_live', 0, 200), self.remotes['second'].calls)
                self.assertEqual(self.catalog.bridges['run_live'].polls, 2)
                self.assertEqual(self.catalog.view('run_live')['task']['worker'], 'second')
            finally:
                release.set()
                slow.join(3)
                if 'quick' in locals():
                    quick.join(3)

    def test_last_explicit_selection_wins_reads_do_not_reclaim_it(self):
        self.catalog.set_focus('run_live')
        self.catalog.set_focus('run_old')
        self.assertEqual(self.catalog.view('run_live')['collector_focus']['run_id'], 'run_old')
        self.assertEqual(self.catalog.view()['collector_focus']['run_id'], 'run_old')

    def test_automatic_follow_cannot_replace_new_explicit_focus(self):
        self.catalog.set_focus('run_live')
        focus = self.catalog.set_focus('run_old', expected_run_id='run_old')
        self.assertEqual(focus['run_id'], 'run_live')
        focus = self.catalog.set_focus('run_old', expected_run_id='run_live')
        self.assertEqual(focus['run_id'], 'run_old')

    def test_shared_worker_tick_cache_does_not_serialize_focused_monitor(self):
        entered, release, complete = threading.Event(), threading.Event(), threading.Event()
        remote = self.remotes['first']
        same_worker = {**self.live, 'worker': 'first'}
        catalog = Catalog(remote, self.old, Path(self.folder.name) / 'shared',
                          discover=lambda: [same_worker], bridge_factory=Bridge)
        catalog.poll()
        catalog.enable_focus_collectors()
        catalog.set_focus('run_live')
        def monitor():
            if threading.current_thread().name == 'history-fixture':
                entered.set()
                release.wait(2)
            return {}
        with patch.object(remote, 'monitor', side_effect=monitor):
            slow = threading.Thread(target=catalog.poll, name='history-fixture')
            quick = threading.Thread(target=lambda: (catalog.poll_focused(), complete.set()))
            slow.start()
            try:
                self.assertTrue(entered.wait(1))
                quick.start()
                self.assertTrue(complete.wait(1))
            finally:
                release.set()
                slow.join(3)
                if quick.ident:
                    quick.join(3)

    def test_catalog_does_not_prune_in_flight_focus_writer(self):
        entered, release = threading.Event(), threading.Event()
        self.catalog.enable_focus_collectors()
        self.catalog.set_focus('run_live')
        original = self.catalog.feeds['run_live']
        def samples(*args):
            entered.set()
            release.wait(2)
            return []
        with patch.object(self.remotes['second'], 'samples', side_effect=samples):
            read = threading.Thread(target=self.catalog.poll_focused)
            read.start()
            try:
                self.assertTrue(entered.wait(1))
                self.catalog.set_focus('run_old')
                self.catalog.discover = lambda: []
                self.catalog.poll()
                self.assertIs(self.catalog.feeds['run_live'], original)
            finally:
                release.set()
                read.join(3)
        self.catalog.poll()
        self.assertNotIn('run_live', self.catalog.feeds)
        with self.assertRaises(KeyError):
            self.catalog.set_focus('missing')
        self.assertEqual(self.catalog.view()['collector_focus']['run_id'], 'run_old')

    def test_runtime_source_change_is_visible_without_disclosing_paths_or_token(self):
        from dashboard.server import RuntimeIdentity
        root = Path(self.folder.name) / 'sources'
        root.mkdir()
        source = root / 'server.py'
        source.write_text('old', encoding='utf-8')
        runtime = RuntimeIdentity(root, version='fixture-version', interval=0)
        self.assertFalse(runtime.view()['restart_required'])
        source.write_text('new', encoding='utf-8')
        runtime.poll()
        view = runtime.view()
        self.assertEqual(view['loaded_version'], 'fixture-version')
        self.assertTrue(view['restart_required'])
        self.assertNotEqual(view['loaded_source_sha256'], view['disk_source_sha256'])
        self.assertNotIn(self.folder.name, json.dumps(view))
        self.assertNotIn('token', json.dumps(view))

    def test_runtime_identity_does_not_read_private_or_runtime_data(self):
        from dashboard.server import RuntimeIdentity
        root = Path(self.folder.name) / 'sources'
        root.mkdir()
        (root / 'server.py').write_text('software', encoding='utf-8')
        for name in ('private', 'data', 'runtime', 'config', '__pycache__'):
            folder = root / name
            folder.mkdir()
            (folder / 'secret.py').write_text('private-runtime-fixture', encoding='utf-8')
        runtime = RuntimeIdentity(root, interval=0)
        (root / 'private' / 'secret.py').write_text('changed', encoding='utf-8')
        runtime.poll()
        self.assertFalse(runtime.view()['restart_required'])


if __name__ == '__main__':
    unittest.main()
