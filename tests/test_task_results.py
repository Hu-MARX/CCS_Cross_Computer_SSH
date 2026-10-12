import copy
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from controller.cli import main
from controller.domain import DomainError, ExperimentSpec, Run
from controller.repository import RunRepository
from controller.results import cached_result, descriptor, metric_values, task_result
from dashboard.catalog import Catalog
from dashboard.feed import Feed, now
from examples.mock_workflow import MockRemote, make_spec, run_demo
from tests.test_dashboard import RUN, ReadOnlyRemote, sample


class TaskResultTests(unittest.TestCase):
    def test_metric_cache_budget_and_very_large_integers(self):
        values = metric_values({'oversized': 10**1000, **{str(i) + '中' * 150: i for i in range(128)}})
        self.assertNotIn('oversized', values)
        self.assertLessEqual(len(json.dumps(values, ensure_ascii=True)), 3072)

    def test_metadata_roundtrip_and_legacy_wire_unchanged(self):
        spec = make_spec('analysis')
        self.assertEqual(ExperimentSpec.from_dict(spec.to_dict()), spec)
        raw = spec.to_dict()
        del raw['task']
        self.assertNotIn('task', ExperimentSpec.from_dict(raw).to_dict())
        self.assertEqual(descriptor(RUN, raw)['kind'], 'unspecified')
        for task in ({'kind': 'oops'}, {'kind': 'test', 'source_run_id': '../escape'},
                     {'kind': 'test', 'checkpoint': {'path': 'best.pt'}},
                     {'kind': 'test', 'inputs': [{'path': 'test', 'sha256': 'wrong'}]}):
            with self.assertRaises(DomainError):
                ExperimentSpec.from_dict({**raw, 'task': task})

    def test_generic_compute_task_kinds_and_resources_reach_catalog(self):
        for kind in ('build', 'simulate', 'benchmark', 'data'):
            spec = make_spec('analysis').to_dict()
            spec['task'] = {'kind': kind}
            spec['resources'] = {'gpu_index': 0}
            item = descriptor(RUN, ExperimentSpec.from_dict(spec).to_dict(), 'worker')
            self.assertEqual(item['kind'], kind)
            self.assertEqual(item['gpu_index'], 0)

    def test_generic_metrics_progress_and_no_scientific_conclusion(self):
        with tempfile.TemporaryDirectory() as folder:
            remote = ReadOnlyRemote()
            remote.rows[0]['payload']['progress'].update(phase='test', metrics={
                'test/mAP': .4, 'progress/completed': 8, 'progress/total': 20,
                'bad': float('nan'), 'flag': True, 'nested': {'a': 1}})
            feed = Feed(remote, RUN, Path(folder) / (RUN + '.json'), name='test', epochs=None,
                        task=descriptor(RUN, make_spec('test').to_dict(), 'worker'))
            feed.poll()
            result = feed.view()['result']
            self.assertEqual(result['metrics']['test/mAP']['value'], .4)
            self.assertEqual(result['progress']['completed'], 8)
            self.assertEqual(result['progress']['total'], 20)
            self.assertNotIn('bad', result['metrics'])
            self.assertNotIn('flag', result['metrics'])
            self.assertIsNone(result['scientific_conclusion'])
            self.assertEqual(cached_result(folder, RUN), task_result(feed.state))
            with self.assertRaises(ValueError):
                cached_result(folder, '../escape')

    def test_stale_success_cannot_be_fresh_on_cli_export(self):
        with tempfile.TemporaryDirectory() as folder:
            remote = ReadOnlyRemote()
            remote.rows = [sample(1, 'SUCCEEDED')]
            feed = Feed(remote, RUN, Path(folder) / 'cache.json', name='test', epochs=None)
            feed.poll()
            state = copy.deepcopy(feed.state)
            state['last_success']['samples'] = '2000-01-01T00:00:00Z'
            self.assertEqual(task_result(state)['observation']['state'], 'stale')
            self.assertEqual(task_result(state)['execution']['status'], 'SUCCEEDED')

    def test_catalog_readonly_bounded_and_worker_filtered(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / 'state.db'
            self.assertEqual(RunRepository.catalog(db, 'worker'), [])
            self.assertFalse(db.exists())
            repo = RunRepository(db)
            repo.create_run(Run.new(make_spec('train'), worker='other'))
            run = repo.create_run(Run.new(make_spec('test'), worker='worker'))
            before = db.read_bytes()
            self.assertEqual(RunRepository.catalog(db, 'worker', 1)[0]['run_id'], run.id)
            self.assertEqual(before, db.read_bytes())

    def test_cli_result_never_constructs_remote_or_writes_db(self):
        with tempfile.TemporaryDirectory() as folder:
            remote = ReadOnlyRemote()
            feed = Feed(remote, RUN, Path(folder) / (RUN + '.json'), name='demo', epochs=None,
                        task=descriptor(RUN, make_spec('analysis').to_dict(), 'worker'))
            feed.poll()
            with patch('controller.cli.OpenSSHRunner', side_effect=AssertionError('No network')):
                output = io.StringIO()
                with redirect_stdout(output):
                    code = main(['--host', 'worker', '--db', str(Path(folder)/'absent.db'),
                                 'result', RUN, '--cache-directory', folder])
                self.assertEqual(code, 0)
                self.assertEqual(json.loads(output.getvalue())['data']['task']['kind'], 'analysis')
                self.assertFalse((Path(folder)/'absent.db').exists())

    def test_mock_success_test_result_restart_and_duplicate_submission(self):
        with tempfile.TemporaryDirectory() as folder:
            first = run_demo(folder)
            self.assertEqual(first['test']['execution']['status'], 'SUCCEEDED')
            self.assertEqual(first['test']['metrics']['test/mAP']['value'], .42)
            self.assertEqual(first['test']['task']['source_run_id'], first['train']['task']['run_id'])
            self.assertEqual(first['test']['artifacts']['state'], 'collected')
            second = run_demo(folder)
            self.assertEqual(second['remote_run_count'], 2)
            self.assertEqual(first['test']['task'], second['test']['task'])
            self.assertEqual(first['test']['observation']['cursor'], second['test']['observation']['cursor'])

    def test_mock_failure_does_not_start_test(self):
        with tempfile.TemporaryDirectory() as folder:
            result = run_demo(folder, 'failure')
            self.assertEqual(result['remote_run_count'], 1)
            self.assertEqual(result['train']['execution']['status'], 'FAILED')
            self.assertIsNone(result['test'])

    def test_test_failure_retains_training_success_without_fabricating_metrics(self):
        with tempfile.TemporaryDirectory() as folder:
            result = run_demo(folder, 'test_failure')
            self.assertEqual(result['train']['execution']['status'], 'SUCCEEDED')
            self.assertEqual(result['test']['execution']['status'], 'FAILED')
            self.assertEqual(result['test']['execution']['exit_code'], 1)
            self.assertNotIn('test/mAP', result['test']['metrics'])
            self.assertEqual(run_demo(folder, 'test_failure')['remote_run_count'], 2)

    def test_mock_disconnect_defers_test_until_reconnection(self):
        with tempfile.TemporaryDirectory() as folder:
            first = run_demo(folder, 'disconnect')
            self.assertEqual(first['remote_run_count'], 1)
            self.assertEqual(first['train']['observation']['state'], 'stale')
            self.assertIsNone(first['test'])
            self.assertEqual(run_demo(folder)['remote_run_count'], 2)

    def test_catalog_views_do_not_call_remote_and_are_bounded(self):
        with tempfile.TemporaryDirectory() as folder:
            remote = ReadOnlyRemote()
            primary = descriptor(RUN, make_spec('train').to_dict())
            catalog = Catalog(remote, primary, folder)
            catalog.poll()
            for _ in range(10):
                catalog.view()
            self.assertEqual(len(remote.calls), 3)
            with self.assertRaises(KeyError):
                catalog.view('not-registered')

    def test_new_metadata_requires_worker_flag(self):
        from controller.capabilities import worker_interface
        from controller.remote import OpenSSHRunner
        from types import SimpleNamespace
        calls = []
        def command(argv, **kwargs):
            calls.append(argv[-1])
            if ' health ' in f' {argv[-1]} ':
                return SimpleNamespace(returncode=0, stdout=json.dumps({'ok': True, 'data': {
                    'ccs_interface': worker_interface(),
                }}), stderr='')
            return SimpleNamespace(returncode=0, stdout=json.dumps({'ok': True, 'data': {'run_id': RUN}}))
        remote = OpenSSHRunner('worker', command_runner=command)
        remote.submit(make_spec('test'), 'test-key')
        self.assertTrue(any('--require-task-metadata' in call for call in calls))

    def test_multirun_http_result_auth_identity_and_no_remote_reads(self):
        import threading
        from http.client import HTTPConnection
        from dashboard.server import server
        with tempfile.TemporaryDirectory() as folder:
            report = run_demo(folder)
            remote = MockRemote(folder)
            train = report['train']['task']
            catalog = Catalog(remote, train, Path(folder)/'cache', discover=lambda:
                              RunRepository.catalog(Path(folder)/'controller.db', 'simulation'))
            catalog.poll()
            http = server(catalog, 0, 'secret')
            thread = threading.Thread(target=http.serve_forever, daemon=True)
            thread.start()
            def request(path, auth=True):
                client = HTTPConnection('127.0.0.1', http.server_port, timeout=3)
                try:
                    client.request('GET', path, headers={'Authorization': 'Bearer secret'} if auth else {})
                    response = client.getresponse()
                    return response.status, json.loads(response.read())
                finally:
                    client.close()
            try:
                with patch.object(remote, 'samples', side_effect=AssertionError('HTTP must not query worker')):
                    code, result = request('/api/result?run_id=' + report['test']['task']['run_id'])
                    self.assertEqual(code, 200)
                    self.assertEqual(result['task']['kind'], 'test')
                    self.assertEqual(result['metrics']['test/mAP']['value'], .42)
                    self.assertEqual(request('/api/result', False)[0], 401)
                    self.assertEqual(request('/api/state?run_id=unknown')[0], 404)
                    self.assertEqual(request('/api/state?run_id=a&run_id=b')[0], 400)
                    self.assertEqual(request('/api/result?path=../../secret')[0], 400)
                    self.assertEqual(len(request('/api/state')[1]['catalog']), 2)
            finally:
                http.shutdown()
                http.server_close()
                thread.join(timeout=2)

    def test_two_runs_share_worker_queries(self):
        with tempfile.TemporaryDirectory() as folder:
            remote = ReadOnlyRemote()
            other = 'run_' + 'b'*32
            primary = descriptor(RUN, make_spec('train').to_dict())
            secondary = descriptor(other, make_spec('analysis').to_dict())
            original = remote.samples
            def samples(run_id, *args):
                rows = original(run_id, *args)
                for row in rows:
                    row['run_id'] = run_id
                return rows
            remote.samples = samples
            catalog = Catalog(remote, primary, folder, discover=lambda: [secondary])
            catalog.poll()
            self.assertEqual([c[0] for c in remote.calls].count('monitor'), 1)
            self.assertEqual([c[0] for c in remote.calls].count('alerts'), 1)
            self.assertEqual([c[0] for c in remote.calls].count('samples'), 2)
            self.assertEqual(catalog.view(other)['result']['task']['kind'], 'analysis')

    def test_catalog_projects_concurrent_summary_and_sanitized_workloads(self):
        with tempfile.TemporaryDirectory() as folder:
            remote = ReadOnlyRemote()
            remote.processes = lambda: {'state': 'available', 'observed_at': now(), 'items': [
                {'pid': 321, 'launcher': 'python', 'script': 'manual.py', 'project': 'embodied',
                 'state': 'R', 'memory_bytes': 1024, 'managed': False}]}
            primary = descriptor(RUN, make_spec('train').to_dict(), recorded_status='RUNNING')
            catalog = Catalog(remote, primary, folder)
            catalog.poll()
            view = catalog.view()
            self.assertEqual(view['catalog'][0]['last_status'], 'RUNNING')
            self.assertEqual(view['catalog'][0]['progress']['epoch'], 0.0)
            self.assertEqual(view['workloads']['items'][0]['script'], 'manual.py')
            self.assertNotIn('argv', view['workloads']['items'][0])
