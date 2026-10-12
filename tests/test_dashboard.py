import copy
from http.client import HTTPConnection
import json
import tempfile
import threading
import unittest
from pathlib import Path

from dashboard.feed import Feed, now
from dashboard.catalog import Catalog
from dashboard.server import server, private_token

RUN = 'run_' + 'a' * 32


def sample(index, status='RUNNING', at=None):
    return {'id': index, 'run_id': RUN, 'created_at': at or now(), 'payload': {
        'lifecycle': status, 'observation_state': 'fresh', 'progress_state': 'advancing',
        'progress': {'phase': 'train', 'step': index, 'metrics': {'epoch': 0,
            'optimizer_updates_this_process': index, 'train/loss_step': 10/index}},
        'checkpoint': {'state': 'committed', 'latest': {'state': {'global_step': index,
            'replay': {'safe_to_resume': False}}}}, 'evidence_runtime': {'state': 'observing', 'drift_detected': False}}}


class ReadOnlyRemote:
    """No mutation methods exist; every query and cursor is audited by tests."""
    def __init__(self):
        self.rows = [sample(1)]
        self.fail = False
        self.calls = []
        self.alert_rows = [
            {'key': 'own', 'run_id': RUN, 'kind': '<script>bad()</script>', 'payload': {}},
            {'key': 'other', 'run_id': 'run_other', 'kind': 'private', 'payload': {}},
        ]

    def monitor(self):
        self.calls.append(('monitor',))
        if self.fail:
            raise ConnectionError('SSH offline')
        return {'observed_at': now(), 'observation_state': 'fresh', 'health': {'gpu': []}}

    def samples(self, run_id, after_id, limit):
        self.calls.append(('samples', run_id, after_id, limit))
        if self.fail:
            raise ConnectionError('SSH offline')
        return [copy.deepcopy(row) for row in self.rows if row['id'] > after_id][:limit]

    def alerts(self):
        self.calls.append(('alerts',))
        if self.fail:
            raise ConnectionError('SSH offline')
        return copy.deepcopy(self.alert_rows)


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.remote = ReadOnlyRemote()
        self.cache = Path(self.directory.name) / 'cache.json'
        self.feed = Feed(self.remote, RUN, self.cache, name='50 epochs', epochs=50)

    def tearDown(self):
        self.directory.cleanup()

    def test_read_only_queries_and_views_never_poll(self):
        self.feed.poll()
        self.assertEqual([call[0] for call in self.remote.calls], ['monitor', 'samples', 'alerts'])
        for _ in range(20):
            result = self.feed.view()
        self.assertEqual(len(self.remote.calls), 3)
        self.assertEqual(result['connection'], 'fresh')
        self.assertEqual(result['epochs'], 50)
        self.assertEqual(len(result['alerts']), 1)
        self.assertFalse(result['latest']['checkpoint']['safe_to_resume'])

    def test_legacy_snapshot_read_race_is_not_presented_as_execution_drift(self):
        self.remote.alert_rows = [{
            'key': 'legacy', 'run_id': RUN, 'kind': 'evidence_drift',
            'opened_at': now(), 'resolved_at': None,
            'payload': {
                'evidence': {'execution_mode': 'snapshot'},
                'issues': [
                    {'kind': 'source', 'path': '/srv/app/code.py',
                     'error': 'Evidence byte budget exceeded'},
                ],
            },
        }]
        self.feed.poll()
        alert = self.feed.view()['alerts'][0]
        self.assertEqual(alert['evidence_scope'], 'observer')
        self.assertEqual(alert['compatibility'], 'legacy_snapshot_verification_race')
        self.assertEqual(alert['error'], 'Evidence byte budget exceeded')
        self.assertNotIn('/srv/app/code.py', json.dumps(alert))

    def test_evidence_v2_status_survives_the_bounded_dashboard_projection(self):
        self.remote.rows[0]['payload']['evidence_runtime'] = {
            'schema_version': 2, 'state': 'observing', 'updated_at': now(),
            'execution_integrity': {'state': 'no_violation_observed', 'issue_count': 0},
            'deployment': {'state': 'changed', 'issue_count': 4},
            'observer': {'state': 'observing', 'health': 'degraded', 'coverage': 'partial',
                         'warning_count': 2},
            'rotations_completed': 3, 'bytes_read': 4096,
        }
        self.feed.poll()
        evidence = self.feed.view()['latest']['evidence']
        self.assertEqual(evidence['schema_version'], 2)
        self.assertEqual(evidence['execution_integrity']['state'], 'no_violation_observed')
        self.assertEqual(evidence['deployment']['state'], 'changed')
        self.assertEqual(evidence['observer']['health'], 'degraded')
        self.assertNotIn('issues', json.dumps(evidence))

    def test_disconnect_preserves_running_state_cursor_and_history(self):
        self.feed.poll()
        self.remote.fail = True
        self.feed.poll()
        result = self.feed.view()
        self.assertEqual(result['connection'], 'stale')
        self.assertEqual(result['latest']['status'], 'RUNNING')
        self.assertEqual(result['cursor'], 1)
        self.assertEqual(len(result['points']), 1)
        self.assertIn('samples', result['errors'])

    def test_restart_reuses_cursor_and_recovers_terminal_state(self):
        self.feed.poll()
        resumed = Feed(self.remote, RUN, self.cache, name='50 epochs', epochs=50)
        self.assertEqual(resumed.view()['connection'], 'stale')
        self.remote.rows.append(sample(2, 'FAILED'))
        resumed.poll()
        self.assertIn(('samples', RUN, 1, 200), self.remote.calls)
        self.assertEqual(resumed.view()['latest']['status'], 'FAILED')
        self.assertEqual(len(resumed.view()['points']), 2)
        resumed.poll()
        self.assertEqual(len(resumed.view()['points']), 2)

    def test_old_active_sample_stale_but_old_terminal_is_not_running(self):
        self.remote.rows = [sample(1, at='2000-01-01T00:00:00Z')]
        self.feed.poll()
        self.assertEqual(self.feed.view()['connection'], 'stale')
        self.remote.rows.append(sample(2, 'SUCCEEDED', '2000-01-01T00:00:00Z'))
        self.feed.poll()
        self.assertEqual(self.feed.view()['connection'], 'fresh')
        self.assertEqual(self.feed.view()['latest']['status'], 'SUCCEEDED')

    def test_history_bounded_and_backlog_explicit(self):
        feed = Feed(self.remote, RUN, self.cache, name='long', epochs=50, max_points=100)
        self.remote.rows = [sample(i) for i in range(1, 502)]
        feed.poll()
        self.assertTrue(feed.view()['catching_up'])
        self.assertEqual(feed.view()['cursor'], 200)
        feed.poll()
        feed.poll()
        view = feed.view()
        self.assertFalse(view['catching_up'])
        self.assertTrue(view['downsampled'])
        self.assertLessEqual(len(view['points']), 100)
        self.assertEqual(view['points'][0]['step'], 1)
        self.assertEqual(view['points'][-1]['step'], 501)

    def test_malformed_page_does_not_advance_cursor(self):
        self.feed.poll()
        state = copy.deepcopy(self.feed.state)
        with self.assertRaises(ValueError):
            self.feed._samples(state, [sample(2), sample(2)])
        self.assertEqual(state['cursor'], 1)
        self.assertEqual(len(state['points']), 1)

    def test_nan_becomes_missing_not_fabricated_zero(self):
        self.remote.rows[0]['payload']['progress']['metrics']['train/loss_step'] = float('nan')
        self.feed.poll()
        self.assertIsNone(self.feed.view()['latest']['loss'])
        json.dumps(self.feed.view(), allow_nan=False)

    def test_private_link_stable_for_same_run_and_rotates_for_other_run(self):
        runtime = Path(self.directory.name) / 'server.json'
        secret = 'x' * 43
        runtime.write_text(json.dumps({'run_id': RUN, 'url': 'http://127.0.0.1:8765/#' + secret}))
        self.assertEqual(private_token(runtime, RUN, 8765), secret)
        self.assertNotEqual(private_token(runtime, 'run_' + 'b' * 32, 8765), secret)
        self.assertNotEqual(private_token(runtime, RUN, 8766), secret)
        runtime.write_text('not-json')
        self.assertGreaterEqual(len(private_token(runtime, RUN, 8765)), 32)

    def test_http_token_origin_and_read_only_routes(self):
        self.feed.poll()
        http = server(self.feed, 0, 'secret-token')
        thread = threading.Thread(target=http.serve_forever, daemon=True)
        thread.start()
        def request(path, method='GET', headers=None):
            client = HTTPConnection('127.0.0.1', http.server_port, timeout=3)
            try:
                client.request(method, path, headers=headers or {})
                response = client.getresponse()
                return response.status, dict(response.getheaders()), response.read()
            finally:
                client.close()
        try:
            self.assertEqual(http.server_address[0], '127.0.0.1')
            self.assertEqual(request('/')[0], 200)
            self.assertEqual(request('/', headers={'Sec-Fetch-Site': 'cross-site'})[0], 200)
            self.assertEqual(request('/api/state')[0], 401)
            headers = {'Authorization': 'Bearer secret-token'}
            code, response_headers, body = request('/api/state', headers=headers)
            self.assertEqual(code, 200)
            self.assertEqual(json.loads(body)['run_id'], RUN)
            self.assertNotIn('Access-Control-Allow-Origin', response_headers)
            self.assertIn("frame-ancestors 'none'", response_headers['Content-Security-Policy'])
            self.assertEqual(request('/api/state', headers={**headers, 'Host': 'evil.example'})[0], 403)
            self.assertEqual(request('/api/state', headers={**headers, 'Origin': 'https://evil.example'})[0], 403)
            self.assertEqual(request('/api/state', headers={**headers, 'Sec-Fetch-Site': 'cross-site'})[0], 403)
            self.assertEqual(request('/api/cancel', 'POST', headers)[0], 405)
            self.assertEqual(request('/../../data/dashboard/server.json')[0], 404)
            self.assertEqual(len(self.remote.calls), 3)
        finally:
            http.shutdown()
            http.server_close()
            thread.join(timeout=2)

    def test_catalog_exposes_bounded_read_only_workflow_state(self):
        workflow = {
            'schema_version': 1,
            'studies': [{
                'schema_version': 1,
                'study': {'id': 'study_' + 'b' * 32, 'name': 'CVFDETR study',
                          'research_question': 'Does it improve?', 'status': 'BLOCKED',
                          'plan_sha256': 'c' * 64, 'created_at': now(), 'updated_at': now()},
                'stages': [{'id': 'test', 'name': 'Held-out test', 'kind': 'test',
                            'status': 'BLOCKED', 'block_reason': 'approval_required',
                            'requires_approval': True, 'approved': False, 'attempt': 0,
                            'run_id': None, 'artifact_bindings': [],
                            'artifact_bindings_sha256': None, 'updated_at': now()}],
                'recommendations': [],
            }],
        }
        catalog = Catalog(self.remote, {'run_id': RUN, 'name': '50 epochs', 'kind': 'train'},
                          Path(self.directory.name) / 'catalog', discover=lambda: [],
                          workflows=lambda: workflow, interval=30)

        catalog.poll()
        view = catalog.view()

        self.assertEqual(view['workflows']['state'], 'available')
        self.assertEqual(view['workflows']['studies'][0]['study']['status'], 'BLOCKED')
        self.assertEqual(view['workflows']['studies'][0]['stages'][0]['block_reason'], 'approval_required')

    def test_catalog_exposes_bounded_worker_compatibility(self):
        original_monitor = self.remote.monitor
        def monitor():
            report = original_monitor()
            report['health']['ccs_interface'] = {'capabilities': {f'private-{index}': 1 for index in range(500)}}
            return report

        def compatibility(required=None):
            self.remote.calls.append(('compatibility', required))
            return {
                'state': 'compatible', 'compatible': True,
                'reason': 'worker interface is compatible',
                'protocol': {'name': 'ccs-worker', 'major': 1, 'minor': 0},
                'software': {'name': 'cross-computer-ssh', 'version': '0.1.0'},
                'available_capabilities': {f'private-{index}': 1 for index in range(500)},
                'missing_capabilities': [],
            }

        self.remote.monitor = monitor
        self.remote.compatibility = compatibility
        catalog = Catalog(self.remote, {'run_id': RUN, 'name': '50 epochs', 'kind': 'train'},
                          Path(self.directory.name) / 'catalog', discover=lambda: [], interval=30)
        catalog.poll()
        report = catalog.view()['worker_compatibility']

        self.assertEqual(report['state'], 'compatible')
        self.assertEqual(report['protocol']['major'], 1)
        self.assertEqual(report['software']['version'], '0.1.0')
        self.assertNotIn('available_capabilities', report)
        self.assertNotIn('ccs_interface', catalog.view()['monitor']['health'])
        self.assertEqual([call[0] for call in self.remote.calls].count('compatibility'), 1)
        catalog.poll()
        self.assertEqual([call[0] for call in self.remote.calls].count('compatibility'), 1)

    def test_catalog_exposes_bounded_fleet_facts_without_selection(self):
        fleet = {
            'schema_version': 1,
            'selection_policy': 'upper_agent_explicit',
            'workers': [{
                'worker_id': 'ubuntu-second',
                'labels': ['cuda', 'rtx5070ti'],
                'disabled_reason': 'memory verification failed',
                'connection': {'host': '100.64.0.2', 'port': 22, 'user': 'mlrunner',
                               'private': 'drop-me'},
                'node': {'hostname': 'ubuntu', 'platform': 'Linux-test', 'python': '3.12.3',
                         'private': 'drop-me'},
                'state': 'available',
                'compatibility': {'state': 'compatible', 'reason': 'compatible'},
                'workload_count': 2,
                'managed_workload_count': 1,
                'workload_state': 'available',
                'gpu': [{'index': '0', 'name': 'RTX 5070 Ti', 'utilization_percent': '17',
                         'memory_used_mb': '1024', 'memory_total_mb': '16303',
                         'temperature_c': '40', 'private': 'drop-me'}],
                'cpu': {'model': 'Ryzen Test CPU', 'logical_cores': 24,
                        'utilization_percent': 31.5, 'load_1m': 3.2,
                        'load_5m': 2.1, 'load_15m': 1.8, 'private': 'drop-me'},
                'memory': {'total_bytes': 32 * 1024**3, 'available_bytes': 20 * 1024**3,
                           'used_bytes': 12 * 1024**3, 'utilization_percent': 37.5,
                           'private': 'drop-me'},
                'disk': {'free_bytes': 1234, 'total_bytes': 5678, 'used_bytes': 4444,
                         'private': 'drop-me'},
                'checked_at': now(),
                'error': None,
            }],
        }
        catalog = Catalog(
            self.remote, {'run_id': RUN, 'name': '50 epochs', 'kind': 'train'},
            Path(self.directory.name) / 'catalog', discover=lambda: [],
            fleet=lambda: fleet, interval=30,
        )

        catalog.poll()
        report = catalog.view()['fleet']

        self.assertEqual(report['state'], 'available')
        self.assertEqual(report['selection_policy'], 'upper_agent_explicit')
        self.assertEqual(report['workers'][0]['worker_id'], 'ubuntu-second')
        self.assertEqual(report['workers'][0]['connection'], {
            'host': '100.64.0.2', 'port': 22, 'user': 'mlrunner',
        })
        self.assertEqual(report['workers'][0]['node'], {
            'hostname': 'ubuntu', 'platform': 'Linux-test', 'python': '3.12.3',
        })
        self.assertEqual(report['workers'][0]['disabled_reason'], 'memory verification failed')
        self.assertEqual(report['workers'][0]['cpu']['model'], 'Ryzen Test CPU')
        self.assertEqual(report['workers'][0]['cpu']['utilization_percent'], 31.5)
        self.assertEqual(report['workers'][0]['memory']['used_bytes'], 12 * 1024**3)
        self.assertNotIn('private', report['workers'][0]['gpu'][0])
        self.assertNotIn('private', report['workers'][0]['cpu'])
        self.assertNotIn('private', report['workers'][0]['memory'])
        self.assertNotIn('private', report['workers'][0]['disk'])
        self.assertNotIn('selected_worker', report)

    def test_catalog_routes_each_discovered_run_to_its_worker_remote(self):
        second_run = 'run_' + 'b' * 32
        second = ReadOnlyRemote()
        second.rows = [sample(1)]
        second.rows[0]['run_id'] = second_run
        second.rows[0]['payload']['progress']['metrics']['optimizer/0/group/0/lr'] = 9.9e-5
        primary = {'run_id': RUN, 'name': 'first', 'kind': 'train', 'worker': 'lab118'}
        discovered = {'run_id': second_run, 'name': 'second', 'kind': 'train',
                      'worker': 'ubuntu-second'}
        remotes = {'lab118': self.remote, 'ubuntu-second': second}
        catalog = Catalog(
            self.remote, primary, Path(self.directory.name) / 'catalog',
            discover=lambda: [discovered], remote_resolver=lambda item: remotes[item['worker']],
            interval=30,
        )

        catalog.poll()

        self.assertIn(('samples', RUN, 0, 200), self.remote.calls)
        self.assertIn(('samples', second_run, 0, 200), second.calls)
        self.assertEqual(catalog.view(second_run)['task']['worker'], 'ubuntu-second')
        second_summary = next(item for item in catalog.view()['catalog']
                              if item['run_id'] == second_run)
        self.assertEqual(second_summary['loss'], 10)
        self.assertEqual(second_summary['live_lr'], 9.9e-5)
