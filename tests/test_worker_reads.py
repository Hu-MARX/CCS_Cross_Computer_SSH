import unittest

from controller.fleet import WorkerBinding, WorkerDirectory, WorkerDirectoryError
from controller.gateway import CCSGateway, CCSRequestError
from controller.remote import FakeRemoteRunner


class DiagnosticRemote(FakeRemoteRunner):
    def __init__(self, hostname, *, legacy=False):
        super().__init__()
        self.hostname = hostname
        self.legacy = legacy
        self.reads = []

    def health(self):
        result = super().health()
        result['hostname'] = self.hostname
        if self.legacy:
            result.pop('ccs_interface')
        return result

    def _diagnostic(self, action):
        self.reads.append(action)
        # Worker identity comes from the controller's binding, not an adapter claim.
        return {'hostname': self.hostname, 'worker_id': 'adapter-claim', 'action': action}

    def monitor(self):
        return self._diagnostic('monitor')

    def alerts(self):
        return self._diagnostic('alerts')

    def storage(self):
        return self._diagnostic('storage')

    def processes(self):
        return self._diagnostic('processes')


class WorkerReadRoutingTests(unittest.TestCase):
    actions = tuple(CCSGateway._WORKER_READS)

    def setUp(self):
        self.first = DiagnosticRemote('first-host')
        self.default = DiagnosticRemote('legacy-default-host')
        self.directory = WorkerDirectory([
            WorkerBinding('first', self.first),
            WorkerBinding('default-node', self.default),
        ])
        self.gateway = CCSGateway(
            self.default, worker='default-node', worker_directory=self.directory,
            audit_path=':memory:',
        )

    @staticmethod
    def request(action, payload, *, dry_run=False):
        return {
            'schema_version': 1, 'request_id': 'worker-read-test',
            'actor': {'id': 'local-test', 'kind': 'agent'},
            'action': action, 'dry_run': dry_run, 'payload': payload,
        }

    def test_each_action_routes_to_both_explicit_workers(self):
        for action in self.actions:
            for worker_id, hostname in (
                    ('first', 'first-host'), ('default-node', 'legacy-default-host')):
                with self.subTest(action=action, worker_id=worker_id):
                    response = self.gateway.execute(self.request(action, {'worker_id': worker_id}))
                    self.assertEqual(response['result']['hostname'], hostname)
                    self.assertEqual(response['result']['worker_id'], worker_id)
                    self.assertEqual(response['classification'], 'read')
                    self.assertTrue(response['executed'])

    def test_empty_payload_preserves_remote_when_directory_first_binding_differs(self):
        for action in self.actions:
            with self.subTest(action=action):
                response = self.gateway.execute(self.request(action, {}))
                self.assertEqual(response['result']['hostname'], 'legacy-default-host')
                self.assertEqual(response['result']['worker_id'], 'default-node')
        self.assertEqual(self.first.reads, [])

    def test_constructor_default_id_can_be_inferred_from_matching_adapter(self):
        gateway = CCSGateway(
            self.default, worker_directory=self.directory, audit_path=':memory:',
        )
        result = gateway.execute(self.request('worker.health', {}))
        self.assertEqual(result['result']['worker_id'], 'default-node')
        self.assertEqual(result['result']['hostname'], 'legacy-default-host')

    def test_unlisted_default_remote_keeps_constructor_identity(self):
        outside = DiagnosticRemote('outside-host')
        gateway = CCSGateway(
            outside, worker='explicit-legacy', worker_directory=self.directory,
            audit_path=':memory:',
        )
        result = gateway.execute(self.request('worker.health', {}))
        self.assertEqual(result['result']['worker_id'], 'explicit-legacy')
        self.assertEqual(result['result']['hostname'], 'outside-host')

    def test_unknown_worker_fails_without_fallback(self):
        with self.assertRaises(WorkerDirectoryError) as raised:
            self.gateway.execute(self.request('worker.health', {'worker_id': 'missing'}))
        self.assertEqual(raised.exception.code, 'WORKER_NOT_FOUND')
        self.assertEqual(self.first.reads, [])
        self.assertEqual(self.default.reads, [])

    def test_invalid_payload_and_worker_id_are_rejected(self):
        for payload in ({'worker_id': None}, {'worker_id': 1}, {'worker_id': ''},
                        {'worker_id': '../node'}, {'worker_id': 'first', 'typo': True}):
            with self.subTest(payload=payload), self.assertRaises(CCSRequestError):
                self.gateway.execute(self.request('worker.health', payload))

    def test_disabled_worker_can_be_diagnosed_but_not_submitted(self):
        directory = WorkerDirectory([
            WorkerBinding('first', self.first),
            WorkerBinding('disabled', self.default, enabled=False,
                          disabled_reason='operator disabled new work'),
        ])
        gateway = CCSGateway(self.first, worker='first', worker_directory=directory,
                             audit_path=':memory:')
        for action in self.actions:
            with self.subTest(action=action):
                result = gateway.execute(self.request(action, {'worker_id': 'disabled'}))
                self.assertEqual(result['result']['hostname'], 'legacy-default-host')
                self.assertEqual(result['result']['worker_id'], 'disabled')
        payload = {
            'worker_id': 'disabled', 'idempotency_key': 'disabled-submit',
            'spec': {'schema_version': 1, 'name': 'disabled-test', 'project': {},
                     'runtime': {}, 'command': {'argv': ['python', 'train.py']}},
        }
        with self.assertRaises(WorkerDirectoryError) as raised:
            gateway.execute(self.request('run.submit', payload, dry_run=True))
        self.assertEqual(raised.exception.code, 'WORKER_DISABLED')
        self.assertEqual(self.default.runs, {})

    def test_dry_run_reports_selected_worker_without_target_diagnostic(self):
        result = self.gateway.execute(self.request('worker.monitor', {'worker_id': 'first'}, dry_run=True))
        self.assertFalse(result['executed'])
        self.assertTrue(result['executable'])
        self.assertEqual(result['result']['worker_id'], 'first')
        self.assertEqual(self.first.reads, [])

    def test_selected_worker_capabilities_are_used_and_legacy_reads_remain_available(self):
        legacy = DiagnosticRemote('legacy-host', legacy=True)
        directory = WorkerDirectory([WorkerBinding('first', self.first), WorkerBinding('legacy', legacy)])
        gateway = CCSGateway(self.first, worker='first', worker_directory=directory, audit_path=':memory:')
        result = gateway.execute(self.request('worker.storage', {'worker_id': 'legacy'}))
        self.assertEqual(result['compatibility']['state'], 'legacy')
        self.assertEqual(result['result']['hostname'], 'legacy-host')


if __name__ == '__main__':
    unittest.main()
