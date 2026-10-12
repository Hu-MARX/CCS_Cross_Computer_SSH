import unittest

from controller.domain import utc_now
from controller.fleet import WorkerBinding, WorkerDirectory, WorkerDirectoryError, directory_from_config
from controller.gateway import CCSGateway, CCSRequestError
from controller.remote import FakeRemoteRunner
from controller.repository import RunRepository, SubmissionError


class LoadAwareRemote(FakeRemoteRunner):
    def __init__(self, *, busy):
        super().__init__()
        self.busy = busy
        self.host = f'192.0.2.{busy + 10}'
        self.port = None
        self.user = 'mlrunner'

    def health(self):
        value = super().health()
        value.update(hostname=f'worker-{self.busy}', platform='Linux-test', python='3.12.3')
        value['gpu'] = [{
            'index': '0', 'name': 'RTX', 'utilization_percent': str(self.busy * 20),
            'memory_used_mb': str(self.busy * 1000), 'memory_total_mb': '24000',
            'temperature_c': '50',
        }]
        value['cpu'] = {
            'model': 'Test CPU', 'logical_cores': 16,
            'utilization_percent': float(self.busy * 20),
            'load_1m': float(self.busy), 'load_5m': 1.0, 'load_15m': 0.5,
        }
        value['memory'] = {
            'total_bytes': 32 * 1024**3,
            'available_bytes': (24 - self.busy) * 1024**3,
            'used_bytes': (8 + self.busy) * 1024**3,
            'utilization_percent': float(25 + self.busy),
        }
        return value

    def processes(self):
        return {
            'state': 'available', 'observed_at': utc_now(),
            'items': [{'managed': True} for _ in range(self.busy)],
        }


class FleetGatewayContractTests(unittest.TestCase):
    def setUp(self):
        self.repository = RunRepository(':memory:')
        self.busy = LoadAwareRemote(busy=2)
        self.idle = LoadAwareRemote(busy=0)
        self.directory = WorkerDirectory([
            WorkerBinding('gpu-busy', self.busy, labels=frozenset({'cuda', 'vision'})),
            WorkerBinding('gpu-idle', self.idle, labels=frozenset({'cuda', 'vision'})),
        ])
        self.gateway = CCSGateway(
            self.busy, repository=self.repository, worker='gpu-busy',
            worker_directory=self.directory, audit_path=':memory:',
        )

    @staticmethod
    def request(*, dry_run, worker_id='gpu-busy', confirm=None):
        payload = {
            'spec': {
                'schema_version': 1, 'name': 'two-worker-experiment',
                'project': {}, 'runtime': {},
                'command': {'argv': ['python', 'experiment.py']},
                'resources': {'gpu_index': 0},
                'task': {'kind': 'train', 'inputs': []},
            },
            'idempotency_key': 'fleet-submit-001',
        }
        if worker_id is not None:
            payload['worker_id'] = worker_id
        value = {
            'schema_version': 1,
            'request_id': 'fleet-submit-001',
            'actor': {'id': 'codex-local', 'kind': 'agent'},
            'action': 'run.submit',
            'dry_run': dry_run,
            'payload': payload,
        }
        if confirm is not None:
            value['confirm_plan_sha256'] = confirm
        return value

    def test_fleet_status_exposes_facts_without_selecting_a_worker(self):
        request = {
            'schema_version': 1, 'request_id': 'fleet-status-001',
            'actor': {'id': 'codex-local', 'kind': 'agent'},
            'action': 'fleet.status', 'dry_run': False, 'payload': {},
        }

        result = self.gateway.execute(request)

        workers = {item['worker_id']: item for item in result['result']['workers']}
        self.assertEqual(workers['gpu-busy']['workload_count'], 2)
        self.assertEqual(workers['gpu-idle']['workload_count'], 0)
        self.assertEqual(workers['gpu-busy']['cpu']['utilization_percent'], 40.0)
        self.assertEqual(workers['gpu-idle']['memory']['used_bytes'], 8 * 1024**3)
        self.assertEqual(self.busy.runs, {})
        self.assertEqual(self.idle.runs, {})

    def test_fleet_status_exposes_bounded_node_and_connection_facts(self):
        result = self.directory.status()

        worker = {item['worker_id']: item for item in result['workers']}['gpu-idle']
        self.assertEqual(worker['connection'], {
            'host': '192.0.2.10', 'port': 22, 'user': 'mlrunner',
        })
        self.assertEqual(worker['node'], {
            'hostname': 'worker-0', 'platform': 'Linux-test', 'python': '3.12.3',
        })

    def test_fleet_config_can_select_an_absolute_runtime_executable(self):
        calls = []

        def factory(host, **kwargs):
            calls.append((host, kwargs))
            remote = FakeRemoteRunner()
            remote.host, remote.user, remote.port = host, kwargs.get('user'), kwargs.get('port')
            return remote

        directory_from_config({
            'schema_version': 1,
            'workers': [{
                'id': 'project-owner', 'host': '100.64.0.3', 'user': 'ubuntu',
                'remote_root': '/home/ubuntu/.ml-orchestrator',
                'trainctl_executable': '/home/ubuntu/.ml-orchestrator/trainctl',
            }],
        }, factory)

        self.assertEqual(calls[0][1]['trainctl_executable'],
                         '/home/ubuntu/.ml-orchestrator/trainctl')

    def test_disabled_worker_stays_observable_but_rejects_new_runs(self):
        disabled = WorkerDirectory([
            WorkerBinding(
                'unsafe-node', self.idle, labels=frozenset({'cuda'}),
                enabled=False, disabled_reason='memory verification failed',
            ),
        ])

        worker = disabled.status()['workers'][0]
        self.assertEqual(worker['state'], 'disabled')
        self.assertEqual(worker['disabled_reason'], 'memory verification failed')
        self.assertEqual(worker['gpu'][0]['name'], 'RTX')
        with self.assertRaises(WorkerDirectoryError) as raised:
            disabled.submission_plan('unsafe-node', {'run.submit': 1})
        self.assertEqual(raised.exception.code, 'WORKER_DISABLED')

    def test_confirmed_submission_obeys_agent_selection_even_when_other_worker_is_idle(self):
        plan = self.gateway.execute(self.request(dry_run=True))

        self.assertEqual(plan['result']['worker_id'], 'gpu-busy')
        self.assertEqual(self.busy.runs, {})
        self.assertEqual(self.idle.runs, {})

        result = self.gateway.execute(self.request(
            dry_run=False, confirm=plan['plan_sha256'],
        ))

        self.assertEqual(result['result']['worker'], 'gpu-busy')
        self.assertEqual(len(self.busy.runs), 1)
        self.assertEqual(self.idle.runs, {})

    def test_multi_worker_submission_requires_an_explicit_agent_selection(self):
        with self.assertRaises(CCSRequestError) as raised:
            self.gateway.execute(self.request(dry_run=True, worker_id=None))

        self.assertEqual(raised.exception.code, 'WORKER_SELECTION_REQUIRED')

    def test_changed_agent_selection_invalidates_old_confirmation_before_submit(self):
        plan = self.gateway.execute(self.request(dry_run=True))

        with self.assertRaises(CCSRequestError) as raised:
            self.gateway.execute(self.request(
                dry_run=False, worker_id='gpu-idle', confirm=plan['plan_sha256'],
            ))

        self.assertEqual(raised.exception.code, 'CONFIRMATION_REQUIRED')
        self.assertEqual(self.busy.runs, {})
        self.assertEqual(self.idle.runs, {})

    def test_idempotency_key_cannot_move_an_existing_run_to_another_worker(self):
        first_plan = self.gateway.execute(self.request(dry_run=True, worker_id='gpu-busy'))
        self.gateway.execute(self.request(
            dry_run=False, worker_id='gpu-busy', confirm=first_plan['plan_sha256'],
        ))
        second_plan = self.gateway.execute(self.request(dry_run=True, worker_id='gpu-idle'))

        with self.assertRaises((WorkerDirectoryError, SubmissionError)) as raised:
            self.gateway.execute(self.request(
                dry_run=False, worker_id='gpu-idle', confirm=second_plan['plan_sha256'],
            ))

        self.assertEqual(raised.exception.code, 'IDEMPOTENCY_CONFLICT')
        self.assertEqual(len(self.busy.runs), 1)
        self.assertEqual(self.idle.runs, {})

    def test_snapshot_routes_by_recorded_run_worker_without_making_a_new_selection(self):
        plan = self.gateway.execute(self.request(dry_run=True, worker_id='gpu-idle'))
        submitted = self.gateway.execute(self.request(
            dry_run=False, worker_id='gpu-idle', confirm=plan['plan_sha256'],
        ))
        run_id = submitted['result']['id']
        request = {
            'schema_version': 1, 'request_id': 'fleet-snapshot-001',
            'actor': {'id': 'codex-local', 'kind': 'agent'},
            'action': 'run.snapshot', 'dry_run': False,
            'payload': {'run_id': run_id},
        }

        snapshot = self.gateway.execute(request)

        self.assertEqual(snapshot['result']['run_id'], run_id)
        self.assertEqual(snapshot['result']['worker_id'], 'gpu-idle')

    def test_unknown_load_is_reported_but_does_not_override_agent_selection(self):
        class UnknownLoadRemote(LoadAwareRemote):
            def processes(self):
                raise RuntimeError('process observation unavailable')

        selected = UnknownLoadRemote(busy=0)
        directory = WorkerDirectory([
            WorkerBinding('selected', selected, labels=frozenset({'cuda'})),
            WorkerBinding('other', self.idle, labels=frozenset({'cuda'})),
        ])
        gateway = CCSGateway(
            selected, repository=RunRepository(':memory:'), worker='selected',
            worker_directory=directory, audit_path=':memory:',
        )
        request = self.request(dry_run=True, worker_id='selected')
        request['payload']['worker_id'] = 'selected'

        plan = gateway.execute(request)

        self.assertTrue(plan['executable'])
        self.assertIsNone(plan['result']['worker']['workload_count'])
        self.assertEqual(plan['result']['worker']['workload_state'], 'unavailable')

    def test_explicit_worker_routes_unregistered_run_observations_and_workspace(self):
        from controller.domain import ExperimentSpec

        spec = ExperimentSpec.from_dict({
            'schema_version': 1, 'name': 'external', 'project': {}, 'runtime': {},
            'command': {'argv': ['python', 'external.py']},
        })
        run_id = self.idle.submit(spec, 'external-idle-001')
        cases = [
            ('run.events', {'run_id': run_id, 'worker_id': 'gpu-idle', 'after_seq': 0}),
            ('run.logs', {'run_id': run_id, 'worker_id': 'gpu-idle',
                          'stream': 'stdout', 'after_offset': 0}),
            ('run.artifacts', {'run_id': run_id, 'worker_id': 'gpu-idle'}),
            ('workspace.execute', {'worker_id': 'gpu-idle', 'request': {
                'action': 'inspect', 'project': '/home/research/project', 'mode': 'tree', 'path': '.',
            }}),
        ]
        for index, (action, payload) in enumerate(cases):
            with self.subTest(action=action):
                request = {
                    'schema_version': 1, 'request_id': f'route-{index}',
                    'actor': {'id': 'codex-local', 'kind': 'agent'},
                    'action': action, 'dry_run': False, 'payload': payload,
                }
                result = self.gateway.execute(request)
                self.assertTrue(result['executed'])
                self.assertEqual(result['result']['worker_id'], 'gpu-idle')

    def test_cancel_targets_only_the_agent_selected_worker(self):
        from controller.domain import ExperimentSpec

        spec = ExperimentSpec.from_dict({
            'schema_version': 1, 'name': 'external', 'project': {}, 'runtime': {},
            'command': {'argv': ['python', 'external.py']},
        })
        run_id = self.idle.submit(spec, 'external-cancel-001')
        payload = {'run_id': run_id, 'worker_id': 'gpu-idle', 'reason': 'agent selected stop'}
        base = {
            'schema_version': 1, 'request_id': 'fleet-cancel-001',
            'actor': {'id': 'codex-local', 'kind': 'agent'},
            'action': 'run.cancel', 'payload': payload,
        }
        plan = self.gateway.execute({**base, 'dry_run': True})
        result = self.gateway.execute({
            **base, 'dry_run': False, 'confirm_plan_sha256': plan['plan_sha256'],
        })

        self.assertEqual(result['result']['worker_id'], 'gpu-idle')
        self.assertEqual(result['result']['status'], 'CANCELLED')
        self.assertEqual(self.busy.runs, {})


if __name__ == '__main__':
    unittest.main()
