import re
import json
import unittest
import tempfile
from pathlib import Path

from controller.domain import ExperimentSpec
from controller.gateway import CCSGateway, CCSRequestError
from controller.remote import FakeRemoteRunner
from controller.checkpoint_replication import CheckpointReplicaStore


class GatewayContractTests(unittest.TestCase):
    def setUp(self):
        self.remote = FakeRemoteRunner()
        self.gateway = CCSGateway(self.remote, audit_path=':memory:')

    @staticmethod
    def request(action, payload, *, dry_run, request_id='request-demo'):
        return {
            'schema_version': 1,
            'request_id': request_id,
            'actor': {'id': 'codex-local', 'kind': 'agent'},
            'action': action,
            'dry_run': dry_run,
            'payload': payload,
        }

    def test_dry_run_returns_read_plan_without_reading_the_target_run(self):
        result = self.gateway.execute(self.request(
            'run.snapshot', {'run_id': 'remote_missing'}, dry_run=True,
        ))

        self.assertEqual(result['schema_version'], 1)
        self.assertEqual(result['request_id'], 'request-demo')
        self.assertEqual(result['action'], 'run.snapshot')
        self.assertEqual(result['classification'], 'read')
        self.assertFalse(result['executed'])
        self.assertEqual(result['required_capabilities'], {'run.observe': 1})
        self.assertEqual(result['compatibility']['state'], 'compatible')
        self.assertTrue(result['executable'])
        self.assertRegex(result['plan_sha256'], re.compile(r'^[a-f0-9]{64}$'))
        self.assertIsNone(result['result'])

    def test_read_execution_returns_json_snapshot_without_confirmation(self):
        spec = ExperimentSpec.from_dict({
            'schema_version': 1, 'name': 'simulation', 'project': {}, 'runtime': {},
            'command': {'argv': ['python', 'simulate.py']},
            'task': {'kind': 'simulate', 'inputs': []},
        })
        run_id = self.remote.submit(spec, 'simulation-key')

        result = self.gateway.execute(self.request(
            'run.snapshot', {'run_id': run_id}, dry_run=False,
        ))

        self.assertTrue(result['executed'])
        self.assertEqual(result['result']['run_id'], run_id)
        self.assertEqual(result['result']['status'], 'QUEUED')
        self.assertIsInstance(result['result']['heartbeat_at'], str)

    def test_mutation_requires_exact_dry_run_confirmation_and_supports_simulation(self):
        spec = {
            'schema_version': 1, 'name': 'embodied-policy-simulation',
            'project': {'path': '/srv/research/embodied'},
            'runtime': {'workdir': '/srv/research/embodied'},
            'command': {'argv': ['python', 'simulate_policy.py']},
            'task': {'kind': 'simulate', 'inputs': []},
        }
        payload = {'spec': spec, 'idempotency_key': 'embodied-sim-001'}
        plan = self.gateway.execute(self.request('run.submit', payload, dry_run=True))

        self.assertEqual(plan['classification'], 'mutation')
        self.assertFalse(plan['executed'])
        self.assertEqual(plan['required_capabilities'], {'run.submit': 1, 'task.metadata': 1})
        self.assertEqual(self.remote.runs, {})

        execute = self.request('run.submit', payload, dry_run=False)
        with self.assertRaises(CCSRequestError) as raised:
            self.gateway.execute(execute)
        self.assertEqual(raised.exception.code, 'CONFIRMATION_REQUIRED')
        self.assertEqual(self.remote.runs, {})

        execute['confirm_plan_sha256'] = plan['plan_sha256']
        result = self.gateway.execute(execute)
        self.assertTrue(result['executed'])
        self.assertEqual(result['result']['status'], 'QUEUED')
        self.assertEqual(result['result']['spec']['task']['kind'], 'simulate')
        self.assertEqual(len(self.remote.runs), 1)

    def test_audit_records_plans_failures_and_success_without_raw_payloads(self):
        payload = {
            'spec': {
                'schema_version': 1, 'name': 'private-model', 'project': {}, 'runtime': {},
                'command': {'argv': ['python', 'secret_simulate_policy.py']},
                'task': {'kind': 'simulate', 'inputs': []},
            },
            'idempotency_key': 'private-model-001',
        }
        plan = self.gateway.execute(self.request('run.submit', payload, dry_run=True))
        with self.assertRaises(CCSRequestError):
            self.gateway.execute(self.request('run.submit', payload, dry_run=False))
        execute = self.request('run.submit', payload, dry_run=False)
        execute['confirm_plan_sha256'] = plan['plan_sha256']
        self.gateway.execute(execute)

        audit = self.gateway.execute(self.request(
            'gateway.audit', {'request_id': 'request-demo', 'limit': 10},
            dry_run=False, request_id='audit-query',
        ))

        records = audit['result']['records']
        self.assertEqual([item['outcome'] for item in records], ['succeeded', 'started', 'failed', 'planned'])
        self.assertEqual(records[0]['classification'], 'mutation')
        self.assertEqual(records[0]['actor'], {'id': 'codex-local', 'kind': 'agent'})
        self.assertEqual(records[2]['error_code'], 'CONFIRMATION_REQUIRED')
        self.assertNotIn('secret_simulate_policy.py', json.dumps(records))
        self.assertNotIn('payload', records[0])

    def test_observation_actions_share_one_request_result_contract(self):
        spec = ExperimentSpec.from_dict({
            'schema_version': 1, 'name': 'observable', 'project': {}, 'runtime': {},
            'command': {'argv': ['python', 'observe.py']},
        })
        run_id = self.remote.submit(spec, 'observable-001')
        cases = [
            ('worker.health', {}, 'worker.health'),
            ('worker.monitor', {}, 'worker.monitor'),
            ('worker.alerts', {}, 'worker.telemetry'),
            ('worker.storage', {}, 'worker.storage'),
            ('worker.processes', {}, 'worker.processes'),
            ('run.events', {'run_id': run_id, 'after_seq': 0}, 'run.observe'),
            ('run.logs', {'run_id': run_id, 'stream': 'stdout', 'after_offset': 0}, 'run.logs'),
            ('run.artifacts', {'run_id': run_id}, 'run.artifacts'),
        ]

        for index, (action, payload, capability) in enumerate(cases):
            with self.subTest(action=action):
                result = self.gateway.execute(self.request(
                    action, payload, dry_run=False, request_id=f'observe-{index}',
                ))
                self.assertTrue(result['executed'])
                self.assertEqual(result['classification'], 'read')
                self.assertEqual(result['required_capabilities'], {capability: 1})
                json.dumps(result, allow_nan=False)

    def test_cancel_is_confirmed_and_remains_available_to_legacy_workers(self):
        class LegacyRemote(FakeRemoteRunner):
            def compatibility(self, required=None):
                return {
                    'state': 'legacy', 'reachable': True, 'compatible': False,
                    'reason': 'no interface declaration', 'required_capabilities': required or {},
                }

        remote = LegacyRemote()
        gateway = CCSGateway(remote, audit_path=':memory:')
        spec = ExperimentSpec.from_dict({
            'schema_version': 1, 'name': 'legacy-run', 'project': {}, 'runtime': {},
            'command': {'argv': ['python', 'run.py']},
        })
        run_id = remote.submit(spec, 'legacy-run-001')
        payload = {'run_id': run_id, 'reason': 'operator safety stop'}
        plan = gateway.execute(self.request('run.cancel', payload, dry_run=True))

        self.assertTrue(plan['executable'])
        execute = self.request('run.cancel', payload, dry_run=False)
        execute['confirm_plan_sha256'] = plan['plan_sha256']
        result = gateway.execute(execute)

        self.assertEqual(result['result']['status'], 'CANCELLED')

    def test_checkpoint_replication_requires_confirmation_and_publishes_receipt(self):
        spec = ExperimentSpec.from_dict({
            'schema_version': 1, 'name': 'checkpoint', 'project': {}, 'runtime': {},
            'command': {'argv': ['python', 'train.py']},
        })
        run_id = self.remote.submit(spec, 'checkpoint-replica-001')
        checkpoint_id = 'ckpt_' + 'b' * 32
        path = '/research/' + run_id + '/checkpoints/' + checkpoint_id + '.ckpt'
        self.remote.set_managed_checkpoint(run_id, checkpoint_id, path, b'checkpoint')
        payload = {'run_id': run_id, 'checkpoint_id': checkpoint_id}

        with tempfile.TemporaryDirectory() as directory:
            self.gateway.replica_store = CheckpointReplicaStore(directory)
            plan = self.gateway.execute(self.request('checkpoint.replicate', payload, dry_run=True))
            self.assertEqual(plan['classification'], 'mutation')
            self.assertEqual(plan['required_capabilities'], {'checkpoint.export': 1})

            execute = self.request('checkpoint.replicate', payload, dry_run=False)
            execute['confirm_plan_sha256'] = plan['plan_sha256']
            result = self.gateway.execute(execute)

        self.assertEqual(result['result']['status'], 'replicated')
        self.assertEqual(result['result']['checkpoint_id'], checkpoint_id)

    def test_checkpoint_replication_dry_run_does_not_create_the_replica_store(self):
        payload = {
            'run_id': 'run_' + 'c' * 32,
            'checkpoint_id': 'ckpt_' + 'd' * 32,
        }
        with tempfile.TemporaryDirectory() as directory:
            replica_root = Path(directory) / 'replicas'
            gateway = CCSGateway(
                self.remote, audit_path=':memory:', replica_root=replica_root,
            )
            plan = gateway.execute(self.request('checkpoint.replicate', payload, dry_run=True))
            self.assertTrue(plan['executable'])
            self.assertFalse(replica_root.exists())

    def test_checkpoint_import_requires_explicit_target_and_confirmation(self):
        spec = ExperimentSpec.from_dict({
            'schema_version': 1, 'name': 'import-source', 'project': {}, 'runtime': {},
            'command': {'argv': ['python', 'train.py']},
        })
        run_id = self.remote.submit(spec, 'checkpoint-import-source-001')
        checkpoint_id = 'ckpt_' + 'e' * 32
        path = '/research/' + run_id + '/checkpoints/' + checkpoint_id + '.ckpt'
        self.remote.set_managed_checkpoint(run_id, checkpoint_id, path, b'import checkpoint')
        payload = {
            'source_worker_id': 'source-worker',
            'source_run_id': run_id,
            'checkpoint_id': checkpoint_id,
            'target_worker_id': 'default',
        }

        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointReplicaStore(directory)
            store.replicate(
                self.remote, worker_id='source-worker', run_id=run_id,
                checkpoint_id=checkpoint_id,
            )
            gateway = CCSGateway(
                self.remote, audit_path=':memory:', replica_store=store,
            )
            plan = gateway.execute(self.request('checkpoint.import', payload, dry_run=True))
            self.assertEqual(plan['classification'], 'mutation')
            self.assertEqual(plan['required_capabilities'], {'checkpoint.import': 1})
            self.assertEqual(plan['result']['worker_id'], 'default')

            execute = self.request('checkpoint.import', payload, dry_run=False)
            execute['confirm_plan_sha256'] = plan['plan_sha256']
            result = gateway.execute(execute)

        self.assertEqual(result['result']['status'], 'imported')
        self.assertEqual(result['result']['target_worker_id'], 'default')

        invalid = dict(payload)
        invalid.pop('target_worker_id')
        with self.assertRaises(CCSRequestError):
            self.gateway.execute(self.request('checkpoint.import', invalid, dry_run=True))

    def test_workspace_reads_and_mutations_are_classified_from_nested_action(self):
        class RecordingRemote(FakeRemoteRunner):
            def __init__(self):
                super().__init__()
                self.workspace_requests = []

            def workspace(self, request):
                self.workspace_requests.append(request)
                return {'action': request['action'], 'accepted': True}

        remote = RecordingRemote()
        gateway = CCSGateway(remote, workspace_remote=remote, audit_path=':memory:')
        inspect_payload = {'request': {
            'action': 'inspect', 'project': '/srv/research/CVFDETR',
            'mode': 'tree', 'path': '.',
        }}
        inspected = gateway.execute(self.request('workspace.execute', inspect_payload, dry_run=False))
        self.assertEqual(inspected['classification'], 'read')
        self.assertEqual(inspected['required_capabilities'], {'workspace.control': 1})
        self.assertEqual(len(remote.workspace_requests), 1)

        stage_payload = {'request': {
            'action': 'stage', 'project': '/srv/research/embodied',
            'operations': [{'op': 'write', 'path': 'models/world_model.py', 'content': '# draft\n'}],
        }}
        plan = gateway.execute(self.request('workspace.execute', stage_payload, dry_run=True))
        self.assertEqual(plan['classification'], 'mutation')
        self.assertEqual(len(remote.workspace_requests), 1)

        execute = self.request('workspace.execute', stage_payload, dry_run=False)
        execute['confirm_plan_sha256'] = plan['plan_sha256']
        applied = gateway.execute(execute)
        self.assertTrue(applied['executed'])
        self.assertEqual(len(remote.workspace_requests), 2)


if __name__ == '__main__':
    unittest.main()
