"""Offline contract validation and canonical schema coverage."""
from copy import deepcopy
from importlib.resources import files
import json
from pathlib import Path
import unittest

from ccs.contracts import ContractError, describe, experiment_spec_schema, request_schema, result_schema, validate_payload
from controller.domain import ExperimentSpec
from ccs.contracts import _validate
from controller.gateway import CCSGateway
from controller.remote import FakeRemoteRunner


class ActionContractTests(unittest.TestCase):
    def test_fake_worker_logs_match_the_published_output_contract(self):
        remote = FakeRemoteRunner()
        example = describe('run.submit')['examples'][0]
        run_id = remote.submit(ExperimentSpec.from_dict(example['spec']), 'output-contract')
        gateway = CCSGateway(remote, audit_path=':memory:')
        result = gateway.execute({
            'schema_version': 1, 'request_id': 'log-contract',
            'actor': {'id': 'contract-test', 'kind': 'agent'},
            'action': 'run.logs', 'dry_run': False,
            'payload': {'run_id': run_id, 'stream': 'stdout', 'after_offset': 0},
        })['result']
        schema = describe('run.logs')['output_schema']
        self.assertEqual(_validate(result, schema, 'result', schema.get('$defs', {})), [])
        self.assertEqual(result['text'], result['content'])

    def error(self, action, payload, path):
        with self.assertRaises(ContractError) as caught:
            validate_payload(action, payload)
        error = caught.exception
        self.assertFalse(error.retryable)
        self.assertEqual(error.execution_state, 'not_sent')
        self.assertIn(path, [field['path'] for field in error.field_errors])
        return error

    def test_all_gateway_actions_are_covered(self):
        expected = {
            'gateway.describe', 'gateway.audit', 'fleet.status', 'worker.health', 'worker.monitor', 'worker.alerts',
            'worker.storage', 'worker.processes', 'run.snapshot', 'run.events', 'run.logs', 'run.artifacts',
            'run.submit', 'run.cancel', 'run.pause', 'checkpoint.replicate', 'checkpoint.import', 'workspace.execute',
            'operation.query', 'operation.reconcile',
        }
        self.assertEqual(set(describe()['actions']), expected)
        self.assertEqual(set(request_schema()['properties']['action']['enum']), expected)
        self.assertEqual(set(result_schema()['properties']['action']['enum']), expected)

    def test_every_example_and_workspace_example_validates(self):
        for action, contract in describe()['actions'].items():
            self.assertTrue(contract['summary'])
            self.assertTrue(contract['examples'])
            self.assertTrue(contract['output_schema'])
            for example in contract['examples']:
                with self.subTest(action=action, example=example):
                    classification = validate_payload(action, example)
                    self.assertIn(classification, ['read', 'mutation'])
            for name, nested in contract.get('workspace_actions', {}).items():
                for example in nested['examples']:
                    with self.subTest(workspace_action=name):
                        self.assertEqual(validate_payload(action, example), nested['classification'])

    def test_describe_is_detached_and_local(self):
        selected = describe('run.submit')
        selected['input_schema']['properties'].clear()
        self.assertIn('spec', describe('run.submit')['input_schema']['properties'])
        resource = files('ccs').joinpath('resources', 'action-contracts.json')
        self.assertEqual(json.loads(resource.read_text(encoding='utf-8'))['schema_version'], 1)
        self.assertIn('local', describe('gateway.describe')['summary'])
        self.assertEqual(describe('workspace.execute', 'stage')['classification'], 'mutation')
        self.error('gateway.describe', {'workspace_action': 'stage'}, 'payload.workspace_action')

    def test_required_unknown_and_type_errors_preserve_paths(self):
        self.error('run.snapshot', {}, 'payload.run_id')
        error = self.error('worker.health', {'worker_ids': 'secret-do-not-echo'}, 'payload.worker_ids')
        self.assertNotIn('secret-do-not-echo', str(error.field_errors))
        self.error('run.events', {'run_id': 'example', 'after_seq': True}, 'payload.after_seq')
        self.error('run.logs', {'run_id': 'example', 'stream': 'output', 'after_offset': 0}, 'payload.stream')
        self.error('operation.query', {}, 'payload')
        self.error('operation.query', {'operation_id': 'example', 'request_id': 'request'}, 'payload')

    def test_spec_nested_fields_and_extensions(self):
        example = deepcopy(describe('run.submit')['examples'][0])
        example['spec']['command'].pop('argv')
        self.error('run.submit', example, 'payload.spec.command.argv')
        example = deepcopy(describe('run.submit')['examples'][0])
        example['spec']['resources']['gpu_index'] = False
        self.error('run.submit', example, 'payload.spec.resources.gpu_index')
        example = deepcopy(describe('run.submit')['examples'][0])
        example['spec']['project']['arbitrary_metadata'] = {'config': {'seed': 7}}
        example['spec']['runtime']['config'] = {'debug': True}
        example['spec']['resources']['future_limit'] = 123
        example['spec']['command']['future_flag'] = 'metadata'
        self.assertEqual(validate_payload('run.submit', example), 'mutation')
        ExperimentSpec.from_dict(example['spec'])
        example['spec']['project'] = None
        example['spec']['evaluation'] = None
        self.assertEqual(validate_payload('run.submit', example), 'mutation')
        self.assertEqual(ExperimentSpec.from_dict(example['spec']).project, {})

    def test_evidence_semantics_fail_before_network(self):
        example = deepcopy(describe('run.submit')['examples'][0])
        example['spec']['evidence'] = {'bundle_id': 'bundle_' + 'a' * 32, 'manifest_sha256': 'b' * 64}
        self.error('run.submit', example, 'payload.spec.command.argv')
        example['spec']['command']['argv'] = ['/usr/bin/python', '-u', '/srv/project/main.py']
        self.assertEqual(validate_payload('run.submit', example), 'mutation')
        ExperimentSpec.from_dict(example['spec'])
        example['spec']['evaluation'] = {'argv': ['/usr/bin/python', '/srv/project/eval.py']}
        self.error('run.submit', example, 'payload.spec.evaluation.argv')

    def test_workspace_required_fields_and_mode_limits(self):
        self.error('workspace.execute', {'request': {'action': 'stage'}}, 'payload.request.project')
        self.error('workspace.execute', {'request': {'action': 'apply', 'change_id': 'change_' + 'a' * 32}},
                   'payload.request.plan_sha256')
        self.error('workspace.execute', {'request': {'action': 'restore', 'change_id': 'change_' + 'a' * 32}},
                   'payload.request.applied_sha256')
        self.error('workspace.execute', {'request': {'action': 'changes', 'limit': False}}, 'payload.request.limit')
        for mode, limit in [('list', 500), ('tree', 5000), ('search', 200)]:
            request = {'action': 'inspect', 'project': '/srv/project', 'mode': mode, 'limit': limit}
            if mode == 'search':
                request['query'] = 'x'
            self.assertEqual(validate_payload('workspace.execute', {'request': request}), 'read')
            request['limit'] += 1
            self.error('workspace.execute', {'request': request}, 'payload.request.limit')
        self.error('workspace.execute', {'request': {'action': 'inspect', 'mode': 'search', 'project': '/srv/project'}},
                   'payload.request.query')
        self.error('workspace.execute', {'request': {'action': 'inspect', 'mode': 'read', 'project': '/srv/project'}},
                   'payload.request.path')
        self.error('workspace.execute', {'request': {'action': 'inspect', 'mode': 'read', 'project': '/srv/project', 'path': '.'}},
                   'payload.request.path')
        self.error('workspace.execute', {'request': {'action': 'inspect', 'mode': 'read', 'project': '/srv/project', 'limit': 10}},
                   'payload.request.limit')

    def test_workspace_stage_path_and_byte_limits(self):
        payload = {'request': {'action': 'stage', 'project': '/srv/project', 'operations': [
            {'op': 'write', 'path': 'main.py', 'content': 'x'},
        ]}}
        payload['request']['operations'][0]['path'] = '../escape.py'
        self.error('workspace.execute', payload, 'payload.request.operations[0].path')
        payload['request']['operations'][0]['path'] = 'main.py'
        payload['request']['operations'][0]['content'] = '汉' * (1024 * 1024 // 3 + 1)
        self.error('workspace.execute', payload, 'payload.request.operations[0].content')
        payload['request']['operations'][0]['content'] = 'x'
        payload['request']['operations'].append({'op': 'trash', 'path': './main.py'})
        self.error('workspace.execute', payload, 'payload.request.operations[1].path')

    def test_finite_json_and_safe_errors(self):
        self.error('run.submit', {'spec': {'metadata': float('nan')}, 'idempotency_key': 'example'}, 'payload.spec.metadata')
        self.error('worker.health', [], 'payload')
        self.error('worker.health', {'worker_id': 'bad value'}, 'payload.worker_id')
        with self.assertRaises(ContractError) as caught:
            validate_payload('unknown', {})
        self.assertEqual(caught.exception.code, 'UNSUPPORTED_ACTION')

    def test_published_schema_copies_match_canonical_when_present(self):
        root = Path(__file__).resolve().parents[1]
        pairs = [('ccs-request.schema.json', request_schema), ('ccs-result.schema.json', result_schema),
                 ('experiment-spec.schema.json', experiment_spec_schema)]
        for name, getter in pairs:
            with self.subTest(schema=name):
                self.assertEqual(json.loads((root / 'schemas' / name).read_text(encoding='utf-8')), getter())


if __name__ == '__main__':
    unittest.main()
