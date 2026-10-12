import unittest
import contextlib
import io
import json
from unittest.mock import patch

from ccs.contracts import _validate, describe, ContractError, validate_payload
from controller.domain import ExperimentSpec
from controller.fleet import WorkerDirectoryError
from controller.gateway import CCSGateway
from controller.remote import FakeRemoteRunner
from controller.gateway_cli import _print


class GatewayPaginationTests(unittest.TestCase):
    def setUp(self):
        self.remote = FakeRemoteRunner()
        self.gateway = CCSGateway(self.remote, audit_path=':memory:')
        self.run_id = self.remote.submit(ExperimentSpec.from_dict(describe('run.submit')['examples'][0]['spec']), 'paging')

    def read(self, action, payload):
        result = self.gateway.execute({'schema_version': 1, 'request_id': 'paging-read',
            'actor': {'id': 'test', 'kind': 'agent'}, 'action': action, 'dry_run': False, 'payload': payload})['result']
        schema = describe(action)['output_schema']
        self.assertEqual(_validate(result, schema, 'result', schema.get('$defs', {})), [])
        return result

    def test_event_page_and_legacy_collection(self):
        result = self.read('run.events', {'run_id': self.run_id, 'after_seq': 0, 'limit': 1})
        self.assertEqual(len(result['events']), 1)
        self.assertEqual(result['next_cursor'], result['events'][-1]['seq'])
        legacy = self.read('run.events', {'run_id': self.run_id, 'after_seq': 0})
        self.assertNotIn('has_more', legacy)

    def test_artifact_pages_resume_and_bound_cursor_identity(self):
        self.remote.runs[self.run_id]['artifacts'] = [
            {'path': '/srv/a', 'size': 1}, {'path': '/srv/b', 'size': 2}, {'path': '/srv/c', 'size': 3}]
        first = self.read('run.artifacts', {'run_id': self.run_id, 'limit': 2})
        self.assertEqual([item['path'] for item in first['artifacts']], ['/srv/a', '/srv/b'])
        self.assertTrue(first['has_more'])
        last = self.read('run.artifacts', {'run_id': self.run_id, 'cursor': first['next_cursor'], 'limit': 2})
        self.assertEqual([item['path'] for item in last['artifacts']], ['/srv/c'])
        self.assertFalse(last['has_more'])

    def test_page_capability_cannot_silently_fall_back(self):
        health = self.remote.health()
        health['ccs_interface']['capabilities'].pop('run.events.page')
        with patch.object(self.remote, 'health', return_value=health):
            with self.assertRaises(WorkerDirectoryError) as error:
                self.read('run.events', {'run_id': self.run_id, 'after_seq': 0, 'limit': 1})
        self.assertEqual(error.exception.code, 'WORKER_INCOMPATIBLE')

    def test_new_bounds_fail_before_remote_routing(self):
        for key, value in [('limit', 1001), ('limit', True), ('max_bytes', 1023)]:
            with self.assertRaises(ContractError):
                validate_payload('run.events', {'run_id': self.run_id, 'after_seq': 0, key: value})

    def test_gateway_output_preserves_utf8_page_budget(self):
        value = {'events': [{'seq': 1, 'kind': 'output', 'payload': {'text': '中文😀' * 1000}}]}
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            _print(value)
        expected = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')) + '\n'
        self.assertEqual(output.getvalue().encode('utf-8'), expected.encode('utf-8'))


if __name__ == '__main__':
    unittest.main()
