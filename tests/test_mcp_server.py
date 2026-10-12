"""Real MCP round trips through the CCS gateway with a local fake Worker."""

import copy
import asyncio
import json
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path

from ccs import CCSClient, CCSError
from ccs.mcp_server import create_server

try:
    from mcp import Client
    from mcp.client.stdio import StdioServerParameters
except ImportError:
    Client = None


GATEWAY = (
    'import sys; from controller import gateway_cli; '
    'from controller.remote import FakeRemoteRunner; '
    'gateway_cli.OpenSSHRunner = lambda *args, **kwargs: FakeRemoteRunner(); '
    'raise SystemExit(gateway_cli.main())'
)


@unittest.skipIf(Client is None, 'optional MCP SDK 2.2+ is not installed')
class MCPIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='ccs mcp ')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.audit = self.root / 'audit.db'
        self.command = [sys.executable, '-c', GATEWAY, '--host', 'fake-worker',
                        '--db', str(self.root / 'runs.db'), '--audit-db', str(self.audit)]
        self.sdk = CCSClient(self.command, cwd=self.root, actor_id='mcp-test', actor_kind='agent')

    async def test_tool_contract_and_read_only_workspace_guard(self):
        async with Client(create_server(self.sdk)) as client:
            listing = await client.list_tools()
            tools = {tool.name: tool for tool in listing.tools}
            self.assertEqual(set(tools), {'ccs_read', 'ccs_plan', 'ccs_execute'})
            self.assertTrue(tools['ccs_read'].annotations.read_only_hint)
            self.assertIn('operation.query', tools['ccs_read'].input_schema['properties']['action']['enum'])
            self.assertFalse(tools['ccs_execute'].annotations.read_only_hint)
            self.assertIn('plan_sha256', tools['ccs_execute'].input_schema['required'])
            blocked = await client.call_tool('ccs_read', {
                'action': 'workspace.execute', 'payload': {'request': {'action': 'stage'}},
            })
            self.assertTrue(blocked.is_error)
            self.assertEqual(blocked.structured_content['error']['code'], 'INVALID_REQUEST')
            self.assertFalse(self.audit.exists())
            read = await client.call_tool('ccs_read', {'action': 'fleet.status', 'payload': {}})
            self.assertFalse(read.is_error)
            self.assertTrue(read.structured_content['data']['executed'])
            self.assertEqual(read.structured_content['data']['result']['workers'][0]['worker_id'],
                             'fake-worker')

    async def test_plan_tampering_rejected_then_exact_confirmation_executes(self):
        spec = json.loads((Path(__file__).resolve().parents[1] /
                           'examples/agent-smoke-spec.json').read_text(encoding='utf-8'))
        spec['name'] = '本地模拟任务'
        payload = {
            'worker_id': 'fake-worker', 'idempotency_key': 'one-mcp-run',
            'spec': spec,
        }
        async with Client(create_server(self.sdk)) as client:
            response = await client.call_tool('ccs_plan', {
                'action': 'run.submit', 'payload': payload, 'request_id': 'mcp-plan-001',
            })
            self.assertFalse(response.is_error)
            plan = response.structured_content['data']
            self.assertFalse(plan['plan']['executed'])
            self.assertEqual(plan['request']['actor'], {'id': 'mcp-test', 'kind': 'agent'})
            arguments = {key: plan['request'][key] for key in ('action', 'payload', 'request_id')}
            arguments['plan_sha256'] = plan['plan']['plan_sha256']
            tampered = copy.deepcopy(arguments)
            tampered['payload']['spec']['name'] = 'changed'
            rejected = await client.call_tool('ccs_execute', tampered)
            self.assertTrue(rejected.is_error)
            self.assertEqual(rejected.structured_content['error']['code'], 'CONFIRMATION_REQUIRED')

        # No adapter-side plan cache: a new MCP server can confirm the original plan.
        async with Client(create_server(self.sdk)) as client:
            executed = await client.call_tool('ccs_execute', arguments)
            self.assertFalse(executed.is_error)
            self.assertTrue(executed.structured_content['data']['executed'])
            self.assertEqual(executed.structured_content['data']['result']['status'], 'QUEUED')
            operation = executed.structured_content['data']['operation']
            queried = await client.call_tool('ccs_read', {
                'action': 'operation.query', 'payload': {'operation_id': operation['operation_id']},
            })
            self.assertFalse(queried.is_error)
            self.assertEqual(queried.structured_content['data']['result']['execution_state'], 'confirmed')
            audit = await client.call_tool('ccs_read', {
                'action': 'gateway.audit', 'payload': {'request_id': 'mcp-plan-001'},
            })
            records = audit.structured_content['data']['result']['records']
            self.assertEqual([record['outcome'] for record in records],
                             ['succeeded', 'started', 'failed', 'planned'])

    async def test_stdio_process_discovery_and_gateway_errors(self):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1] / 'src')
        params = StdioServerParameters(
            command=sys.executable,
            args=['-m', 'ccs.mcp_server', '--command-json', json.dumps(self.command),
                  '--cwd', str(self.root), '--actor-id', 'mcp-stdio'],
            cwd=self.root, env=env,
        )
        async with Client(params, read_timeout_seconds=20) as client:
            self.assertEqual(len((await client.list_tools()).tools), 3)
            read = await client.call_tool('ccs_read', {'action': 'worker.health', 'payload': {}})
            self.assertFalse(read.is_error)
            error = await client.call_tool('ccs_execute', {
                'action': 'run.cancel', 'payload': {'run_id': 'remote_0001', 'reason': 'test'},
                'request_id': 'stdio-error-001', 'plan_sha256': '0' * 64,
            })
            self.assertTrue(error.is_error)
            self.assertEqual(error.structured_content['error']['code'], 'CONFIRMATION_REQUIRED')
            self.assertEqual(error.structured_content['error']['request_id'], 'stdio-error-001')

    async def test_readonly_discovery_and_runtime_action_allowlist(self):
        server = create_server(self.sdk, read_only=True,
                               allow_actions={'fleet.status', 'workspace.execute.inspect'})
        async with Client(server) as client:
            tools = {item.name for item in (await client.list_tools()).tools}
            self.assertEqual(tools, {'ccs_read'})
            forbidden = await client.call_tool('ccs_read', {'action': 'worker.health', 'payload': {}})
            self.assertTrue(forbidden.is_error)
            self.assertEqual(forbidden.structured_content['error']['code'], 'ACTION_DENIED')
            for action in ('stage', 'apply', 'restore', 'discard'):
                forbidden = await client.call_tool('ccs_read', {
                    'action': 'workspace.execute', 'payload': {'request': {'action': action}},
                })
                self.assertTrue(forbidden.is_error)
            self.assertFalse(self.audit.exists())
            allowed = await client.call_tool('ccs_read', {'action': 'fleet.status', 'payload': {}})
            self.assertFalse(allowed.is_error)

    async def test_stdio_readonly_flag_exposes_one_read_tool(self):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1] / 'src')
        params = StdioServerParameters(
            command=sys.executable,
            args=['-m', 'ccs.mcp_server', '--command-json', json.dumps(self.command),
                  '--cwd', str(self.root), '--read-only', '--allow-action', 'fleet.status',
                  '--max-concurrent', '1', '--max-queued', '0'],
            cwd=self.root, env=env,
        )
        async with Client(params, read_timeout_seconds=20) as client:
            self.assertEqual({item.name for item in (await client.list_tools()).tools}, {'ccs_read'})
            denied = await client.call_tool('ccs_read', {'action': 'worker.health', 'payload': {}})
            self.assertEqual(denied.structured_content['error']['code'], 'ACTION_DENIED')
        self.assertFalse(self.audit.exists())

    async def test_allowlist_guards_plan_and_execute_before_gateway_spawn(self):
        async with Client(create_server(self.sdk, allow_actions={'run.cancel'})) as client:
            for tool, arguments in (
                ('ccs_plan', {'action': 'run.submit', 'payload': {}}),
                ('ccs_execute', {'action': 'run.submit', 'payload': {}, 'request_id': 'blocked',
                                 'plan_sha256': '0' * 64}),
                ('ccs_plan', {'action': 'workspace.execute', 'payload': {'request': {'action': 'apply'}}}),
            ):
                response = await client.call_tool(tool, arguments)
                self.assertTrue(response.is_error)
                self.assertEqual(response.structured_content['error']['code'], 'ACTION_DENIED')
        self.assertFalse(self.audit.exists())

    async def test_finite_queue_reports_busy_and_active_cancellation_drains(self):
        class BlockingClient(CCSClient):
            def __init__(self):
                super().__init__([sys.executable])
                self.started = threading.Event()
                self.stopped = threading.Event()
                self.calls = 0

            def request(self, *args, cancel_event=None, **kwargs):
                self.calls += 1
                self.started.set()
                if not cancel_event.wait(5):
                    raise AssertionError('MCP did not cancel its local request')
                self.stopped.set()
                raise CCSError('GATEWAY_CANCELLED', 'local request cancelled')

        sdk = BlockingClient()
        async with Client(create_server(sdk, max_concurrent=1, max_queued=1)) as client:
            first = asyncio.create_task(client.call_tool('ccs_read', {'action': 'fleet.status', 'payload': {}}))
            self.assertTrue(await asyncio.to_thread(sdk.started.wait, 2))
            queued = asyncio.create_task(client.call_tool('ccs_read', {'action': 'fleet.status', 'payload': {}}))
            await asyncio.sleep(0.05)
            overloaded = await client.call_tool('ccs_read', {'action': 'fleet.status', 'payload': {}})
            self.assertTrue(overloaded.is_error)
            self.assertEqual(overloaded.structured_content['error']['code'], 'MCP_BUSY')
            queued.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await queued
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            self.assertTrue(await asyncio.to_thread(sdk.stopped.wait, 2))
            self.assertEqual(sdk.calls, 1)


if __name__ == '__main__':
    unittest.main()
