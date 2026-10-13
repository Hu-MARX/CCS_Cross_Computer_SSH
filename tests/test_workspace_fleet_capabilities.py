"""Workspace planning and dispatch must negotiate the same endpoint."""

import unittest

from controller.fleet import WorkerBinding, WorkerDirectory, WorkerDirectoryError
from controller.gateway import CCSGateway, CCSRequestError
from controller.remote import FakeRemoteRunner


class Endpoint(FakeRemoteRunner):
    def __init__(self, user, root, executable, *, state='compatible'):
        super().__init__()
        self.host, self.port, self.user = '192.0.2.10', 22, user
        self.remote_root, self.trainctl_executable = root, executable
        self.state = state
        self.health_calls, self.process_calls = 0, 0
        self.workspace_requests = []

    def health(self):
        self.health_calls += 1
        value = super().health()
        if self.state == 'legacy':
            value.pop('ccs_interface')
        elif self.state == 'incompatible':
            value['ccs_interface']['protocol']['major'] = 99
        return value

    def processes(self):
        self.process_calls += 1
        return super().processes()

    def workspace(self, request):
        self.workspace_requests.append(dict(request))
        return {'accepted': True, 'user': self.user, 'root': self.remote_root,
                'executable': self.trainctl_executable}


class WorkspaceFleetCapabilityTests(unittest.TestCase):
    def setUp(self):
        self.runner = Endpoint('runner', '/srv/worker', '/srv/worker/venv/bin/trainctl')
        self.workspace = Endpoint('owner', '/home/owner/.ml-orchestrator',
                                  '/home/owner/ccs-venv/bin/trainctl')

    def gateway(self, *, enabled=True):
        directory = WorkerDirectory([WorkerBinding(
            'selected', self.runner, workspace_remote=self.workspace,
            enabled=enabled, disabled_reason=None if enabled else 'operator disabled',
        )])
        return CCSGateway(self.runner, worker='selected', worker_directory=directory,
                          audit_path=':memory:')

    @staticmethod
    def request(*, action='stage', dry_run=True):
        nested = {'action': action, 'project': '/srv/research/example'}
        if action == 'stage':
            nested['operations'] = [{'op': 'write', 'path': 'module.py', 'content': '# draft\n'}]
        return {
            'schema_version': 1, 'request_id': 'workspace-endpoint-test',
            'actor': {'id': 'operator', 'kind': 'agent'},
            'action': 'workspace.execute', 'dry_run': dry_run,
            'payload': {'worker_id': 'selected', 'request': nested},
        }

    def confirmed(self, gateway):
        planned = gateway.execute(self.request())
        self.assertTrue(planned['executable'])
        self.assertFalse(self.workspace.workspace_requests)
        request = self.request(dry_run=False)
        request['confirm_plan_sha256'] = planned['plan_sha256']
        return gateway.execute(request)

    def test_compatible_runner_does_not_authorize_legacy_workspace_mutation(self):
        self.workspace.state = 'legacy'
        with self.assertRaises(WorkerDirectoryError) as raised:
            self.gateway().execute(self.request())
        self.assertEqual(raised.exception.code, 'WORKER_CAPABILITIES_UNDECLARED')
        self.assertFalse(self.runner.workspace_requests)
        self.assertFalse(self.workspace.workspace_requests)

    def test_compatible_runner_does_not_authorize_incompatible_workspace(self):
        self.workspace.state = 'incompatible'
        with self.assertRaises(WorkerDirectoryError) as raised:
            self.gateway().execute(self.request())
        self.assertEqual(raised.exception.code, 'WORKER_INCOMPATIBLE')
        self.assertFalse(self.workspace.workspace_requests)

    def test_legacy_or_incompatible_runner_does_not_block_compatible_workspace(self):
        for runner_state in ('legacy', 'incompatible'):
            with self.subTest(runner_state=runner_state):
                self.runner.state = runner_state
                self.workspace.workspace_requests.clear()
                result = self.confirmed(self.gateway())
                self.assertTrue(result['executed'])
                self.assertEqual(len(self.workspace.workspace_requests), 1)
                self.assertFalse(self.runner.workspace_requests)

    def test_separate_user_root_executable_use_workspace_health_and_dispatch(self):
        gateway = self.gateway()
        result = self.confirmed(gateway)
        self.assertEqual(self.runner.health_calls, 0)
        self.assertEqual(self.runner.process_calls, 0)
        self.assertGreater(self.workspace.health_calls, 0)
        self.assertGreater(self.workspace.process_calls, 0)
        self.assertEqual(result['result'], {
            'accepted': True, 'user': 'owner', 'root': '/home/owner/.ml-orchestrator',
            'executable': '/home/owner/ccs-venv/bin/trainctl', 'worker_id': 'selected',
        })
        plan = gateway.worker_directory.operation_plan(
            'selected', {'workspace.control': 1}, allow_legacy=False, workspace=True,
        )
        self.assertEqual(plan['connection'], {'host': '192.0.2.10', 'port': 22, 'user': 'owner'})

    def test_legacy_workspace_read_remains_available_and_reports_legacy(self):
        self.workspace.state = 'legacy'
        result = self.gateway().execute(self.request(action='inspect', dry_run=False))
        self.assertTrue(result['executed'])
        self.assertEqual(result['compatibility']['state'], 'legacy')
        self.assertEqual(self.runner.health_calls, 0)
        self.assertEqual(len(self.workspace.workspace_requests), 1)

    def test_workspace_mutation_still_requires_exact_confirmation(self):
        gateway = self.gateway()
        gateway.execute(self.request())
        request = self.request(dry_run=False)
        request['confirm_plan_sha256'] = '0' * 64
        with self.assertRaises(CCSRequestError) as raised:
            gateway.execute(request)
        self.assertEqual(raised.exception.code, 'CONFIRMATION_REQUIRED')
        self.assertFalse(self.workspace.workspace_requests)

    def test_disabled_worker_still_rejects_workspace_mutation(self):
        with self.assertRaises(WorkerDirectoryError) as raised:
            self.gateway(enabled=False).execute(self.request())
        self.assertEqual(raised.exception.code, 'WORKER_DISABLED')
        self.assertFalse(self.workspace.workspace_requests)

    def test_default_operation_plan_still_negotiates_runner(self):
        self.workspace.state = 'legacy'
        directory = self.gateway().worker_directory
        plan = directory.operation_plan('selected', {'run.submit': 1}, allow_legacy=False)
        self.assertEqual(plan['compatibility']['state'], 'compatible')
        self.assertEqual(self.workspace.health_calls, 0)
        self.assertEqual(plan['connection']['user'], 'runner')


if __name__ == '__main__':
    unittest.main()
