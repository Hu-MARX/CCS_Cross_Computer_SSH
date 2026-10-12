"""SDK transport tests use real local subprocesses and never contact a worker."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import MappingProxyType

from ccs import CCSClient, CCSError, CCSPlan


FAKE_GATEWAY = r'''
import hashlib
import json
import os
import sys
import time
from pathlib import Path

mode, record = sys.argv[1:3]
request = json.loads(sys.stdin.buffer.read().decode('utf-8'))
with Path(record).open('a', encoding='utf-8') as output:
    output.write(json.dumps(request, ensure_ascii=False) + '\n')
if mode == 'timeout':
    Path(request['payload']['effect_path']).write_text('executed', encoding='utf-8')
    time.sleep(10)
if mode == 'non_json':
    print('not a JSON envelope')
    raise SystemExit(0)
if mode == 'invalid_utf8':
    sys.stdout.buffer.write(b'\xff')
    raise SystemExit(0)
plan = {key: request[key] for key in ('schema_version', 'request_id', 'actor', 'action', 'payload')}
digest = hashlib.sha256(json.dumps(plan, ensure_ascii=True, sort_keys=True,
    separators=(',', ':'), allow_nan=False).encode()).hexdigest()
classification = 'mutation' if request['action'] == 'run.submit' else 'read'
error = None
if mode in ('error', 'error_bad_exit'):
    error = {'code': 'SSH_TIMEOUT', 'message': 'server did not answer', 'retryable': True}
elif not request['dry_run'] and classification == 'mutation' and request.get('confirm_plan_sha256') != digest:
    error = {'code': 'CONFIRMATION_REQUIRED', 'message': 'exact plan required', 'retryable': False}
data = {'schema_version': 1, 'request_id': request['request_id'], 'action': request['action'],
    'classification': classification, 'dry_run': request['dry_run'], 'executed': not request['dry_run'],
    'executable': True, 'plan_sha256': digest, 'required_capabilities': {},
    'compatibility': {'state': 'compatible'},
    'result': {'received': request, 'argv': sys.argv[3:], 'cwd': os.getcwd()}}
envelope = {'ok': error is None, 'request_id': request['request_id'],
    'data': data if error is None else None, 'error': error}
if mode == 'mismatched_id':
    envelope['request_id'] = 'another-request'
if mode == 'mismatched_data_id':
    data['request_id'] = 'another-request'
if mode == 'missing_data':
    del data['compatibility']
if mode == 'bad_shape':
    envelope['ok'] = 'yes'
if mode == 'bad_classification':
    data['classification'] = []
if mode == 'nan_result':
    data['result'] = float('nan')
if mode == 'mismatched_action':
    data['action'] = 'run.cancel'
if mode == 'mismatched_dry_run':
    data['dry_run'] = not request['dry_run']
if mode == 'mismatched_executed':
    data['executed'] = request['dry_run']
sys.stdout.buffer.write(json.dumps(envelope, ensure_ascii=False).encode('utf-8'))
raise SystemExit(0 if mode == 'error_bad_exit' else 1 if error or mode == 'success_bad_exit' else 0)
'''


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="ccs sdk ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.directory = self.root / "本地 子进程"
        self.directory.mkdir()
        self.script = self.directory / "fake gateway.py"
        self.script.write_text(FAKE_GATEWAY, encoding="utf-8")
        self.record = self.root / "requests.jsonl"

    def client(self, mode="ok", **kwargs):
        return CCSClient(
            [sys.executable, str(self.script), mode, str(self.record),
             "--fleet", "服务器 池.json", "--db", "runs with spaces.db"],
            cwd=self.directory, **kwargs,
        )

    def requests(self):
        return [json.loads(line) for line in self.record.read_text(encoding="utf-8").splitlines()]

    def test_request_uses_utf8_stdin_and_preserves_arguments_and_cwd(self):
        client = self.client(actor_id="my-harness", actor_kind="agent")
        payload = {"text": "服务器训练 中文", "path": "目录 with spaces/a.py",
                   "literal": "$(not a shell) `echo hello` & |"}
        result = client.request("fleet.status", payload, request_id="unicode-001")
        received = result["result"]["received"]
        self.assertEqual(received["payload"], payload)
        self.assertEqual(received["actor"], {"id": "my-harness", "kind": "agent"})
        self.assertTrue(received["dry_run"])
        self.assertFalse(result["executed"])
        self.assertEqual(result["result"]["argv"], ["--fleet", "服务器 池.json", "--db",
                         "runs with spaces.db", "request", "--file", "-"])
        self.assertEqual(Path(result["result"]["cwd"]), self.directory)

    def test_plan_and_confirm_keep_identity_and_nested_payload_snapshot(self):
        client = self.client(actor_id="experiment-agent", actor_kind="agent")
        payload = {"worker_id": "lab118", "idempotency_key": "one-run",
                   "spec": {"command": {"argv": ["python", "训练 a.py"]}}}
        plan = client.plan("run.submit", payload, request_id="plan-001")
        original = plan.request
        digest = plan.plan_sha256
        payload["worker_id"] = "another-worker"
        payload["spec"]["command"]["argv"].append("changed")
        plan.request["actor"]["id"] = "changed-actor"
        plan.request["payload"]["spec"]["command"]["argv"].append("also-changed")
        plan.data["plan_sha256"] = "0" * 64
        with self.assertRaises(FrozenInstanceError):
            plan._request_json = "{}"
        result = client.confirm(plan)
        actual = result["result"]["received"]
        expected = {**original, "dry_run": False, "confirm_plan_sha256": digest}
        self.assertEqual(actual, expected)
        self.assertEqual(plan.request, original)
        self.assertEqual(plan.plan_sha256, digest)
        self.assertTrue(result["executed"])
        self.assertEqual(len(self.requests()), 2)

    def test_request_generates_an_id_and_explicit_read_execution(self):
        client = self.client()
        result = client.request("fleet.status", {}, dry_run=False)
        self.assertRegex(result["request_id"], r"^sdk_[a-f0-9]{32}$")
        self.assertTrue(result["executed"])
        self.assertEqual(client.actor, {"id": "python-sdk", "kind": "service"})
        changed = client.actor
        changed["id"] = "mutated"
        self.assertEqual(client.actor_id, "python-sdk")

    def test_execute_preserves_a_complete_request_actor(self):
        request = {"schema_version": 1, "request_id": "complete-001",
                   "actor": {"id": "supplied-actor", "kind": "user"},
                   "action": "fleet.status", "payload": {}, "dry_run": False}
        result = self.client().execute(MappingProxyType(request))
        self.assertEqual(result["result"]["received"], request)

    def test_mapping_payload_is_supported(self):
        result = self.client().request("fleet.status", MappingProxyType({"text": "中文"}))
        self.assertEqual(result["result"]["received"]["payload"], {"text": "中文"})

    def test_structured_gateway_error_is_preserved_without_retry(self):
        with self.assertRaises(CCSError) as caught:
            self.client("error").request("fleet.status", {}, request_id="error-001")
        error = caught.exception
        self.assertEqual(error.code, "SSH_TIMEOUT")
        self.assertEqual(error.message, "server did not answer")
        self.assertTrue(error.retryable)
        self.assertEqual(error.request_id, "error-001")
        self.assertEqual(len(self.requests()), 1)

    def test_mutation_without_confirmation_is_not_automatically_approved(self):
        with self.assertRaises(CCSError) as caught:
            self.client().request("run.submit", {"idempotency_key": "one"}, dry_run=False)
        self.assertEqual(caught.exception.code, "CONFIRMATION_REQUIRED")
        self.assertFalse(caught.exception.retryable)
        self.assertEqual(len(self.requests()), 1)

    def test_timeout_warns_that_side_effect_may_already_have_executed(self):
        effect = self.root / "effect.txt"
        with self.assertRaises(CCSError) as caught:
            self.client("timeout", timeout=0.6).request(
                "run.submit", {"effect_path": str(effect)}, dry_run=False,
                request_id="timeout-001",
            )
        self.assertEqual(caught.exception.code, "GATEWAY_TIMEOUT")
        self.assertFalse(caught.exception.retryable)
        self.assertEqual(caught.exception.request_id, "timeout-001")
        self.assertIn("may already have executed", caught.exception.message)
        self.assertEqual(effect.read_text(encoding="utf-8"), "executed")
        self.assertEqual(len(self.requests()), 1)

    def test_invalid_output_and_exit_mismatches_never_report_success(self):
        for mode in ("non_json", "invalid_utf8", "mismatched_id", "mismatched_data_id",
                     "missing_data", "bad_shape", "bad_classification", "nan_result",
                     "mismatched_action", "mismatched_dry_run", "mismatched_executed",
                     "success_bad_exit", "error_bad_exit"):
            with self.subTest(mode=mode):
                with self.assertRaises(CCSError) as caught:
                    self.client(mode).request("fleet.status", {}, request_id="bad-output")
                self.assertEqual(caught.exception.code, "INVALID_GATEWAY_RESPONSE")
                self.assertEqual(caught.exception.request_id, "bad-output")

    def test_missing_executable_is_a_transport_error(self):
        client = CCSClient([str(self.root / "missing-ccsctl")])
        with self.assertRaises(CCSError) as caught:
            client.request("fleet.status", {}, request_id="unavailable-001")
        self.assertEqual(caught.exception.code, "GATEWAY_UNAVAILABLE")
        self.assertEqual(caught.exception.request_id, "unavailable-001")

    def test_command_must_be_argument_sequence_and_is_snapshotted(self):
        for command in ("ccsctl --host x", b"ccsctl", [], ["ccsctl", 1], [""]):
            with self.subTest(command=command):
                with self.assertRaises(ValueError):
                    CCSClient(command)
        command = ["ccsctl", "--fleet", "pool.json"]
        client = CCSClient(command)
        command.append("changed")
        self.assertEqual(client.command, ("ccsctl", "--fleet", "pool.json"))

    def test_invalid_local_request_never_starts_a_gateway(self):
        client = self.client()
        for values in ({"request_id": "bad space"}, {"dry_run": "true"},
                       {"payload": {"invalid": float("nan")}}, {"payload": []}):
            with self.subTest(values=values):
                with self.assertRaises(CCSError) as caught:
                    client.request("fleet.status", **{"payload": {}, **values})
                self.assertEqual(caught.exception.code, "INVALID_REQUEST")
        self.assertFalse(self.record.exists())

    def test_oversized_utf8_request_is_rejected_before_starting_gateway(self):
        with self.assertRaises(CCSError) as caught:
            self.client().request("workspace.request", {"content": "中" * (7 * 1024 * 1024)},
                                  request_id="oversized-001")
        self.assertEqual(caught.exception.code, "REQUEST_TOO_LARGE")
        self.assertEqual(caught.exception.request_id, "oversized-001")
        self.assertFalse(caught.exception.retryable)
        self.assertFalse(self.record.exists())

    def test_plan_constructor_rejects_nonmatching_results(self):
        plan = self.client().plan("fleet.status", {}, request_id="plan-shape")
        for change in ({"dry_run": False}, {"executed": True},
                       {"request_id": "other-id"}, {"plan_sha256": "invalid"}):
            with self.subTest(change=change):
                with self.assertRaises(CCSError):
                    CCSPlan(plan.request, {**plan.data, **change})
        with self.assertRaises(TypeError):
            self.client().confirm({"plan_sha256": "0" * 64})


if __name__ == "__main__":
    unittest.main()
