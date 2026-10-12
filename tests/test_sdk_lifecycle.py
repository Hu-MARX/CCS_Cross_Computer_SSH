"""Bounded process lifetime and resumable delivered cursors; no remote workers."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ccs import CCSClient, CCSError, CCSPlan, CCSReadCursor
from ccs.process import ProcessStopped, run_bounded
import ccs.process as process_module
from test_client import FAKE_GATEWAY


def alive(pid):
    if os.name == 'nt':
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            return False
        status = wintypes.DWORD()
        try:
            return bool(kernel.GetExitCodeProcess(handle, ctypes.byref(status))) and status.value == 259
        finally:
            kernel.CloseHandle(handle)
    try:
        if sys.platform.startswith('linux'):
            stat = Path(f'/proc/{pid}/stat')
            if stat.exists() and stat.read_text().rpartition(')')[2].lstrip().startswith('Z'):
                return False
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='ccs lifecycle 中文 ')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_stdout_and_stderr_overflow_retains_only_budget(self):
        code = "import os,threading; t=threading.Thread(target=lambda:os.write(2,b'e'*2000000)); t.start(); os.write(1,b'o'*2000000); t.join()"
        with self.assertRaises(ProcessStopped) as caught:
            run_bounded([sys.executable, '-c', code], timeout=5,
                        max_stdout_bytes=4096, max_stderr_bytes=2048)
        failure = caught.exception
        self.assertEqual(failure.code, 'OUTPUT_LIMIT')
        self.assertTrue(failure.started)
        self.assertLessEqual(len(failure.stdout), 4096)
        self.assertLessEqual(len(failure.stderr), 2048)

    def test_cancel_before_spawn_is_not_sent(self):
        cancel = threading.Event()
        cancel.set()
        with patch('ccs.process.subprocess.Popen') as spawn:
            with self.assertRaises(ProcessStopped) as caught:
                run_bounded([sys.executable, '-c', 'pass'], cancel_event=cancel)
        spawn.assert_not_called()
        self.assertFalse(caught.exception.started)

    def test_sdk_active_cancel_and_overload_kill_local_descendants_preserve_unknown(self):
        marker = self.root / 'pids.json'
        code = ("import json,os,subprocess,sys,time; from pathlib import Path; "
                "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
                "Path(sys.argv[1]+'.tmp').write_text(json.dumps([os.getpid(),child.pid])); "
                "os.replace(sys.argv[1]+'.tmp',sys.argv[1]); request=json.load(sys.stdin); time.sleep(60)")
        sdk = CCSClient([sys.executable, '-c', code, str(marker)], timeout=20,
                        max_concurrent=1, max_queued=0)
        cancel = threading.Event()
        errors = []

        def request():
            try:
                sdk.request('run.submit', {'worker_id': 'lab118', 'idempotency_key': 'one'},
                            dry_run=False, request_id='started', cancel_event=cancel)
            except CCSError as exc:
                errors.append(exc)

        original_job_init = process_module._WindowsJob.__init__

        def delayed_assignment(job, process):
            time.sleep(0.1)
            self.assertFalse(marker.exists(), 'gateway executed before containment was established')
            original_job_init(job, process)

        thread = threading.Thread(target=request)
        with patch('ccs.process._WindowsJob.__init__', delayed_assignment):
            thread.start()
            deadline = time.monotonic() + 5
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.02)
        self.addCleanup(cancel.set)
        self.assertTrue(marker.exists())
        pids = json.loads(marker.read_text())
        with self.assertRaises(CCSError) as caught:
            sdk.request('run.submit', {}, dry_run=False, request_id='never-sent')
        self.assertEqual(caught.exception.code, 'GATEWAY_BUSY')
        self.assertEqual(caught.exception.execution_state, 'not_sent')
        cancel.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors[0].code, 'GATEWAY_CANCELLED')
        self.assertEqual(errors[0].execution_state, 'unknown')
        self.assertEqual(errors[0].idempotency_key, 'one')
        for pid in pids:
            self.assertFalse(alive(pid), f'local descendant {pid} survived cancellation')

    def test_postspawn_reader_start_failure_is_unknown_and_kills_child(self):
        children = []
        original_spawn = process_module.subprocess.Popen

        def spawn(*args, **kwargs):
            child = original_spawn(*args, **kwargs)
            children.append(child)
            return child

        with patch('ccs.process.subprocess.Popen', spawn), patch('ccs.process.threading.Thread.start',
                                                               side_effect=OSError('no reader resources')):
            with self.assertRaises(CCSError) as caught:
                CCSClient([sys.executable, '-c', 'import time; time.sleep(30)']).request(
                    'run.submit', {'worker_id': 'lab118', 'idempotency_key': 'same'}, dry_run=False)
        self.assertEqual(caught.exception.execution_state, 'unknown')
        self.assertEqual(caught.exception.idempotency_key, 'same')
        self.assertEqual(len(children), 1)
        self.assertFalse(alive(children[0].pid))

    def test_cleanup_failure_before_native_cleanup_still_kills_immediate_descendant(self):
        marker = self.root / 'cleanup-pids.json'
        code = ("import json,os,subprocess,sys; from pathlib import Path; "
                "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'], "
                "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); "
                "Path(sys.argv[1]).write_text(json.dumps([os.getpid(),child.pid])); "
                "sys.stdin.buffer.read(); print('not json')")
        original_group_kill = getattr(os, 'killpg', None)
        kill_attempts = []

        def flaky_group_kill(*args):
            kill_attempts.append(args)
            if len(kill_attempts) == 1:
                raise OSError('injected before group kill')
            return original_group_kill(*args)

        fault = (patch('ccs.process._WindowsJob.close', side_effect=OSError('injected before close'))
                 if os.name == 'nt' else patch('ccs.process.os.killpg', flaky_group_kill))
        with fault:
            with self.assertRaises(CCSError) as caught:
                CCSClient([sys.executable, '-c', code, str(marker)]).request('run.submit', {}, dry_run=False)
        self.assertEqual(caught.exception.execution_state, 'unknown')
        for pid in json.loads(marker.read_text()):
            self.assertFalse(alive(pid), f'descendant {pid} survived failed cleanup wrapper')

    def test_bounded_sdk_response_has_truncation_and_continuation(self):
        code = 'import sys; sys.stdin.buffer.read(); sys.stdout.buffer.write(b"x"*2000000)'
        with self.assertRaises(CCSError) as caught:
            CCSClient([sys.executable, '-c', code], max_response_bytes=4096).request(
                'run.submit', {'idempotency_key': 'same'}, dry_run=False)
        self.assertEqual(caught.exception.code, 'GATEWAY_OUTPUT_LIMIT')
        self.assertTrue(caught.exception.output_truncated)
        self.assertIn('operation.query', caught.exception.continuation)
        self.assertEqual(caught.exception.execution_state, 'unknown')

    def test_cancelled_queued_sdk_request_never_starts_a_child(self):
        marker = self.root / 'started'
        code = ('import sys,time; from pathlib import Path; sys.stdin.buffer.read(); '
                'Path(sys.argv[1]).write_text("started"); time.sleep(30)')
        sdk = CCSClient([sys.executable, '-c', code, str(marker)], max_concurrent=1, max_queued=1)
        first_cancel, queued_cancel = threading.Event(), threading.Event()
        errors = {}

        def call(key, cancellation):
            try:
                sdk.request('run.submit', {}, dry_run=False, request_id=key, cancel_event=cancellation)
            except CCSError as exc:
                errors[key] = exc

        first = threading.Thread(target=call, args=('first', first_cancel))
        queued = threading.Thread(target=call, args=('queued', queued_cancel))
        self.addCleanup(first_cancel.set)
        self.addCleanup(queued_cancel.set)
        first.start()
        deadline = time.monotonic() + 5
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertTrue(marker.exists())
        queued.start()
        with sdk._condition:
            sdk._condition.wait_for(lambda: sdk._queued == 1, timeout=1)
        self.assertEqual(sdk._queued, 1)
        queued_cancel.set()
        queued.join(timeout=2)
        first_cancel.set()
        first.join(timeout=2)
        self.assertFalse(first.is_alive() or queued.is_alive())
        self.assertEqual(errors['queued'].code, 'GATEWAY_CANCELLED')
        self.assertEqual(errors['queued'].execution_state, 'not_sent')
        self.assertEqual(errors['first'].execution_state, 'unknown')

    def test_plan_file_preserves_original_identity_actor_payload_hash_and_refuses_overwrite(self):
        script = self.root / 'gateway.py'
        script.write_text(FAKE_GATEWAY, encoding='utf-8')
        record = self.root / 'requests.jsonl'
        sdk = CCSClient([sys.executable, str(script), 'ok', str(record)], actor_id='original-agent')
        original = sdk.plan('run.submit', {'spec': {'name': '中文'}, 'idempotency_key': 'one'}, request_id='saved')
        path = self.root / '批准 计划.json'
        original.save(path)
        with self.assertRaises(FileExistsError):
            original.save(path)
        loaded = CCSPlan.load(path)
        self.assertEqual(loaded.request, original.request)
        self.assertEqual(loaded.plan_sha256, original.plan_sha256)
        result = sdk.confirm(loaded)
        self.assertEqual(result['result']['received']['actor']['id'], 'original-agent')
        self.assertEqual(result['request_id'], 'saved')
        self.assertEqual(len(record.read_text(encoding='utf-8').splitlines()), 2)

    def test_json_error_message_budget_is_utf8_safe_and_marked(self):
        request = {'request_id': 'error-budget', 'action': 'fleet.status', 'dry_run': False, 'payload': {}}
        response = SimpleNamespace(returncode=1, stdout=json.dumps({
            'ok': False, 'request_id': 'error-budget', 'data': None,
            'error': {'code': 'TEST_ERROR', 'message': '中文' * 100, 'retryable': False},
        }).encode('utf-8'))
        with self.assertRaises(CCSError) as caught:
            CCSClient._response(response, request, max_error_bytes=32)
        self.assertTrue(caught.exception.output_truncated)
        self.assertTrue(caught.exception.message.endswith('[truncated]'))
        self.assertLessEqual(len(caught.exception.message.encode('utf-8')), 44)
        self.assertIn('gateway.audit', caught.exception.continuation)
        response.stdout = json.dumps({
            'ok': False, 'request_id': 'error-budget', 'data': None,
            'error': {'code': 'SSH_OUTPUT_LIMIT', 'message': 'worker capture limit', 'retryable': False,
                      'output_truncated': True, 'continuation': 'Reuse the last event cursor.'},
        }).encode('utf-8')
        with self.assertRaises(CCSError) as propagated:
            CCSClient._response(response, request)
        self.assertTrue(propagated.exception.output_truncated)
        self.assertEqual(propagated.exception.continuation, 'Reuse the last event cursor.')


class PollClient(CCSClient):
    def __init__(self):
        super().__init__([sys.executable])
        self.calls = []
        self.status = 'RUNNING'
        self.events = [{'seq': index, 'kind': 'progress', 'payload': {}} for index in range(1, 5)]

    def request(self, action, payload, **kwargs):
        self.calls.append((action, dict(payload)))
        if action == 'run.snapshot':
            return {'result': {'status': self.status}}
        if action == 'run.events':
            events = [item for item in self.events if item['seq'] > payload['after_seq']]
            page = events[:payload['limit']]
            return {'result': {'events': page, 'next_cursor': page[-1]['seq'] if page else payload['after_seq'],
                               'has_more': len(events) > len(page), 'truncated': False}}
        if action == 'run.logs':
            start = payload['after_offset']
            text = 'abcdef'[start:start + 2]
            end = start + len(text)
            return {'result': {'offset': start, 'text': text, 'next_offset': end,
                               'has_more': end < 6, 'eof': end == 6}}
        raise AssertionError(action)


class PollingTests(unittest.TestCase):
    def test_event_cursor_acknowledges_only_delivered_and_resumes_without_loss(self):
        sdk = PollClient()
        cursor = CCSReadCursor('run.events', 'run1', 'lab118')
        iterator = sdk.iter_events('run1', worker_id='lab118', total_timeout=2, cursor=cursor, limit=3)
        self.assertEqual(next(iterator)['seq'], 1)
        self.assertEqual(cursor.value, 1)
        iterator.close()
        resumed = CCSReadCursor.from_dict(json.loads(json.dumps(cursor.to_dict())))
        self.assertEqual([item['seq'] for item in sdk.iter_events('run1', worker_id='lab118', total_timeout=2,
                                                               cursor=resumed, limit=2)], [2, 3, 4])
        self.assertEqual(resumed.value, 4)

    def test_terminal_event_follow_drains_tail_and_log_cursor_stops_at_delivered_chunk(self):
        sdk = PollClient()
        sdk.status = 'SUCCEEDED'
        cursor = CCSReadCursor('run.events', 'run1')
        self.assertEqual([item['seq'] for item in sdk.iter_events('run1', total_timeout=2, cursor=cursor, follow=True)],
                         [1, 2, 3, 4])
        self.assertGreaterEqual(sum(action == 'run.events' for action, _ in sdk.calls), 2)
        logs = CCSReadCursor('run.logs', 'run1', stream='stdout')
        chunks = sdk.iter_logs('run1', total_timeout=2, cursor=logs)
        self.assertEqual(next(chunks)['text'], 'ab')
        chunks.close()
        self.assertEqual(logs.value, 2)
        self.assertEqual(''.join(page['text'] for page in sdk.iter_logs('run1', total_timeout=2, cursor=logs)), 'cdef')

    def test_wait_total_timeout_and_local_cancel_never_cancel_remote_run(self):
        sdk = PollClient()
        with self.assertRaises(CCSError) as caught:
            sdk.wait_run('run1', total_timeout=0.04, poll_interval=0.01, max_poll_interval=0.02)
        self.assertEqual(caught.exception.code, 'WAIT_TIMEOUT')
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(CCSError) as caught:
            sdk.wait_run('run1', total_timeout=1, cancel_event=cancel)
        self.assertEqual(caught.exception.code, 'WAIT_CANCELLED')
        self.assertTrue(all(action == 'run.snapshot' for action, _ in sdk.calls))

    def test_undelivered_or_invalid_page_does_not_advance_cursor(self):
        class Bad(PollClient):
            def request(self, action, payload, **kwargs):
                return {'result': {'events': [{'seq': 1}], 'next_cursor': 2, 'has_more': False}}
        cursor = CCSReadCursor('run.events', 'run1')
        with self.assertRaises(CCSError):
            next(Bad().iter_events('run1', total_timeout=2, cursor=cursor))
        self.assertEqual(cursor.value, 0)

    def test_only_transient_read_poll_is_retried_under_deadline(self):
        class Transient(PollClient):
            def request(self, action, payload, **kwargs):
                if not self.calls:
                    self.calls.append((action, dict(payload)))
                    raise CCSError('SSH_TIMEOUT', 'read transient', retryable=True)
                self.status = 'SUCCEEDED'
                return super().request(action, payload, **kwargs)
        sdk = Transient()
        self.assertEqual(sdk.wait_run('run1', total_timeout=1)['status'], 'SUCCEEDED')
        self.assertEqual([action for action, _ in sdk.calls], ['run.snapshot', 'run.snapshot'])


if __name__ == '__main__':
    unittest.main()
