import io
import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import time
import tracemalloc
import unittest
from contextlib import redirect_stdout
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from controller.capabilities import worker_interface
from controller.domain import ExperimentSpec, RunStatus
from controller.orchestrator import Orchestrator
from controller.pagination import artifact_cursor, artifact_after, PageItemTooLarge
from controller.remote import FakeRemoteRunner, OpenSSHRunner, RemoteEvent, RemoteRunnerError
from controller.repository import RunRepository
from trainctl.cli import main
from trainctl.run_store import RunStore, TrainCtlError


RUN_ID = 'run_' + 'a' * 32


def make_store(directory):
    store = RunStore(Path(directory) / 'store')
    run_dir = store.runs / RUN_ID
    run_dir.mkdir()
    store._atomic_json(run_dir / 'spec.json', {
        'schema_version': 1, 'name': 'pages', 'project': {},
        'runtime': {'workdir': str(Path(directory).absolute())},
        'command': {'argv': ['python', 'train.py']}, 'outputs': {'artifact_glob': '*.ckpt'},
    })
    store._atomic_json(run_dir / 'manifest.json', {'submission_cwd': str(Path(directory).absolute())})
    return store, run_dir


def write_events(run_dir, count):
    with (run_dir / 'events.jsonl').open('wb') as stream:
        for seq in range(1, count + 1):
            stream.write((json.dumps({'seq': seq, 'kind': 'progress', 'payload': {'step': seq}}) + '\n').encode())


class PagedReadTests(unittest.TestCase):
    def test_one_representative_scale_and_reconnect_tail(self):
        with tempfile.TemporaryDirectory() as directory:
            store, run_dir = make_store(directory)
            # The delivery batch uses a small regression fixture. The one-off
            # representative measurement is explicit, avoiding repeated suites.
            target = os.environ.get('CCS_P1_SCALE_REPORT')
            count = 100000 if target else 2000
            write_events(run_dir, count)
            started = time.perf_counter()
            first = store.events_page(RUN_ID, limit=1000)
            migration_seconds = time.perf_counter() - started
            seen = [event['seq'] for event in first['events']]
            page = first
            while page['has_more']:
                # A new client/store continues from the last delivered cursor.
                store = RunStore(store.root)
                page = store.events_page(RUN_ID, page['next_cursor'], limit=1000)
                seen.extend(event['seq'] for event in page['events'])
            self.assertEqual(seen, list(range(1, count + 1)))
            real_loads = json.loads
            with patch('trainctl.event_index.json.loads', wraps=real_loads) as loads:
                tracemalloc.start()
                started = time.perf_counter()
                tail = store.events_page(RUN_ID, count - 10, limit=5)
                tail_seconds = time.perf_counter() - started
                peak_bytes = tracemalloc.get_traced_memory()[1]
                tracemalloc.stop()
                parsed = loads.call_count
            self.assertEqual(parsed, 5)
            self.assertEqual([event['seq'] for event in tail['events']], list(range(count - 9, count - 4)))
            self.assertTrue(tail['has_more'])
            self.assertLess(peak_bytes, 2 * 1024 * 1024)
            with patch('trainctl.event_index.json.loads', wraps=real_loads) as loads:
                store._append_event(run_dir, 'terminal', {'status': 'SUCCEEDED'})
                self.assertEqual(loads.call_count, 0)
            final = store.events_page(RUN_ID, count)
            self.assertEqual(final['events'][0]['kind'], 'terminal')
            self.assertFalse(final['has_more'])
            report = {'events': count, 'jsonl_bytes': (run_dir / 'events.jsonl').stat().st_size,
                      'migration_seconds': migration_seconds, 'tail_seconds': tail_seconds,
                      'tail_parsed_events': parsed, 'tail_peak_bytes': peak_bytes,
                      'all_sequences_delivered_once': True, 'append_history_parses': 0}
            if target:
                Path(target).write_text(json.dumps(report, indent=2), encoding='utf-8')

    def test_index_recovers_full_append_and_incomplete_fragment(self):
        with tempfile.TemporaryDirectory() as directory:
            store, run_dir = make_store(directory)
            write_events(run_dir, 2)
            store.events_page(RUN_ID)
            # A complete fsynced record survived before index commit.
            with (run_dir / 'events.jsonl').open('ab') as stream:
                stream.write(b'{"seq":3,"kind":"terminal","payload":{}}\n')
            self.assertEqual(store.events_page(RUN_ID, 2)['next_cursor'], 3)
            with (run_dir / 'events.jsonl').open('ab') as stream:
                stream.write(b'{"seq":4,"kind":"interrupted')
            self.assertEqual(store.events_page(RUN_ID, 3)['events'], [])
            store._append_event(run_dir, 'recovered', {})
            self.assertEqual(store.events_page(RUN_ID, 3)['events'][0]['seq'], 4)
            fragments = list(run_dir.glob('events-fragment-*.bin'))
            self.assertEqual(len(fragments), 1)
            self.assertIn(b'interrupted', fragments[0].read_bytes())

    def test_event_byte_budget_does_not_advance_past_undelivered_item(self):
        with tempfile.TemporaryDirectory() as directory:
            store, run_dir = make_store(directory)
            for _ in range(4):
                store._append_event(run_dir, 'payload', {'value': '中' * 200})
            page = store.events_page(RUN_ID, limit=4, max_bytes=1024)
            self.assertEqual(page['next_cursor'], 1)
            self.assertTrue(page['truncated'])
            self.assertTrue(page['has_more'])
            resumed = store.events_page(RUN_ID, page['next_cursor'], limit=4)
            self.assertEqual([item['seq'] for item in resumed['events']], [2, 3, 4])
            store._append_event(run_dir, 'large', {'value': 'x' * 2000})
            with self.assertRaises(TrainCtlError) as raised:
                store.events_page(RUN_ID, 4, max_bytes=1024)
            self.assertEqual(raised.exception.code, 'PAGE_ITEM_TOO_LARGE')
            self.assertIn('after_seq unchanged', raised.exception.message)

    def test_artifact_keyset_cursor_scope_and_only_requested_path_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            store, _ = make_store(directory)
            for name in ['b.ckpt', 'a.ckpt', 'c.ckpt']:
                (Path(directory) / name).write_bytes(name.encode())
            real_hash = hashlib.sha256
            def cursor_hash_only(value=None):
                if value is None:
                    raise AssertionError('inventory created file digest')
                return real_hash(value)
            with patch('trainctl.run_store.hashlib.sha256', side_effect=cursor_hash_only):
                first = store.artifacts_page(RUN_ID, limit=2)
                last = store.artifacts_page(RUN_ID, cursor=first['next_cursor'], limit=2)
            self.assertEqual([Path(item['path']).name for item in first['artifacts']], ['a.ckpt', 'b.ckpt'])
            self.assertEqual([Path(item['path']).name for item in last['artifacts']], ['c.ckpt'])
            self.assertFalse(last['has_more'])
            with self.assertRaises(ValueError):
                store.artifacts_page(RUN_ID, path=str(Path(directory) / 'b.ckpt'), cursor=first['next_cursor'])
            with self.assertRaises(ValueError):
                store.artifacts_page(RUN_ID, cursor=artifact_cursor('other-run', None, 'path'))
            selected = store.artifacts_page(RUN_ID, path=str(Path(directory) / 'a.ckpt'))
            self.assertEqual(len(selected['artifacts'][0]['sha256']), 64)

    def test_oversized_item_has_machine_readable_code_and_long_exact_path_cursor_fits(self):
        remote = FakeRemoteRunner()
        remote.runs[RUN_ID] = {'events': [RemoteEvent(1, 'large', {'value': 'x' * 2000})]}
        with self.assertRaises(PageItemTooLarge) as error:
            remote.events_page(RUN_ID, 0, max_bytes=1024)
        self.assertEqual(error.exception.code, 'PAGE_ITEM_TOO_LARGE')
        path = '/' + 'a' * 4000
        cursor = artifact_cursor(RUN_ID, path, path)
        self.assertLess(len(cursor), 8192)
        self.assertEqual(artifact_after(cursor, RUN_ID, path), path)

    def test_worker_cli_page_envelope_and_legacy_jsonl_compatibility(self):
        with tempfile.TemporaryDirectory() as directory:
            store, run_dir = make_store(directory)
            write_events(run_dir, 3)
            output = io.StringIO()
            with redirect_stdout(output):
                code = main(['--root', str(store.root), 'events', RUN_ID, '--page', '--limit', '2'])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output.getvalue())['data']['next_cursor'], 2)
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(main(['--root', str(store.root), 'events', RUN_ID, '--after-seq', '2', '--jsonl']), 0)
            self.assertEqual(json.loads(output.getvalue())['seq'], 3)

    def test_watermark_migrates_existing_db_and_never_scans_history_on_poll(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'runs.db'
            repository = RunRepository(database)
            repository.append_event('run-one', 'remote', {}, remote_seq=17)
            with closing(sqlite3.connect(database)) as db:
                db.execute('DELETE FROM repository_migrations WHERE version=2')
                db.execute('DELETE FROM remote_event_watermarks')
                db.commit()
            repository = RunRepository(database)
            self.assertEqual(repository.remote_event_watermark('run-one'), 17)
            with patch.object(repository, 'events', side_effect=AssertionError('history scanned')):
                self.assertEqual(Orchestrator(repository, FakeRemoteRunner())._last_remote_seq('run-one'), 17)
            repository.append_event('run-one', 'remote', {}, remote_seq=18)
            repository.append_event('run-one', 'duplicate', {}, remote_seq=18)
            self.assertEqual(repository.remote_event_watermark('run-one'), 18)

    def test_terminal_reconcile_drains_bounded_pages_before_local_terminal(self):
        remote = FakeRemoteRunner()
        repository = RunRepository(':memory:')
        orchestrator = Orchestrator(repository, remote, event_page_size=2, max_event_pages=1)
        spec = ExperimentSpec.from_dict({'schema_version': 1, 'name': 'terminal', 'project': {},
                                         'runtime': {}, 'command': {'argv': ['python', 'train.py']}})
        run = orchestrator.submit(spec, idempotency_key='terminal-pages')
        remote.runs[run.id]['events'].extend(RemoteEvent(seq, 'progress', {}) for seq in range(2, 8))
        remote.advance(run.id, RunStatus.SUCCEEDED, exit_code=0)
        orchestrator.reconcile()
        self.assertFalse(repository.get_run(run.id).status.terminal)
        for _ in range(5):
            orchestrator.reconcile()
        self.assertEqual(repository.get_run(run.id).status, RunStatus.SUCCEEDED)
        self.assertEqual([event.remote_seq for event in repository.events(run.id) if event.remote_seq], list(range(1, 9)))

    def test_openssh_pages_negotiate_and_validate_sequence(self):
        calls = []
        def transport(argv, **kwargs):
            calls.append(argv[-1])
            data = ({'ccs_interface': worker_interface()} if ' health ' in ' ' + argv[-1] + ' ' else
                    {'events': [{'seq': 2, 'kind': 'progress', 'payload': {}}],
                     'next_cursor': 2, 'has_more': False, 'truncated': False})
            return SimpleNamespace(returncode=0, stdout=json.dumps({'ok': True, 'data': data}), stderr='')
        page = OpenSSHRunner('worker', command_runner=transport).events_page(RUN_ID, 1, limit=3)
        self.assertEqual(page['events'][0].seq, 2)
        self.assertIn('--page --limit 3', calls[-1])
        with patch('controller.remote.OpenSSHRunner.require_capabilities'):
            runner = OpenSSHRunner('worker', command_runner=lambda *a, **k: SimpleNamespace(
                returncode=0, stdout=json.dumps({'ok': True, 'data': {
                    'events': [], 'next_cursor': 8, 'has_more': True, 'truncated': False}}), stderr=''))
            with self.assertRaises(RemoteRunnerError):
                runner.events_page(RUN_ID, 1)

    def test_observation_deadline_is_shared_and_cancel_prevents_new_command(self):
        captured = []
        def transport(argv, **kwargs):
            captured.append(kwargs['timeout'])
            return SimpleNamespace(returncode=0, stdout='{"ok":true,"data":{}}', stderr='')
        runner = OpenSSHRunner('worker', command_runner=transport)
        cancelled = threading.Event()
        with runner.observation_budget(.5, cancelled):
            runner.health()
            cancelled.set()
            with self.assertRaises(RemoteRunnerError):
                runner.processes()
        self.assertLessEqual(captured[0], .5)
        self.assertEqual(len(captured), 1)

    def test_bounded_ssh_output_for_started_mutation_is_unknown(self):
        from ccs.process import ProcessStopped
        runner = OpenSSHRunner('worker')
        with patch('ccs.process.run_bounded', side_effect=ProcessStopped('OUTPUT_LIMIT', started=True)):
            with self.assertRaises(RemoteRunnerError) as raised:
                runner._json_command(['cancel', RUN_ID, '--reason', 'test'])
        self.assertEqual(raised.exception.code, 'SSH_OUTPUT_LIMIT')
        self.assertEqual(raised.exception.execution_state, 'unknown')
        self.assertFalse(raised.exception.retryable)
