import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path
from unittest.mock import patch

from controller.domain import ExperimentSpec, Run
from controller.fleet import WorkerBinding, WorkerDirectory
from controller.orchestrator import Orchestrator
from controller.remote import FakeRemoteRunner, RemoteRunnerError
from controller.repository import RunRepository, SubmissionError


def spec(name='demo'):
    return ExperimentSpec.from_dict({
        'schema_version': 1, 'name': name, 'project': {}, 'runtime': {},
        'command': {'argv': ['python', 'train.py']},
    })


class CountingRemote(FakeRemoteRunner):
    def __init__(self, *, blocked=False):
        super().__init__()
        self.submit_count = 0
        self.lookup_count = 0
        self.entered = threading.Event()
        self.release = threading.Event()
        if not blocked:
            self.release.set()

    def submit(self, value, key):
        self.submit_count += 1
        self.entered.set()
        if not self.release.wait(5):
            raise AssertionError('test dispatch gate timed out')
        return super().submit(value, key)

    def lookup_submission(self, key):
        self.lookup_count += 1
        return self.keys.get(key)


class SubmissionOperationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / 'state.db'
        self.repository = RunRepository(self.database)
        self.remote = CountingRemote()
        self.orchestrator = Orchestrator(self.repository, self.remote, worker='worker-a')

    def test_same_worker_and_spec_concurrent_callers_only_one_dispatches(self):
        remote = CountingRemote(blocked=True)
        directory = WorkerDirectory([WorkerBinding('worker-a', remote)])
        barrier = threading.Barrier(2)

        def submit():
            repository = RunRepository(self.database)
            barrier.wait(timeout=3)
            return directory.submit('worker-a', spec(), 'shared-key', repository)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(submit) for _ in range(2)]
            try:
                completed, _ = wait(futures, timeout=3, return_when=FIRST_COMPLETED)
                self.assertEqual(len(completed), 1)
                loser = next(iter(completed))
                with self.assertRaises(SubmissionError) as raised:
                    loser.result()
                self.assertEqual(raised.exception.execution_state, 'unknown')
                self.assertFalse(raised.exception.retryable)
                # A blocked SSH dispatch holds no SQLite write transaction.
                other, available = self.repository.reserve_submission(spec(), 'worker-a', 'other-key')
                self.assertTrue(available)
                self.assertEqual(other['state'], 'pending')
            finally:
                remote.release.set()
            winner = next(future for future in futures if future is not loser).result(timeout=3)
        replay = directory.submit('worker-a', spec(), 'shared-key', RunRepository(self.database))
        self.assertEqual(winner.id, replay.id)
        self.assertEqual(remote.submit_count, 1)
        self.assertEqual(len(remote.runs), 1)

    def test_cross_worker_race_rejects_conflicting_target_before_remote(self):
        remotes = [CountingRemote(blocked=True), CountingRemote(blocked=True)]
        directory = WorkerDirectory([WorkerBinding('worker-a', remotes[0]), WorkerBinding('worker-b', remotes[1])])
        barrier = threading.Barrier(2)

        def submit(worker):
            repository = RunRepository(self.database)
            barrier.wait(timeout=3)
            return directory.submit(worker, spec(), 'shared-key', repository)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(submit, worker) for worker in ('worker-a', 'worker-b')]
            try:
                completed, _ = wait(futures, timeout=3, return_when=FIRST_COMPLETED)
                self.assertEqual(len(completed), 1)
                with self.assertRaises(SubmissionError) as raised:
                    next(iter(completed)).result()
                self.assertEqual(raised.exception.code, 'IDEMPOTENCY_CONFLICT')
                self.assertEqual(raised.exception.execution_state, 'not_sent')
                self.assertEqual(sum(remote.submit_count for remote in remotes), 1)
            finally:
                for remote in remotes:
                    remote.release.set()
            winner = next(future for future in futures if future not in completed).result(timeout=3)
        operation = self.repository.get_operation(idempotency_key='shared-key')
        self.assertEqual(operation['state'], 'confirmed')
        self.assertEqual(operation['run_id'], winner.id)
        self.assertEqual(sum(len(remote.runs) for remote in remotes), 1)

    def test_changed_spec_is_rejected_before_remote(self):
        self.orchestrator.submit(spec(), idempotency_key='same')
        with self.assertRaises(SubmissionError) as raised:
            self.orchestrator.submit(spec('different'), idempotency_key='same')
        self.assertEqual(raised.exception.code, 'IDEMPOTENCY_CONFLICT')
        self.assertEqual(self.remote.submit_count, 1)

    def test_pending_reservation_survives_restart_and_one_caller_can_resume(self):
        pending, _ = self.repository.reserve_submission(spec(), 'worker-a', 'pending')
        restarted = Orchestrator(RunRepository(self.database), self.remote, worker='worker-a')
        unchanged = restarted.reconcile_submission(pending['operation_id'])
        self.assertEqual(unchanged['state'], 'pending')
        self.assertEqual(self.remote.lookup_count, 0)
        run = restarted.submit(spec(), idempotency_key='pending')
        self.assertEqual(self.repository.get_operation(idempotency_key='pending')['run_id'], run.id)
        self.assertEqual(self.remote.submit_count, 1)

    def test_crash_after_started_marker_never_dispatches_on_replay(self):
        operation, _ = self.repository.reserve_submission(spec(), 'worker-a', 'started')
        self.assertTrue(self.repository.begin_submission(operation['operation_id']))
        restarted = Orchestrator(RunRepository(self.database), self.remote, worker='worker-a')
        with self.assertRaises(SubmissionError):
            restarted.submit(spec(), idempotency_key='started')
        reconciled = restarted.reconcile_submission(operation['operation_id'])
        self.assertEqual(reconciled['state'], 'unknown')
        self.assertEqual(reconciled['error_code'], 'SUBMISSION_NOT_OBSERVED')
        with self.assertRaises(SubmissionError):
            restarted.submit(spec(), idempotency_key='started')
        self.assertEqual(self.remote.submit_count, 0)

    def test_remote_accepted_then_control_process_exits_is_recovered_by_lookup(self):
        operation, _ = self.repository.reserve_submission(spec(), 'worker-a', 'exit')
        self.repository.begin_submission(operation['operation_id'])
        accepted = self.remote.submit(spec(), 'exit')
        # No local acceptance write occurred before the simulated process exit.
        restarted = Orchestrator(RunRepository(self.database), self.remote, worker='worker-a')
        reconciled = restarted.reconcile_submission(operation['operation_id'])
        self.assertEqual(reconciled['state'], 'confirmed')
        self.assertEqual(reconciled['run_id'], accepted)
        self.assertEqual(restarted.submit(spec(), idempotency_key='exit').id, accepted)
        self.assertEqual(self.remote.submit_count, 1)

    def test_lost_submit_response_is_unknown_and_lookup_recovers_without_dispatch(self):
        original = self.remote.submit

        def lost(value, key):
            original(value, key)
            raise RemoteRunnerError('SSH_TIMEOUT', 'response was lost', retryable=True)

        with patch.object(self.remote, 'submit', side_effect=lost):
            with self.assertRaises(SubmissionError) as raised:
                self.orchestrator.submit(spec(), idempotency_key='lost')
        error = raised.exception
        self.assertEqual(error.execution_state, 'unknown')
        self.assertEqual(error.worker_id, 'worker-a')
        self.assertFalse(error.retryable)
        operation = Orchestrator(RunRepository(self.database), self.remote, worker='worker-a').reconcile_submission(
            error.operation_id)
        self.assertEqual(operation['state'], 'confirmed')
        self.assertEqual(self.remote.submit_count, 1)

    def test_local_failure_rolls_back_run_event_and_confirmation_then_recovers(self):
        with closing(sqlite3.connect(self.database)) as db, db:
            db.execute("CREATE TRIGGER fail_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'disk fault'); END")
        with self.assertRaises(SubmissionError) as raised:
            self.orchestrator.submit(spec(), idempotency_key='local-fail')
        error = raised.exception
        self.assertEqual(error.code, 'LOCAL_COMMIT_FAILED')
        self.assertEqual(error.execution_state, 'unknown')
        self.assertIsNotNone(error.run_id)
        self.assertEqual(self.repository.list_runs(), [])
        operation = self.repository.get_operation(operation_id=error.operation_id)
        self.assertEqual(operation['state'], 'unknown')
        with closing(sqlite3.connect(self.database)) as db, db:
            db.execute('DROP TRIGGER fail_event')
        restarted = Orchestrator(RunRepository(self.database), self.remote, worker='worker-a')
        first = restarted.reconcile_submission(error.operation_id)
        second = restarted.reconcile_submission(error.operation_id)
        self.assertEqual(first['state'], 'confirmed')
        self.assertEqual(first['run_id'], second['run_id'])
        self.assertEqual(len(self.repository.events(first['run_id'])), 1)
        self.assertEqual(self.remote.submit_count, 1)

    def test_db_marker_failure_does_not_hide_accepted_run_or_enable_replay(self):
        with patch.object(self.repository, 'confirm_submission', side_effect=OSError('disk fault')), \
                patch.object(self.repository, 'mark_operation', side_effect=OSError('disk fault')):
            with self.assertRaises(SubmissionError) as raised:
                self.orchestrator.submit(spec(), idempotency_key='markers')
        self.assertEqual(raised.exception.execution_state, 'unknown')
        self.assertIsNotNone(raised.exception.run_id)
        self.assertEqual(self.repository.get_operation(idempotency_key='markers')['state'], 'started')
        result = Orchestrator(RunRepository(self.database), self.remote, worker='worker-a').reconcile_submission(
            idempotency_key='markers')
        self.assertEqual(result['state'], 'confirmed')
        self.assertEqual(self.remote.submit_count, 1)

    def test_explicit_not_sent_can_resume_but_confirmed_rejection_cannot(self):
        for state in ('not_sent', 'rejected'):
            with self.subTest(state=state):
                remote_error = RemoteRunnerError('NO_SEND', 'no submission accepted')
                remote_error.execution_state = state
                with patch.object(self.remote, 'submit', side_effect=remote_error):
                    with self.assertRaises(SubmissionError) as raised:
                        self.orchestrator.submit(spec(), idempotency_key=state)
                self.assertEqual(raised.exception.execution_state, state)
                self.assertEqual(self.repository.get_operation(idempotency_key=state)['state'], state)
                if state == 'not_sent':
                    self.orchestrator.submit(spec(), idempotency_key=state)
                else:
                    with self.assertRaises(SubmissionError):
                        self.orchestrator.submit(spec(), idempotency_key=state)
        self.assertEqual(self.remote.submit_count, 1)

    def test_unsupported_lookup_retains_unknown_and_does_not_submit(self):
        operation, _ = self.repository.reserve_submission(spec(), 'worker-a', 'unsupported')
        self.repository.begin_submission(operation['operation_id'])
        with patch.object(self.remote, 'lookup_submission', side_effect=RemoteRunnerError(
                'WORKER_INCOMPATIBLE', 'run.lookup unavailable', retryable=False)):
            result = self.orchestrator.reconcile_submission(operation['operation_id'])
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(result['error_code'], 'WORKER_INCOMPATIBLE')
        self.assertEqual(self.remote.submit_count, 0)

    def test_request_aliases_and_changed_intent_are_recorded_without_task_contents(self):
        actor = {'id': 'agent', 'kind': 'agent'}
        run = self.orchestrator.submit(spec(), idempotency_key='identity', request_id='first', actor=actor,
                                       plan_sha256='a' * 64)
        self.orchestrator.submit(spec(), idempotency_key='identity', request_id='second', actor=actor,
                                 plan_sha256='b' * 64)
        first = self.repository.get_operation(request_id='first', actor=actor)
        second = self.repository.get_operation(request_id='second')
        self.assertEqual(first['operation_id'], second['operation_id'])
        self.assertEqual(first['run_id'], run.id)
        self.assertNotIn('spec_json', first)
        self.assertNotIn('command', json.dumps(first))
        with self.assertRaises(SubmissionError) as raised:
            self.orchestrator.submit(spec(), idempotency_key='new-key', request_id='first', actor=actor,
                                     plan_sha256='a' * 64)
        self.assertEqual(raised.exception.code, 'OPERATION_CONFLICT')
        self.assertEqual(raised.exception.operation_id, first['operation_id'])
        self.assertIsNone(self.repository.get_operation(idempotency_key='new-key'))
        self.assertEqual(self.remote.submit_count, 1)

    def test_canonical_spec_digest_ignores_mapping_order(self):
        first = spec()
        first.runtime.update({'python': 'python', 'environment': {'B': '2', 'A': '1'}})
        second = spec()
        second.runtime.update({'environment': {'A': '1', 'B': '2'}, 'python': 'python'})
        initial = self.orchestrator.submit(first, idempotency_key='canonical')
        replay = self.orchestrator.submit(second, idempotency_key='canonical')
        self.assertEqual(initial.id, replay.id)
        self.assertEqual(self.remote.submit_count, 1)

    def test_generic_mutation_request_replay_does_not_grant_dispatch(self):
        actor = {'id': 'agent', 'kind': 'agent'}
        first, dispatch = self.repository.start_operation('run.cancel', 'cancel', actor, 'worker-a', 'a' * 64)
        self.assertTrue(dispatch)
        self.assertEqual(first['state'], 'started')
        second, dispatch = RunRepository(self.database).start_operation('run.cancel', 'cancel', actor, 'worker-a', 'a' * 64)
        self.assertFalse(dispatch)
        self.assertEqual(first['operation_id'], second['operation_id'])
        with self.assertRaises(SubmissionError) as raised:
            self.repository.start_operation('run.cancel', 'cancel', actor, 'worker-b', 'a' * 64)
        self.assertEqual(raised.exception.code, 'OPERATION_CONFLICT')
        self.repository.mark_operation(first['operation_id'], 'not_sent', error_code='AUDIT_UNAVAILABLE')
        resumed, dispatch = self.repository.start_operation('run.cancel', 'cancel', actor, 'worker-a', 'a' * 64)
        self.assertTrue(dispatch)
        self.repository.mark_operation(first['operation_id'], 'confirmed')
        unchanged = self.repository.mark_operation(first['operation_id'], 'unknown', error_code='AUDIT_UNAVAILABLE')
        self.assertEqual(unchanged['state'], 'confirmed')

    def test_legacy_database_migrates_and_replay_checks_worker_and_spec(self):
        legacy = self.repository.create_run(Run.new(spec(), worker='worker-a', idempotency_key='legacy'))
        with closing(sqlite3.connect(self.database)) as db, db:
            db.execute('DROP TABLE operation_requests')
            db.execute('DROP TABLE operations')
            db.execute('DROP TABLE repository_migrations')
        migrated = RunRepository(self.database)
        operation = migrated.get_operation(idempotency_key='legacy')
        self.assertEqual(operation['state'], 'confirmed')
        self.assertEqual(operation['run_id'], legacy.id)
        restarted = Orchestrator(migrated, self.remote, worker='worker-a')
        self.assertEqual(restarted.submit(spec(), idempotency_key='legacy').id, legacy.id)
        with self.assertRaises(SubmissionError):
            restarted.submit(spec('changed'), idempotency_key='legacy')
        with self.assertRaises(SubmissionError):
            Orchestrator(migrated, self.remote, worker='worker-b').submit(spec(), idempotency_key='legacy')
        self.assertEqual(self.remote.submit_count, 0)
        self.assertEqual(len(migrated.list_runs()), 1)


if __name__ == '__main__':
    unittest.main()
