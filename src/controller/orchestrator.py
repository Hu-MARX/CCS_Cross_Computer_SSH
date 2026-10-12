from __future__ import annotations

from .domain import Decision, Run, RunStatus
from .policy import PolicyEngine, PolicyResult
from .remote import RemoteRunner, RemoteRunnerError
from .repository import RunRepository, SubmissionError


class Orchestrator:
    """Small control-plane interface hiding persistence, event sync and remote details."""

    def __init__(self, repository: RunRepository, remote: RemoteRunner, *, worker: str = "default",
                 policy: PolicyEngine | None = None, event_page_size: int = 1000, max_event_pages: int = 10):
        self.repository = repository
        self.remote = remote
        self.worker = worker
        self.policy = policy or PolicyEngine()
        self.last_errors: list[dict] = []
        if not 1 <= event_page_size <= 1000 or not 1 <= max_event_pages <= 100:
            raise ValueError('invalid reconciliation event page budget')
        self.event_page_size, self.max_event_pages = event_page_size, max_event_pages

    def submit(self, spec, *, idempotency_key: str, request_id=None, actor=None, plan_sha256=None) -> Run:
        operation, _ = self.repository.reserve_submission(
            spec, self.worker, idempotency_key, request_id=request_id, actor=actor, plan_sha256=plan_sha256,
        )
        if operation['state'] == 'confirmed':
            return self.repository.get_run(operation['run_id'])
        if not self.repository.begin_submission(operation['operation_id']):
            operation = self.repository.get_operation(operation_id=operation['operation_id'])
            if operation['state'] == 'confirmed':
                return self.repository.get_run(operation['run_id'])
            raise self._submission_error(
                operation, operation['error_code'] or 'SUBMISSION_OUTCOME_UNKNOWN',
                'submission is already reserved; use operation reconciliation before any further action',
            )
        remote_id = None
        try:
            remote_id = self.remote.submit(spec, idempotency_key)
        except Exception as exc:
            execution_state = getattr(exc, 'execution_state', 'unknown')
            state = execution_state if execution_state in {'not_sent', 'rejected'} else 'unknown'
            self._mark_submission_error(operation, state, getattr(exc, 'code', type(exc).__name__))
            operation.update(state=state, execution_state=state)
            raise self._submission_error(operation, getattr(exc, 'code', 'SUBMISSION_OUTCOME_UNKNOWN'),
                                         getattr(exc, 'message', str(exc))) from exc
        try:
            return self.repository.confirm_submission(operation['operation_id'], remote_id)
        except Exception as exc:
            # A failed local commit cannot undo an already accepted remote Run.
            code = getattr(exc, 'code', 'LOCAL_COMMIT_FAILED')
            self._mark_submission_error(operation, 'unknown', code, run_id=remote_id)
            operation.update(state='unknown', execution_state='unknown', run_id=remote_id)
            raise self._submission_error(operation, code,
                                         'worker accepted submission but local confirmation could not be persisted') from exc

    @staticmethod
    def _submission_error(operation, code, message):
        return SubmissionError(code, message, execution_state=operation['execution_state'],
                               operation_id=operation['operation_id'], worker_id=operation['worker_id'],
                               run_id=operation['run_id'], idempotency_key=operation['idempotency_key'])

    def _mark_submission_error(self, operation, state, code, *, run_id=None):
        try:
            return self.repository.mark_operation(operation['operation_id'], state, run_id=run_id, error_code=code)
        except Exception:
            # The durable started marker still forbids a blind replay after a DB failure.
            return None

    def reconcile_submission(self, operation_id=None, *, idempotency_key=None, request_id=None, actor=None):
        """Read worker acceptance by key and repair local state without dispatching."""
        operation = self.repository.get_operation(operation_id=operation_id, idempotency_key=idempotency_key,
                                                 request_id=request_id, actor=actor)
        if operation is None:
            raise SubmissionError('OPERATION_NOT_FOUND', 'submission operation was not found',
                                  execution_state='not_sent', worker_id=self.worker)
        if operation['action'] != 'run.submit':
            raise self._submission_error(operation, 'RECOVERY_NOT_SUPPORTED',
                                         'this operation requires action-specific manual recovery')
        if operation['worker_id'] != self.worker:
            raise self._submission_error(operation, 'WORKER_CONFLICT', 'operation belongs to a different worker')
        if operation['state'] in {'pending', 'not_sent', 'confirmed', 'rejected'}:
            return operation
        operation = self.repository.mark_operation(operation['operation_id'], 'unknown',
                                                   error_code=operation['error_code'])
        lookup = getattr(self.remote, 'lookup_submission', None)
        if lookup is None:
            return self.repository.mark_operation(operation['operation_id'], 'unknown',
                                                  error_code='SUBMISSION_LOOKUP_UNSUPPORTED')
        try:
            remote_id = lookup(operation['idempotency_key'])
        except Exception as exc:
            return self.repository.mark_operation(operation['operation_id'], 'unknown',
                                                  error_code=getattr(exc, 'code', 'SUBMISSION_LOOKUP_FAILED'))
        if remote_id is None:
            return self.repository.mark_operation(operation['operation_id'], 'unknown',
                                                  error_code='SUBMISSION_NOT_OBSERVED')
        try:
            self.repository.confirm_submission(operation['operation_id'], remote_id)
        except Exception as exc:
            self._mark_submission_error(operation, 'unknown', getattr(exc, 'code', 'LOCAL_COMMIT_FAILED'), run_id=remote_id)
            operation.update(state='unknown', execution_state='unknown', run_id=remote_id)
            raise self._submission_error(operation, getattr(exc, 'code', 'LOCAL_COMMIT_FAILED'),
                                         'accepted submission could not be reconciled with local state') from exc
        return self.repository.get_operation(operation_id=operation['operation_id'])

    def reconcile(self) -> list[Run]:
        updated: list[Run] = []
        self.last_errors = []
        for run in self.repository.list_active():
            if run.worker != self.worker:
                continue
            try:
                caught_up = self._sync_events(run.id)
                snapshot = self.remote.snapshot(run.id)
                if snapshot.status.terminal:
                    # A final event may have arrived after the first event fetch.
                    if not caught_up or not self._sync_events(run.id):
                        # Keep the local Run active so the next pass drains the
                        # remaining terminal tail before it leaves list_active.
                        continue
            except RemoteRunnerError as exc:
                error = {"run_id": run.id, "code": exc.code, "message": exc.message, "retryable": exc.retryable}
                self.last_errors.append(error)
                self.repository.append_event(run.id, "sync_failed", error)
                continue
            if snapshot.status != run.status:
                self._reconcile_status(run, snapshot.status, snapshot.exit_code)
                self.repository.update_run(run)
                self.repository.append_event(run.id, "reconciled", {
                    "status": snapshot.status.value, "exit_code": snapshot.exit_code,
                })
                updated.append(run)
        return updated

    @staticmethod
    def _reconcile_status(run: Run, target: RunStatus, exit_code: int | None) -> None:
        if target not in {RunStatus.PREPARING, RunStatus.RUNNING, RunStatus.EVALUATING,
                          RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.LOST}:
            raise ValueError(f"cannot reconcile to unsupported status: {target}")
        if target in {RunStatus.SUCCEEDED, RunStatus.EVALUATING, RunStatus.RUNNING}:
            lifecycle = [RunStatus.PREPARING, RunStatus.RUNNING, RunStatus.EVALUATING, RunStatus.SUCCEEDED]
            target_index = lifecycle.index(target)
            if run.status == RunStatus.LOST:
                run.transition(RunStatus.RUNNING)
            start_index = lifecycle.index(run.status) + 1 if run.status in lifecycle else 0
            if start_index > target_index + 1:
                raise ValueError(f"remote status regressed: {run.status} -> {target}")
            for status in lifecycle[start_index:target_index + 1]:
                if status != run.status:
                    run.transition(status, exit_code=exit_code if status == target else None)
            return
        if target == RunStatus.FAILED:
            if run.status == RunStatus.QUEUED:
                run.transition(RunStatus.PREPARING)
            if run.status != target:
                run.transition(target, exit_code=exit_code)
            return
        if target == RunStatus.LOST:
            if run.status == RunStatus.QUEUED:
                run.transition(RunStatus.PREPARING)
            if run.status == RunStatus.PREPARING:
                run.transition(RunStatus.RUNNING)
            if run.status != target:
                run.transition(target)
            return
        if run.status != target:
            run.transition(target, exit_code=exit_code)

    def cancel(self, run_id: str, reason: str) -> Run:
        run = self.repository.get_run(run_id)
        if run.worker != self.worker:
            raise ValueError('run belongs to a different worker')
        snapshot = self.remote.cancel(run_id, reason)
        if run.status != snapshot.status:
            self._reconcile_status(run, snapshot.status, snapshot.exit_code)
        self.repository.update_run(run)
        self.repository.append_event(run_id, "cancel_requested", {"reason": reason})
        return run

    def decide(self, run_id: str, raw_decision: dict) -> PolicyResult:
        run = self.repository.get_run(run_id)
        decision = Decision.from_dict(raw_decision)
        self.repository.record_decision(run_id, raw_decision)
        self.repository.append_event(run_id, "decision_recorded", {
            "classification": decision.classification, "action": decision.next_action.get("type"),
            "confidence": decision.confidence,
        })
        result = self.policy.evaluate(decision, run)
        self.repository.append_event(run_id, "policy_evaluated", {
            "verdict": result.verdict.value, "action": result.action, "reason": result.reason,
        })
        return result

    def _last_remote_seq(self, run_id: str) -> int:
        return self.repository.remote_event_watermark(run_id)

    def _sync_events(self, run_id: str) -> bool:
        paged = getattr(self.remote, 'events_page', None)
        if paged is not None:
            for _ in range(self.max_event_pages):
                after = self._last_remote_seq(run_id)
                try:
                    page = paged(run_id, after, limit=self.event_page_size)
                except RemoteRunnerError as exc:
                    if exc.code not in {'WORKER_CAPABILITIES_UNDECLARED', 'WORKER_INCOMPATIBLE'}:
                        raise
                    # Rolling upgrades retain the old read-only JSONL seam;
                    # transport output budgets still apply to this fallback.
                    break
                if page['has_more'] and (not page['events'] or page['next_cursor'] <= after):
                    raise RemoteRunnerError('INVALID_REMOTE_RESPONSE', 'event page did not advance', retryable=False)
                for event in page['events']:
                    self.repository.append_event(run_id, event.kind, event.payload, remote_seq=event.seq)
                if not page['has_more']:
                    return True
            else:
                return False
        for event in self.remote.events(run_id, self._last_remote_seq(run_id)):
            self.repository.append_event(run_id, event.kind, event.payload, remote_seq=event.seq)
        return True
