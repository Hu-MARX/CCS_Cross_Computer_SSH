from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
import json
import math
import re
import ntpath
from pathlib import PurePosixPath
from typing import Any
from uuid import uuid4


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class RunStatus(StrEnum):
    QUEUED = "QUEUED"
    PREPARING = "PREPARING"
    RUNNING = "RUNNING"
    EVALUATING = "EVALUATING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    LOST = "LOST"

    @property
    def terminal(self) -> bool:
        return self in {self.SUCCEEDED, self.FAILED, self.CANCELLED}


_TRANSITIONS: dict[RunStatus, frozenset[RunStatus]] = {
    RunStatus.QUEUED: frozenset({RunStatus.PREPARING, RunStatus.CANCELLED}),
    RunStatus.PREPARING: frozenset({RunStatus.RUNNING, RunStatus.FAILED, RunStatus.CANCELLED}),
    RunStatus.RUNNING: frozenset({RunStatus.EVALUATING, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.LOST}),
    RunStatus.EVALUATING: frozenset({RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}),
    RunStatus.LOST: frozenset({RunStatus.RUNNING, RunStatus.FAILED}),
    RunStatus.SUCCEEDED: frozenset(),
    RunStatus.FAILED: frozenset(),
    RunStatus.CANCELLED: frozenset(),
}


class DomainError(ValueError):
    pass


class StudyStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    BLOCKED = "BLOCKED"
    STOPPING = "STOPPING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class StageStatus(StrEnum):
    PENDING = "PENDING"
    BLOCKED = "BLOCKED"
    READY = "READY"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def terminal(self) -> bool:
        return self in {self.SUCCEEDED, self.FAILED, self.CANCELLED}


class RecommendationDecision(StrEnum):
    PROCEED = "PROCEED"
    WAIT = "WAIT"
    STOP = "STOP"


_STAGE_ID = re.compile(r"[a-z][a-z0-9_-]{0,63}")


@dataclass(frozen=True)
class ArtifactBindingSpec:
    role: str
    from_stage: str
    path: str

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ArtifactBindingSpec":
        if not isinstance(raw, dict) or set(raw) != {"role", "from_stage", "selector"}:
            raise DomainError("invalid artifact binding shape")
        role = raw.get("role")
        source = raw.get("from_stage")
        selector = raw.get("selector")
        if role != "checkpoint":
            raise DomainError("artifact binding role must be checkpoint in schema v1")
        if not isinstance(source, str) or not _STAGE_ID.fullmatch(source):
            raise DomainError("artifact binding from_stage must be a valid stage id")
        if not isinstance(selector, dict) or set(selector) != {"path"}:
            raise DomainError("artifact binding selector must contain only path")
        path = selector.get("path")
        if (not isinstance(path, str) or not 1 <= len(path) <= 4096 or "\x00" in path or
                not path.replace("${RUN_ID}", "run_0").startswith("/")):
            raise DomainError("artifact binding selector.path must be an absolute POSIX path")
        if "${" in path.replace("${RUN_ID}", ""):
            raise DomainError("artifact binding selector.path only supports ${RUN_ID}")
        return cls(role, source, path)

    def to_dict(self) -> dict[str, Any]:
        return {"role": self.role, "from_stage": self.from_stage, "selector": {"path": self.path}}


@dataclass(frozen=True)
class StageSpec:
    id: str
    name: str
    kind: str
    experiment: "ExperimentSpec"
    depends_on: tuple[str, ...] = ()
    requires_approval: bool = False
    bindings: tuple[ArtifactBindingSpec, ...] = ()

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "StageSpec":
        if not isinstance(raw, dict) or set(raw) - {
            "id", "name", "kind", "spec", "depends_on", "requires_approval", "bindings"
        }:
            raise DomainError("invalid StageSpec shape")
        stage_id = raw.get("id")
        name = raw.get("name")
        kind = raw.get("kind")
        dependencies = raw.get("depends_on", [])
        approval = raw.get("requires_approval", False)
        bindings_raw = raw.get("bindings", [])
        if not isinstance(stage_id, str) or not _STAGE_ID.fullmatch(stage_id):
            raise DomainError("stage.id must match [a-z][a-z0-9_-]{0,63}")
        if not isinstance(name, str) or not name.strip() or len(name) > 200:
            raise DomainError("stage.name must contain 1-200 characters")
        if not isinstance(kind, str):
            raise DomainError("stage.kind is required")
        if (not isinstance(dependencies, list) or len(dependencies) > 64 or
                any(not isinstance(item, str) or not _STAGE_ID.fullmatch(item) for item in dependencies) or
                len(set(dependencies)) != len(dependencies)):
            raise DomainError("stage.depends_on must contain unique valid stage ids")
        if not isinstance(approval, bool):
            raise DomainError("stage.requires_approval must be a boolean")
        if not isinstance(bindings_raw, list) or len(bindings_raw) > 32:
            raise DomainError("stage.bindings must be a bounded list")
        bindings = tuple(ArtifactBindingSpec.from_dict(item) for item in bindings_raw)
        if len({item.role for item in bindings}) != len(bindings):
            raise DomainError("stage artifact binding roles must be unique")
        experiment = ExperimentSpec.from_dict(raw.get("spec"))
        task_kind = (experiment.task or {}).get("kind")
        if task_kind != kind:
            raise DomainError("stage.kind must equal spec.task.kind")
        if kind == "test":
            if not approval:
                raise DomainError("test stages require explicit approval")
            checkpoint = (experiment.task or {}).get("checkpoint")
            checkpoint_bindings = [item for item in bindings if item.role == "checkpoint"]
            if bool(checkpoint) == bool(checkpoint_bindings):
                raise DomainError("test stages require exactly one static or bound checkpoint")
        return cls(stage_id, name.strip(), kind, experiment, tuple(dependencies), approval, bindings)

    def to_dict(self) -> dict[str, Any]:
        result = {
            "id": self.id,
            "name": self.name,
            "kind": self.kind,
            "spec": self.experiment.to_dict(),
            "depends_on": list(self.depends_on),
            "requires_approval": self.requires_approval,
        }
        if self.bindings:
            result["bindings"] = [item.to_dict() for item in self.bindings]
        return result


@dataclass(frozen=True)
class StudySpec:
    schema_version: int
    name: str
    research_question: str
    stages: tuple[StageSpec, ...]
    planning: dict[str, Any] = field(default_factory=dict)
    budget: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "StudySpec":
        if not isinstance(raw, dict) or set(raw) - {
            "schema_version", "name", "research_question", "stages", "planning", "budget"
        }:
            raise DomainError("invalid StudySpec shape")
        if raw.get("schema_version") != 1:
            raise DomainError("unsupported StudySpec schema_version")
        name = raw.get("name")
        question = raw.get("research_question")
        if not isinstance(name, str) or not name.strip() or len(name) > 200:
            raise DomainError("study.name must contain 1-200 characters")
        if not isinstance(question, str) or not question.strip() or len(question) > 4000:
            raise DomainError("study.research_question must contain 1-4000 characters")
        planning = raw.get("planning", {})
        budget = raw.get("budget", {})
        if not isinstance(planning, dict) or not isinstance(budget, dict):
            raise DomainError("study.planning and study.budget must be objects")
        if set(budget) - {"max_runs"}:
            raise DomainError("study.budget only supports max_runs in schema v1")
        stages_raw = raw.get("stages")
        if not isinstance(stages_raw, list) or not 1 <= len(stages_raw) <= 64:
            raise DomainError("study.stages must contain 1-64 stages")
        stages = tuple(StageSpec.from_dict(item) for item in stages_raw)
        ids = [stage.id for stage in stages]
        if len(set(ids)) != len(ids):
            raise DomainError("stage ids must be unique")
        known = set(ids)
        for stage in stages:
            if stage.id in stage.depends_on:
                raise DomainError("a stage cannot depend on itself")
            missing = set(stage.depends_on) - known
            if missing:
                raise DomainError("stage dependency does not exist: " + sorted(missing)[0])
            for binding in stage.bindings:
                if binding.from_stage not in stage.depends_on:
                    raise DomainError("artifact binding source must be a direct stage dependency")
        cls._validate_acyclic(stages)
        max_runs = budget.get("max_runs", len(stages))
        if isinstance(max_runs, bool) or not isinstance(max_runs, int) or not 1 <= max_runs <= 1024:
            raise DomainError("study.budget.max_runs must be an integer between 1 and 1024")
        budget = {"max_runs": max_runs}
        spec = cls(1, name.strip(), question.strip(), stages, planning, budget)
        try:
            encoded = json.dumps(spec.to_dict(), ensure_ascii=False, sort_keys=True, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise DomainError("StudySpec must contain finite JSON values") from exc
        if len(encoded) > 1024 * 1024:
            raise DomainError("StudySpec exceeds 1 MiB")
        return spec

    @staticmethod
    def _validate_acyclic(stages: tuple[StageSpec, ...]) -> None:
        dependencies = {stage.id: set(stage.depends_on) for stage in stages}
        ready = [stage.id for stage in stages if not dependencies[stage.id]]
        visited = 0
        while ready:
            current = ready.pop()
            visited += 1
            for stage_id, required in dependencies.items():
                if current in required:
                    required.remove(current)
                    if not required:
                        ready.append(stage_id)
        if visited != len(stages):
            raise DomainError("study stage dependencies contain a cycle")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "research_question": self.research_question,
            "planning": self.planning,
            "budget": self.budget,
            "stages": [stage.to_dict() for stage in self.stages],
        }


@dataclass(frozen=True)
class Recommendation:
    schema_version: int
    idempotency_key: str
    stage_id: str
    plan_sha256: str
    artifact_bindings_sha256: str | None
    proposer: dict[str, str]
    decision: RecommendationDecision
    summary: str
    rationale: str
    confidence: float
    skills_used: tuple[str, ...]
    evidence: tuple[dict[str, str], ...]
    limitations: tuple[str, ...]

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Recommendation":
        required = {
            "schema_version", "idempotency_key", "stage_id", "plan_sha256", "proposer",
            "decision", "summary", "rationale", "confidence", "skills_used", "evidence", "limitations",
        }
        optional = {"artifact_bindings_sha256"}
        if (not isinstance(raw, dict) or not required.issubset(raw) or set(raw) - required - optional or
                raw.get("schema_version") != 1):
            raise DomainError("invalid Recommendation shape or schema_version")
        key = raw.get("idempotency_key")
        stage_id = raw.get("stage_id")
        plan_sha256 = raw.get("plan_sha256")
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", key):
            raise DomainError("invalid Recommendation idempotency_key")
        if not isinstance(stage_id, str) or not _STAGE_ID.fullmatch(stage_id):
            raise DomainError("invalid Recommendation stage_id")
        if not isinstance(plan_sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", plan_sha256):
            raise DomainError("invalid Recommendation plan_sha256")
        artifact_bindings_sha256 = raw.get("artifact_bindings_sha256")
        if (artifact_bindings_sha256 is not None and
                (not isinstance(artifact_bindings_sha256, str) or
                 not re.fullmatch(r"[a-f0-9]{64}", artifact_bindings_sha256))):
            raise DomainError("invalid Recommendation artifact_bindings_sha256")
        proposer = raw.get("proposer")
        if (not isinstance(proposer, dict) or set(proposer) != {"id", "kind"} or
                proposer.get("kind") not in {"agent", "human", "pipeline"} or
                not isinstance(proposer.get("id"), str) or
                not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", proposer["id"])):
            raise DomainError("invalid Recommendation proposer")
        try:
            decision = RecommendationDecision(raw.get("decision"))
        except ValueError as exc:
            raise DomainError("Recommendation decision must be PROCEED, WAIT, or STOP") from exc
        summary = raw.get("summary")
        rationale = raw.get("rationale")
        confidence = raw.get("confidence")
        if not isinstance(summary, str) or not summary.strip() or len(summary) > 1000:
            raise DomainError("Recommendation summary must contain 1-1000 characters")
        if not isinstance(rationale, str) or not rationale.strip() or len(rationale) > 4000:
            raise DomainError("Recommendation rationale must contain 1-4000 characters")
        if (isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or
                not math.isfinite(confidence) or not 0 <= confidence <= 1):
            raise DomainError("Recommendation confidence must be between 0 and 1")
        skills = raw.get("skills_used")
        if (not isinstance(skills, list) or len(skills) > 32 or len(set(skills)) != len(skills) or
                any(not isinstance(item, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", item)
                    for item in skills)):
            raise DomainError("Recommendation skills_used must be a bounded unique string list")
        evidence = raw.get("evidence")
        if not isinstance(evidence, list) or len(evidence) > 64:
            raise DomainError("Recommendation evidence must be a bounded list")
        normalized_evidence = []
        for item in evidence:
            if (not isinstance(item, dict) or set(item) != {
                    "run_id", "result_sha256", "observed_at", "observation_state"
            } or not isinstance(item.get("run_id"), str) or
                    not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", item["run_id"]) or
                    not isinstance(item.get("result_sha256"), str) or
                    not re.fullmatch(r"[a-f0-9]{64}", item["result_sha256"]) or
                    item.get("observation_state") not in {"fresh", "stale"} or
                    not isinstance(item.get("observed_at"), str)):
                raise DomainError("invalid Recommendation evidence reference")
            try:
                observed_at = datetime.fromisoformat(item["observed_at"].replace("Z", "+00:00"))
            except ValueError as exc:
                raise DomainError("invalid Recommendation evidence observed_at") from exc
            if observed_at.tzinfo is None:
                raise DomainError("Recommendation evidence observed_at must include a timezone")
            normalized_evidence.append(dict(item))
        limitations = raw.get("limitations")
        if (not isinstance(limitations, list) or len(limitations) > 32 or
                any(not isinstance(item, str) or not item.strip() or len(item) > 1000 for item in limitations)):
            raise DomainError("Recommendation limitations must be a bounded string list")
        recommendation = cls(
            1, key, stage_id, plan_sha256, artifact_bindings_sha256, dict(proposer), decision,
            summary.strip(), rationale.strip(),
            float(confidence), tuple(skills), tuple(normalized_evidence), tuple(item.strip() for item in limitations),
        )
        try:
            encoded = json.dumps(recommendation.to_dict(), ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise DomainError("Recommendation must contain finite JSON values") from exc
        if len(encoded) > 128 * 1024:
            raise DomainError("Recommendation exceeds 128 KiB")
        return recommendation

    def to_dict(self) -> dict[str, Any]:
        result = {
            "schema_version": self.schema_version,
            "idempotency_key": self.idempotency_key,
            "stage_id": self.stage_id,
            "plan_sha256": self.plan_sha256,
            "proposer": self.proposer,
            "decision": self.decision.value,
            "summary": self.summary,
            "rationale": self.rationale,
            "confidence": self.confidence,
            "skills_used": list(self.skills_used),
            "evidence": [dict(item) for item in self.evidence],
            "limitations": list(self.limitations),
        }
        if self.artifact_bindings_sha256 is not None:
            result["artifact_bindings_sha256"] = self.artifact_bindings_sha256
        return result


@dataclass(frozen=True)
class ExperimentSpec:
    schema_version: int
    name: str
    project: dict[str, Any]
    runtime: dict[str, Any]
    command: dict[str, Any]
    resources: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, Any] = field(default_factory=dict)
    evaluation: dict[str, Any] = field(default_factory=dict)
    success: dict[str, Any] = field(default_factory=dict)
    automation: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] | None = None
    task: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ExperimentSpec":
        if not isinstance(raw, dict):
            raise DomainError("ExperimentSpec must be an object")
        spec = cls(
            schema_version=int(raw.get("schema_version", 0)),
            name=str(raw.get("name", "")),
            project=dict(raw.get("project") or {}),
            runtime=dict(raw.get("runtime") or {}),
            command=dict(raw.get("command") or {}),
            resources=dict(raw.get("resources") or {}),
            outputs=dict(raw.get("outputs") or {}),
            evaluation=dict(raw.get("evaluation") or {}),
            success=dict(raw.get("success") or {}),
            automation=dict(raw.get("automation") or {}),
            evidence=raw.get('evidence'),
            task=raw.get('task'),
        )
        spec.validate()
        return spec

    def validate(self) -> None:
        if self.schema_version != 1:
            raise DomainError("unsupported ExperimentSpec schema_version")
        if not self.name.strip():
            raise DomainError("name is required")
        if self.task is not None:
            from .results import validate_task
            validate_task(self.task)
        argv = self.command.get("argv")
        if not isinstance(argv, list) or not argv or any(not isinstance(x, str) or not x for x in argv):
            raise DomainError("command.argv must be a non-empty list of non-empty strings")
        if self.command.get("shell") is True:
            raise DomainError("shell commands are not permitted; use command.argv")
        evaluation_argv = self.evaluation.get("argv")
        if evaluation_argv is not None and (not isinstance(evaluation_argv, list) or not evaluation_argv or
                                             any(not isinstance(x, str) or not x for x in evaluation_argv)):
            raise DomainError("evaluation.argv must be a non-empty list of non-empty strings")
        max_duration = self.resources.get("max_duration_seconds")
        if max_duration is not None and (isinstance(max_duration, bool) or not isinstance(max_duration, (int, float))
                                         or not math.isfinite(max_duration) or max_duration <= 0):
            raise DomainError("resources.max_duration_seconds must be positive")
        gpu = self.resources.get('gpu_index')
        if gpu is not None and (isinstance(gpu, bool) or not isinstance(gpu, int) or gpu < 0):
            raise DomainError('resources.gpu_index must be a nonnegative integer')
        queue = self.resources.get('max_queue_seconds')
        if queue is not None and (isinstance(queue, bool) or not isinstance(queue, (int, float))
                                  or not math.isfinite(queue) or queue <= 0):
            raise DomainError('resources.max_queue_seconds must be positive')
        max_attempts = self.automation.get("max_attempts", 1)
        if not isinstance(max_attempts, int) or max_attempts < 1:
            raise DomainError("automation.max_attempts must be a positive integer")
        allowed = self.automation.get("allowed_actions", [])
        if not isinstance(allowed, list) or any(action not in ALLOWED_ACTIONS for action in allowed):
            raise DomainError("automation.allowed_actions contains an unsupported action")
        if self.evidence is not None:
            binding = self.evidence
            if not isinstance(binding, dict) or set(binding) - {'bundle_id', 'manifest_sha256', 'timeout_seconds', 'execution_mode'}:
                raise DomainError('Invalid evidence binding')
            if binding.get('execution_mode', 'source') not in ('source', 'snapshot'):
                raise DomainError('evidence.execution_mode must be source or snapshot')
            for key, pattern in [('bundle_id', r'bundle_[a-f0-9]{32}'), ('manifest_sha256', r'[a-f0-9]{64}')]:
                if not isinstance(binding.get(key), str) or not re.fullmatch(pattern, binding[key]):
                    raise DomainError('Invalid evidence.' + key)
            timeout = binding.get('timeout_seconds', 600)
            if type(timeout) is not int or not 1 <= timeout <= 3600:
                raise DomainError('evidence.timeout_seconds must be between 1 and 3600')
            absolute = lambda value: PurePosixPath(value).is_absolute() or ntpath.isabs(value)
            index = 1
            while index < len(argv) and argv[index] in ('-u', '-B'):
                index += 1
            if not absolute(argv[0]) or index >= len(argv) or not absolute(argv[index]) or not argv[index].endswith('.py'):
                raise DomainError('Bound runs require absolute Python and .py entry paths (optional -u/-B)')
            if self.evaluation.get('argv'):
                raise DomainError('Bound runs currently require evaluation inside the same Python entry')

    def to_dict(self) -> dict[str, Any]:
        result = {
            "schema_version": self.schema_version,
            "name": self.name,
            "project": self.project,
            "runtime": self.runtime,
            "command": self.command,
            "resources": self.resources,
            "outputs": self.outputs,
            "evaluation": self.evaluation,
            "success": self.success,
            "automation": self.automation,
        }
        if self.evidence is not None:
            result['evidence'] = self.evidence
        if self.task is not None:
            result['task'] = self.task
        return result


@dataclass
class Run:
    id: str
    spec: ExperimentSpec
    status: RunStatus = RunStatus.QUEUED
    worker: str | None = None
    parent_run_id: str | None = None
    idempotency_key: str | None = None
    attempt: int = 1
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    started_at: str | None = None
    finished_at: str | None = None
    exit_code: int | None = None

    @classmethod
    def new(cls, spec: ExperimentSpec, *, worker: str | None = None, parent_run_id: str | None = None,
            idempotency_key: str | None = None, attempt: int = 1) -> "Run":
        return cls(
            id=f"run_{uuid4().hex}", spec=spec, worker=worker, parent_run_id=parent_run_id,
            idempotency_key=idempotency_key, attempt=attempt,
        )

    def transition(self, target: RunStatus, *, exit_code: int | None = None, at: str | None = None) -> None:
        if target == self.status:
            return
        if target not in _TRANSITIONS[self.status]:
            raise DomainError(f"invalid run transition: {self.status} -> {target}")
        now = at or utc_now()
        self.status = target
        self.updated_at = now
        if target == RunStatus.RUNNING and self.started_at is None:
            self.started_at = now
        if target.terminal:
            self.finished_at = now
        if exit_code is not None:
            self.exit_code = exit_code


@dataclass(frozen=True)
class RunEvent:
    run_id: str
    seq: int
    kind: str
    payload: dict[str, Any]
    created_at: str = field(default_factory=utc_now)
    remote_seq: int | None = None


ALLOWED_ACTIONS = frozenset({
    "wait", "evaluate", "retry_same", "resume_checkpoint", "run_test",
    "request_approval", "complete", "stop_workflow",
})


@dataclass(frozen=True)
class Decision:
    schema_version: int
    classification: str
    summary: str
    evidence: tuple[str, ...]
    next_action: dict[str, Any]
    confidence: float
    requires_approval: bool

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Decision":
        action = raw.get("next_action")
        evidence = raw.get("evidence")
        if raw.get("schema_version") != 1 or not isinstance(action, dict) or not isinstance(evidence, list):
            raise DomainError("invalid Decision shape")
        action_type = action.get("type")
        if action_type not in ALLOWED_ACTIONS:
            raise DomainError("Decision contains unsupported next_action.type")
        confidence = raw.get("confidence")
        if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise DomainError("Decision confidence must be between 0 and 1")
        return cls(
            schema_version=1,
            classification=str(raw.get("classification", "")),
            summary=str(raw.get("summary", "")),
            evidence=tuple(str(item) for item in evidence),
            next_action=action,
            confidence=float(confidence),
            requires_approval=bool(raw.get("requires_approval", False)),
        )
