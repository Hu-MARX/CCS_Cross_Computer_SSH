from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .domain import Decision, Run


class Verdict(StrEnum):
    AUTO_EXECUTE = "AUTO_EXECUTE"
    NEEDS_APPROVAL = "NEEDS_APPROVAL"
    DENIED = "DENIED"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True)
class PolicyResult:
    verdict: Verdict
    action: str
    reason: str


class PolicyEngine:
    """Deep policy module: keeps decision intent separate from side effects."""

    def evaluate(self, decision: Decision, run: Run) -> PolicyResult:
        action = str(decision.next_action.get("type"))
        allowed = set(run.spec.automation.get("allowed_actions", []))
        if action not in allowed:
            return PolicyResult(Verdict.DENIED, action, "action is not in the ExperimentSpec allow-list")
        if action in {"complete", "stop_workflow"}:
            return PolicyResult(Verdict.COMPLETE, action, "decision ends the workflow")
        required = set(run.spec.automation.get("require_approval", []))
        proposed = decision.next_action.get("params", {}).get("proposed_action", action)
        if decision.requires_approval or action == "request_approval" or proposed in required:
            return PolicyResult(Verdict.NEEDS_APPROVAL, action, "policy requires explicit user approval")
        if action in {"retry_same", "resume_checkpoint"}:
            max_attempts = int(run.spec.automation.get("max_attempts", 1))
            if run.attempt >= max_attempts:
                return PolicyResult(Verdict.DENIED, action, "maximum workflow attempts reached")
        return PolicyResult(Verdict.AUTO_EXECUTE, action, "action is allowed within current policy")
