"""Research planning and claim-evidence contracts.

This module deliberately does not submit remote work. It validates the scientific
inputs that an upper agent must establish before it crosses the CCS JSON seam.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


MAX_DOCUMENT_BYTES = 2 * 1024 * 1024
PROJECT_ID = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
CLAIM_ID = re.compile(r"^claim-[a-z0-9][a-z0-9_-]{0,63}$")
SHA256 = re.compile(r"^[a-f0-9]{64}$")
PLACEHOLDER_MARKERS = ("AUTHOR_INPUT_NEEDED", "CHANGE_ME", "TBD", "TODO")


class ResearchContractError(ValueError):
    """Raised when a research contract is malformed or unsafe to resolve."""


@dataclass(frozen=True)
class AuditReport:
    kind: str
    valid: bool
    ready: bool
    issues: tuple[str, ...]
    summary: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "valid": self.valid,
            "ready": self.ready,
            "issues": list(self.issues),
            "summary": dict(self.summary),
        }


def _load_json(path: Path) -> dict[str, Any]:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise ResearchContractError(f"cannot read {path}: {exc}") from exc
    if size > MAX_DOCUMENT_BYTES:
        raise ResearchContractError(f"document exceeds {MAX_DOCUMENT_BYTES} bytes: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ResearchContractError(f"document must be UTF-8 JSON: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ResearchContractError(f"document must contain one JSON object: {path}")
    return value


def _require_exact_keys(value: Mapping[str, Any], required: set[str], optional: set[str], label: str) -> None:
    missing = sorted(required - set(value))
    extra = sorted(set(value) - required - optional)
    if missing or extra:
        raise ResearchContractError(f"{label} keys invalid; missing={missing}, extra={extra}")


def _require_text(value: Any, label: str, *, maximum: int = 8000) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ResearchContractError(f"{label} must be a non-empty string of at most {maximum} characters")
    return value.strip()


def _require_string_list(value: Any, label: str, *, minimum: int = 0, maximum: int = 128) -> list[str]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise ResearchContractError(f"{label} must contain {minimum}-{maximum} strings")
    result = [_require_text(item, f"{label}[{index}]", maximum=1000) for index, item in enumerate(value)]
    if len(result) != len(set(result)):
        raise ResearchContractError(f"{label} must not contain duplicates")
    return result


def _placeholder_paths(value: Any, prefix: str = "$") -> list[str]:
    paths: list[str] = []
    if isinstance(value, str):
        if any(marker in value.upper() for marker in PLACEHOLDER_MARKERS):
            paths.append(prefix)
    elif isinstance(value, dict):
        for key, item in value.items():
            paths.extend(_placeholder_paths(item, f"{prefix}.{key}"))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            paths.extend(_placeholder_paths(item, f"{prefix}[{index}]"))
    return paths


def _resolve_relative(root: Path, raw_path: Any, label: str) -> Path:
    text = _require_text(raw_path, label, maximum=4096)
    relative = Path(text)
    if relative.is_absolute():
        raise ResearchContractError(f"{label} must be relative to the workspace root")
    root = root.resolve()
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ResearchContractError(f"{label} escapes the workspace root") from exc
    return resolved


def _stable_sha256(path: Path) -> str:
    """Hash a potentially large artifact without accepting a changing file."""
    try:
        before = path.stat()
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        after = path.stat()
    except OSError as exc:
        raise ResearchContractError(f"cannot hash evidence {path}: {exc}") from exc
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ResearchContractError(f"evidence changed while hashing: {path}")
    return digest.hexdigest()


def audit_project(path: str | Path, *, workspace_root: str | Path = ".") -> AuditReport:
    """Validate a research project contract and report whether planning may start."""
    project_path = Path(path)
    root = Path(workspace_root)
    value = _load_json(project_path)
    required = {
        "schema_version", "project_id", "title", "research_question", "hypothesis",
        "scope_boundary", "worker_pool", "analysis_contract", "literature",
        "manuscript", "budget", "ethics",
    }
    _require_exact_keys(value, required, set(), "research project")
    if value["schema_version"] != 1:
        raise ResearchContractError("research project schema_version must be 1")
    project_id = _require_text(value["project_id"], "project_id", maximum=64)
    if not PROJECT_ID.fullmatch(project_id):
        raise ResearchContractError("project_id must match ^[a-z][a-z0-9_-]{0,63}$")
    for field in ("title", "research_question", "hypothesis", "scope_boundary"):
        _require_text(value[field], field)

    worker_pool = value["worker_pool"]
    if not isinstance(worker_pool, dict):
        raise ResearchContractError("worker_pool must be an object")
    _require_exact_keys(worker_pool, {"config_path", "selection_policy", "expected_workers"}, set(), "worker_pool")
    if worker_pool["selection_policy"] != "upper_agent_explicit":
        raise ResearchContractError("worker_pool.selection_policy must be upper_agent_explicit")
    expected_workers = _require_string_list(worker_pool["expected_workers"], "worker_pool.expected_workers", minimum=1, maximum=64)
    pool_path = _resolve_relative(root, worker_pool["config_path"], "worker_pool.config_path")

    analysis = value["analysis_contract"]
    if not isinstance(analysis, dict):
        raise ResearchContractError("analysis_contract must be an object")
    analysis_fields = {
        "independent_unit", "primary_outcome", "effect_size", "uncertainty_interval",
        "multiplicity", "replication", "randomization", "blinding", "exclusions",
        "missing_data", "held_out_test",
    }
    _require_exact_keys(analysis, analysis_fields, set(), "analysis_contract")
    for field in analysis_fields - {"replication"}:
        _require_text(analysis[field], f"analysis_contract.{field}")
    replication = analysis["replication"]
    if not isinstance(replication, dict):
        raise ResearchContractError("analysis_contract.replication must be an object")
    _require_exact_keys(replication, {"independent_runs", "seed_policy"}, set(), "analysis_contract.replication")
    if not (
        isinstance(replication["independent_runs"], int)
        and not isinstance(replication["independent_runs"], bool)
        and replication["independent_runs"] >= 1
    ) and not isinstance(replication["independent_runs"], str):
        raise ResearchContractError("independent_runs must be a positive integer or an explicit input marker")
    _require_text(replication["seed_policy"], "analysis_contract.replication.seed_policy")

    literature = value["literature"]
    if not isinstance(literature, dict):
        raise ResearchContractError("literature must be an object")
    _require_exact_keys(literature, {"profile_path", "raw_archive"}, set(), "literature")
    literature_profile = _resolve_relative(root, literature["profile_path"], "literature.profile_path")
    _resolve_relative(root, literature["raw_archive"], "literature.raw_archive")

    manuscript = value["manuscript"]
    if not isinstance(manuscript, dict):
        raise ResearchContractError("manuscript must be an object")
    _require_exact_keys(manuscript, {"paper_type", "source_language", "target_journal"}, set(), "manuscript")
    if manuscript["paper_type"] not in {"research", "methods", "hypothesis", "algorithmic", "review"}:
        raise ResearchContractError("manuscript.paper_type is unsupported")
    if manuscript["source_language"] not in {"en", "zh-to-en"}:
        raise ResearchContractError("manuscript.source_language must be en or zh-to-en")
    _require_text(manuscript["target_journal"], "manuscript.target_journal")

    budget = value["budget"]
    if not isinstance(budget, dict):
        raise ResearchContractError("budget must be an object")
    _require_exact_keys(budget, {"max_runs", "max_gpu_hours", "max_storage_gb"}, set(), "budget")
    for field, amount in budget.items():
        if not (isinstance(amount, (int, float)) and not isinstance(amount, bool) and amount > 0) and not isinstance(amount, str):
            raise ResearchContractError(f"budget.{field} must be positive or an explicit input marker")

    ethics = value["ethics"]
    if not isinstance(ethics, dict):
        raise ResearchContractError("ethics must be an object")
    _require_exact_keys(ethics, {"review_state", "sensitive_data"}, set(), "ethics")
    _require_text(ethics["review_state"], "ethics.review_state")
    if not isinstance(ethics["sensitive_data"], bool):
        raise ResearchContractError("ethics.sensitive_data must be boolean")

    issues = [f"unresolved placeholder at {item}" for item in _placeholder_paths(value)]
    if not pool_path.is_file():
        issues.append(f"worker pool file is missing: {pool_path}")
    else:
        pool = _load_json(pool_path)
        workers = pool.get("workers")
        actual = {
            item.get("id") for item in workers if isinstance(item, dict) and isinstance(item.get("id"), str)
        } if isinstance(workers, list) else set()
        missing_workers = sorted(set(expected_workers) - actual)
        if missing_workers:
            issues.append(f"worker pool is missing expected workers: {missing_workers}")
    if not literature_profile.is_file():
        issues.append(f"literature profile is missing: {literature_profile}")

    return AuditReport(
        kind="research_project",
        valid=True,
        ready=not issues,
        issues=tuple(issues),
        summary={
            "project_id": project_id,
            "expected_workers": expected_workers,
            "paper_type": manuscript["paper_type"],
            "source_language": manuscript["source_language"],
            "target_journal": manuscript["target_journal"],
        },
    )


def audit_claim_ledger(path: str | Path, *, workspace_root: str | Path = ".") -> AuditReport:
    """Validate manuscript claims and verify every referenced evidence digest."""
    ledger_path = Path(path)
    root = Path(workspace_root)
    value = _load_json(ledger_path)
    _require_exact_keys(value, {"schema_version", "study_id", "claims"}, set(), "claim ledger")
    if value["schema_version"] != 1:
        raise ResearchContractError("claim ledger schema_version must be 1")
    study_id = _require_text(value["study_id"], "study_id", maximum=128)
    claims = value["claims"]
    if not isinstance(claims, list) or not 1 <= len(claims) <= 256:
        raise ResearchContractError("claims must contain 1-256 entries")

    issues: list[str] = []
    claim_ids: set[str] = set()
    verified_evidence = 0
    resolved_claims = 0
    for index, claim in enumerate(claims):
        label = f"claims[{index}]"
        if not isinstance(claim, dict):
            raise ResearchContractError(f"{label} must be an object")
        _require_exact_keys(
            claim,
            {"id", "text", "boundary", "status", "evidence", "limitations"},
            set(),
            label,
        )
        claim_id = _require_text(claim["id"], f"{label}.id", maximum=70)
        if not CLAIM_ID.fullmatch(claim_id):
            raise ResearchContractError(f"{label}.id must match ^claim-[a-z0-9][a-z0-9_-]{{0,63}}$")
        if claim_id in claim_ids:
            raise ResearchContractError(f"duplicate claim id: {claim_id}")
        claim_ids.add(claim_id)
        _require_text(claim["text"], f"{label}.text")
        _require_text(claim["boundary"], f"{label}.boundary")
        status = claim["status"]
        if status not in {"planned", "supported", "refuted", "inconclusive"}:
            raise ResearchContractError(f"{label}.status is unsupported")
        limitations = _require_string_list(claim["limitations"], f"{label}.limitations", maximum=32)
        if status != "planned":
            resolved_claims += 1
        evidence = claim["evidence"]
        if not isinstance(evidence, list) or len(evidence) > 128:
            raise ResearchContractError(f"{label}.evidence must be a list of at most 128 entries")
        if status in {"supported", "refuted"} and not evidence:
            issues.append(f"{claim_id} is {status} but has no evidence")
        if status == "inconclusive" and not limitations:
            issues.append(f"{claim_id} is inconclusive but records no limitation")
        for evidence_index, reference in enumerate(evidence):
            reference_label = f"{label}.evidence[{evidence_index}]"
            if not isinstance(reference, dict):
                raise ResearchContractError(f"{reference_label} must be an object")
            _require_exact_keys(reference, {"path", "sha256", "role"}, set(), reference_label)
            if reference["role"] not in {"primary", "supporting", "limitation"}:
                raise ResearchContractError(f"{reference_label}.role is unsupported")
            digest = _require_text(reference["sha256"], f"{reference_label}.sha256", maximum=64)
            if not SHA256.fullmatch(digest):
                raise ResearchContractError(f"{reference_label}.sha256 must be lowercase SHA-256")
            resolved = _resolve_relative(root, reference["path"], f"{reference_label}.path")
            if not resolved.is_file():
                issues.append(f"{claim_id} evidence is missing: {resolved}")
                continue
            actual = _stable_sha256(resolved)
            if actual != digest:
                issues.append(f"{claim_id} evidence digest mismatch: {resolved}")
                continue
            verified_evidence += 1

    issues.extend(f"unresolved placeholder at {item}" for item in _placeholder_paths(value))
    if any(claim["status"] == "planned" for claim in claims):
        issues.append("planned claims remain unresolved")
    if resolved_claims == 0:
        issues.append("no claim has a result status")

    return AuditReport(
        kind="claim_evidence_ledger",
        valid=True,
        ready=not issues,
        issues=tuple(issues),
        summary={
            "study_id": study_id,
            "claim_count": len(claims),
            "resolved_claim_count": resolved_claims,
            "verified_evidence_count": verified_evidence,
        },
    )
