from __future__ import annotations

import json
from typing import Any

from .domain import Decision, DomainError


class CodexDecisionParser:
    """Parses the final JSON object from codex exec JSONL without executing Codex."""

    def parse(self, output: str) -> Decision:
        candidates: list[dict[str, Any]] = []
        for line in output.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                candidates.append(value.get("decision", value))
        for candidate in reversed(candidates):
            try:
                return Decision.from_dict(candidate)
            except (DomainError, TypeError):
                continue
        raise DomainError("no valid Decision found in Codex JSONL output")
