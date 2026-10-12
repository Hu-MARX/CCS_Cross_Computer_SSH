"""Command-line readiness checks for the research layer."""

from __future__ import annotations

import argparse
import json
import sys

from .contracts import ResearchContractError, audit_claim_ledger, audit_project


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="researchctl")
    commands = parser.add_subparsers(dest="command", required=True)

    project = commands.add_parser("project", help="audit a research project contract")
    project.add_argument("--file", required=True)
    project.add_argument("--root", default=".")
    project.add_argument("--require-ready", action="store_true")

    ledger = commands.add_parser("claims", help="audit claims and verify evidence digests")
    ledger.add_argument("--file", required=True)
    ledger.add_argument("--root", default=".")
    ledger.add_argument("--require-ready", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "project":
            report = audit_project(args.file, workspace_root=args.root)
        else:
            report = audit_claim_ledger(args.file, workspace_root=args.root)
        print(json.dumps({"ok": True, "audit": report.to_dict()}, ensure_ascii=False, separators=(",", ":")))
        return 2 if args.require_ready and not report.ready else 0
    except ResearchContractError as exc:
        print(
            json.dumps(
                {"ok": False, "audit": None, "error": {"code": "RESEARCH_CONTRACT_INVALID", "message": str(exc)}},
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
