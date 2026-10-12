"""Read worker facts, or prepare a run plan without submitting training."""

import argparse
import json
import sys
from pathlib import Path

from ccs.client import CCSClient, CCSError


def command_array(value):
    try:
        result = json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"Invalid command JSON: {exc}") from exc
    if not isinstance(result, list) or not result or not all(
        isinstance(item, str) and item for item in result
    ):
        raise argparse.ArgumentTypeError("Expected a non-empty JSON array of argument strings")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--command-json", required=True, type=command_array,
        help="Complete CCS CLI prefix as a JSON argv array; omit request --file -",
    )
    parser.add_argument("--cwd", type=Path, help="Local working directory for the CCS command")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--actor-id", default="python-sdk-example")
    parser.add_argument("--plan-submit", action="store_true", help="Only prepare a run.submit plan")
    parser.add_argument("--spec", type=Path, help="Existing ExperimentSpec JSON file")
    parser.add_argument("--worker-id", help="Explicit worker selected by the caller")
    parser.add_argument("--idempotency-key", help="Stable submission key for this proposed run")
    args = parser.parse_args()

    if args.plan_submit and not all((args.spec, args.worker_id, args.idempotency_key)):
        parser.error("--plan-submit requires --spec, --worker-id and --idempotency-key")
    if not args.plan_submit and any((args.spec, args.worker_id, args.idempotency_key)):
        parser.error("Use --plan-submit when supplying a spec, worker or submission key")

    client = CCSClient(
        args.command_json, cwd=args.cwd, timeout=args.timeout,
        actor_id=args.actor_id, actor_kind="service",
    )
    try:
        if args.plan_submit:
            spec = json.loads(args.spec.read_text(encoding="utf-8"))
            if not isinstance(spec, dict):
                parser.error("The spec file must contain an ExperimentSpec JSON object")
            plan = client.plan("run.submit", {
                "worker_id": args.worker_id,
                "spec": spec,
                "idempotency_key": args.idempotency_key,
            })
            output = {
                "request": plan.request,
                "data": plan.data,
                "plan_sha256": plan.plan_sha256,
                "note": "Plan only; this example does not confirm or submit the run.",
            }
        else:
            output = client.request("fleet.status", {}, dry_run=False)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    except CCSError as exc:
        print(json.dumps({
            "code": exc.code,
            "message": exc.message,
            "retryable": exc.retryable,
            "request_id": exc.request_id,
        }, ensure_ascii=False), file=sys.stderr)
        return 1
    except (OSError, json.JSONDecodeError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
