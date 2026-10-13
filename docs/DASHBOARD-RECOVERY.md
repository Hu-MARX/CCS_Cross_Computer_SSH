# Dashboard recovery facts

The dashboard is a read-only observation surface. Process liveness, logged
progress, trainer-reported checkpoint commitment and verified replay capability
are separate facts. This module never loads model weights or starts/resumes a
Run. A recovery operation still needs its own dry-run and exact plan confirmation.

## Recovery projection

When a task has a LogBridge, its dashboard result includes `recovery`:

| Field | Meaning |
| --- | --- |
| `logged_step` | Last accepted stdout `training_progress` step; null when only a report was read. |
| `progress_step` | Progress counter used for the checkpoint comparison. |
| `progress_source` | Native progress, bound trainer report, or stdout progress. |
| `checkpoint_step` | Checkpoint counter; never derived from the latest loss/step or a filename alone. |
| `checkpoint_state`, `checkpoint_source` | Worker checkpoint facts or `reported_committed` trainer receipt. |
| `resume_capability` | `unknown`, `reported_only`, `verified`, or `not_resumable`. |
| `safe_to_resume` | True/false only from native checkpoint monitoring; always null for trainer receipts. |
| `replay_steps`, `replay_basis` | Difference between same-source, same-unit observed progress and checkpoint counters. |
| `replay_steps_scope` | `observed_progress_only`; progress after the last observation is not included. |
| `reason` | Explains the capability and any counter inconsistency. |

`verified` means the worker reports a committed replay-safe checkpoint. It is
not a successful restore test. `reported_only` means a configured trainer
reports an atomic commit, but the dashboard has not rehashed the binary,
inspected its state or exercised restoration. It must not enable automatic resume.

Native checkpoint facts take precedence when the worker reports a committed
checkpoint or an explicit failure. Native metrics likewise keep precedence over
log metrics, but do not suppress independently bound trainer checkpoint receipts.
The module never subtracts a native framework step from a trainer's batch counter.
A checkpoint ahead of observed progress produces unknown replay rather than a
negative or zero value. Missing receipts produce unknown checkpoint/replay, not
an assumed checkpoint at the latest training step.

## Existing full-fit adapter

`format: fullfit_v1` retains the existing training-signature, execution-key and
epoch-schedule checks. Its checkpoint receipt is now validated by the same
projection module. Legacy full-fit receipts without an explicit optimizer
boundary marker remain **reported**, not verified.

## Ordinary training-progress snapshot adapter

An existing trainer emitting ordinary `training_progress` JSON with list-valued
learning rates can opt into a bounded `progress.json` read. Declare its exact
configuration, report identity, path and checkpoint signature in the local,
Run/spec/evidence-bound configuration record:

```json
{
  "log_bridge": {
    "format": "training_progress_v1",
    "worker_id": "worker-example",
    "config_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "max_steps": 100,
    "report_digest_key": "effective_config_sha256",
    "snapshot": {
      "project": "/project",
      "path": "training/current/progress.json",
      "identity": {"experiment_id": "experiment-example"}
    },
    "checkpoint_step_key": "optimizer_updates",
    "checkpoint_signature": {
      "key": "scientific_signature",
      "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    },
    "expected_metric_interval_seconds": 120
  }
}
```

The report must contain the declared digest and identity fields, `total_steps`,
`global_batches`, and `last_progress`. Identity keys are limited to `run_id`,
`execution_key`, `experiment_id`, and `source_run_id`. Digest keys are limited
to `config_sha256`, `effective_config_sha256`, `training_signature`, and
`scientific_signature`. The checkpoint signature must match both report and receipt.

Checkpoint receipts must contain `path`, `sha256`, the declared counter,
the declared signature and `committed_optimizer_boundary: true`. The path must
be the exact `checkpoints/batch-%07d.pth` file under this report's directory.
`checkpoint_step_key` may be `global_batches` or `optimizer_updates`. For the
optimizer counter, both the report and its last-progress event must confirm that
the observed step is an optimizer update, not a microbatch count.

Only the existing `workspace.execute` read/inspect seam reads the declared JSON;
the returned project/path and SHA-256 must match its bounded UTF-8 contents.
No checkpoint file is read. Native lifecycle, connection state, progress health
and advancing flags are not changed by this adapter. Configuration/Run isolation
is preserved; changing any declared log binding resets the bridge cache scope.

For an optional future adapter, a stdout `checkpoint_committed` event may carry
the same receipt fields, the configuration digest and snapshot identity. It is
accepted only after a matching `training_launch` and same-unit progress event.
Existing trainers are not assumed to emit this event. Unbound checkpoint text
or bare paths do not establish recovery capability.

## Collection cadence and freshness

Without a declared cadence, the prior 90-second collection-freshness threshold
is retained. `expected_metric_interval_seconds` accepts a finite numeric value
between 1 and 86400. The threshold becomes the greater of 90 seconds and two
declared output periods plus the bridge's 30-second polling interval. For 120
seconds, the threshold is 270 seconds.

The result exposes `freshness_threshold_seconds` and `freshness_basis`.
An invalid/unparseable timestamp cannot be fresh. Collection timestamps are
local receipt times, not reconstructed training event times. Fresh collection
does not prove training advancement or connectivity; the standard monitor
continues to own those observations.

## Targeted regression coverage

`tests/test_dashboard_checkpoint_projection.py` exercises the real
`LogBridge.supplement` and `LogBridge.poll` Interfaces. It covers the native-step
early-return regression, reported-versus-verified semantics, counter units,
foreign configuration/path/identity/receipt rejection, JSON readback digest
mismatch, and declared collection cadence. No remote failure injection or model
checkpoint deserialization is required.
