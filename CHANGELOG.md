# Changelog

## 0.6.3 — cooperative checkpoint pause (candidate)

- Add opt-in `run.pause` across exact-plan gateway, worker CLI, SDK and MCP;
  unsupported tasks reject before publishing a request or cancelling training.
- Commit an immutable bounded pause request, verify a pinned complete checkpoint
  ACK and reserved exit code, and require independent systemd cgroup quiescence
  before reporting a safe pause. Timeouts do not forcibly stop training.
- Compare trusted adapter state after recovery separately from file integrity;
  require both real optimizer progress and model/optimizer state changes.
- Show request delivery, checkpoint commitment, exit confirmation and stale
  pause records separately on the read-only dashboard.
- Keep post-publication ambiguity non-retryable; explicit pre-publication
  rejections retain their execution state through the real worker CLI chain.
- Single-process v1 only; no automatic resume or retrofit into active training,
  no remote rollout and no claim of native Linux/GPU power-loss acceptance.

## 0.6.2 — recovery observation and safe maintenance (candidate)

- Negotiate workspace capabilities against the actual workspace endpoint.
- Separate logged progress, trainer-reported checkpoint commitment, native
  replay capability and observed replay gap; preserve native metric authority.
- Add a declared ordinary JSON progress-report adapter and cadence-aware
  collection freshness without loading model state or automatically resuming.
- Follow authenticated local collector preferences with fixed native/log
  collectors; show startup software identity and source-change restart warnings.
- Add offline upgrade role matrices and preview-only side-by-side checklists.
- Count parent tests separately from subtest and fixture errors in verification
  reports; keep raw failures, errors and required-skip gates unchanged.
- Keep remote mutations on the existing exact-plan JSON Interface; no new
  remote deployment, cancellation or automatic training-retry surface.

## 0.6.1 — public repository preparation

- Add Chinese installation/operation guides, a documentation index and explicit
  controller/worker configuration templates.
- Require explicit dashboard worker selection; remove personal host/path defaults
  from public adapters and optional acceptance recipes.
- Check the selected install interpreter before worker installation and clarify
  source-copy and environment ownership.
- Publish a source-only repository. Keep local runtime state, host keys,
  personal configurations and experiment acceptance records outside Git.
- Correct SDK documentation for delivered run IDs and concurrency defaults.

## 0.6.0 — P1-01 through P1-04

- Bounded event/artifact pages, incremental event indexes, persisted controller
  watermarks, bounded subprocess output and fleet observation budgets.
- SDK waiting deadlines, resumable delivered-item cursors, saved plans, bounded
  SDK/MCP admission and local cancellation cleanup.
- MCP read-only/allowlist startup, offline doctor and exclusive configuration
  creation after an exact preview digest.
- Source/digest-bound replica/import transaction journals and interruption
  recovery without replacing valid published checkpoints.

## 0.5.2 and 0.5.1 — P0 foundations

- Packaged canonical action contracts, core/MCP/research verification groups and
  independent wheel/sdist checks.
- Explicit worker reads, atomic shared-database submission admission, durable
  unknown operations and read-only submission reconciliation.

See [support and acceptance](docs/SUPPORT-MATRIX.md) for completed validation and
remaining native Linux/remote acceptance boundaries.
