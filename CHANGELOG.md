# Changelog

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
