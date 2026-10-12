# Bounded reads and incremental event synchronization

Gateway schema version 1 keeps legacy event/artifact list responses when no new
page fields are supplied. Add `limit` or `max_bytes` to `run.events` to select
bounded pages. Add `cursor`, `limit`, or `max_bytes` to `run.artifacts` to select
bounded inventory pages. SDK iterators use the paged form.

```json
{"action":"run.events","dry_run":false,"payload":{"worker_id":"worker-a","run_id":"run_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","after_seq":0,"limit":100,"max_bytes":1048576}}
```

Both pages return the item array, `next_cursor`, `has_more`, and `truncated`.
`limit` defaults to 100 and accepts 1–1000. `max_bytes` defaults to 1 MiB and
accepts 1024–16777216 bytes. This is the UTF-8 item-array budget; the gateway
envelope adds small metadata overhead, and the subprocess transport separately
caps stdout at 20 MiB and stderr at 64 KiB. Event cursors are the last delivered
sequence number, including when a page ends at the byte budget. Reconnect using
that cursor. An oversized first event returns `PAGE_ITEM_TOO_LARGE` with the
required budget and keeps the cursor unchanged; increase `max_bytes` within the
allowed range. A record larger than the maximum needs operator-side inspection.

Artifact cursors are opaque versioned keys bound to the run and exact path
filter. Entries are ordered by absolute path; a cursor reused with another run
or filter is rejected. Inventory pages do not hash every checkpoint: only a
specific `path` lookup computes its SHA-256. Pagination is a live inventory,
not a snapshot. Existing stable names are delivered once; files created with
names before the current cursor are discovered by starting a fresh scan. A
deleted file may disappear between pages. The scanner keeps at most
`limit + 1` selected paths in memory, although directory enumeration still
visits matching names to order each page.

The worker advertises `run.events.page=1` and `run.artifacts.page=1`.
Explicit page requests require those capabilities and never silently invoke
an old worker's unbounded protocol. Legacy list calls remain available for
existing consumers, including checkpoint replication. Controller reconciliation
uses pages on upgraded workers; a capability-only incompatibility permits its
explicit legacy JSONL compatibility path, subject to the transport budget.

`events.jsonl` remains authoritative. The disposable SQLite companion stores
sequence-to-byte offsets. A legacy log is streamed once to build the index;
later reads seek directly to requested offsets and later appends index only
new bytes. A complete event persisted before the index transaction is recovered
from the tail. An incomplete trailing record is not delivered: before a new
append its bytes are preserved in `events-fragment-*.bin`, then the unfinished
tail is truncated and the next contiguous sequence is appended. No complete
event is discarded by this repair. Deleted indexes rebuild from JSONL. Corrupt
complete records or noncontiguous sequences require operator repair.

Controller repository migration 2 backfills remote watermarks once using SQL.
Every remote event insertion updates its watermark in the same SQLite
transaction. Reconciliation consumes at most 10 pages of 1000 events per Run
per pass by default. A terminal Run stays locally active until all its terminal
tail has been persisted, so the next pass can continue draining it. This bound
controls catch-up memory/work; it does not cancel or restart remote training.

OpenSSH capture uses bounded local subprocess trees. A local timeout,
cancellation, or output-limit failure after a mutation starts leaves its remote
execution state `unknown`. Use the operation query/reconcile path; do not replay
the mutation. Oversized read output reports `SSH_OUTPUT_LIMIT` with continuation
guidance instead of parsing a partial JSON response. Fleet observations pass a
shared per-worker deadline/cancellation event through health and process reads.

Worker upgrade is needed for paged CLI behavior and offset indexing. The
controller-only watermark migration is additive; older controllers can still
read the original tables. Rollback may retain or delete the disposable index;
JSONL files and fragment evidence must be preserved. These local contracts and
simulated interruption tests do not establish real Linux/SSH failure acceptance.
