# Local collector focus and running source identity

The dashboard remains loopback-only and exposes no remote mutation. The sole
POST endpoint, `/api/focus`, changes an in-memory **local read preference**. It
does not submit, cancel, resume, choose an execution worker or modify a Run.

## Interface and tab competition

`POST /api/focus` accepts only JSON `{"run_id":"<registered Run>"}` (256 bytes
maximum). The Run must already be in the bounded local catalog and have a worker
binding. Worker routing comes only from that registered descriptor, never from
the request. Authorization uses the dashboard's private bearer capability plus
same-origin `Origin` or `Sec-Fetch-Site: same-origin`; unknown Runs fail closed.
Do not put the private token into URLs, documentation or request/access logs.

`GET /api/state` and `/api/result` still read memory only. Reading another Run
does not change focus. Explicit task selection and follow-mode transitions send
the preference once. Across tabs, the last explicit selection wins; ordinary
10-second page refreshes do not compete. Each tab keeps its own displayed Run
and sees `collector_focus` identifying which Run/worker currently has priority.
The startup Run and private launch link remain unchanged.

An automatic follow transition supplies optional `expected_run_id` (current
focus) with its `run_id`. If another tab selected a different focus since that
observation, the server keeps that newer preference and returns its focus.
Automatic follow changes the tab's display but does not steal another tab's
explicit selection. User clicks omit the conditional field and take priority.

## Bounded collection

The server starts exactly three fixed collectors: catalog/other Runs, focused
native observations, and focused log metrics. Switching focus coalesces wakeups;
it never starts a thread per Run or tab. Slow fleet/history collection cannot
hold the focused collector. Native and log reads are isolated from one another.
The existing active-first window and one rotating history Run per tick remain.
Per-Run nonblocking locks prevent concurrent writers; in-flight readers retain
their original Run/worker binding during rapid selection and catalog pruning.

A read already in flight is not forcefully interrupted; a focus change can wait
for that collector's current bounded remote timeout. Worker SSH latency, a full
log backlog, or an unreachable worker can still delay **that worker's** source.
Other non-focused Runs remain best-effort catalog observations, not guaranteed
real-time streams. Missing/stale observations never imply training stopped.

## Runtime identity

`dashboard_runtime` contains the software version captured at module import,
startup time, startup source digest, current source digest, last check time,
source-check availability and `restart_required`. The source digest identifies
the package's Python/UI files at startup, not an attestation of loaded bytecode.
It is independent of a remote worker's software version.

The background check is rate-limited to 30 seconds and bounded to 512 source
files, 128 directories, 4 MiB per file and 16 MiB total. It does not walk runtime,
private, configuration, release, snapshot, hidden or cache directories, follow
links, read task payloads, or return local source paths. HTTP requests do not
hash source. Source changes show a visible reminder to restart the **local
dashboard only**; no automatic dashboard or training restart occurs. If reading
source fails, the UI says the check is unavailable instead of asserting parity.

## Recovery display

When a configured metrics adapter supplies `recovery`, the UI separates latest
stdout step, native/trainer progress step, checkpoint step, and replay gap.
Replay is shown only for same-unit records from the same declared provenance;
it is an **observed step difference**, not an upper bound on actual work lost.
A trainer commit receipt is labelled **reported, not restoration-verified**;
native resumability is **worker-reported, not an actual restoration test**.
missing recovery evidence stays unknown, not zero. This display never authorizes
or starts a resume and never turns a receipt into scientific evidence.

## Targeted verification

Python: `test_dashboard_focus`, `test_dashboard`, `test_dashboard_selection`,
`test_dashboard_multirun_bridge`. UI: `node --test tests/dashboard_ui.test.js`.
These use local fixture records and read-only fake remotes; no worker deployment,
training restart, GPU load or production dashboard restart is part of them.
