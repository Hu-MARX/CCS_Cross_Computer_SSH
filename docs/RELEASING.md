# CCS releases

CCS 0.6.1 retains the portable core, optional MCP adapter, and separate research-project integration. Installing the core requires Python 3.11 or later and no third-party runtime packages. The optional MCP extra requires `mcp>=2.2,<3`.

## Source verification

Copy the complete CCS directory to an independent directory. The core does not read its parent's research project. Run these commands with any current working directory; the runner resolves its own source and test paths. On restricted Windows hosts, set `TEMP` and `TMP` to a writable workspace directory before invoking Python.

```powershell
python C:\independent\CCS\scripts\verify.py --group core --report C:\evidence\core.json
pwsh -NoProfile -File C:\independent\CCS\scripts\verify-local.ps1 -Mode core
```

The default PowerShell entry point runs `core`. The core discovers all original Python tests except the MCP module and external research integration. Optional PyTorch replay checks remain explicit skipped tests when the training environment is absent. Every JSON record lists actual passes separately from skips, including the reason for each skip and modules excluded by group selection. A core pass does not establish MCP, PyTorch replay, or Linux process acceptance.

```powershell
python -m pip install -e 'C:\independent\CCS[mcp]'
python C:\independent\CCS\scripts\verify.py --group mcp --report C:\evidence\mcp.json
python C:\independent\CCS\scripts\verify.py --group research --research-root C:/path/to/research-project --report C:\evidence\research.json
python C:\independent\CCS\scripts\verify.py --group all --research-root C:/path/to/research-project --report C:\evidence\all.json
```

The research root is the directory containing `scripts/register_online_joint_dashboard.py`. The MCP group fails before tests when the dependency is absent or outside the supported range; skipped MCP tests also fail acceptance. Its tests include a real subprocess stdio handshake, tool discovery, error propagation, and exact plan confirmation. The research group requires explicit input and treats skipped tests as failure. These groups use local fixtures and do not deploy or submit remote work.

Dashboard JavaScript checks are a separate command requiring Node.js: `node --test C:\independent\CCS\tests\dashboard_ui.test.js`. Their result must be reported separately from the Python suite.

## Wheel and sdist acceptance

The developer-only `build` package is not a CCS runtime dependency. Run the release verifier with a build interpreter that has it installed. Choose a fresh evidence directory outside the CCS source tree. The script builds one wheel and one sdist, checks package contents, and installs each into a different clean virtual environment. It executes installed CLI and SDK calls from unrelated directories with `PYTHONPATH`, `PYTHONHOME`, and user site packages removed. It checks offline schema discovery, the installed package resource, and a plan/confirmation round trip through a local Fake Worker. No SSH, training process, or GPU is used.

```powershell
python -m pip install build
python C:\independent\CCS\scripts\verify-release.py --output-dir C:\evidence\release-core
python C:\independent\CCS\scripts\verify-release.py --skip-build --artifacts-dir C:\evidence\release-core\artifacts --output-dir C:\evidence\release-mcp-2.2 --mcp-version 2.2.0
python C:\independent\CCS\scripts\verify-release.py --skip-build --artifacts-dir C:\evidence\release-core\artifacts --output-dir C:\evidence\release-mcp-2.3 --mcp-version 2.3.0
```

The MCP variants install the exact requested SDK version in both clean environments and use the installed `ccs-mcp` entry point. Acceptance includes real stdio discovery, rejection of a tampered plan, execution of the exact original confirmation, and a separate read-only server exposing only the read tool. Installed core checks also cover plan save/load, paged event iteration, wait/log helpers, offline doctor and exact configuration creation. A core artifact test reports MCP as `not-requested`; it cannot certify MCP readiness. Preserve `release-summary.json`, `inventory.json`, SHA-256 digests, and per-command logs with the delivered artifacts. Failed variants remain failed evidence and need a new output directory on retry.

Do not run repetitive full suites for each small edit. Run the affected tests during development, then one grouped batch at delivery and the necessary package/dependency variants.

## Linux acceptance

POSIX permission, process-group cleanup, and snapshot subprocess checks must run on Linux. Run `python /independent/CCS/scripts/verify.py --group core --require-linux --report /evidence/linux-core.json` in an isolated Linux checkout, then run `verify-release.py --require-linux` there for installed artifact checks. The first command refuses Windows and fails if a required Linux check skips; the explicitly Windows-only Job fault-injection test may skip on Linux and remains recorded. Optional training-environment skips remain explicitly outside this acceptance scope.

Until actual Linux records exist, label Linux acceptance **pending**. Windows platform skips provide no substitute. No release command permits use of an occupied worker or grants deployment authorization.

## Install and rollback

Record the previous installed version and retain its original wheel before upgrading. Use a versioned controller virtual environment; install the new verified wheel there, then update the local launcher to its absolute executable path. Offline `ccsctl describe` and `CCSClient.describe()` need no worker target or runtime database. Existing gateway requests still require their configured worker/fleet and explicit state paths.

This release adds repository migration 2 for remote-event watermarks. It backfills from existing events once and updates the watermark in the same transaction as each event; original P0 tables and JSONL remain intact. Workers gain a disposable byte-offset index and page capabilities. Existing complete checkpoint replicas/imports need no rewrite; new interrupted transfers recover only with verified source-bound journals. Do not copy verification databases into production, overwrite a running deployment, or change a Run's captured source/environment to perform this local release.

For controller rollback, stop new local submissions, preserve current state files, and restore the previous launcher/virtual environment. Retain operation/audit databases, watermark tables, checkpoint transaction journals and incomplete files: an unknown submission still requires query/reconciliation before any retry. A 0.5.2 controller reads the original repository tables; on re-upgrade, remote event duplicates are ignored and watermarks catch up. Worker rollback may remove the disposable event index, but must preserve JSONL/fragment evidence. Older code cannot repair a new incomplete checkpoint transaction; finish or quarantine it before downgrading. Downgrading to versions before the P0 operation tables is a separate compatibility decision and must not discard state silently. Worker upgrade, systemd deployment, Linux fault injection, and production rollback drills remain separate acceptance tasks.
