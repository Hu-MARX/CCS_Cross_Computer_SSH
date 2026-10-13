# CCS 0.6.1 操作指南

本指南从已安装的 CCS 和已部署的 Linux Worker 开始。安装、SSH 身份与固定主机密钥配置见 [安装说明](INSTALL.md)；完整输入、输出和能力要求见 [动作契约](ACTION-CONTRACTS.md)。示例中的 `C:\path\to\CCS`、`worker-a`、`worker-b` 和远端目录都需要替换为自己的配置。

CCS 管理 Worker 池中的独立 Run。由调用方明确选择 Worker、命令和检查点；一个 Run 跨两台机器的 NCCL/DeepSpeed 协调启动尚未实现。不要向正在进行其他训练、尚未授权使用的机器提交示例。

## 1. 固定本机入口，先检查离线契约

以下使用 PowerShell 7。`$Python` 必须指向安装了 CCS 的环境；本机工作目录与远端 `runtime.workdir` 是两个独立设置。路径带空格或中文时保持为独立参数，不拼接 shell 命令字符串。

```powershell
$ErrorActionPreference = 'Stop'
$Root = 'C:\path\to\CCS'
$Python = Join-Path $Root '.venv\Scripts\python.exe'
Set-Location -LiteralPath $Root
$State = Join-Path $Root 'data'
New-Item -ItemType Directory -Force -Path $State | Out-Null
$GatewayArgs = @(
    '-m', 'controller.gateway_cli',
    '--fleet', (Join-Path $Root 'config\worker-pool.json'),
    '--db', (Join-Path $State 'orchestrator.db'),
    '--audit-db', (Join-Path $State 'ccs-audit.db'),
    '--replica-root', (Join-Path $State 'checkpoint-replicas')
)

& $Python -m controller.gateway_cli --version
& $Python -m controller.gateway_cli describe --action run.submit
& $Python -m controller.gateway_cli describe --schema experiment-spec
```

`describe` 不需要 Worker、SSH 或运行数据库。用同一份已安装契约准备 payload，避免给协议增加未支持的字段。

下面两个辅助函数只写本机请求文件，再调用 JSON gateway；不绕过预演确认。请求以无 BOM 的 UTF-8 写入，`ConvertTo-Json -Depth 60` 保留嵌套 Spec。CLI stdout 应为一个 JSON envelope，成功为 `ok=true`；错误信息、关联身份和字段路径位于 `error`。

```powershell
function New-CcsRequest {
    param(
        [string]$Action,
        [System.Collections.IDictionary]$Payload,
        [bool]$DryRun = $false,
        [string]$RequestId = ('request-' + [Guid]::NewGuid().ToString('N'))
    )
    [ordered]@{
        schema_version = 1
        request_id = $RequestId
        actor = @{ id = 'local-operator'; kind = 'user' }
        action = $Action
        dry_run = $DryRun
        payload = $Payload
    }
}

function Save-CcsJson {
    param([object]$Value, [string]$Path)
    $json = $Value | ConvertTo-Json -Depth 60
    [IO.File]::WriteAllText($Path, $json, [Text.UTF8Encoding]::new($false))
}

function Invoke-CcsRequest {
    param([object]$Request)
    $path = Join-Path $State ($Request.request_id + '.json')
    Save-CcsJson $Request $path
    $raw = & $Python @GatewayArgs request --file $path
    $exitCode = $LASTEXITCODE
    $envelope = ($raw -join [Environment]::NewLine) | ConvertFrom-Json
    if ($exitCode -ne 0 -or -not $envelope.ok) {
        throw ($envelope.error | ConvertTo-Json -Depth 20 -Compress)
    }
    return $envelope
}
```

`data/` 是本机运行记录，包含请求、审计、缓存和副本；不要把它上传到公共仓库。多个本机入口使用同一个 controller 数据库，才能共享提交准入与操作账本。

## 2. 读取 Worker 事实，并明确选择执行位置

```powershell
$fleet = Invoke-CcsRequest (New-CcsRequest 'fleet.status' @{
    timeout_seconds = 10
    worker_timeout_seconds = 5
})
$fleet.data.result | ConvertTo-Json -Depth 30

$Worker = 'worker-a'
$health = Invoke-CcsRequest (New-CcsRequest 'worker.health' @{ worker_id = $Worker })
$health.data.result | ConvertTo-Json -Depth 30
```

`worker.monitor`、`worker.alerts`、`worker.storage`、`worker.processes` 同样支持显式 `worker_id`。检查返回的 `worker_id`，以及可达性、能力版本和当前受管负载。`enabled=false` 的 Worker 仍可诊断，但拒绝新 Run；未知 ID 不会回退到另一台机器。

`fleet.status` 最多并行观察 4 个 Worker。达到时间预算会报告该 Worker 的 `WORKER_OBSERVATION_TIMEOUT`，拿不到槽位时报告 `WORKER_OBSERVATION_BUSY`；这两项都不会取消训练。

## 3. 预演一个已获授权的短 CPU Run

本例只运行 Python 标准库，没有 GPU 请求，最多 30 秒。先由部署者在选定 Worker 准备一个可写、独立且存在的工作目录，例如 `/srv/ccs/workloads/smoke-001`，并确认 `python3` 可用。每次新实验使用新的工作目录及幂等键，避免覆盖既有输出；不要把本机 Windows 路径写进远端 Spec。

```powershell
$spec = [ordered]@{
    schema_version = 1
    name = 'ccs-cpu-smoke'
    project = @{ repository = 'local-smoke' }
    runtime = @{ workdir = '/srv/ccs/workloads/smoke-001' }
    command = @{
        shell = $false
        argv = @(
            'python3', '-c',
            "from pathlib import Path; import json; value={'message':'hello from CCS'}; Path('ccs-smoke-output.json').write_text(json.dumps(value), encoding='utf-8'); print(json.dumps(value))"
        )
    }
    resources = @{ gpu_index = $null; max_duration_seconds = 30 }
    outputs = @{ artifact_glob = 'ccs-smoke-output.json' }
    task = @{ kind = 'analysis'; inputs = @() }
    automation = @{ max_attempts = 1; allowed_actions = @('wait', 'complete') }
}
$Key = 'cpu-smoke-001'
$submit = New-CcsRequest 'run.submit' @{
    worker_id = $Worker
    spec = $spec
    idempotency_key = $Key
} $true 'cpu-smoke-plan-001'
$plan = Invoke-CcsRequest $submit
Save-CcsJson @{ request = $submit; plan = $plan.data } (Join-Path $State 'cpu-smoke-plan.json')
$plan.data | ConvertTo-Json -Depth 60
```

此时只有预演。检查 `executable`、兼容状态、Worker、命令、工作目录、资源预算和 `plan_sha256`。计划摘要约束原始 `request_id`、actor、action 和完整 payload，但不代表用户授权，也不是密码学签名。

## 4. 用同一请求和摘要确认执行

仅在上一步计划符合已有授权后执行下面的命令。保持 actor、请求 ID、幂等键、Worker 和 Spec 不变；只改变 `dry_run` 并增加确认摘要。

```powershell
if (-not $plan.data.executable) { throw '计划不可执行，请先解决兼容或输入问题' }
$submit.dry_run = $false
$submit.confirm_plan_sha256 = $plan.data.plan_sha256
$accepted = Invoke-CcsRequest $submit
$RunId = $accepted.data.result.id
$accepted.data | ConvertTo-Json -Depth 30
```

返回 `confirmed` 表示提交结果已确认、Run 被接收；完成状态仍需后续观察。记录 `RunId`、`operation`、原始请求与计划。不要在超时后自动换新幂等键、Worker 或数据库重发提交。

取消一个已有 Run 也是独立变更：先预演，确认后执行。`reason` 必须包含非空原因，不要省略或只写空白。

```powershell
$cancel = New-CcsRequest 'run.cancel' @{
    worker_id = $Worker
    run_id = $RunId
    reason = '操作者要求停止这次独立 CPU 验证'
} $true 'cpu-smoke-cancel-001'
$cancelPlan = Invoke-CcsRequest $cancel
# 审阅并确认取消已获授权后，使用同一 cancel 请求：
# $cancel.dry_run = $false
# $cancel.confirm_plan_sha256 = $cancelPlan.data.plan_sha256
# Invoke-CcsRequest $cancel
```

### 完整断点确认后暂停

`run.pause` 与强制停止的 `run.cancel` 不同：只有提交时显式设置
`task.pause = {schema_version: 1, protocol: 'managed-checkpoint-v1', contract: '<64位SHA-256>'}`
的训练任务，且从机声明 `run.pause=1` 时才能使用。训练适配器需要遵循暂停协议，
保存并提交完整恢复点后确认本次请求。旧任务不能靠后来增加元数据变成可安全暂停任务。

```powershell
$pause = New-CcsRequest 'run.pause' @{
    worker_id = $Worker
    run_id = $RunId
    reason = '操作者要求维护暂停，先保存完整断点'
    timeout_seconds = 600
} $true 'maintenance-pause-001'
$pausePlan = Invoke-CcsRequest $pause
# 审阅计划并确认已获授权后，保持请求身份与 payload 不变：
# $pause.dry_run = $false
# $pause.confirm_plan_sha256 = $pausePlan.data.plan_sha256
# Invoke-CcsRequest $pause
```

返回 `confirmed` 与 `pause.state=requested` 只表示请求已送达，并不代表训练已停止。
继续使用 `run.snapshot` 读取原 Run 的 `pause`、完整断点与进程停止事实；没有完整保存确认，
不得关机。协议超时或确认无效不自动退化为强制取消。响应丢失时查询 `operation.query`
及原 Run 中匹配 `request_id` 的暂停回执，不重发，也不更换账号、从机或请求身份。
恢复仍需独立批准的新 `run.submit`，带已验证的 `task.checkpoint` 与 `task.source_run_id`。

## 5. 按游标读取日志、事件和产物

```powershell
$snapshot = Invoke-CcsRequest (New-CcsRequest 'run.snapshot' @{
    worker_id = $Worker; run_id = $RunId
})
$snapshot.data.result | ConvertTo-Json -Depth 30

$eventPage = Invoke-CcsRequest (New-CcsRequest 'run.events' @{
    worker_id = $Worker; run_id = $RunId
    after_seq = 0; limit = 100; max_bytes = 1048576
})
$eventPage.data.result.events | ConvertTo-Json -Depth 30
# 消费并保存后，下一页使用 after_seq = $eventPage.data.result.next_cursor。

$logPage = Invoke-CcsRequest (New-CcsRequest 'run.logs' @{
    worker_id = $Worker; run_id = $RunId
    stream = 'stdout'; after_offset = 0
})
$logPage.data.result.text
# 保存 next_offset；下一页仍用 after_offset，不增加自定义 limit/max_bytes。

$artifactPage = Invoke-CcsRequest (New-CcsRequest 'run.artifacts' @{
    worker_id = $Worker; run_id = $RunId
    limit = 100; max_bytes = 1048576
})
$artifactPage.data.result.artifacts | ConvertTo-Json -Depth 30
# has_more=true 时继续，用 cursor = $artifactPage.data.result.next_cursor。
```

事件以已交付的序列号继续，日志复用原有字节偏移/轮转协议，产物使用绑定 Run 和路径筛选的 opaque cursor。应用应在成功消费后持久化游标；获取整页并不表示已消费整页。产物是实时清单，游标之前新增的路径需要重新扫描。

事件与产物分页默认 100 项、1 MiB，最大 1000 项、16 MiB；预算针对 UTF-8 item 数组。`has_more` 表示可继续，`truncated` 表示本页达到字节预算。第一项太大时返回 `PAGE_ITEM_TOO_LARGE`，游标不前进。显式分页需要 Worker 分别声明 `run.events.page=1`、`run.artifacts.page=1`；旧 Worker 应按 [升级与回退边界](PAGED-READS.md) 单独部署更新。省略分页选项的旧调用仍可用，但可能因输出预算超限而失败。

验收结果时，应同时保存最终 snapshot、终态事件尾部、退出码和产物回执。`SUCCEEDED` 表示任务进程成功结束，具体产物质量仍由调用方验证；旧的 `RUNNING` 记录不等于实时进程仍存在。可用 `worker.processes` 补充受管进程事实。读取日志或页面不应触发新训练。

## 6. SDK：等待期限、断线游标和保存计划

在安装了 CCS 的 Python 环境内使用同一 gateway 前缀。下面读取现有 Run；它不会提交新任务。

```python
import json
import sys
from pathlib import Path
from ccs import CCSClient, CCSPlan, CCSReadCursor, CCSError

root = Path(r"C:\path\to\CCS")
client = CCSClient(
    [sys.executable, "-m", "controller.gateway_cli",
     "--fleet", str(root / "config/worker-pool.json"),
     "--db", str(root / "data/orchestrator.db"),
     "--audit-db", str(root / "data/ccs-audit.db")],
    cwd=root, actor_id="local-sdk", actor_kind="user", timeout=120,
)
worker_id, run_id = "worker-a", "run_example"
cursor_file = root / "data/events-cursor.json"
cursor = (CCSReadCursor.from_dict(json.loads(cursor_file.read_text(encoding="utf-8")))
          if cursor_file.exists() else CCSReadCursor("run.events", run_id, worker_id=worker_id))
try:
    snapshot = client.wait_run(run_id, worker_id=worker_id, total_timeout=60)
    print(snapshot["status"], snapshot.get("exit_code"))
except CCSError as exc:
    print(exc.to_dict())  # WAIT_TIMEOUT/WAIT_CANCELLED 只结束本机等待。

for event in client.iter_events(run_id, worker_id=worker_id, cursor=cursor,
                                total_timeout=30, limit=100, follow=False):
    print(event)  # 换成实际消费逻辑；成功消费后再保存。
    cursor_file.write_text(json.dumps(cursor.to_dict()), encoding="utf-8")
for chunk in client.iter_logs(run_id, worker_id=worker_id, stream="stdout",
                              total_timeout=30, follow=False):
    print(chunk["text"], end="")
```

游标绑定 action、Run、Worker 和日志 stream。`follow=True` 持续跟随至总期限；事件迭代器在观察终态后再次排空事件尾部。需要可靠消费语义的应用保留自己的最后确认游标，并以原子写入方式持久化。

SDK 提交的两个阶段保持原始身份：

```python
# payload 是上文已准备、已获授权的 run.submit 载荷。
plan = client.plan("run.submit", payload, request_id="sdk-smoke-plan-001")
plan.save(root / "data/sdk-smoke-plan.json")  # 已存在时拒绝覆盖。
print(plan.data)
# 审阅计划并确认属于授权范围之后，另一次调用执行：
# accepted = client.confirm(CCSPlan.load(root / "data/sdk-smoke-plan.json"))
# run_id = accepted["result"]["id"]
```

`cancel_event=threading.Event()` 可停止本机调用、排队或等待；它不发送 `run.cancel`。等待期限包含轮询和调用，期限后另有有界的本机进程/管道清理。SDK 默认最多 4 个活动调用、8 个等待者，MCP 默认 2 个活动调用、8 个等待者；超载分别返回 `GATEWAY_BUSY`、`MCP_BUSY`。详见 [SDK 生命周期](sdk-lifecycle.md)。

## 7. 处理 unknown：先查账本，再做只读协调

已启动的变更因超时、取消、本机进程错误或输出预算中断，可能仍在远端执行。保留 `execution_state=unknown`，先使用原来的数据库和幂等键查询：

```powershell
$operation = Invoke-CcsRequest (New-CcsRequest 'operation.query' @{ idempotency_key = $Key })
$operation.data.result | ConvertTo-Json -Depth 30
$reconciled = Invoke-CcsRequest (New-CcsRequest 'operation.reconcile' @{ idempotency_key = $Key })
$reconciled.data.result | ConvertTo-Json -Depth 30
```

也可按 `operation_id` 或原始 `request_id` 查询，每个 payload 只选一个定位字段。`operation.query` 不发 SSH；`operation.reconcile` 目前只协调 `run.submit`，从记录的原 Worker 按键查找接收事实，并更新本机账本，不重新提交。Worker 需声明 `run.lookup=1`。查询失败、能力缺失或暂未查到都保留 unknown；另一个数据库的 `OPERATION_NOT_FOUND` 不能证明远端未执行。

其他变更的 unknown 需要按动作读取事实，例如取消后的 snapshot、检查点的来源回执或工作区 diff。操作账本会阻止已开始的操作直接重放；修复或重新执行需要确认当前事实并采用独立、明确的恢复操作。不要自动重试变更。

## 8. 检查点副本、导入与真正的模型恢复

先从来源 Run snapshot 中选择当前 `latest`、状态为 `committed` 的确切 checkpoint，记录 ID、绝对路径、大小与 SHA-256。`checkpoint.replicate` 与 `checkpoint.import` 必须分别预演和确认。

```powershell
$CheckpointId = 'ckpt_00000000000000000000000000000000' # 换成 snapshot 返回的真实 ID。
$replicate = New-CcsRequest 'checkpoint.replicate' @{
    worker_id = $Worker; run_id = $RunId; checkpoint_id = $CheckpointId
} $true 'checkpoint-replicate-plan-001'
$replicaPlan = Invoke-CcsRequest $replicate
# 审阅后，按第 4 节同一请求/摘要流程确认 replicate。

$import = New-CcsRequest 'checkpoint.import' @{
    source_worker_id = $Worker; source_run_id = $RunId
    checkpoint_id = $CheckpointId; target_worker_id = 'worker-b'
} $true 'checkpoint-import-plan-001'
# 先完成并核验本机 replica，再单独预演和确认 import。
```

副本复制要求来源 `checkpoint.export=1`，目标导入要求 `checkpoint.import=1`。事务回执绑定来源、内容摘要和发布步骤。文件已完整但 manifest 发布中断时，新的明确复制/导入恢复操作只在来源和 SHA-256 全部吻合后补齐；完整副本返回 `present`，不覆盖有效文件。只读核验不会自行修复。

对 `INCOMPLETE_REPLICA`、`REPLICA_IDENTITY_MISMATCH`、摘要损坏或导入来源冲突，保留文件和错误回执，核实连接配置与来源。不能靠手工编造 manifest/transaction 来通过验证；将问题目录移出副本根目录留存证据后再明确重新复制。具体恢复条件见 [检查点迁移](CHECKPOINT-REPLICATION.md)。

导入成功只证明字节传输和来源校验。真正恢复需另建 Run，将导入的精确路径、SHA-256 和回执绑定到 Spec，通过 [恢复输入门禁](TASK-CHECKPOINT-GATE.md)，并由训练代码验证模型、优化器、调度器、随机数和数据重放状态。导入不会自动启动训练。

## 9. 离线诊断与只读 MCP 配置

先离线检查环境。`doctor` 默认不执行配置中的 gateway，不联系远端、不提交任务；它检查解释器、版本、路径和配置结构。只有明确添加 `--probe-remote --worker-id worker-a` 并提供目标配置，才执行一次只读 Worker health。

```powershell
$CommandJson = (@($Python) + $GatewayArgs) | ConvertTo-Json -Compress
& $Python -m controller.gateway_cli doctor `
    --python $Python --command-json $CommandJson --cwd $Root --require-mcp
```

MCP 可选依赖安装见 [安装说明](INSTALL.md)。生成器默认使用绝对 Python/module 入口，并加入 `--read-only`。先预览具体目标文件和完整内容，再用同一个摘要创建新文件；已有文件拒绝覆盖。

```powershell
$ConfigPath = Join-Path $Root 'mcp-server.local.json'
$previewRaw = & $Python -m controller.gateway_cli mcp-config `
    --python $Python --command-json $CommandJson --cwd $Root --output $ConfigPath
if ($LASTEXITCODE -ne 0) { throw 'MCP 配置预览失败' }
$preview = ($previewRaw -join [Environment]::NewLine) | ConvertFrom-Json
$preview.data | ConvertTo-Json -Depth 30
# 审阅 target 与 content 后，执行相同参数及精确摘要：
& $Python -m controller.gateway_cli mcp-config `
    --python $Python --command-json $CommandJson --cwd $Root --output $ConfigPath `
    --write --confirm-sha256 $preview.data.plan_sha256
```

将生成的 `mcpServers.ccs` 条目交给支持 stdio MCP 的客户端。`--read-only` 只公布 `ccs_read`，同时拒绝 submit/cancel/检查点变更和 workspace 的 stage/apply/restore/discard。重复 `--allow-action` 可进一步限制，例如只允许 `fleet.status` 与 `workspace.execute.inspect`。需要计划/执行工具时，由部署者明确调整启动配置。动作范围配置不替代 SSH 账户权限或隔离。

## 10. 常见问题与本机看板

| 现象 | 处理方式 |
| --- | --- |
| `PROGRAM_NOT_FOUND` / `CWD_NOT_FOUND` | 使用安装环境的绝对 Python 路径，检查 `--cwd` 存在；本机配置路径相对该 cwd 解析。 |
| `ModuleNotFoundError` 或 MCP 版本不符 | 用同一 `$Python -m pip` 安装 CCS 或 MCP extra，再运行 `doctor --require-mcp`；MCP 要求 `>=2.2,<3`。 |
| 中文/空格路径失败、JSON 解析失败 | 逐参数传递 argv，写无 BOM UTF-8 文件；删除向 stdout 打印横幅的外层包装器，诊断信息写 stderr。 |
| `INVALID_REQUEST` / `field_errors` | 根据字段路径和本机 `describe` 修正请求；检查 `dry_run` 为 JSON boolean，`reason` 非空，嵌套 JSON 未截断。 |
| 主机密钥不匹配 | 由管理员核对服务器身份后更新固定 known_hosts；保持严格主机密钥检查。 |
| `GATEWAY_OUTPUT_LIMIT` / `SSH_OUTPUT_LIMIT` | 查看 `output_truncated` 和 continuation；读取采用更小分页并保留最后已消费游标，变更按原身份查询账本。 |
| `WAIT_TIMEOUT` / `WAIT_CANCELLED` | 本机等待结束，远端训练未被取消；重新读取状态，确需停止时走独立 `run.cancel` 计划确认。 |
| `GATEWAY_BUSY` / `MCP_BUSY` | 减少并发/排队数量，稍后重试读取；检查取消后本机 gateway/SSH 子进程是否已清理，勿停止无关训练进程。 |
| `unknown` / `OPERATION_NOT_FOUND` | 保留原请求与计划，使用原数据库 query/reconcile；缺失本机记录不能直接推出远端未执行。 |
| `CONFIG_ALREADY_EXISTS` | 选择新的输出文件，重新预览并确认；生成器不覆盖客户端已有配置。 |

可选 Windows 看板的启动/关闭和数据来源见 [Windows 只读看板](WINDOWS-DASHBOARD.md)。默认只监听 `127.0.0.1:8765`，访问令牌保存在本机私有状态中；不要分享含令牌的链接或上传状态文件。关闭网页、停止看板和停止远端训练是不同操作。

本指南说明 0.6.1 的使用契约；实际 Windows、Linux、真实 SSH/Worker 和训练恢复验收范围以 [支持矩阵](SUPPORT-MATRIX.md) 中的证据为准。
