# CCS Agent Interface v1

`ccsctl` 是面向 Codex、OpenCode、Kimi、普通脚本和人工 CLI 的统一进程接口。调用方写入一个 UTF-8 JSON 请求，CCS 完成输入校验、能力协商、安全分类、远端调用和元数据审计，并始终输出一个 JSON 信封。

Python SDK 和可选的 stdio MCP 接入复用该协议，见 [Agent 与 harness 接入指南](AGENT-INTEGRATION.md)。

## 离线动作合同

`ccsctl describe` 列出安装包内全部动作；`ccsctl describe --action run.submit`
查询字段、必填项、限制、分类、结果结构和示例。工作区子动作使用
`ccsctl describe --action workspace.execute --workspace-action stage`。
查询不需要 worker、SSH、网络或数据库；指定了连接选项时也不读取它们。

`ccsctl describe --schema request`、`--schema result`、`--schema experiment-spec`
输出自包含的 JSON Schema。共同合同位于包资源 `ccs/resources/action-contracts.json`，
发布的 schema 与 [生成的动作参考](ACTION-CONTRACTS.md) 均由同一来源生成。
开发时用 `python scripts/sync-contract-schemas.py --check` 检查 schema 漂移。

JSON gateway 的等价动作是 `gateway.describe`，payload 为 `{}`，或包含 `action`、
`workspace_action`；它是无需目标配置的本地读取。SDK 使用 `client.describe("run.submit")`；
MCP 使用 `ccs_read(action="gateway.describe", payload={"action":"run.submit"})`。

请求信封、动作载荷和 workspace 子请求会在目标读取前进行字段验证，错误包含
`field_errors` 的 `path/code/message`。合法旧请求、默认 worker 规则和元数据扩展保持兼容；
以前被忽略的未知字段、非法类型和缺失必填项现在本地拒绝。bool 不再充当整数。
动态 worker 能力、文件路径准入和确认摘要仍由原执行流程检查。

## 调用

```powershell
ccsctl `
  --host ccs-worker `
  --user mlrunner `
  --workspace-user worker-a `
  --db data/orchestrator.db `
  --audit-db data/ccs-audit.db `
  request --file examples/ccs-snapshot-plan.json
```

也可将 `--file -` 与标准输入组合。请求上限为 20 MiB。成功退出码为 `0`，校验、兼容或远端错误为 `1`，用户中断为 `130`。标准输出只有一个 JSON 信封：`ok`、`request_id`、`data`、`error`。

多从机模式使用 `--fleet` 代替 `--host`：

```powershell
ccsctl --fleet .\config\worker-pool.json `
  --db data/orchestrator.db --audit-db data/ccs-audit.db `
  request --file .\examples\ccs-fleet-status.json
```

从机池格式由 `schemas/worker-pool.schema.json` 描述。配置只保存连接身份和非敏感标签，不保存密码或私钥内容；`identity_file` 只引用本地私钥路径。

开发目录无需安装入口时，可把上述 `ccsctl` 替换为 `.\scripts\ccs.ps1`。

## 请求与计划确认

公共字段由 `schemas/ccs-request.schema.json` 描述。`dry_run` 必须显式提供。预演返回 `classification`、所需能力、兼容状态、是否可执行和 `plan_sha256`，但不执行目标动作。

只读动作可将 `dry_run` 设为 `false` 直接执行。变更动作需要两次调用：

1. 保持请求身份、动作和载荷不变，以 `dry_run: true` 获取 `plan_sha256`。
2. 改为 `dry_run: false`，增加 `confirm_plan_sha256`，其值必须与上一步完全一致。

改变 `request_id`、`actor`、`action` 或 `payload` 都会改变摘要并使旧确认失效。
`run.submit` 在远端调用前原子登记幂等键、目标 worker 和规范化 spec 摘要；
共享同一控制端数据库的调用者只能由一次原子状态转换取得派发资格。
不同 worker/spec 的同键请求在远端副作用前拒绝。各自使用独立数据库的控制端不具有跨库全局幂等保证。

## 动作

| 动作 | 分类 | 载荷摘要 |
|---|---|---|
| `gateway.describe` | 无目标的本地只读 | 可选 `action`、`workspace_action`；返回安装包合同 |
| `fleet.status` | 本地聚合只读 | 空对象或 `timeout_seconds` / `worker_timeout_seconds`；有限并发返回各从机事实和超时/容量状态 |
| `worker.health`、`worker.monitor`、`worker.alerts`、`worker.storage`、`worker.processes` | 只读 | 空对象或可选 `worker_id`；返回实际目标身份 |
| `run.snapshot` | 只读 | `run_id` |
| `run.events` | 只读 | `run_id`、`after_seq`；可加 `limit` / `max_bytes` 启用有界分页 |
| `run.logs` | 只读 | `run_id`、`stream`、`after_offset` |
| `run.artifacts` | 只读 | `run_id`，可选绝对 `path`；可加 `cursor` / `limit` / `max_bytes` 启用有界分页 |
| `run.submit` | 变更 | `spec`、`idempotency_key`；多从机时必须有 Agent 选择的 `worker_id` |
| `run.cancel` | 变更 | `run_id`、`reason`，未纳管 Run 需给出 `worker_id` |
| `checkpoint.replicate` | 变更 | `run_id`、`checkpoint_id`；未纳管 Run 需给出 `worker_id` |
| `checkpoint.import` | 变更 | `source_worker_id`、`source_run_id`、`checkpoint_id`、`target_worker_id` |
| `workspace.execute` | 由子动作决定 | `request`，多从机时必须给出 `worker_id` |
| `operation.query` | 本地只读 | `operation_id`、`idempotency_key`、`request_id` 三者恰好一个 |
| `operation.reconcile` | 远端只读及本地对账 | 同上；目前只支持 `run.submit`，不重新派发 |
| `gateway.audit` | 本地只读 | 可选 `request_id`、`action`、`outcome`、`operation_id`、`worker_id`、`run_id`、`limit` |

`workspace.execute` 的 `inspect`、`diff`、`changes` 为只读；`stage`、`apply`、`restore`、`discard` 为变更。详细路径约束和可恢复删除语义见 `docs/REMOTE-WORKSPACE.md`。

`run.submit` 接受通用 `ExperimentSpec`。任务种类包含训练、测试、分析、构建、仿真、基准测试和数据处理，因此同一接口可用于深度学习或具身智能项目；CCS 不理解或决定模型的科研含义。

## 多从机协调边界

上层 Agent 先调用 `fleet.status` 获取事实，再把明确的 `worker_id` 放入 `run.submit`。CCS 不按空闲程度自动选择，也不会在执行时悄悄换机；`worker_id` 属于计划摘要，改变它会使旧确认失效。同一幂等键若已经在另一从机标识一个 Run，CCS 会在远程提交前拒绝冲突。

五种 `worker.*` 诊断读取可显式选择任意已注册从机；未知 ID 返回 `WORKER_NOT_FOUND`，
不会回退到默认从机。disabled 从机仍可接受这些读取，不能提交新 Run。
空 payload 保持旧默认 `gateway.remote`：CLI 的 `--host` 使用指定 host，
`--fleet` 使用配置首项。自定义嵌入调用方传入的默认 adapter 与目录首项不同时，
仍查询原 adapter；身份规则见 [P0-01 交付说明](P0-01-WORKER-READS.md)。

已由 CCS 登记的 Run 可按本地记录自动路由快照；未纳管 Run 必须显式提供 `worker_id`。事件、日志、产物、取消和工作区操作都遵循相同路由规则。

`checkpoint.replicate` 只复制 Worker 状态中标为 `committed` 的当前最新检查点，并在 Windows 的 `--replica-root` 下保存不可变文件与来源回执。`checkpoint.import` 只接收该副本库中重新验签通过的文件，把它传到明确指定的目标 Worker；目标端再次验签并原子发布。两个动作均须各自预演和确认，不会自行选择 checkpoint、目标 Worker 或启动恢复 Run。详细契约见 [跨节点检查点迁移](CHECKPOINT-REPLICATION.md)。

从机池 v1 协调的是同一 Study 中的多个独立 Run。单个 Run 同时跨两台服务器进行 NCCL/DeepSpeed 分布式训练尚未实现。

## 审计与隐私

`data/ccs-audit.db` 仅记录请求标识、调用者、动作、只读/变更分类、预演摘要、结果状态、
错误码、时间，以及可获得的 `operation_id`、`worker_id`、`run_id`、`execution_state`。
原始载荷、模型源码、命令、路径、日志和响应正文不会写入审计表。
可通过 `gateway.audit` 组合筛选最近 1–200 条记录；`outcome` 支持
`planned`、`started`、`unknown`、`succeeded`、`failed`。`started` 审计表示处理开始，
不独自证明远端派发或接受。

## 执行结果未知与操作查询

已执行变更的成功数据增加 `operation` 与 `execution_state`，错误增加可获得的关联身份。
`execution_state` 区分 `not_sent`、`rejected`、`confirmed`、`unknown`；
`confirmed` 对提交表示 worker 已接受 Run，不表示训练完成。
超时、传输断开、控制端退出和本地提交失败都可能留下 unknown。
兼容字段 `executed`、`retryable` 保留，但不能单靠错误码或 `retryable` 推断远端未执行。

使用 [本地查询示例](../examples/ccs-operation-query.json) 读取持久化操作身份；
此动作不调用 SSH。使用 [提交对账示例](../examples/ccs-operation-reconcile.json)
从原目标 worker 按原幂等键查询接受记录，并修复本地 Run 登记。
对账虽分类为 read，可能写入控制端数据库；它不提交、取消或改派远端任务。
缺少查询能力、暂未查到或失联时仍保持 unknown，不自动重新提交。

两个动作的 payload 都必须恰好含一个合法选择字段；未记录返回 `OPERATION_NOT_FOUND`。
`request_id` 若匹配不同 actor 的多个操作，返回 `AMBIGUOUS_OPERATION`；优先保存并使用
`operation_id`，提交也可使用原 `idempotency_key`。查询接口不接受 actor 筛选。

已确认变更的最终审计写入失败时，保留成功结果并附带 `AUDIT_UNAVAILABLE` warning。
审计数据库初始化失败时，只读动作及 dry-run 仍可执行并返回同类 warning；
确认执行的变更会在发送前因审计不可用而被阻止。操作查询不因此失去访问入口。
操作账本是恢复依据，审计缺失不证明远端未执行。其他变更有操作记录与重放护栏，
但尚无通用自动对账；具体状态与恢复方法见 [P0-03 操作说明](P0-03-OPERATIONS.md)。
