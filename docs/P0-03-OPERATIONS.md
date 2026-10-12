# P0-03：执行状态、操作查询与恢复

交付日期：2026-10-12。范围为本地控制端实现、Fake Worker、传输故障模拟与文档。
测试数量和批次结果以本批交付记录为准；没有真实远端任务、Linux 故障演练或 harness 验收。

## 操作身份与事实

确认后的变更在远端副作用前保存操作身份。`run.submit` 将幂等键绑定到 worker、
规范化 spec 摘要及请求身份；原子状态转换决定哪个调用者可以派发。
SQLite 写事务在 SSH 调用前结束，不持有写事务等待网络。
其他变更也保存 `operation_id`、请求与 actor 身份、worker 和计划摘要。

同一提交键的 worker/spec 不同返回 `IDEMPOTENCY_CONFLICT`；同一请求与 actor 身份
被用于不同变更意图时返回 `OPERATION_CONFLICT`。共享同一控制端数据库的调用者
共同遵守原子登记；使用各自数据库的控制端没有跨库全局幂等保证。

`state` 是账本阶段，`execution_state` 是对外表达的执行事实：

| state | execution_state | 含义及下一步 |
|---|---|---|
| `pending` | `not_sent` | 已保留身份，尚未取得派发阶段；可用原身份继续已授权的确认 |
| `not_sent` | `not_sent` | 已知未发送目标副作用；修复原因后可用原身份重试 |
| `started` | `unknown` | 已取得派发资格；退出或响应丢失后不能推断是否接受 |
| `unknown` | `unknown` | 已派发或可能派发，当前证据不足；查询并对账 |
| `confirmed` | `confirmed` | 已确认动作响应；提交时仅表示 Run 被接受 |
| `rejected` | `rejected` | worker 明确拒绝；保留拒绝事实，不自动重派 |

从机接受 Run、训练进程运行和训练成功是不同事实。`confirmed` 提交不把 Run 标成成功。
未知结果不触发自动换键、换 worker 或远端提交重放。泛型变更进入 started 后，
相同请求身份再执行返回 `OPERATION_ALREADY_RECORDED`；先读原操作。
`pending/not_sent` 允许原身份接续，因为尚未取得派发资格或已明确未发送。

## 查询合同

`operation.query` 与 `operation.reconcile` 的 payload 必须恰好含一个合法字段：

```json
{"operation_id": "op_00000000000000000000000000000000"}
```

也可替换为 `{"idempotency_key":"my-proposed-run-v1"}` 或
`{"request_id":"original-submit-001"}`，不能组合多个选择字段。
ID 使用现有安全字符合同：首字符为字母或数字，之后只允许字母、数字、点、
下划线和连字符，总长度最多 128。

`operation.query` 读取原 `--db` 的操作账本，不调用 SSH；不存在时返回
`OPERATION_NOT_FOUND`。成功的 `data.result` 包含 `operation_id`、`action`、
`request_id`、actor、worker、可获得的幂等键/Run ID、状态、摘要和时间。
它不返回保存的原始 spec。`request_id` 可以在不同 actor 下复用，多个匹配时返回
`AMBIGUOUS_OPERATION`；接口不接受 actor 过滤，优先使用 `operation_id` 或提交键。

`operation.reconcile` 目前只接受 `run.submit` 操作；其他动作返回
`RECONCILIATION_UNSUPPORTED`。它分类为 read，远端仅查询，但会持久化本地对账结果。
对 started/unknown 提交，在原目标 worker 按原 key 调用 `lookup-submission`：

- 已找到接受记录：原子保存 Run、提交事件及 confirmed 操作事实。
- 未找到、能力缺失或暂时失联：保持 unknown，记录相应错误事实。
- 原记录已是 pending、not_sent、confirmed 或 rejected：返回已有事实，不重新派发。

worker 必须声明 `run.lookup=1` 才可使用自动接受记录查询。不能把一次 absent
响应当成原请求未执行的证明；它不解除派发护栏。对账本身即使调用成功，
也必须检查 `data.result.execution_state`，不能只看信封的 `ok`。

示例：[本地查询](../examples/ccs-operation-query.json)、
[提交对账](../examples/ccs-operation-reconcile.json)。
配置 `--db` 应与原提交一致；数据库换了、丢了或只查到别人的数据库时，
`OPERATION_NOT_FOUND` 不能证明远端没有 Run。

## 传输、SDK、MCP 与审计

OpenSSH 已启动后的超时、非结构化错误和损坏响应，对变更采取 unknown 保守语义。
本地无法启动 gateway/SSH 的明确失败可报告 not_sent；结构化 worker 错误
只有明确声明拒绝或未发送时才使用相应事实。变更错误不自动重试。
SDK 超时限制本地交换，不取消训练；SDK、CLI 和 MCP 错误都保留
`execution_state`、`operation_id`、`worker_id`、`run_id`、`idempotency_key`
等可获得的关联字段。SDK 的 `CCSError.to_dict()` 返回统一元数据结构。

成功变更的 `data` 增加 `operation` 和 `execution_state`，保留旧合同字段。
MCP `ccs_read` 提供两种操作查询，不绕过 gateway 调用 worker 内部类。
`retryable` 只是错误提示，不能独自证明副作用未发送；错误/超时不能直接归结为“未执行”。

审计增加 started、unknown，以及 operation/worker/run/execution_state 关联。
`gateway.audit` 可按 `operation_id`、`worker_id`、`run_id` 筛选，原过滤字段仍可使用。
started 审计可能早于真正派发，派发资格和恢复依据由操作账本决定。
审计不保存源码、原始任务载荷、密钥或日志。提交恢复所需的规范化 spec 由现有
Run 数据库的操作表保存；操作查询只输出元数据和摘要。

审计数据库初始化遇到 SQLite/文件系统错误时，gateway 的只读动作和 dry-run 仍可用，
并附带 `AUDIT_UNAVAILABLE` warning；因此审计损坏后仍可查询操作状态。
已确认的变更在派发前检查审计可用性；不可用会阻止本次发送，并报告可查询的原身份。
派发后的错误审计写入失败不能抹掉操作账本的 unknown 事实。
已确认动作的最终审计失败保留成功响应，并增加 `AUDIT_UNAVAILABLE` warning；
不会将已知接受误报成未发送。若本地操作记录失败但已拿到明确响应，
错误保留已知确认事实，重启后仍应查询账本并按动作核查。

## 其他变更的恢复策略

本批给这些动作增加持久化身份与重放护栏；不会将它们纳入提交查询的自动对账。
只读核查不能自行将 generic 操作账本改成 confirmed；保留原记录和观测证据。

| 动作 | 结果未知后应查的事实 | 当前限制 |
|---|---|---|
| `run.cancel` | 原 worker/Run 的 `run.snapshot`、事件及终态；必要时观察受管进程是否退出 | 取消请求已发出不证明终止完成；原 Run 已终态时核对该终态，不反复取消 |
| `workspace.execute` | 用 `changes` 找原变更，若已有 change ID 则查 `diff` 与记录；用受限 `inspect` 比对项目状态和摘要 | stage 响应丢失不能通过另发 stage 证明旧 stage 失败；apply/restore 必须保留原摘要，继续遵守并发文件保护 |
| `checkpoint.replicate` | 原 worker/Run/checkpoint 对应的本地 manifest、来源回执、大小与 SHA-256 | 文件存在不等于归档完整；`INCOMPLETE_REPLICA` 的自动修复仍留给 P1-04，不直接补造 manifest |
| `checkpoint.import` | 原目标 worker 的导入 manifest 和 receipt，以确定的 import ID 使用已有只读 `trainctl checkpoint-import show <import_id>` 核验 | 不能只凭 SCP 返回或文件名确认完整导入；目标回执也不证明模型恢复成功 |

工作区子动作与路径约束见 [受控远程工作区](REMOTE-WORKSPACE.md)；
检查点字节与回执核验见 [跨节点检查点迁移](CHECKPOINT-REPLICATION.md)。
新的远端变更要先完成这些核查，并遵守用户授权和原有 dry-run 确认合同；
不能通过新 request ID 规避已有未知操作。

## 升级与回滚边界

控制端数据库采用增加表/字段的最小迁移，历史 Run 的接受记录可补入 confirmed 操作。
迁移前保留数据库备份；停止控制端写入后再复制 SQLite 数据库，或使用 SQLite 备份接口。
原 Run 表不重写，保留历史 Run 身份。

P0-01 的诊断路由及本地原子准入不需要 worker 升级。
自动提交对账需要新 worker 的 `run.lookup=1`；远端升级是独立部署工作，
本批未实施，不在正在训练的 worker 上覆盖安装目录。

回退控制端版本前先停用写入口，保留新数据库与操作表证据。旧控制端能读原表
不等于仍具有新派发护栏；不得让旧、新控制端同时对同库执行变更，也不得让旧版本
重放 unresolved 的操作。P1-06 将完善版本化部署和完整迁移/回滚演练。
