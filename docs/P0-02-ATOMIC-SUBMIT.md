# P0-02：共享控制端数据库下的原子提交

交付日期：2026-10-12。本项同时提供 P0-03 使用的操作账本；仅完成本地实现和模拟验收。

## 解决的问题

原实现先读取本地 Run，再提交 worker，最后写入 Run。两个共享数据库的调用者可能同时通过空记录检查并向不同 worker 提交同一个键。本次将键登记放在任何远端提交前；worker、规范化 ExperimentSpec 摘要与操作身份在同一个 SQLite 事务中保留。

`reserve_submission` 返回“可以尝试领取”的状态，不代表调用者已经拥有派发权。只有 `begin_submission` 将 `pending/not_sent` 原子更新为 `started` 的调用者能够提交。事务在 SSH 调用之前结束。远端返回后，Run、首个 submitted 事件和 `confirmed` 操作在一个本地事务内提交，避免半条 Run 或重复首事件。

同一键改变 worker 或 spec，立即返回 `IDEMPOTENCY_CONFLICT`。同一 actor/request 身份改变操作意图，返回 `OPERATION_CONFLICT`。actor 只用于关联，不构成身份认证。spec 摘要来自 `ExperimentSpec.to_dict()` 的 UTF-8、排序键、紧凑 JSON；映射顺序不影响比较。

## 状态与恢复

|账本 state|对外 execution_state|下一步|
|---|---|---|
|pending|not_sent|尚未领取；显式原请求可领取一次派发权|
|not_sent|not_sent|有明确未发送证据；显式相同请求可继续|
|started|unknown|已经越过派发边界，不能根据控制端退出认定未执行|
|unknown|unknown|只读对账；绝不自动换键、换 worker 或再次派发|
|confirmed|confirmed|返回已记录 Run；重复提交不调用 worker|
|rejected|rejected|worker 已明确拒绝；本身份不重新派发|

`Orchestrator.reconcile_submission` 只调用 worker 的 `lookup_submission(idempotency_key)`，查询接受记录并修复本地 Run；它从不调用 submit。查询不到、worker 不可达或不支持 `run.lookup: 1` 时保持 `unknown`，因为查不到也可能与正在进行的提交竞争。保留阶段的 `pending` 对账仅返回本地事实，不偷偷开始任务。

控制端在远端接受后、本地确认前退出，可以在重启后通过相同键查回 Run。若本地写入或错误标记均失败，原先持久化的 `started` 仍阻止重派。错误携带 `operation_id`、`worker_id`、已知 `run_id`、幂等键及 `execution_state`，并固定 `retryable=False`。旧 worker 可以继续使用原有提交能力；未知结果的自动对账需要升级 worker 以提供只读 lookup 能力。

通用变更复用相同 operations / operation_requests 账本。它们按 actor/request 身份及 action、worker、plan SHA-256 绑定，重复 `started/unknown/confirmed/rejected` 不获得派发权。取消、workspace、checkpoint 的结果未知记录可以查询；其动作特定恢复应按 P0-03 文档执行，不能以重新派发代替恢复。

## 数据库迁移与兼容

新增 `operations`、`operation_requests`、`repository_migrations`，保留原 runs/events/decisions 的表结构。首次初始化记录迁移版本 1，并把已有幂等键 Run 转为 `confirmed` 账本；保留历史时间、worker 和规范化 spec 摘要。迁移及补录事务可重入，不更新历史 Run/事件。初始化后经旧 API 导入的 Run，在首次提交登记时也会补录并检查 worker/spec。

原 `Orchestrator.submit(spec, idempotency_key=...)` 与 `WorkerDirectory.submit(worker, spec, key, repository)` 调用形式保留；新增 request/actor/plan 元数据参数为可选。直接 WorkerDirectory 提交也检查 disabled，防止绕过 gateway 准入。操作查询仅返回身份、摘要和状态；完整 spec 只在本地内部恢复数据中存储，延续原 Run 数据库的本地保护边界，不进入操作错误或审计。

升级前停止新的控制端变更调用，备份 SQLite 数据库，随后再打开新版控制端。附加表允许旧程序读取已有 Run，但有 `pending/started/unknown` 操作时不能回滚旧程序继续提交：旧程序不读取账本，会重新打开原风险窗口。先用新版只读对账解决或保留这些操作，再将旧程序限于只读。不要删除 unknown 记录来解除阻挡，也不要只恢复旧数据库后继续提交同一批键。

本保证仅覆盖共享同一 SQLite 数据库的控制端。两个独立数据库的控制端仍无法阻止分别向不同 worker 派发；这一情形需要单独设计共享协调，不能称为全局幂等。

## 改动与本地证据

- `src/controller/repository.py`：操作表、最小迁移、原子保留/CAS、请求身份关联和原子确认。
- `src/controller/orchestrator.py`：先登记再派发、结果未知错误及只读恢复。
- `src/controller/fleet.py`：submit 使用统一账本并阻止 disabled worker 派发。
- `tests/test_submission_operations.py`：两线程且各自使用独立 repository 实例共享数据库；同目标与跨目标竞争、spec 冲突、pending/started 重启、远端接受后退出、丢失响应、本地事务失败、错误标记失败、能力缺失、请求身份和旧库迁移。

开发相关验证：`python -m unittest tests.test_submission_operations tests.test_core -q`。完整批次验收记录在批次交付文档；未进行远端部署、SSH 故障注入、GPU 任务或 Linux 真实生命周期验收。
