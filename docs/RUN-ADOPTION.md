# 现有 Run 纳管

Run Adoption 用于把工作流引入前已经启动的真实任务关联到一个计划中的 Stage。它只建立可审计关系：不创建、不重启、不停止远端进程，也不会把旧任务追溯解释为已经获得测试授权。

## 请求

```json
{
  "action": "adopt_run",
  "study_id": "study_...",
  "stage_id": "train",
  "run_id": "run_...",
  "run_spec_sha256": "64位小写SHA-256",
  "idempotency_key": "adopt-existing-training-v1",
  "adopted_by": "codex-on-behalf-of-user"
}
```

调用前，Agent 应先展示将要关联的 Study、Stage、Run 和规格摘要。`run_spec_sha256` 必须来自本地 Run 登记记录的规范化 `ExperimentSpec`，不能根据进程命令行猜测。

## 安全约束

- Run 必须已登记在当前 Windows 编排数据库中，且属于当前配置的从机。
- 纳管前执行一次只读远端对账；目标 Run 当前状态无法确认时拒绝写入关系。
- Stage 必须尚未关联 Run，并且是无依赖的根 Stage。
- v1 不允许纳管 `test` 或任何 `requires_approval=true` 的 Stage，防止绕过 Recommendation 和用户授权。
- Stage 的 `ExperimentSpec` 与既有 Run 必须完全匹配。唯一兼容项是：历史 Run 没有 `task`，而计划只补充与 Stage 类型一致的 `task.kind` 和空 `inputs`；此时写入 `metadata_upgrade=legacy_kind_and_empty_inputs`。
- 一个 Run 只能属于一个 Stage。相同幂等键和完全相同请求可安全重放，不同内容会被拒绝。

## 审计与展示

数据库保存操作者、请求摘要、Run 规格摘要、兼容升级类型和创建时间，并记录 `stage_run_adopted` 事件。Workflow `show` 返回 `stage.run_link.kind=adopted`；Windows 看板在关联 Run 一栏显示“已纳管”，与 `submitted` 区分。

纳管后，Stage 状态跟随 Run 的已确认状态。后续 `reconcile` 使用与普通工作流 Run 相同的状态机，但不会因为建立关系而执行尚未授权的下游测试。
