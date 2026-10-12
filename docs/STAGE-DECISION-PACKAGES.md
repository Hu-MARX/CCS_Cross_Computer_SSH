# 阶段决策包

阶段决策包（Stage Decision Package）把已结束 Stage 的执行、进展、指标、证据状态和 checkpoint 身份冻结成一个带 SHA-256 的事实对象，供研究者、Codex 或其他 Agent 进行后续科研判断。它不包含科研结论，`checkpoint_selection` 始终为 `null`，也不会创建或批准 test。

## 请求

```json
{
  "action": "prepare_decision_package",
  "study_id": "study_...",
  "stage_id": "train",
  "checkpoint_directory": "/absolute/workdir/orchestrator_runs/run_.../checkpoints"
}
```

运行中的 Stage 返回 `readiness=waiting_for_terminal`，不扫描 checkpoint，也不写入决策包。目标 Run 状态无法确认时拒绝生成。

成功终态要求 worker 报告 `checkpoint.state=committed`。软件只构造 worker 报告的最后一个 checkpoint 精确路径，并通过 `trainctl artifacts --path` 重新读取文件大小和 SHA-256；报告与文件不一致时拒绝生成。成功包返回 `readiness=ready_for_scientific_review`，但候选不等于已选择。

失败或取消终态返回 `readiness=ready_for_failure_review`，checkpoint 候选为空，避免把中间恢复点误当成测试权重。

## 不可变性与审计

首次成功生成后，完整 JSON、`package_sha256`、Run、Stage、checkpoint 目录和生成时间写入 SQLite，并记录 `stage_decision_package_prepared` 事件。相同请求直接返回已保存内容，不重新读取远端；同一 Stage 改用其他目录会被拒绝。

Workflow `show` 只投影摘要：readiness、摘要、生成时间和候选数量。完整决策包由显式请求返回，可作为后续 Recommendation 的事实输入；Recommendation 和用户批准仍遵循 Agent Gateway。

## 当前托管 checkpoint 约定

worker 的精确核验只允许：

```text
<Experiment workdir>/orchestrator_runs/<RUN_ID>/checkpoints/<worker已报告的checkpoint ID>.ckpt
```

文件必须位于 workdir 内且不是符号链接。worker 会重新计算 SHA-256，并与原子提交记录中的大小和摘要比较。该规则不会开放目录遍历，也不会提供含义模糊的“最新文件”选择。
