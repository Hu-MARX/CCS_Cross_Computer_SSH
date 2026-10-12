# Agent Gateway v1

Agent Gateway 是 `WorkflowEngine.execute(request)` 上的厂商无关 JSON Interface。Codex、其他 Agent、人工脚本和后续 Python SDK 都通过同一入口提交科研建议；软件不调用或模拟任何特定大模型。

## Recommendation v1

Recommendation 是意见，不是授权。`PROCEED`、`WAIT`、`STOP` 都只改变目标 Stage 的准入状态；它们不会直接启动、取消或删除任务。

```json
{
  "action": "recommend",
  "study_id": "study_00000000000000000000000000000000",
  "recommendation": {
    "schema_version": 1,
    "idempotency_key": "study-a-test-recommendation-v1",
    "stage_id": "test",
    "plan_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
    "artifact_bindings_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
    "proposer": {"id": "codex-local", "kind": "agent"},
    "decision": "PROCEED",
    "summary": "训练阶段完成，建议进入预注册的最终测试。",
    "rationale": "主要训练与验证检查均已满足计划条件。",
    "confidence": 0.9,
    "skills_used": ["nature-statistics"],
    "evidence": [{
      "run_id": "run_00000000000000000000000000000000",
      "result_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
      "observed_at": "2026-09-05T08:00:00Z",
      "observation_state": "fresh"
    }],
    "limitations": ["尚未执行 held-out test"]
  }
}
```

完整结构见 `schemas/recommendation.schema.json`。单条建议最多 128 KiB；列表均有数量和字符串长度限制。

## PROCEED 准入约束

- 目标必须是当前 Study 的 `test` Stage。
- `plan_sha256` 必须匹配当前不可变 StudySpec。
- 如果 Stage 使用动态输入，`artifact_bindings_sha256` 必须匹配软件已经核验并展示的当前 Artifact Binding 集合；静态输入不得伪造该字段。
- 所有直接依赖 Stage 必须成功，并且其 Run 必须出现在 evidence 中。
- evidence 只能引用当前 Study 已关联的 Run。
- evidence 必须是 fresh，时间戳必须含时区且处于十分钟新鲜窗口内。
- 至少声明一种实际使用的分析方法或 Skill；标识不绑定特定厂商。
- 相同幂等键和相同内容返回原 Recommendation；同键不同内容被拒绝。
- 最新 Recommendation 覆盖同一 Stage 的旧建议。新的 WAIT/STOP 会让旧 PROCEED 及其批准失效。

## 用户批准

批准必须同时指向 Study、Stage、当前计划摘要和最新 PROCEED Recommendation。动态产物摘要已包含在 Recommendation 中，因此批准也间接锁定了精确输入：

```json
{
  "action": "approve",
  "study_id": "study_00000000000000000000000000000000",
  "stage_id": "test",
  "plan_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
  "recommendation_id": "rec_00000000000000000000000000000000",
  "approved_by": "researcher-id"
}
```

只有随后执行 `reconcile` 时，软件才会重新检查 Recommendation、新鲜度、批准、依赖和预算，并按幂等键提交 test Run。CLI 调用方式：

```powershell
orchestratorctl --host HOST --db data/orchestrator.db workflow --request REQUEST.json
```

Recommendation 门只适用于最终 test。非 test Stage 如果在 StudySpec 中单独声明 `requires_approval`，仍使用绑定计划摘要的普通人工批准，不需要伪造科研建议。

当前信任边界是本机调用者：`proposer.id` 和 `approved_by` 是审计标识，不是密码学身份。多用户或网络开放之前必须增加认证与签名；当前本地回环和命令行模式不应被误称为多租户安全。
