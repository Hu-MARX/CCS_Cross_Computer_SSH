# Artifact Binding v1

Artifact Binding 把成功上游 Run 的已验证产物转换为下游 Stage 的不可变输入。调用者只在 StudySpec 中声明来源和选择规则；路径、大小、SHA-256、来源 Run 与核验时间由工作流软件记录，Agent 提供的任意摘要都不能替代从机产物清单。

## 声明

动态 test Stage 不在 `spec.task.checkpoint` 中预填不存在的摘要，而是在 Stage 上声明：

```json
{
  "id": "test",
  "kind": "test",
  "depends_on": ["train"],
  "requires_approval": true,
  "bindings": [{
    "role": "checkpoint",
    "from_stage": "train",
    "selector": {
      "path": "/srv/research/outputs/${RUN_ID}/best.ckpt"
    }
  }],
  "spec": {
    "schema_version": 1,
    "name": "Held-out test",
    "project": {},
    "runtime": {},
    "command": {"argv": ["/absolute/python", "/absolute/test.py"]},
    "task": {"kind": "test", "inputs": []}
  }
}
```

v1 只支持 `checkpoint` 角色；`from_stage` 必须是直接依赖；selector 是精确绝对 POSIX 路径，仅允许 `${RUN_ID}` 占位符。训练 Stage 的 `outputs.checkpoint_glob` 或 `outputs.artifact_glob` 必须覆盖该文件。

## 对账与准入

1. 上游 Stage 必须已 `SUCCEEDED`。
2. 工作流通过 RemoteRunner 的产物清单查找精确路径，要求唯一记录同时带有非负大小和 64 位小写 SHA-256。
3. 首次成功核验后，绑定及其摘要写入 SQLite，并显示在 Stage 的 `artifact_bindings` 与 `artifact_bindings_sha256`。
4. 目标 test 的 PROCEED Recommendation 必须携带同一个 `artifact_bindings_sha256`；用户批准该 Recommendation 后才可执行。
5. 提交 test Run 时，软件从绑定生成 `task.source_run_id` 和 `task.checkpoint`，但不修改原始 StudySpec 或 `plan_sha256`。

找不到产物时为 `artifact_binding_required`；连接不可用时为 `artifact_binding_unavailable`；清单缺少摘要或含重复精确路径时为 `artifact_binding_invalid`；已经绑定的路径、大小、摘要或来源发生变化时为 `artifact_binding_changed`。这些状态都不会启动下游 Run。

## 边界

SHA-256 在从机的 `trainctl artifacts --path <精确路径>` 中按需计算；普通清单不会逐个读取大型模型文件。v1 适合成功后不再修改的最终产物；没有把“文件名最新”或“修改时间最新”当成科研意义上的最佳模型。最佳 checkpoint 的命名和选择策略应由训练代码及实验计划明确产生。
