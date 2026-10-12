# 跨节点检查点迁移

`CheckpointReplicaStore` 是 CCS 的主机侧检查点归档与迁移模块。它解决 Worker 故障或临时离线时恢复依据只存在于单台服务器的问题，但不替上层 Agent 选择“最佳模型”，也不自动在另一台 Worker 上启动任务。

## 安全契约

一次 `checkpoint.replicate` 必须明确绑定 `worker_id`、`run_id` 和 `checkpoint_id`。CCS 依次验证：

1. Worker 声明 `checkpoint.export=1`；
2. Run 快照将该 ID 报告为 `checkpoint.state=committed` 的当前 `latest`；
3. Worker 对精确绝对路径返回唯一 Artifact，且其大小与 SHA-256 和快照一致；
4. SCP 下载到临时文件后，Windows 重新计算大小与 SHA-256；
5. 下载前持久化包含完整来源、Artifact 摘要和待发布 manifest 的事务回执；
6. 只有全部一致才原子发布 checkpoint 和带摘要的来源回执。

旧检查点、仅文件名匹配的文件、半写文件、摘要漂移、重复路径、本地损坏副本和符号链接路径都会被拒绝。重复请求在已有副本完整时返回 `present`，不会重新覆盖。

## 本地布局

默认根目录是 `data/checkpoint-replicas`，也可用 `ccsctl --replica-root` 覆盖：

```text
<replica-root>/<worker-id>/<run-id>/<checkpoint-id>/
  checkpoint.ckpt
  manifest.json
  transaction.json
  replica.lock
```

`manifest.json` 记录 Worker SSH 身份、远端 CCS 根目录、原始 Artifact 路径、大小、SHA-256 和回执摘要，不记录密码或私钥。

`transaction.json` 在下载前保存相同来源和内容身份，并记录受限临时文件名及
`prepared`、`verified`、`published`、`committed` 状态。每次写入先同步文件，再原子
替换；POSIX 平台同时同步目录。恢复以实际文件、回执和 SHA-256 为依据，状态字段
只提供过程证据。完整的旧版副本没有事务文件时仍可读取和重复验证。

## 中断恢复

在文件发布前后或 manifest 写入前后中断，可再次发起 `checkpoint.replicate` 的预演
及确认。确认执行会在原有锁内检查当前 Worker 的主机、用户、端口、CCS 根目录及
Artifact 是否与事务记录完全一致，然后重新计算候选文件的大小和 SHA-256：

- 临时文件已完整时，复用该文件完成发布，不再次下载；
- checkpoint 已发布而 manifest 缺失时，只补写原事务绑定的 manifest；
- manifest 完整时返回 `present`，不改写 checkpoint；
- 不存在候选文件时，可以根据原事务重新下载。

`verified()` 和目标端 `show` 是只读核验，不补写回执。只有明确的复制或导入变更
才能执行修复。损坏文件、来源变化、事务摘要不符、符号链接及只有 checkpoint
而无可靠来源回执的目录都会拒绝，不凭文件名或字节内容猜测来源。错误会保留
现有文件并指出路径；将问题文件或整个问题目录移到存储根目录外留作检查，再
通过预演和确认重新复制。不要为旧版不完整副本手工编造事务回执。

来源或 Artifact 的变化需要核实 Worker 选择和连接配置；副本修复仍要求源 Worker
将该 checkpoint 报告为当前已提交的 `latest`。这里的摘要用于检测损坏及身份漂移，
不构成密码学签名，存储目录仍依赖本机访问控制。

## Agent Gateway 调用

先提交预演请求：

```json
{
  "schema_version": 1,
  "request_id": "replicate-001",
  "actor": {"id": "codex-local", "kind": "agent"},
  "action": "checkpoint.replicate",
  "dry_run": true,
  "payload": {
    "worker_id": "worker-a",
    "run_id": "run_00000000000000000000000000000000",
    "checkpoint_id": "ckpt_00000000000000000000000000000000"
  }
}
```

从结果读取 `plan_sha256`，保持身份、动作和载荷完全不变，把 `dry_run` 改为 `false` 并增加 `confirm_plan_sha256` 后再次调用。多 Worker 环境中，由上层 Agent 显式决定来源 Worker；CCS 不自动改派。

```powershell
.\scripts\ccs.ps1 --fleet .\config\worker-pool.json `
  --db .\data\orchestrator.db --audit-db .\data\ccs-audit.db `
  --replica-root .\data\checkpoint-replicas `
  request --file .\replicate-request.json
```

## 导入目标 Worker

完成本地副本后可单独预演 `checkpoint.import`：

```json
{
  "schema_version": 1,
  "request_id": "import-001",
  "actor": {"id": "codex-local", "kind": "agent"},
  "action": "checkpoint.import",
  "dry_run": true,
  "payload": {
    "source_worker_id": "worker-a",
    "source_run_id": "run_00000000000000000000000000000000",
    "checkpoint_id": "ckpt_00000000000000000000000000000000",
    "target_worker_id": "worker-b"
  }
}
```

导入要求目标声明 `checkpoint.import=1`。目标 Worker 先创建仅限该导入 ID 的暂存位置；SCP 完成后，Worker 重新计算大小与 SHA-256，验证来源副本回执，再原子发布到：

```text
<worker-root>/checkpoint-imports/<import-id>/
  checkpoint.ckpt
  manifest.json
  expected.json
  transaction.json
```

Worker 导入先在 `.incoming/<import-id>` 中持久化来源及计划回执，完整上传通过
SHA-256 后发布文件和 manifest，最后原子移动整个目录。重新 `prepare` 或 `commit`
会复用已验证的文件和原始导入回执；文件、manifest 或目录发布中断都可以重试。
已发布目录缺少 manifest 时，仅在来源记录、事务回执和文件摘要全部吻合时补写；
缺少可靠事务或内容损坏则拒绝。完整的旧版已发布导入仍可使用。
POSIX 上已发布目录为 `0550`；修复先完成来源及内容核验，再临时增加目录所有者
写权限，补写回执后在 `finally` 中恢复原权限，checkpoint 始终保持只读。无法写入
或恢复权限时返回明确错误，不声称修复成功。Windows 本地测试不替代 Linux 权限
及 fsync 的真实平台验收。

CCS 不反序列化 checkpoint。导入结果提供精确 `checkpoint_path`、SHA-256 和目标端回执；上层 Agent 应将这些值绑定到一个新的 `ExperimentSpec.task.checkpoint`，再通过独立的 `run.submit` 预演和审批创建恢复 Run。`TaskInputGate` 会在 GPU 排队前再次核验这些输入，详见 [恢复输入启动门禁](TASK-CHECKPOINT-GATE.md)。这样导入成功不会被误当成恢复授权。

## 当前边界

本版本实现 Worker 到主机的受控导出，以及主机到指定 Worker 的受控导入。它不自动改写模型命令、不判断 checkpoint 与另一模型代码是否兼容，也不自动创建恢复 Run。恢复入口必须显式支持导入路径，并在训练环境内验证模型、优化器、调度器、随机数和数据重放契约。
