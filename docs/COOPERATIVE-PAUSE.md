# 完整断点确认后暂停与恢复

`run.pause` 是训练适配器参与的安全点握手，不是发送停止信号，也不是自动续训。
只有提交时明确声明支持的单进程训练才能使用。旧 Run 不能靠修改元数据补上支持。
从机必须声明 `run.pause=1`；不能仅凭软件版本判断。所有服务器变更仍经过
Agent Interface 的预演和完全一致的 `plan_sha256` 确认，严格主机密钥检查不变。

## 1. 提交时声明支持

在 ExperimentSpec 中增加以下字段。`contract` 是调用项目确定的完整恢复兼容合同
SHA-256，应绑定代码、配置和数据恢复语义，不是随意填写的标记。

```json
{
  "task": {
    "kind": "train",
    "pause": {
      "schema_version": 1,
      "protocol": "managed-checkpoint-v1",
      "contract": "<64位小写SHA-256>"
    }
  }
}
```

此声明不会自动改造训练循环。适配器必须在**完整 optimizer 更新结束后**调用下面的
Interface，不能在梯度累积中途调用。v1 不支持多进程/DDP；不能让单个 rank 的 ACK
冒充所有 rank 都已安全保存。`WORLD_SIZE != 1` 时辅助函数明确拒绝。

## 2. 在训练安全点保存并确认

使用 [pause_control Module](../src/trainctl/pause_control.py)。Worker 向训练进程传入
`TRAINCTL_RUN_DIR`、`TRAINCTL_SPEC_SHA256` 和 `TRAINCTL_CHECKPOINT_DIR`。
checkpoint 目录必须严格对应本 Run 的
`<workdir>/orchestrator_runs/<run_id>/checkpoints`，不得跨目录或穿过符号链接。

以下是适配器接入位置；`write_full_state` 和 `inspect_full_state` 必须由训练项目实现，
前者保存真实状态，后者在受信训练环境中检查该文件，不能返回编造的摘要。

```python
from trainctl.checkpoints import CheckpointStore
from trainctl.pause_control import checkpoint_and_pause, request_digest

storage = CheckpointStore(checkpoint_directory)

def save_for_pause(request):
    return storage.commit(write_full_state, inspect_full_state, metadata={
        "contract": declaration["contract"],
        "run_id": request["run_id"],
        "pinned": True,
        "pause_request_sha256": request_digest(request),
    })

# 在完整 optimizer.step 完成及数据游标更新后，使用实际 global_step：
checkpoint_and_pause(
    run_dir, declaration,
    checkpoint_directory=checkpoint_directory,
    spec_digest=spec_sha256,
    global_step=global_step,
    save_checkpoint=save_for_pause,
)
```

`inspect_full_state` 返回的 state 必须包含：

- `contract`、实际 `global_step`、实际 `optimizer_steps`，满足
  `0 <= optimizer_steps <= global_step <= 2**63-1`。
- `recovery_fingerprints`：恰好包含 `model`、`optimizer`、`scheduler`、`precision`、
  `rng`、`sampler`、`loop` 七个实际内容摘要，每项为64位小写 SHA-256。
- `replay.safe_to_resume = true`，由适配器核验完整安全点语义后产生。

成功提交后，辅助函数发布 `checkpoint-status.json`、绑定本次请求的 `pause-ack.json`
及有文件签名的 `pause-verified.json`，然后抛出 `TrainingPaused(SystemExit)`，退出码为85。
不要捕获它之后继续训练或改成退出码0。保存失败、状态不完整或 ACK 无效时不能宣称暂停。
Worker 不反序列化模型；它校验请求绑定、managed manifest、大小和文件 SHA-256。

## 3. 通过公共 Interface 请求暂停

先读取 `fleet.status` 和所选 Worker 的能力声明，明确指定 `worker_id`。
使用 [操作指南](OPERATION-GUIDE.md) 的 `New-CcsRequest` / `Invoke-CcsRequest`：

```powershell
$pause = New-CcsRequest 'run.pause' @{
    worker_id = $Worker
    run_id = $RunId
    reason = '计划维护：先提交完整断点'
    timeout_seconds = 600
} $true 'maintenance-pause-001'
$plan = Invoke-CcsRequest $pause

# 审阅计划并确认本次操作已获授权；不改变请求身份或 payload：
$pause.dry_run = $false
$pause.confirm_plan_sha256 = $plan.data.plan_sha256
Invoke-CcsRequest $pause
```

`timeout_seconds` 为1至3600的整数。v1 每个 Run 只有一个不可替换的暂停请求；相同身份
与完全相同输入不会延长有效期，不同身份或不同输入会冲突。超时不会强杀、不会自动转为
`run.cancel`；保存跨超时会保留已经提交的恢复点，但不发布 ACK、不以85退出。

`confirmed` 和 `pause.state=requested` 仅表示请求送达。响应丢失时读取原身份的
`operation.query` 与 `run.snapshot` 回执，不自动重发，不更换 Worker、账号或请求身份。
只有明确另行授权时才使用强制停止的 `run.cancel`；它不保证刚刚的完整状态已经保存。

## 4. 区分保存确认与完整退出

安全暂停要求三个事实同时成立：**退出码85、有效且匹配请求的 ACK、systemd Run cgroup
已确认无进程**。生命周期记录为 `CANCELLED / error=PAUSED / exit_code=85`，但这条记录
本身不证明完整退出；还要读取 `pause` 与执行清理事实。退出85没有有效 ACK 是失败；
其他退出码的崩溃即便留下 ACK 也仍是失败，不会被保存确认掩盖。

| `pause.state` | 含义与操作边界 |
| --- | --- |
| `unsupported` / `not_requested` | 没有协议支持／尚无请求。 |
| `requested` | 等待训练进入安全点，不得当作已停止。 |
| `expired` | 未在有效期内确认，训练可能仍运行；不自动杀进程。 |
| `checkpoint_committed` | 已有保存确认，但未证明整个 Run 完整退出。 |
| `checkpoint_unverified` / `invalid` | 验证记录缺失、文件签名变化或协议记录无效；不得据此恢复。 |
| `paused` | 完整保存和独立 quiescence 均已确认；同时检查 `safe_to_resume=true`。 |

普通 process backend 不提供 systemd cgroup 级退出保证，最多报告保存确认，不能宣称
所有后代进程都已退出。即使本地 CPU 示例完成，也不能把该结果升级成服务器关机保证。
维护前只有拿到 systemd quiescence 与完整断点确认，才可把 CCS 暂停流程视为完成。

周期状态采集不会每次重新 hash 大 checkpoint。它展示先前的字节验证时间并核对当前文件
签名，明确标注 `current_bytes_rehashed=false`；文件变化或缺少验证记录会保守降级。
显式 ACK 接受与后续 `RecoveryInput` 恢复选择仍做完整字节核验。

## 5. 新 Run 恢复并证明实际推进

恢复是独立的新 `run.submit`，保留 `task.source_run_id`，以已验证的路径与 SHA-256
绑定 `task.checkpoint`，重新预演并确认。本次暂停授权不构成后续恢复授权；CCS 不自行
选择另一份恢复点，也不在断点不可用时静默从头训练。

训练侧流程是：

1. `RecoveryInput(validator).select(...)` 选择并核验恢复点，生成不受来源清理影响的独立副本。
2. 适配器从副本**实际加载**模型、optimizer 等状态，重新计算真实七分量摘要。
3. `RecoveryAudit(selection, run_dir, contract).confirm_loaded(actual_state)` 确认摘要和计数一致。
4. 真正执行一次 optimizer 更新，再调用 `confirm_advanced(actual_state)`；步数、optimizer
   更新数必须增加，模型和 optimizer 的实际摘要都必须变化。

日志出现“已加载”、进程存活或计数单独增加都不能替代以上确认。状态摘要来自受信训练
适配器，不是针对同账号恶意进程的认证；完整文件 SHA 是字节完整性事实，两者也不等于
检测精度、模型改进有效或任何科研结论。

## CPU 示例与适用范围

[cooperative_pause_smoke.py](../examples/cooperative_pause_smoke.py) 是独立标准库 JSON
计数器，无 GPU 或历史科研模型依赖。它保存七个真实状态分量，经 `RecoveryInput` 恢复后
核对加载状态，再核对真实一步推进。`--steps` 控制目标总步数，`--delay` 控制每步间隔；
本地隔离演示也可用 `--resume-from-run` 指定来源。生产调用应使用受控 `task.checkpoint`。

定向验收见 [test_pause_control.py](../tests/test_pause_control.py) 和
[test_cooperative_pause_worker.py](../tests/test_cooperative_pause_worker.py)。示例应只在明确
授权的隔离 CPU 工作区运行，不用于暂停忙碌的科研训练，也不自动触发远端部署。
