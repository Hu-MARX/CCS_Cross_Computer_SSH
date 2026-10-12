# 恢复输入启动门禁

`TaskInputGate` 是 CCS 0.4.0 的 Worker 侧恢复输入门禁。它只处理 `ExperimentSpec.task.checkpoint` 声明的单个 checkpoint，并且在证据预检、GPU 资源获取和训练子进程启动之前运行。

## 验证顺序

1. checkpoint 路径必须是绝对路径、普通文件且不是符号链接；
2. 读取前后比较文件大小、修改时间和 inode，拒绝核验期间发生的变化；
3. 重新计算 SHA-256，并与 `task.checkpoint.sha256` 比较；
4. 如果路径位于 `<worker-root>/checkpoint-imports`，还必须是精确的已发布目录，并通过目标端导入清单、来源身份和回执复核；
5. 成功后才原子写入当前 Run 的 `task-inputs.json`。

任何失败都会把 Run 标记为 `FAILED`，不会获得 GPU 租约，也不会创建训练子进程。门禁同时声明 `checkpoint.input_gate=1`，旧 Worker 会在远端提交前被主机能力协商拒绝。

## 训练入口

核验成功后，训练进程得到以下环境变量：

- `TRAINCTL_TASK_CHECKPOINT`：已核验的精确绝对路径；
- `TRAINCTL_TASK_CHECKPOINT_SHA256`：已核验摘要；
- `TRAINCTL_TASK_INPUTS_REPORT`：本 Run 的输入核验记录路径。

上层 Agent 仍需构造完整、可审批的恢复 `ExperimentSpec`。训练入口读取 `TRAINCTL_TASK_CHECKPOINT` 后，必须在自己的可信 Conda 环境内验证模型结构、优化器、调度器、精度插件、随机数和数据重放状态；CCS 不加载 pickle、PyTorch 或 Lightning checkpoint。

CCS 0.5.0 的 `RecoveryInput` Module 实现了这段可信训练侧流程：它要求三个门禁环境变量与本 Run 的 `task-inputs.json` 完全一致，边复制边重新计算摘要，在发布独立恢复副本前调用训练 Adapter 提供的反序列化验证器，并要求序列化实验契约匹配。门禁 checkpoint 与旧式 `--resume-from-run` 同时出现时会拒绝启动，避免来源优先级含糊。

示例任务片段：

```json
{
  "task": {
    "kind": "train",
    "source_run_id": "run_source",
    "inputs": [],
    "checkpoint": {
      "path": "/worker/root/checkpoint-imports/import_id/checkpoint.ckpt",
      "sha256": "64位小写SHA-256"
    }
  }
}
```

## 保证边界

门禁证明“启动前读取的字节与批准输入及导入回执一致”，不证明 checkpoint 对目标模型科学上合适，也不承诺恶意同权限进程无法在核验后改写文件。导入文件在 POSIX Worker 上以只读权限发布，以降低意外修改风险；训练适配器仍须在反序列化后执行语义契约检查。
