# 模型恢复 Adapter

CCS 0.5.0 将恢复分成两个信任层。Worker 侧 `TaskInputGate` 在启动前验证文件字节、目标端导入回执和 Run 声明；训练进程内的 `RecoveryInput` Module 在可信 Conda 环境中复制独立恢复文件，并调用模型 Adapter 的检查器验证序列化训练状态与实验契约。CCS 守护进程始终不加载 PyTorch pickle。

## 已接入入口

- `examples/cvfdetr_replay.py` 使用 `inspect_replay_checkpoint`，要求完整 Lightning 状态和安全的数据重放游标；
- `examples/rfdetr_managed.py` 使用同一完整状态检查器，并继续验证恢复后模型、优化器和步数均实际推进；
- `examples/recovery_input_smoke.py` 是不依赖 GPU 或研究数据的验收 Adapter，只验证端到端门禁、独立复制、契约和来源记录。

两个研究 Adapter 均保留同机 `--resume-from-run` 模式，同时支持 `ExperimentSpec.task.checkpoint`。两种来源互斥；跨 Worker 恢复时，上层 Agent 必须使用 `checkpoint.replicate`、`checkpoint.import` 的真实回执构造新的 Run，不能直接填写未经导入的来源路径。

## 恢复结果

成功选择会在新 Run 输出中写入 `resume-selection.json`。其中记录来源 Run、导入标识、目标端回执、最终恢复副本摘要和经过训练环境检查的状态。`restore-audit.json` 随后比较 Lightning 实际加载状态，`managed-result.json` 证明恢复后发生了真实优化更新。

## 保证边界

实验契约仍是严格相等关系。源码、Adapter、训练参数、运行时或数据标注发生契约化变化时，旧 checkpoint 会被拒绝，而不是降级为从预训练权重重新开始。跨版本兼容必须由上层 Agent 明确规划和重新验证，CCS 不自动放宽合同。

## 2026-09-06 真实 GPU 验收

- CVFDETR：源 Run `run_a1da1ca457e54bf9a52499b629327758` 在第 2 步提交 271,584,236-byte 检查点后受控取消；恢复 Run `run_76de5ccc269b4aef828b8ba5d1bcda54` 通过 `task.checkpoint` 从第 2 步继续到第 4 步，产生 2 次真实 optimizer 更新。模型、优化器、调度器、精度、回调、RNG 和 replay-v1 数据游标均通过恢复比较。记录为 `data/cvfdetr-task-checkpoint-acceptance-18831c449bd140329d7ac74886a0771f.json`。
- RF-DETR：源 Run `run_363c792b215d4f83b995be274ebbb012` 只在模型和优化器均实际变化后，于第 5 步提交 400,938,596-byte 检查点并受控取消；恢复 Run `run_6432615ff98b4dd2a769594d75c1b795` 通过 `task.checkpoint` 继续到第 10 步，产生 5 次真实 optimizer 更新，并验证 replay-v1 数据位置恢复。记录为 `data/rfdetr-task-checkpoint-acceptance-4bf34ac802034d80bcefe5f375f19213.json`。

验收脚本为 `scripts/verify_model_task_checkpoint.py`。它固定使用 `num_workers=0`、串行占用单张 GPU，并在 GPU 已有计算进程时拒绝启动。RF-DETR 的一次 4 步探针因模型仍处于早期冻结阶段而被拒绝，不计为恢复通过；最终测试跨过冻结阶段后才允许中断。
