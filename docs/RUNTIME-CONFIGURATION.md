# 训练框架运行时配置采集（本机开发版）

2026-09-05。本轮只更新 Windows 工作区代码及本机看板；未部署 worker-a，未修改或重启当前50epoch训练。
因此当前运行仍明确显示“当前任务未采集”，不会把静态源码配置冒充运行中优化器状态。

## 采集内容

未来任务在 Lightning `on_train_start`、优化器和调度器已构造后，写入一次 `runtime-configuration.json`：

- Trainer类型、precision、world_size、accumulate_grad_batches、max_epochs；
- 实际优化器类型；每个参数组的index、可选name、lr、initial_lr、weight_decay；
- 可序列化的betas、eps、momentum等标量超参数；
- 调度器类型、interval和frequency；
- DataModule已有train_config时记录batch_size、grad_accum_steps、num_workers、resolution；
- 模型和DataModule的类型名称。

采集明确排除params、张量、模型权重和参数名称，不读取checkpoint或数据集。最多8个优化器、64个参数组、
16个调度器，记录上限64KiB；超过范围标记truncated。非有限数、复杂对象和超长字符串不进入记录。

每次已有进度上报同时读取构造后优化器的当前param_group[`lr`]，指标命名为：

```text
optimizer/<optimizer_index>/group/<group_index>/lr
```

它使用现有15秒进度节流和64KiB记录限制，不增加单独轮询线程。该指标可进入统一结果、指标表和通用曲线。
采样不是每step完整日志；未采到的瞬时变化不能重建。动态参数组重排会改变index语义，未来若需要支持应显式命名/记录事件。

## 失败与安全语义

运行时配置和进度上报属于观察能力。序列化或写入失败只发出一次warning，不终止训练。
worker对可选记录做大小和schema检查；损坏时标为invalid，但保持运行生命周期和其他观察为fresh。
临时文件写入、fsync、原子替换，避免看板读到半写JSON。

worker把记录纳入结构化Observation；RunStore snapshot也可读。Dashboard Feed、统一结果JSON和页面复用同一字段，
没有新增第二套状态来源。页面将静态审阅配置和训练框架实际状态分区展示，失联时仍按Observation新鲜度处理。

当前版本不验证优化器参数是否符合研究设计，只忠实记录可支持字段；执行成功也不等于配置科学合理。
记录包含类型名称和超参数，可能是未公开研究信息，不应未经授权公开导出。

## 测试边界

专项测试覆盖有效多参数组、非有限值、复杂值排除、数量上限、原子文件、worker传递、snapshot读取、
损坏可选记录不影响生命周期，以及前端静态/实际配置分离、实时lr展示和旧任务空状态。

这些测试使用纯Python假Trainer/optimizer或本机模拟记录，不加载PyTorch模型、不使用GPU、不连接从机。
当前Windows缺少的Lightning/Torch环境测试仍按原规则跳过；部署前必须在独立短任务验证真实Lightning钩子，
包括AdamW各组数量、学习率变化、日志开销和记录大小。不能拿正在运行的正式训练做故障注入。

## 安全部署顺序

1. 等当前正式训练结束，或先建立不会覆盖生产目录的并行测试部署。
2. 在从机安装新trainctl代码，但不替换冻结运行目录中的当前代码。
3. 用CVFDETR环境提交独立、限批、独立输出的GPU短任务；确认实际参数组与预期对应。
4. 验证实时lr曲线、worker重启补传、损坏观察记录降级和性能开销。
5. 重新生成并批准用于后续长训练的证据快照。新快照哈希不得伪装成当前bundle。

真实短任务和远端部署尚未执行，需要在当前训练安全窗口及用户批准下进行。
