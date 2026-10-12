# 多从机桥梁运行方式

## 定位

CCS 提供连接、能力查询、长任务托管、受限工作区操作、状态回传与审计。上层 Agent 负责把科研计划拆成 Run、比较从机事实并显式选择执行位置。CCS 不内置科研策略，也不依据 GPU 空闲率自动调度。

## 配置示例

仓库提供 `config/worker-pool.example.json`，不登记任何实际服务器。复制成运行时
`config/worker-pool.json` 后填写自己的 SSH 别名、账号、状态根目录和绝对入口。
以下两台 worker 仅说明运行身份与工作区身份可以不同：

| Worker ID | 运行身份 | 工作区身份 | 允许的项目根目录 |
|---|---|---|---|
| `worker-a` | `ccs-runner` | `project-owner` | `/srv/research/project-a` |
| `worker-b` | `ccs-runner` | `project-owner` | `/srv/research/project-b` |

配置只包含连接身份、路径和非敏感标签，不包含密码或私钥内容。运行账号承担长任务运行与观测；项目所有者身份只通过独立包装器执行 allowlist 内的事务式工作区操作。

SSH 账号必须对配置中的目录拥有实际所需权限；受限工作区还必须设置 `workspace_roots`。
选择 process 或 systemd 后端时分别配置其运行生命周期和监测，不通过放宽文件权限解决身份错配。
`enabled=false` 可把节点隔离：状态仍可查询，看板仍显示设备事实，但新 Run 会返回 `WORKER_DISABLED`。

## Agent 调用顺序

1. 调用 `fleet.status` 获取每台机器的可达性、兼容状态、GPU、磁盘和当前受管负载。
2. 上层 Agent 根据实验约束选择一个明确的 `worker_id`，并在自身计划中记录理由。
3. 对变更操作先执行 `dry_run`，向用户展示包含 `worker_id` 的计划摘要。
4. 用户确认后用原请求和 `plan_sha256` 执行；改变 Worker 会使旧确认失效。
5. 后续快照、日志、事件、产物和取消均按 Run 的 Worker 绑定路由。

```powershell
.\scripts\ccs.ps1 --fleet .\config\worker-pool.json `
  --db .\data\orchestrator.db --audit-db .\data\ccs-audit.db `
  request --file .\examples\ccs-fleet-status.json
```

## 协调能力边界

同一科研目标可以拆成多组训练、验证、测试、分析、构建或仿真 Run，由上层 Agent 把这些独立 Run 分配到两台服务器并并行监测。CCS 的通用任务协议不局限于 CVFDETR，也可承载具身智能代码的构建、仿真和模型训练。

当前从机池 v1 不负责把一个 Run 组成跨机 NCCL、DeepSpeed 或其他分布式进程组，也不自动同步两台机器之间的大型数据集。CCS 0.3.0 提供显式 `checkpoint.replicate` 和 `checkpoint.import`：上层 Agent 分别选择来源 Worker、Run、检查点和目标 Worker，CCS 通过 Windows 不可变副本完成两端验签的迁移。导入不会自动提交恢复 Run。
