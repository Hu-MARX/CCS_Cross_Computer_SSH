# Cross Computer SSH（CCS）

CCS 是通过 SSH 管理可信 Linux worker 的执行层。它为本地 Agent、Python 程序和人工操作者提供统一的 JSON 接口，支持长任务、worker 状态查询、事件与日志读取、检查点迁移、受控工作区操作和审计记录。

当前源码候选版本为 **0.6.3**；发布及验收边界见 [0.6.3 记录](docs/RELEASE-0.6.3.md)。控制端、CLI 和 Python SDK 只依赖 Python 标准库；可选 MCP 适配器通过 stdio 接入。

## 能做什么

- 为每个任务保存独立 Run 身份、状态、事件、日志与产物记录；客户端断开后，任务由 worker 的执行后端继续管理。
- 在多个 worker 中显式选择执行目标，检查能力与资源条件；不确定的提交结果可通过持久操作账本查询和核对。
- 对变更先生成计划，再以相同请求身份、载荷及计划摘要确认执行。
- 对事件、日志与产物进行有界增量读取；SDK 提供等待、迭代、游标和计划保存接口。
- 校验检查点来源、大小和 SHA-256，恢复迁移中断留下的可验证事务。
- 已接入的单进程训练器可在安全点保存并响应暂停请求；保存、完整退出和恢复后推进分别确认，见 [安全暂停协议](docs/COOPERATIVE-PAUSE.md)。
- 提供 MCP 只读模式、动作白名单、离线安装诊断与配置预览。

CCS 当前管理多台机器上的**独立 Run**。一个 Run 跨机器的 NCCL、DeepSpeed 或多节点 `torchrun` 协调启动尚未实现。检查点字节迁移成功也不等于模型、优化器和随机数状态已实际恢复；训练环境及模型适配器仍需要单独验证。

## 快速安装：Windows 控制端

需要 Git、PowerShell 7、OpenSSH 客户端和 Python 3.11 或更高版本。下面显式选择 Python 3.11；若使用其他满足要求的版本，请替换 `py -3.11`。

```powershell
git clone https://github.com/Hu-MARX/CCS_Cross_Computer_SSH.git
Set-Location .\CCS_Cross_Computer_SSH
py -3.11 -m venv .venv
$CcsRoot = (Get-Location).Path
$CcsPython = (Resolve-Path .\.venv\Scripts\python.exe).Path
& $CcsPython -m pip install -e .
& $CcsPython -m controller.gateway_cli --version
& $CcsPython -m controller.gateway_cli doctor --python $CcsPython --cwd $CcsRoot
& $CcsPython -m controller.gateway_cli describe --action run.submit
```

`doctor` 默认只检查本地安装，不连接 worker、不启动训练。需要 MCP 时安装可选依赖并重新检查：

```powershell
& $CcsPython -m pip install -e '.[mcp]'
& $CcsPython -m controller.gateway_cli doctor --python $CcsPython --cwd $CcsRoot --require-mcp
```

安装 worker、配置 SSH 主机密钥和运行时 worker 池，按 [安装说明](docs/INSTALL.md) 完成。首次复制配置时不要覆盖已有配置：

```powershell
if (Test-Path .\config\worker-pool.json) { throw 'worker-pool.json 已存在，请编辑现有文件。' }
Copy-Item .\config\worker-pool.example.json .\config\worker-pool.json
```

编辑生成文件中的占位主机、远端绝对目录、解释器入口和 SSH 密钥路径。完成后可以查询已配置 worker：

```powershell
& $CcsPython -m controller.gateway_cli `
  --fleet "$CcsRoot\config\worker-pool.json" `
  --db "$CcsRoot\data\orchestrator.db" --audit-db "$CcsRoot\data\ccs-audit.db" `
  request --file "$CcsRoot\examples\ccs-fleet-status.json"
```

## 文档入口

| 目标 | 文档 |
| --- | --- |
| 安装控制端、Linux worker、SSH 与 MCP | [INSTALL](docs/INSTALL.md) |
| 查询、提交、跟踪、取消、恢复及排错 | [操作指南](docs/OPERATION-GUIDE.md) |
| 完整文档导航 | [文档索引](docs/INDEX.md) |
| Python SDK 和 MCP 接入 | [Agent integration](docs/AGENT-INTEGRATION.md) |
| 请求格式与动作字段 | [协议](docs/CCS-AGENT-INTERFACE.md)、[动作参考](docs/ACTION-CONTRACTS.md) |
| 分页、超时、取消和恢复游标 | [Paged reads](docs/PAGED-READS.md)、[SDK lifecycle](docs/sdk-lifecycle.md) |
| 检查点迁移与模型恢复边界 | [Checkpoint replication](docs/CHECKPOINT-REPLICATION.md)、[Model adapters](docs/MODEL-RECOVERY-ADAPTERS.md) |
| 断点观测、动态关注和实际加载版本 | [Recovery facts](docs/DASHBOARD-RECOVERY.md)、[Collector focus](docs/dashboard-collector-focus.md) |
| 升级前角色版本矩阵与只读计划预览 | [Upgrade preflight](docs/UPGRADE-PREFLIGHT.md) |
| 构建、验证、升级与回退 | [Releasing](docs/RELEASING.md)、[Support matrix](docs/SUPPORT-MATRIX.md) |

## 使用边界与验证状态

本仓库不包含个人 SSH 私钥、生产 worker 配置、运行数据库、训练数据或模型检查点。运行数据由使用者保存在各自环境中。CCS 复用既有 SSH 身份和固定主机密钥，当前面向可信的本地调用者；计划摘要及 MCP 动作限制不提供网络身份认证、多租户隔离或不可信代码沙箱。

0.6.1 已通过 Windows 核心验收（435 通过、14 明确跳过）、MCP 源码组 7 项、看板 JavaScript 27 项，以及 core/MCP 2.2.0/MCP 2.3.0 共六套 wheel/sdist 干净安装。Linux 本地验收、真实远端故障验收及多节点训练不能从这些结果推出；各验证项的范围和待办见 [支持矩阵](docs/SUPPORT-MATRIX.md)，具体变更见 [0.6.1 发布说明](docs/RELEASE-0.6.1.md)。
