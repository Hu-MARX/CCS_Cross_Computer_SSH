# CCS 文档导航

当前代码版本为 **0.6.1**。安装环境、Worker 部署、运行授权与验收范围应分别确认；本机代码测试通过不代表已经部署了远端能力。

## 第一次使用

1. 阅读仓库首页 [README](../README.md)，了解 CCS 的控制面与能力边界。
2. 按 [安装说明](INSTALL.md) 安装本机 controller/SDK，以及按需安装 MCP、部署 Worker 和配置固定 SSH 主机密钥。
3. 按 [操作指南](OPERATION-GUIDE.md) 从离线诊断、只读 Worker 事实开始，再明确选择 Worker，预演并确认一次已授权 Run。
4. 根据 [支持与验收矩阵](SUPPORT-MATRIX.md) 区分声明兼容与实际验证；升级前阅读 [发布与回退](RELEASING.md)。

## 按场景查找

| 场景 | 文档 |
| --- | --- |
| 接入 Python SDK、Agent 或 stdio MCP | [Agent 接入](AGENT-INTEGRATION.md)、[SDK 长任务与 MCP 生命周期](sdk-lifecycle.md) |
| 查询动作、参数、输入输出 schema | [生成的动作契约](ACTION-CONTRACTS.md)、[版本化 JSON 接口](CCS-AGENT-INTERFACE.md)、[Gateway 设计](AGENT-GATEWAY.md) |
| 管理多 Worker、显式选择执行位置 | [多 Worker](MULTI-WORKER.md)、[能力协商](CAPABILITY-NEGOTIATION.md) |
| 事件/产物有界分页、日志游标、断线继续读取 | [增量读取与分页](PAGED-READS.md)、[SDK 生命周期](sdk-lifecycle.md) |
| 超时后查询操作账本与协调 uncertain submission | [操作指南第 7 节](OPERATION-GUIDE.md#7-处理-unknown先查账本再做只读协调)、[Agent 接入的错误与重试](AGENT-INTEGRATION.md#errors-retries-and-execution-evidence) |
| 复制检查点、导入到指定 Worker、处理中断 | [检查点迁移与中断恢复](CHECKPOINT-REPLICATION.md) |
| 把恢复输入绑定到新 Run、验证真正模型恢复 | [恢复输入门禁](TASK-CHECKPOINT-GATE.md)、[模型恢复适配契约](MODEL-RECOVERY-ADAPTERS.md) |
| 事务式远端工作区修改 | [受限远端工作区](REMOTE-WORKSPACE.md) |
| 区分观察、产物绑定与执行证据 | [产物绑定](ARTIFACT-BINDING.md)、[证据状态](EVIDENCE-STATUS.md) |
| Windows 本机只读看板 | [Windows 看板](WINDOWS-DASHBOARD.md)、[看板配置面板](CONFIGURATION-PANEL.md)、[训练指标 JSON](DASHBOARD-JSON-TRAINING-METRICS.md) |
| 研究 Study/Stage 工作流 | [研究自动化](RESEARCH-AUTOMATION.md)、[Study 工作流](STUDY-WORKFLOWS.md)、[阶段决策包](STAGE-DECISION-PACKAGES.md) |
| 构建 wheel/sdist、分组验证、升级回退 | [发布流程](RELEASING.md)、[支持矩阵](SUPPORT-MATRIX.md) |

## 接口与示例入口

`ccsctl describe` 可离线查询随安装包发布的契约，`ccsctl doctor` 默认只做本机诊断。`ccsctl mcp-config` 默认预览带绝对路径的只读 MCP 配置；确认摘要后才创建新文件，已有文件不覆盖。具体命令见操作指南。

仓库中的 [examples](../examples/) 和 [schemas](../schemas/) 用于构造请求；示例中的 Worker、用户名、主机、目录与 Python 路径都应替换为自己的值。运行状态、请求/计划、数据库、SSH 凭据、看板令牌和检查点副本保存在本机运行目录，不应提交到公共仓库。

## 设计记录

- [Worker 托管长任务](adr/0001-worker-local-long-running-control.md)
- [建议与执行分离](adr/0002-separate-recommendation-from-execution.md)
- [新 Run 前协商能力](adr/0003-negotiate-worker-capabilities-before-new-runs.md)
- [Agent 接口预演确认](adr/0004-agent-interface-plan-confirmation.md)
- [上层 Agent 显式选择 Worker](adr/0005-upper-agent-selects-worker.md)
