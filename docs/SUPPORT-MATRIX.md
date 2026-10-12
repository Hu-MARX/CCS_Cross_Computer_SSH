# CCS 0.6.1 支持与验收矩阵

声明兼容、源代码测试、独立安装和真实远端验收分别记录。
本表基于 Windows / Python 3.11.15 的本地记录；CI 配置不等于 CI 已通过。

| 接入面 | 要求 | 已验证范围 |
|---|---|---|
| 核心 controller / CLI / SDK | Python >=3.11，零第三方运行依赖 | 449 个核心用例：435 通过，14 明确跳过 |
| MCP stdio | 可选 `mcp>=2.2,<3` | 源码 MCP 组 7 项通过；安装包变体见下表 |
| 看板 JavaScript | Node.js `node:test` | 27 项通过 |
| Python 最低版本 | 3.11 | 本次使用 3.11.15；未据此声明其他 Python 版本已验收 |
| PyTorch replay | 配好的 PyTorch/NumPy 训练环境 | 9 个训练环境检查跳过，不计作训练恢复已验收 |
| Linux 本地行为 | 原生 Linux、权限与进程环境 | 本地发布记录待完成；Windows 的 5 个平台跳过不能替代 |
| 真实 worker / harness | 固定 SSH 主机密钥、能力协商、隔离且已授权任务 | 本次未部署、启动、取消或故障注入实际 worker |
| 外部研究看板注册 | 显式 `--research-root` | 当前公开版未重跑外部项目接入；0.6.0 基线另有两项本地记录 |

## 独立安装验收

验证器对 wheel 与 sdist 各创建干净环境，从独立 cwd 运行实际安装包，移除
PYTHONPATH / PYTHONHOME / 用户 site-package。验收包含 CLI、打包契约资源、
SDK 计划保存/等待/迭代、doctor、配置预览和独占创建；MCP 变体另外验证真实
stdio、原计划确认、篡改拒绝、只读模式只注册一个工具。

| 安装变体 | Windows 验收 |
|---|---|
| core wheel / sdist | 通过 |
| MCP 2.2.0 wheel / sdist | 通过 |
| MCP 2.3.0 wheel / sdist | 通过 |

0.6.1 上述六套干净安装均已通过。原始日志不上传个人运行环境；可公开的计数/版本/范围汇总
记录在 [本版发布说明](RELEASE-0.6.1.md)。

原生 Linux 验收使用 `scripts/verify.py --group core --require-linux`。
它拒绝 Windows，并要求 Linux 专属检查实际执行；Windows-only Job 故障用例可在 Linux
明确跳过，缺失训练依赖仍属可选范围。发行操作与状态保留式回退见 [RELEASING](RELEASING.md)。
