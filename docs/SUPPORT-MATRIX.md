# CCS 0.6.1 支持与验收矩阵

声明兼容、源代码测试、独立安装和真实远端验收分别记录。
本表区分 Windows / Python 3.11.15 的本地安装记录与实际 GitHub Actions 源码验收。
源码提交 `21321bc19413c32a26b7664f783e8d55f44b9991` 的 [六组 CI 已全部通过](https://github.com/Hu-MARX/CCS_Cross_Computer_SSH/actions/runs/38278526903)。

| 接入面 | 要求 | 已验证范围 |
|---|---|---|
| 核心 controller / CLI / SDK | Python >=3.11，零第三方运行依赖 | CI：Linux 440 通过/9 跳过；Windows 437 通过/12 跳过；各执行 449 项 |
| MCP stdio | 可选 `mcp>=2.2,<3` | 源码 MCP 组 7 项通过；安装包变体见下表 |
| 看板 JavaScript | Node.js `node:test` | 27 项通过 |
| Python 最低版本 | 3.11 | 本地 3.11.15；CI Linux 3.11.17、Windows 3.11.9；其他 Python 系列未验收 |
| PyTorch replay | 配好的 PyTorch/NumPy 训练环境 | 9 个训练环境检查跳过，不计作训练恢复已验收 |
| Linux 本地行为 | 原生 Linux、权限与进程环境 | Ubuntu CI 的核心组通过，启用 `--require-linux`；Linux 独立 wheel/sdist 安装仍待验收 |
| 真实 worker / harness | 固定 SSH 主机密钥、能力协商、隔离且已授权任务 | 本次未部署、启动、取消或故障注入实际 worker |
| 外部研究看板注册 | 显式 `--research-root` | 当前公开版未重跑外部项目接入；0.6.0 基线另有两项本地记录 |

## 独立安装验收

本地 Windows 核心记录为 435 通过/14 跳过（5 项平台限制、9 项训练依赖缺失）。
CI Windows 的 12 项跳过为 3 项平台限制及相同的 9 项训练依赖缺失；Linux 仅跳过后者。
MCP 2.2.0/2.3.0 在 Windows、Ubuntu 的四组 CI 均通过；Linux 看板 JavaScript 27 项通过。
初次 CI 的权限夹具、异步终止观察、提交竞态及 Windows 临时目录问题已修正，
生产权限与路径保护未放宽；失败记录仍可在 [初次运行](https://github.com/Hu-MARX/CCS_Cross_Computer_SSH/actions/runs/38276572172) 查阅。

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
