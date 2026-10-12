# CCS 0.6.1 公共仓库发布

发布日期：2026-10-13。上一版 0.6.0 的 P1-01～P1-04 功能继续保留。
本版整理公开源码、安装模板和中文指南，移除默认真实服务器目标和个人路径。

## 变更

- 中文 [安装说明](INSTALL.md)、[操作指南](OPERATION-GUIDE.md) 与 [文档导航](INDEX.md)。
- 运行时配置由 `config/*.example.json` 生成，个人池配置、主机密钥、数据库、
  训练数据、模型文件和旧实验记录不纳入仓库。
- 看板必须显式指定 `--fleet` 或 `--host`；新增 `--local-root`，默认当前目录，
  不向 wheel 的 site-packages 写状态。启动脚本优先使用专用 `.venv`。
- RF-DETR 演示适配器要求显式提供项目、数据与预训练权重，缺失时在导入训练栈前退出。
  研究配方是需单独适配和授权的示例，不附带模型或数据。
- Evidence capture 明确选择目录/适配器/spec；periodic 故障配方默认仅显示本地预览，
  只有 `--confirm-run` 才进入专用远端验收。它不是日常安装步骤。
- Installer 先检查 Python >=3.11，独立创建指定 venv，支持 `--python` / `--venv-dir`。
  旧的无效 `--install-dir` 明确报错；`--source` 仍是原地 editable 安装，源目录不能删除。
- 指南修正 SDK 默认 4 个执行槽（MCP 默认 2 个）与提交结果的 `result.id`。
- 增加 GitHub Actions 的 Windows/Linux 核心、MCP 和 JavaScript 分组验证，保留 JSON 记录。

## 本地验收

Windows / Python 3.11.15：核心 449 项中 **435 通过、14 跳过**，MCP 组 **7 通过**，
看板 JavaScript **27 通过**。14 个跳过明确为 5 项平台限制、9 项可选训练环境缺失。
完整核心验收在构建前独立执行，未重复规模套件。

core、MCP 2.2.0、MCP 2.3.0 的 wheel/sdist 共六套独立安装通过，验收范围见 [支持矩阵](SUPPORT-MATRIX.md)。
公开发布检查只记录文件名、摘要和候选类别，不输出凭据。
GitHub Actions 的实际运行结果在 Actions 页面查看，不能把工作流配置当作已通过。

## 边界

本次未部署或操作实际 worker。Linux 原生安装/权限/进程及真实远端故障验收仍需要独立记录；
本地 Fake Worker、CPU fixture、byte-copy 成功不能代替模型真实恢复或多节点训练验收。
原 0.6.0 首轮失败与补测证据仍在发布者本地保留，不作为新版本一次无失败的完整运行。
