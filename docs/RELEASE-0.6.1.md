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
- CI 使用规范化临时目录，避免 Windows 短路径别名触发路径保护；仅文档变更不重复运行完整矩阵。
- 修正 POSIX 只读夹具的损坏注入与 snapshot 封存、异步子进程终止观察及并发派发断言；产品保护逻辑保持原样。

## 本地验收

Windows / Python 3.11.15：核心 449 项中 **435 通过、14 跳过**，MCP 组 **7 通过**，
看板 JavaScript **27 通过**。14 个跳过明确为 5 项平台限制、9 项可选训练环境缺失。
完整核心验收在构建前独立执行，未重复规模套件。

core、MCP 2.2.0、MCP 2.3.0 的 wheel/sdist 共六套独立安装通过，验收范围见 [支持矩阵](SUPPORT-MATRIX.md)。
公开发布检查只记录文件名、摘要和候选类别，不输出凭据。
源码修正提交 `21321bc19413c32a26b7664f783e8d55f44b9991` 的 [GitHub Actions](https://github.com/Hu-MARX/CCS_Cross_Computer_SSH/actions/runs/38278526903)
六组全部通过：Linux 核心 449 项中 **440 通过、9 跳过**；Windows 核心 449 项中
**437 通过、12 跳过**；MCP 2.2.0/2.3.0 在两个平台均通过；Linux JavaScript **27 通过**。
9 项共同跳过为可选训练环境缺失，Windows 额外 3 项是平台限制。
[初次失败记录](https://github.com/Hu-MARX/CCS_Cross_Computer_SSH/actions/runs/38276572172) 保留，以上结果属于修正后的独立运行。

## 边界

本次未部署或操作实际 worker。Linux 原生源码权限与进程验收已由 CI 覆盖；
Linux 独立 wheel/sdist 安装及真实远端故障验收仍需独立记录。
本地 Fake Worker、CPU fixture、byte-copy 成功不能代替模型真实恢复或多节点训练验收。
原 0.6.0 首轮失败与补测证据仍在发布者本地保留，不作为新版本一次无失败的完整运行。
