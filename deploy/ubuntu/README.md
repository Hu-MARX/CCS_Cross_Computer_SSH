# Ubuntu worker 部署模板

CCS 控制层要求 **Python >=3.11**。Ubuntu 自带的 `python3` 可能低于此版本；先按系统支持的方式准备符合要求的解释器和对应 `venv`。训练任务使用的 PyTorch/CUDA 环境由 ExperimentSpec 单独指定。完整控制端与 SSH 配置见 [安装说明](../../docs/INSTALL.md)。

## 普通账号安装

下面以已经存在的 `ccs-runner` 账号为例；在该账号会话中执行。其他账号必须同步替换 worker-pool 和 service 中的绝对路径。安装脚本不会创建账号、配置 SSH、安装 wrapper 或启动服务。

```bash
git clone https://github.com/Hu-MARX/CCS_Cross_Computer_SSH.git "$HOME/src/CCS_Cross_Computer_SSH"
cd "$HOME/src/CCS_Cross_Computer_SSH"
python3.11 -c 'import sys; assert sys.version_info >= (3, 11), sys.version'
bash deploy/ubuntu/install-trainctl.sh \
  --source "$PWD" \
  --python "$(command -v python3.11)" \
  --store-dir "$HOME/.local/state/ccs-worker" \
  --venv-dir "$HOME/.local/share/ccs-venv"
install -m 0600 deploy/ubuntu/worker.json "$HOME/.local/state/ccs-worker/worker.json"
"$HOME/.local/share/ccs-venv/bin/trainctl" \
  --root "$HOME/.local/state/ccs-worker" health --json
```

`--source` 安装现有 checkout，不复制源码；因此保留 checkout 原位置。`--python` 在创建目录和环境前检查版本。已存在的 `--venv-dir` 会被拒绝；升级时选择新环境或明确使用现有环境的 pip。旧 `--install-dir` 参数已移除，避免暗示源码被复制但实际没有复制。

控制端 worker-pool 必须填入：

- `user`: `ccs-runner`，或配置 SSH 别名对应的账号。
- `remote_root`: `/home/ccs-runner/.local/state/ccs-worker`。
- `trainctl_executable`: `/home/ccs-runner/.local/share/ccs-venv/bin/trainctl`。

通过已固定并核实的主机密钥进行只读检查；把 `ccs-worker` 换成自己的 SSH 别名：

```bash
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes ccs-worker \
  '/home/ccs-runner/.local/share/ccs-venv/bin/trainctl --root /home/ccs-runner/.local/state/ccs-worker health --json'
```

私钥和口令留在现有 SSH 配置/agent 中，worker-pool 只存路径和连接参数。

## 可选 user systemd 服务

`ml-orchestrator-project-worker.service` 是 `ccs-runner` 的普通用户模板，使用已经安装的独立 venv。安装、检查 unit 并启动前，先确认模板中的绝对账号/解释器/状态路径与实际一致。命令均在 worker 的普通账号会话中执行：

```bash
mkdir -p "$HOME/.config/systemd/user"
install -m 0644 deploy/ubuntu/ml-orchestrator-project-worker.service \
  "$HOME/.config/systemd/user/ccs-worker.service"
systemctl --user daemon-reload
systemctl --user enable --now ccs-worker.service
systemctl --user status ccs-worker.service
```

user systemd manager、登出后的保活和 GPU 访问权限由系统环境决定，参见 [安装说明](../../docs/INSTALL.md)。`ml-orchestrator-worker.service` 和 `trainctl-wrapper` 另行保留为 `/srv/ml-orchestrator` 管理员部署模板，不适用于上面的普通账号路径；它们必须由管理员先审查修改。

## 可选研究配方

这些入口与零依赖控制层分开，只有显式参数才能选择项目和目标：

- `examples/rfdetr_managed.py` 需要 `--project`、`--dataset`、`--pretrain` 绝对路径；也可使用 `CCS_RFDETR_PROJECT`、`CCS_RFDETR_DATASET`、`CCS_RFDETR_PRETRAIN`。查看 `--help` 不导入训练栈。
- `scripts/capture_cvfdetr_evidence.py` 需要 `--root`、`--project`、`--dataset`、`--app` 绝对目录；`--adapter` 显式列出相对 app 的适配器文件，`--spec` 可复制显式指定的实验 JSON。它仅采集文件与摘要，不导入模型；`--include-images` 只处理 train/val 引用。
- `scripts/verify_periodic_cvfdetr.py` 需要 `--host`、`--user`、`--stage`、`--spec`、`--probe-python`，以及与 worker 对应的状态根和可执行文件。默认仅打印本地预览，明确追加 `--confirm-run` 才会连接并执行 GPU 中断、损坏与恢复配方。stage 内的项目适配器和故障注入脚本必须事先准备，故障脚本必须使用预览中的状态根。

公开版本的本地验证使用 Fake Worker；Linux systemd、SSH 和真实 GPU 验收的完成状态见 [兼容性与验收矩阵](../../docs/SUPPORT-MATRIX.md)。
