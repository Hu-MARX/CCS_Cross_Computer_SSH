# CCS 0.6.1 安装说明

本指南安装 Windows 控制端和可信 Linux worker，并生成本地 MCP 配置。控制端可以与训练环境分开安装。CCS 核心运行时没有第三方依赖；PyTorch、CUDA、Lightning 等由每个训练任务指定的环境提供。

## 1. 准备环境

| 位置 | 必需项 |
| --- | --- |
| 控制端 | Python >=3.11、Git、OpenSSH；Windows 示例使用 PowerShell 7 |
| Linux worker | Python >=3.11、可用的 `venv`、OpenSSH 服务、对源码和运行目录的读写权限 |
| 可选 MCP | 安装 `.[mcp]`；依赖范围为 `mcp>=2.2,<3` |
| 可选 systemd 后端 | 可用的 user systemd manager、`systemd-run --user` 和所需用户服务权限 |

先查看 Python 实际版本。Ubuntu 的 `apt install python3` 不保证获得 Python 3.11 或以上；版本不足时，先通过操作系统支持的渠道准备符合要求的解释器及对应 `venv`。不要用训练环境的包升级来修补 CCS 控制端环境。

## 2. Windows 控制端

```powershell
git clone https://github.com/Hu-MARX/CCS_Cross_Computer_SSH.git
Set-Location .\CCS_Cross_Computer_SSH
py -3.11 -c "import sys; assert sys.version_info >= (3, 11), sys.version"
py -3.11 -m venv .venv
$CcsRoot = (Get-Location).Path
$CcsPython = (Resolve-Path .\.venv\Scripts\python.exe).Path
& $CcsPython -m pip install -e .
& $CcsPython -m controller.gateway_cli --version
& $CcsPython -m controller.gateway_cli doctor --python $CcsPython --cwd $CcsRoot
```

预期版本为 `0.6.1`；`doctor` 返回的 `data.ok` 应为 `true`。以下命令也可以离线使用，不要求配置 SSH 或运行数据库：

```powershell
& $CcsPython -m controller.gateway_cli describe
& $CcsPython -m controller.gateway_cli describe --schema request
```

本指南使用绝对解释器及模块入口，不依赖当前终端是否激活虚拟环境。源码可编辑安装要求保留克隆目录；移动目录后应重新安装。安装发布包时，也可在独立虚拟环境中执行 `python -m pip install /absolute/path/to/cross_computer_ssh-0.6.1-py3-none-any.whl`。

## 3. Linux worker：普通账号与独立目录

在准备接收 SSH 连接的普通账号下执行。下面的路径属于当前账号，不需要对训练进程授予 root 权限。

```bash
CCS_WORKER_PYTHON="${CCS_WORKER_PYTHON:-python3}"
"$CCS_WORKER_PYTHON" -c 'import sys; assert sys.version_info >= (3, 11), sys.version'
```

该命令失败时先安装满足版本要求的解释器，再继续。选择其他解释器时，例如设置 `CCS_WORKER_PYTHON=/absolute/path/to/python3.11`。

```bash
mkdir -p "$HOME/src" "$HOME/.local/share" "$HOME/.local/state/ccs-worker"
git clone https://github.com/Hu-MARX/CCS_Cross_Computer_SSH.git "$HOME/src/CCS_Cross_Computer_SSH"
CCS_WORKER_SOURCE="$HOME/src/CCS_Cross_Computer_SSH"
CCS_WORKER_ROOT="$HOME/.local/state/ccs-worker"
CCS_WORKER_VENV="$HOME/.local/share/ccs-venv"
"$CCS_WORKER_PYTHON" -m venv "$CCS_WORKER_VENV"
"$CCS_WORKER_VENV/bin/python" -m pip install -e "$CCS_WORKER_SOURCE"
"$CCS_WORKER_VENV/bin/python" -m controller.gateway_cli --version
"$CCS_WORKER_VENV/bin/trainctl" --root "$CCS_WORKER_ROOT" capabilities --json
"$CCS_WORKER_VENV/bin/trainctl" --root "$CCS_WORKER_ROOT" health --json
```

记录 `CCS_WORKER_ROOT` 与 `CCS_WORKER_VENV/bin/trainctl` 展开后的绝对路径，填入控制端 worker 配置。`trainctl` 的 `--root` 参数位于子命令之前。初次运行会创建 RunStore 目录；健康检查会读取本机硬件和可用空间，不提交训练。

没有 `worker.json` 时默认后端为 `process`。这个后端适合初次安装与功能学习，通过独立进程启动任务；它不具备 systemd cgroup 的整个进程树退出核验。不要把 `process` 的运行状态当作 systemd 清理保证。

### 可选：systemd 后端与常驻观察 worker

需要持久用户服务时，由管理员为该 worker 账号准备可用的 user manager；需要退出 SSH 后保持 user manager 时配置该账号的 linger。管理员执行 `sudo loginctl enable-linger WORKER_USER`；不向训练账号提供任意 sudo 权限。

在 worker 账号下先检查 `systemctl --user status` 是否能连接用户服务管理器。然后在运行根目录的 `worker.json` 中设置，例如：

```json
{
  "backend": "systemd",
  "monitor_interval": 15,
  "min_free_bytes": 5368709120
}
```

已有配置需合并字段，保留原来的日志、存储、工作区等约束。systemd 后端会通过 `systemd-run --user` 为每个新 Run 创建独立单元，使用启动 `trainctl` 的解释器。训练任务不设置自动重启；监控单元与训练单元分开。

常驻采集入口为：

```bash
"$CCS_WORKER_VENV/bin/python" -m trainctl.worker --root "$CCS_WORKER_ROOT"
```

这条命令在前台运行。正式部署可使用 [Ubuntu 服务模板](../deploy/ubuntu/README.md)，并将单元的 `ExecStart` 改为已验证的绝对虚拟环境解释器及运行根目录。模板中的默认 `/srv/ml-orchestrator` 路径只适用于对应的管理员部署。不要保留指向未验证系统 Python 的 `ExecStart`。管理员创建 `/srv` 目录时需把所有权授予实际 worker 账号；改变后端不会为既有 `process` Run 补充 systemd 保证。

管理员部署也可使用 `deploy/ubuntu/install-trainctl.sh`；参数、解释器版本检查和源码目录规则见 [Ubuntu 部署说明](../deploy/ubuntu/README.md)。不要把准备删除的暂存目录当作可编辑安装的长期源码输入。

## 4. SSH 身份与固定主机密钥

在控制端配置已有 SSH key 或 SSH agent。SSH 别名示例：

```text
Host ccs-worker
    HostName worker.example.net
    User ccs-runner
    IdentityFile ~/.ssh/id_ed25519_ccs
    IdentitiesOnly yes
```

从服务器管理员或可信控制台取得 SSH host key 指纹。可在 worker 可信控制台执行 `ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub`，再与控制端候选 host key 比对。`ssh-keyscan` 只收集当前网络返回的候选密钥，单独使用不能证明服务器身份。核验后将匹配密钥加入控制端 `known_hosts`，保留严格校验。

```powershell
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes ccs-worker true
```

连接成功后验证绝对 worker 入口。替换为第 3 节记录的真实远端绝对路径：

```powershell
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes ccs-worker `
  '/home/ccs-runner/.local/share/ccs-venv/bin/trainctl --root /home/ccs-runner/.local/state/ccs-worker capabilities --json'
```

如果使用非默认 `known_hosts`，在这些 SSH 检查中同时添加 `-o UserKnownHostsFile=ABSOLUTE_PATH`。不要用关闭 `StrictHostKeyChecking` 的方式处理主机身份变化。

## 5. 控制端 worker 配置

在克隆目录中执行；若换了终端，重新设置 `$CcsRoot` 和 `$CcsPython`。

```powershell
if (Test-Path .\config\worker-pool.json) { throw 'worker-pool.json 已存在，请编辑现有文件。' }
Copy-Item .\config\worker-pool.example.json .\config\worker-pool.json
$CcsPool = "$CcsRoot\config\worker-pool.json"
```

示意结构如下，实际值以你的账号和机器为准：

```json
{
  "schema_version": 1,
  "workers": [
    {
      "id": "worker-a",
      "host": "ccs-worker",
      "user": "ccs-runner",
      "remote_root": "/home/ccs-runner/.local/state/ccs-worker",
      "trainctl_executable": "/home/ccs-runner/.local/share/ccs-venv/bin/trainctl",
      "known_hosts": "C:/Users/YOUR_USER/.ssh/known_hosts",
      "identity_file": "C:/Users/YOUR_USER/.ssh/id_ed25519_ccs",
      "enabled": true,
      "labels": ["linux"]
    }
  ]
}
```

Windows JSON 路径使用 `/` 或转义的 `\\`。`id` 是请求中显式指定的 worker 身份；`host` 可以使用 SSH 别名。若依赖 SSH agent，可省略 `identity_file`。配置只保存密钥路径，不能填写私钥内容。工作区修改还需要单独配置 workspace 身份、入口及 `workspace_roots`；见 [Remote workspace](REMOTE-WORKSPACE.md)。

固定 SDK/MCP 的网关命令前缀：

```powershell
$CcsGatewayArgs = @(
  $CcsPython, '-m', 'controller.gateway_cli',
  '--fleet', $CcsPool,
  '--db', "$CcsRoot\data\orchestrator.db",
  '--audit-db', "$CcsRoot\data\ccs-audit.db",
  '--replica-root', "$CcsRoot\data\checkpoint-replicas"
)
$CcsCommandJson = ConvertTo-Json -Compress -InputObject $CcsGatewayArgs
& $CcsPython -m controller.gateway_cli doctor `
  --python $CcsPython --cwd $CcsRoot --command-json $CcsCommandJson
```

前缀是参数数组，**不包含** `request --file -`；SDK 会自行追加它并通过标准输入发送 UTF-8 JSON。`--cwd` 只影响控制端本地目录，不改变训练命令的远端目录。

默认 doctor 是离线检查。需要显式检查一台已配置 worker 时：

```powershell
& $CcsPython -m controller.gateway_cli --fleet $CcsPool doctor `
  --python $CcsPython --cwd $CcsRoot --probe-remote --worker-id worker-a
```

该探测只读 worker health。接下来按 [操作指南](OPERATION-GUIDE.md) 查询状态、审阅计划并启动独立短任务。

## 6. 可选 MCP：先预览，后创建

```powershell
& $CcsPython -m pip install -e '.[mcp]'
& $CcsPython -m controller.gateway_cli doctor `
  --python $CcsPython --cwd $CcsRoot --command-json $CcsCommandJson --require-mcp
$CcsMcpOutput = "$CcsRoot\mcp-local.json"
$CcsPreview = & $CcsPython -m controller.gateway_cli mcp-config `
  --python $CcsPython --command-json $CcsCommandJson --cwd $CcsRoot `
  --output $CcsMcpOutput | ConvertFrom-Json
$CcsPreview.data | ConvertTo-Json -Depth 20
```

检查输出的 `target`、`content` 和 `plan_sha256`。确认内容符合你的本地安装后，以相同参数和原摘要创建新文件：

```powershell
& $CcsPython -m controller.gateway_cli mcp-config `
  --python $CcsPython --command-json $CcsCommandJson --cwd $CcsRoot `
  --output $CcsMcpOutput --write --confirm-sha256 $CcsPreview.data.plan_sha256
& $CcsPython -m controller.gateway_cli doctor `
  --python $CcsPython --cwd $CcsRoot --mcp-config $CcsMcpOutput --require-mcp
```

父目录必须存在，已有文件不会覆盖。生成文件默认使用绝对 Python/module 入口和 `--read-only`，只注册 `ccs_read`。按所用 MCP 客户端的配置入口导入 `mcpServers` 内容。CCS 提供 stdio 传输；请让客户端启动该进程，日志或诊断信息不要混入 stdout。

确实需要提交、取消或工作区变更时，可在配置预览命令中加入 `--allow-mutations`，重新审阅摘要并创建另一个配置文件。服务也支持重复 `--allow-action` 收窄动作，详见 [Agent integration](AGENT-INTEGRATION.md)。这些启动约束面向可信本地客户端，不代替 SSH 身份、网络认证或任务授权。

## 7. 安装排错与升级

| 现象 | 处理 |
| --- | --- |
| 找不到 `ccsctl` 或模块 | 使用上述绝对虚拟环境 Python；确认在该环境安装了 CCS |
| Python/MCP 版本不符 | 在指定 venv 中安装符合声明的版本，重新运行 `doctor` |
| `CWD_NOT_FOUND`、入口脚本缺失 | 更正 `--cwd` 和绝对路径；中文及空格路径作为完整参数传入 |
| SSH host key 不匹配 | 通过可信渠道复核服务器身份及变更原因，再更新固定记录 |
| `trainctl` 在非交互 SSH 中找不到 | 配置绝对 `trainctl_executable`，不依赖登录 shell 激活环境 |
| `systemctl --user` 无法连接 | 先由管理员修复用户服务管理器；不要将任务切换为 root 执行 |
| 分页或 lookup 能力缺失 | 读 worker capabilities，升级对应 worker；不能将本地升级当作远端升级完成 |

升级前保留当前版本、运行目录和数据库。对繁忙 worker 使用独立目录或等待安全窗口，不替换活动训练依赖的源码。验证分组、发布包安装、状态保留回退及 Linux 验收要求见 [RELEASING](RELEASING.md) 和 [SUPPORT-MATRIX](SUPPORT-MATRIX.md)。
