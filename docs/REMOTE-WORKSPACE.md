# 受控远程工作区

该模块让 CLI 或研究代理在不直接执行远程 shell 写操作的情况下检查、暂存、审查、应用和恢复源码变更。

## 权限拆分

- 训练 Adapter：`mlrunner@ccs-worker`，状态根目录 `/srv/ml-orchestrator`。
- 工作区 Adapter：`worker-a@ccs-worker`，状态根目录 `/home/worker-a/.ml-orchestrator`。
- 允许的研究目录：`/srv/research`。
- 工作区可执行文件：`/home/worker-a/.ml-orchestrator/venv/bin/trainctl`。

不要给 `mlrunner` 增加研究目录写权限。两个 Adapter 使用同一个协议，但承担不同权限。

## Windows 调用前缀

```powershell
$WorkspaceArgs = @(
  "-m", "controller.cli",
  "--db", "data/orchestrator.db",
  "--host", "ccs-worker",
  "--user", "mlrunner",
  "--workspace-user", "worker-a",
  "--workspace-root", "/home/worker-a/.ml-orchestrator",
  "--workspace-executable", "/home/worker-a/.ml-orchestrator/venv/bin/trainctl",
  "--known-hosts", "$env:USERPROFILE\.ssh\known_hosts",
  "--identity-file", "$env:USERPROFILE\.ssh\id_ed25519",
  "workspace", "--request"
)
$env:PYTHONPATH = "src"
python @WorkspaceArgs "examples/workspace-inspect.json"
```

任何其他 CLI 都可以生成相同 JSON 请求并调用 `orchestratorctl workspace --request <file>`。

## 请求生命周期

1. `inspect`：有界目录列表、受限递归文件树、文本读取或搜索。
2. `stage`：创建候选覆盖层，不修改项目。
3. `diff`：读取统一差异和 `plan_sha256`。
4. 人或上层工作流批准精确的 `plan_sha256`。
5. `apply`：重新验证原文件哈希，备份后原子应用。
6. `restore`：使用精确 `applied_sha256` 恢复应用前状态。
7. `discard`：丢弃尚未应用的覆盖内容，保留审计清单。
8. `changes`：列出工作区变更记录。

`stage` 请求示例：

```json
{
  "action": "stage",
  "project": "/srv/research/embodied",
  "operations": [
    {
      "op": "write",
      "path": "models/world_model.py",
      "content": "class WorldModel:\n    pass\n"
    },
    {
      "op": "trash",
      "path": "obsolete_experiment.py"
    }
  ]
}
```

应用请求必须携带暂存响应返回的摘要：

```json
{
  "action": "apply",
  "change_id": "change_...",
  "plan_sha256": "..."
}
```

恢复请求必须携带应用响应返回的摘要：

```json
{
  "action": "restore",
  "change_id": "change_...",
  "applied_sha256": "..."
}
```

## 安全规则

- 拒绝白名单外绝对路径、`..`、反斜杠路径和符号链接。
- 单文件文本读取/写入最多 1 MiB，单次变更最多 16 MiB、128 个操作。
- 搜索最多检查 10,000 个文件，不跟随符号链接目录。
- 递归文件树使用 `mode=tree`，深度限制为1–8、返回条目限制为1–5,000；只返回路径、类型和文件大小，不读取文件内容，不进入符号链接或版本控制目录。
- 数据集、检查点、权重、模型产物、版本控制元数据和常见凭据文件不可修改。
- 常见凭据文件及版本控制内部文件不可读取或搜索。
- `apply` 前检测暂存后发生的外部修改，冲突时拒绝覆盖。
- `restore` 前检测应用后发生的外部修改，冲突时拒绝覆盖。
- `trash` 只表示可恢复删除；该接口不提供永久清理研究文件的动作。

工作区模块是防误操作和并发覆盖的执行约束，不是抵御同一 Unix 账号恶意进程的安全沙箱。

递归清单示例：

```json
{
  "action": "inspect",
  "mode": "tree",
  "project": "/srv/research",
  "path": "cvfdetr_gcr/src",
  "depth": 4,
  "limit": 2000
}
```

响应中的 `limit_reached` 和 `depth_limited` 分别说明条目上限或深度上限是否截断了结果；
`restricted: true` 表示只展示名称、不进入的凭据或版本控制条目。
