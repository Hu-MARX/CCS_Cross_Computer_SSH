# CCS 从机版本与能力协商

能力协商用于回答“这个可达从机能否安全执行本次操作”，不用于判断训练成功、GPU 健康或观测数据是否新鲜。

## 从机声明

新从机在 `health` 数据的 `ccs_interface` 字段中返回声明，也提供独立命令：

```bash
trainctl --root /srv/ml-orchestrator capabilities --json
```

声明包含：

- `schema_version`：声明文档结构版本；
- `protocol.name/major/minor`：CCS 从机协议版本；
- `software.name/version`：用于诊断的软件身份，不作为功能判断依据；
- `capabilities`：能力名到正整数语义版本的映射。

当前协议为 `ccs-worker 1.0`。客户端要求协议主版本一致，并要求从机的每项能力版本大于或等于最低版本。

## 主机侧查询

任意 CLI 或 Agent 都可以读取 JSON 结果：

```powershell
$env:PYTHONPATH = 'src'
python -m controller.cli `
  --host ccs-worker `
  --user mlrunner `
  --known-hosts "$env:USERPROFILE\.ssh\known_hosts" `
  --identity-file "$env:USERPROFILE\.ssh\id_ed25519" `
  compatibility `
  --require run.submit=1 `
  --require evidence.runtime=2
```

也可使用项目入口：

```powershell
.\scripts\worker-a.ps1 compatibility --require run.submit=1
```

返回状态：

- `compatible`：协议和全部最低能力均满足；
- `legacy`：SSH 与 `health` 正常，但从机没有接口声明；
- `incompatible`：接口声明无效、协议主版本不同或能力不足。

`legacy` 和 `incompatible` 都不是离线。SSH 超时、认证失败和远端命令错误继续使用原有错误码。

## 新建任务护栏

`OpenSSHRunner.submit` 会先从 `ExperimentSpec` 推导最低能力：

- 所有提交要求 `run.submit=1`；
- 存在 `task` 时要求 `task.metadata=1`；
- 绑定证据包时要求 `evidence.bundle=1` 和 `evidence.launch_gate=1`；
- 使用 `execution_mode=snapshot` 时还要求 `source.snapshot=1`。

检查点迁移另外按动作协商：导出要求 `checkpoint.export=1`，导入要求目标 Worker 的 `checkpoint.import=1`。包含 `task.checkpoint` 的新 Run 还要求 `checkpoint.input_gate=1`。导入能力只表示目标端能够安全暂存、验签和原子发布文件，不表示任意训练代码都能恢复该 checkpoint。

协商未通过时，客户端在发送 `submit` 前返回 `WORKER_CAPABILITIES_UNDECLARED` 或 `WORKER_INCOMPATIBLE`。已有 Run 的只读监测、日志和状态同步不会因此中断。

## 提交接受记录查询

新增 `run.lookup=1` 表示 worker 提供按幂等键查询已接受 Run 的只读接口：

```bash
trainctl --root /srv/ml-orchestrator lookup-submission --idempotency-key my-run-v1 --json
```

返回相同幂等键，以及 `state=found` 和 `run_id`，或 `state=absent` 和空 `run_id`。
查询不会提交 Run。控制端的 `operation.reconcile` 在原 worker 上使用此能力，
并将已查到的接受事实写回本地操作账本。没有此能力或查询失败时保留 unknown；
`absent` 也不解锁自动重派，因为原始请求仍可能在处理或观测尚未可见。

普通 `run.submit` 不新增 `run.lookup` 能力要求，旧 worker 仍按原提交合同接入。
P0-01 的 worker 显式只读路由及 P0-02 的本地原子幂等登记无需升级 worker。
要自动对账未知提交，需单独安排 worker 升级并确认其 `run.lookup=1` 声明；
此本地交付没有部署或真实 Linux/远端验收证据。

五种 `worker.*` 诊断仍允许 legacy worker，并对实际选择的 adapter 协商能力。
disabled 是是否接受新工作的本地配置，与能力兼容性分开：disabled worker 可诊断，
不能提交新 Run。协议主版本保持 1，新增能力不根据软件版本号推断。

Windows 看板只展示经过裁剪的协议、软件版本和缺失能力摘要，不把完整能力表或其他远端原始字段发送给浏览器。兼容状态最多每五分钟重新检查一次。
