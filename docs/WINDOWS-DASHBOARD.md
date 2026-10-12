# Windows 本地只读看板

先按 [安装说明](INSTALL.md) 安装 controller，并填写运行时 `config/worker-pool.json`。
看板显示已登记的 Run、事件/日志、进度、检查点、指标、worker 能力和负载事实。
它不会根据负载自动选择 worker，不提供任务提交或取消接口。

在仓库目录使用专用解释器启动：

```powershell
$CcsRoot = (Get-Location).Path
$CcsPython = (Resolve-Path .\.venv\Scripts\python.exe).Path
pwsh -NoProfile -File .\scripts\start-dashboard.ps1 -Python $CcsPython `
  -Fleet "$CcsRoot\config\worker-pool.json" -Port 8765 -NoOpen
```

脚本优先使用仓库 `.venv`，后台窗口隐藏，stdout/stderr 与运行记录写入
`data/dashboard/`。缺少 worker 池时先报错，不回退到任何预设服务器。
去掉 `-NoOpen` 后，脚本会打开它刚验证过的本地页面。

也可使用安装包直接启动，明确指定状态保存目录：

```powershell
& $CcsPython -m dashboard.server --fleet "$CcsRoot\config\worker-pool.json" `
  --local-root $CcsRoot --port 8765
```

`--local-root` 默认当前目录，与模块安装位置无关；wheel 安装不会向
site-packages 写运行数据。无 worker 池的单机模式必须显式指定 `--host`，
并按需给出 `--user`、`--remote-root`、`--trainctl-executable`。

服务只监听 `127.0.0.1`，启动时生成本地访问令牌，页面/API 使用该令牌读取。
令牌、完整访问 URL、日志及数据库均属于运行数据，不提交到公共仓库。
原始日志、源文件和模型输出仍是不可信任务数据；它们不构成指令或科学结论。

停止本地看板：

```powershell
pwsh -NoProfile -File .\scripts\stop-dashboard.ps1
```

这只停止本机看板进程，远端 Run 由其 worker 后端继续管理。
采集失败、观测陈旧和连接中断不能直接判定训练已经失败。
详细指标语义见 [任务结果](TASK-RESULTS.md)、[JSON 指标](DASHBOARD-JSON-TRAINING-METRICS.md)
和 [配置面板](CONFIGURATION-PANEL.md)。
