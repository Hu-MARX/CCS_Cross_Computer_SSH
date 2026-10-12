# 统一任务结果 Interface（本机开发版）

2026-09-04—09-05：本轮仅更新 Windows 工作区和本机看板；不部署从机，不启动真实测试，不修改正在运行的50epoch训练。

## 职责

`controller.results` 是共享的结果 Module。工作流调用方和看板使用同一投影；没有新增自动科研调度器。
训练/测试/分析仍是独立 Run，提交通过既有 Orchestrator。是否继续实验由外部工作流及批准计划决定。

本轮不是生产环境“训练完成自动测试”上线：真实测试入口、测试数据批准、检查点选择及文件校验仍待接入。
`trainctl evaluate` 仍保留明确的未实现错误；不要把生命周期中的 EVALUATING 当作已经跑过 test。

## 任务声明

ExperimentSpec schema_version 仍为1，可选增加 `task`，旧配置不增加该字段，旧数据及证据绑定保持不变。

```json
{
  "kind": "test",
  "source_run_id": "run_0123456789abcdef0123456789abcdef",
  "checkpoint": {"path": "/approved/output/selected.ckpt", "sha256": "填写64位小写十六进制摘要"},
  "inputs": [{"path": "/approved/test-manifest.json", "sha256": "填写64位小写十六进制摘要"}]
}
```

这是 `task` 的结构示意，不是可直接提交的真实测试配置。kind 为 train/test/analysis；旧配置显示 unspecified，
不靠任务名猜测训练或测试类型。当前50epoch主任务由明确的启动配置标为 train。
source_run_id 是输入来源关系，不复用恢复/重试含义的 Run.parent_run_id。
路径和哈希是可追溯的**声明**，本轮不据此读取/校验文件，也不能据此获得测试授权。
已有 evidence 门禁保持独立；真正执行时仍须批准输入、验证选定权重和数据版本。

带 task 的 SSH 提交会附带 `--require-task-metadata`。未升级的 worker 会明确拒绝该参数，
避免旧版解析器忽略新字段后悄悄运行。只有本机 trainctl 增加了该标志，远端未升级。

## 结果合同

schema_version=1 包含 task、execution、observation、progress、metrics、artifacts、checkpoint、errors、logs、failure。

- execution.status 表示上次观测生命周期；observation.state 独立表示 fresh/stale。
- observation 保存采样时间、查询时间、游标和来源。任何一项查询过期、断线、历史未追平都不会算作 fresh。
- progress 保留 phase/step/epoch，以及生产者可选上报的 `progress/completed`、`progress/total`。
- metrics 保留原始指标名，例如 `test/AP50`、`analysis/sample_count`；每项记录数值、上报时间和阶段。
  当前协议是整份进度快照的时间，并非每项指标的独立计算时间；Lightning 可能携带上一阶段指标。
  不把这类缓存包装成最终测试报告。拒绝非有限数/布尔/嵌套对象，最多检查128个字段，每份指标约3KB。
- artifacts 是声明输出路径下的文件清单，未下载文件；远端未提供哈希时显示 null，不自行声称已校验。
- failure 仅在上次状态 FAILED 时提供一般错误提示，详细原因查询 stderr/events；errors 是缓存或查询错误。
- scientific_conclusion 固定为 null：执行成功和合成指标都不是科研结论。

HTTP（仅现有本机令牌授权、只读）：

```text
GET /api/state
GET /api/state?run_id=<本机已登记ID>
GET /api/result?run_id=<本机已登记ID>
```

`/api/state` 含 catalog、曲线和同源 result；`/api/result` 返回同一个 result。
未知任务返回404，多个run_id或未知参数返回400；结果查询不触发SSH。
浏览器“导出结果 JSON”下载当前显示结果，令牌不进入文件。结果可能含本地/远端研究路径，请勿直接公开。

CLI（Windows PowerShell，先在项目目录设置模块路径）：

```powershell
$env:PYTHONPATH = 'src'
python -m controller.cli --host ccs-worker catalog
python -m controller.cli --host ccs-worker result run_21eb7b7bc8cb411c873947855de413dc
```

catalog 用 SQLite mode=ro 读取最近20个本机记录；result 只读已有看板缓存，不建立SSH、不初始化数据库。
缓存不存在则报错，不假造空任务。查询时会重新判断时间新鲜度；缓存不是服务器权威全量记录或唯一备份。

## 多任务采集边界

主任务每30秒采集一次；每轮最多加一个轮询历史任务，每页200个采样。monitor/alerts 同轮共享，
最多四次常规SSH查询，不为每个页面用户增加轮询。最近20个本机登记记录 + 显式主任务，最多21项。
历史任务轮询间隔随数量增加，未轮到时可能显示过期；不会把本机目录状态冒充实时服务器状态。
未由本机登记的远端任务暂不自动发现。

终态且观测新鲜后，对已声明的非递归产物路径采集一次文件清单，最多额外两次查询/轮。
不遍历原始数据目录、不读模型内容；通配目录/递归模式/无声明路径需另行显式采集。
结果显示最多200项，超过会标记 truncated；旧远端 artifacts 返回本身尚未分页，这是兼容限制。
失败300秒后再尝试，结果清单不代表输出完整性或最终验收。原地继续写入终态产物不会自动刷新已收集清单。

每Run保留最多6000个曲线点并抽稀老记录；约3KB的指标字段预算避免无限增长，读取缓存上限32MB。
旧缓存不伪造通用指标，需要新采样补充。验证曲线包含 validation_finished。
前端使用 textContent 展示任务名、指标名和路径；保持令牌、来源校验、只读和无公网端口。

## CPU 合成验收

```powershell
python examples/mock_workflow.py --directory data/mock-workflow-acceptance
# 再运行相同命令，验证重启后仍只有两个合成Run
python examples/mock_workflow.py --directory data/mock-workflow-acceptance
```

该程序使用持久化 Fake Adapter，不调用 SSH、Python 训练命令或 GPU。合成训练成功后显式提交关联测试；
报告含 synthetic=true。固定指标只供接口验收，不代表 CVFDETR 性能。模拟数据与真实看板目录分离。
--outcome failure/disconnect/test_failure 可在各自新的目录验证训练失败、失联、测试失败。
失联恢复后可再次运行；失败终态不自动重试，不改写成成功。该示例不是通用审批/预算/并发调度实现。

新测试覆盖元数据与旧格式、缓存新鲜度、通用指标预算、只读数据库/CLI、HTTP鉴权和任务选择、
共享查询、成功链路、训练失败、测试失败、失联恢复、重启和幂等提交。另有前端状态测试验证验证结束曲线、
测试完成数量、任务切换清空旧结果；不包含浏览器截图/点击式视觉验收。

## 本轮验收记录

- Python 全量122项：110通过、12因Linux/Torch条件跳过。最后结果合同修改后32项相关测试再次通过。
- JavaScript 7项通过，包含断连/过期后导出必须标记 stale，不能把旧 RUNNING 状态当作当前确认。
- 首次受限执行的两项原有子进程清理测试失败；未改代码，在允许清理本机合成子进程的执行权限下，
  两项隔离复测及完整回归均通过。未对服务器执行训练故障注入。
- 本机看板已重启、私有链接保持不变；HTTP首页200，结果接口与页面任务ID一致。
- 真实缓存接口验收：14项本机登记任务；当前50epoch任务 RUNNING / validation / step 6013，
  connection=fresh，无采集或目录错误。此为验收时快照，不是后续实时状态。
- `data/mock-workflow-acceptance/acceptance.json` 为合成成功链路报告，不能用于论文或模型性能判断。
