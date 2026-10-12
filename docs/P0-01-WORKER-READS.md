# P0-01：显式 worker 诊断读取

交付日期：2026-10-12。范围为控制端本地实现与 Fake Worker 验收。

## 行为

`worker.health`、`worker.monitor`、`worker.alerts`、`worker.storage`、
`worker.processes` 接受空 payload 或 `{"worker_id":"目标 ID"}`。
显式 ID 使用 `WorkerDirectory.remote_for` 路由；未知 ID 返回
`WORKER_NOT_FOUND`，非法 ID 返回 `INVALID_WORKER_SELECTION`，其他字段返回
`INVALID_REQUEST`。没有任何回退或自动改派。

执行结果保留原来的诊断字段，并增加 `result.worker_id`。该 ID 来自控制端路由，
覆盖 worker 返回的同名字段。只读 dry-run 的 `result` 同样报告目标 ID，
`executed` 仍为 false；能力协商仍可能读取 health。

兼容性协商使用实际选择的 adapter。仍允许旧 worker 的 legacy 只读调用。
disabled 从机可以接受这五种诊断读取，但 `run.submit` 继续经过原来的停用准入检查。
没有复用会拒绝 disabled 从机的 `operation_plan`。

## 空 payload 的兼容规则

- 保留原来的 `gateway.remote` 对象；不按目录首项重新选择默认 adapter。
- 如果该对象已注册在目录中，响应使用匹配的 worker ID；多个 ID 指向同一对象时，
  优先使用构造器的 `worker` 身份，否则使用第一个匹配身份。
- 如果默认 adapter 不在目录中，响应使用构造器的 `worker` 身份。
  自定义嵌入调用方应传入正确身份；不会为了补齐目录而切换 adapter。
- 现有 CLI 的 `--host` 身份就是 host；`--fleet` 默认 adapter 和身份均来自配置首项。
  因而旧的空 payload 行为保持不变。多从机调用建议总是显式传 ID。

SDK 和 MCP 的 payload 沿原有 JSON gateway 传递，无需改变 worker 协议或升级 worker。

## 改动与验收

主要实现：`src/controller/gateway.py`。新增测试：`tests/test_worker_reads.py`。
协议 schema、MCP 描述和统一协议文档由本批集成同步更新。

新增 9 个测试先在旧实现失败，证明显式读取、身份标记和默认兼容行为的缺口。
修复后，与 gateway、fleet、CLI 相关测试合计 **35 个通过**：

```powershell
$env:PYTHONPATH='src;tests'
python -m unittest test_worker_reads test_gateway test_fleet_gateway test_gateway_cli
```

实际执行时 TEMP/TMP 指向 staged CCS 中的 `.test-tmp-worker-reads`，
避免此 Windows sandbox 的系统 TEMP 目录 ACL 导致现有文件型测试失败。
覆盖双 Fake Worker 的五种读取、未知/非法 ID、disabled 诊断与提交拒绝、
目录首项不同于旧默认 adapter、未注册的旧默认 adapter、legacy 协商和 dry-run。

没有真实 SSH、远端 SDK/MCP 任务或 GPU 验收。回滚仅需回退控制端实现和相应合同说明；
没有 worker 数据或数据库结构变更。
