# SDK 长任务与 MCP 生命周期

`CCSClient` 只调用版本化 JSON gateway；没有直接访问 worker 内部。单次调用默认超时
120 秒，stdout 默认 20 MiB、stderr 默认 64 KiB，同时最多 4 个调用和 8 个等待者。
预算可通过 `max_response_bytes`、`max_error_bytes`、`max_concurrent`、`max_queued` 配置。
超过捕获预算会终止本地进程树并返回 `GATEWAY_OUTPUT_LIMIT`，含 `output_truncated`
和继续读取建议。分页调用保留最后已交付的游标，再请求更小页面。

```python
from ccs import CCSClient, CCSPlan, CCSReadCursor

client = CCSClient([absolute_python, '-m', 'controller.gateway_cli', '--fleet', absolute_fleet])
plan = client.plan('run.submit', payload, request_id='experiment-001')
plan.save('reviewed-plan.json')  # UTF-8；存在则拒绝覆盖
saved = CCSPlan.load('reviewed-plan.json')
submitted = client.confirm(saved)  # 保持原 actor、request_id、payload 和 plan_sha256
run_id = submitted['result']['id']
snapshot = client.wait_run(run_id, worker_id='worker-a', total_timeout=3600)

cursor = CCSReadCursor('run.events', run_id, worker_id='worker-a')
for event in client.iter_events(run_id, worker_id='worker-a', total_timeout=3600,
                                cursor=cursor, limit=100, max_bytes=1048576, follow=True):
    consume(event)
    persist(cursor.to_dict())
# 重连：CCSReadCursor.from_dict(previously_saved_object)
```

`iter_events` 明确请求分页，需要 worker 的 `run.events.page` 能力。游标只随交付给
调用者的事件前进；获取页面后提前关闭迭代器，不会确认尚未交付的尾部。
`follow=True` 在读到终态 snapshot 后再次读取事件尾部。`iter_logs` 继续使用原有
绝对字节游标，参数为 `stream`、`after_offset`，交付单位为一个日志 chunk。
两种迭代器都要求显式 `total_timeout`；默认 `follow=False` 读取当前可用内容。

`wait_run`、两个迭代器，以及 request/plan/confirm/execute 都接受
`cancel_event=threading.Event()`。取消本地等待和等待超时均不调用 `run.cancel`，
远程训练继续。已启动的变更请求遇到取消、超时、输出截断时保留 `unknown`，
应按原身份查询 `operation.query` / `operation.reconcile`，不自动重发变更。
排队期间取消/超时或队列满时为 `not_sent`。读取轮询只对瞬态读取错误退避重试；
LOST 为当前状态模型中的非终态，继续受总等待期限限制。

MCP 默认最多 2 个执行中请求、8 个排队请求。排队取消不启动 gateway；执行中
取消通知 SDK 终止本地进程树并等待有界清理。队列满返回 `MCP_BUSY`，不会无限
生成线程或子进程。运行上限通过 `--max-concurrent` / `--max-queued` 设置。
Windows 子进程先以挂起状态创建，加入 kill-on-close Job 后再恢复主线程，
子进程一启动即创建的后代也处于清理范围内；POSIX 使用独立进程组。
捕获建立和清理发生错误均按已启动请求报告，变更结果保持 unknown。
等待截止后另有有界的本地进程与管道清理时间。

`ccs-mcp --read-only` 只公布 `ccs_read`，同时检查实际 action 和 workspace 子动作。
`--allow-action` 可重复使用，例如 `fleet.status` 或 `workspace.execute.inspect`；
同一检查同时限制计划与执行。默认模式保留 ccs_read/ccs_plan/ccs_execute 三个工具。
这些入口限制是调用范围配置；凭据及服务器权限仍由部署方管理。
