# P0-03：按幂等键只读找回远端提交

Worker 声明 `run.lookup: 1`，提供以下命令：

```text
trainctl --root /srv/ccs-state lookup-submission --idempotency-key example-key --json
```

成功响应沿用 trainctl 的 `{ok, request_id, data, error}` 信封。`data` 包含：

```json
{
  "idempotency_key": "example-key",
  "run_id": "run_0123456789abcdef0123456789abcdef",
  "state": "found",
  "observed_at": "2026-10-12T00:00:00Z"
}
```

`state=absent` 时 `run_id=null`。这是读取原子发布的 `idempotency.json` 时，键尚未出现的事实；断开的原提交或并发提交仍可能随后发布它。因此，absent **不授权重发已进入 started 的操作**。Controller 应保留结果未知状态，在以后重新查询或人工解决，不自动新建另一项训练。

找到键时，worker 校验安全的 run ID、对应目录、`manifest.json` 与 `status.json` 的 run ID，以及 `events.jsonl` 第一条 submitted 事件的幂等键。初始提交先持久化这些记录，再发布幂等映射，所以只接受身份一致的已发布记录。后续事件可并发追加，查询只读取第一条提交事件。

映射损坏、映射指向的目录或初始记录缺失、记录身份冲突返回 `SUBMISSION_LOOKUP_INCONSISTENT`，不会报告 absent。临时读权限或 I/O 故障返回可重试的 `SUBMISSION_LOOKUP_UNAVAILABLE`；其重试范围只涵盖查询。

查询以 `RunStore(root, initialize=False)` 打开状态目录，不初始化 worker，不创建锁文件，不调用提交、runner、健康检测或运行状态修复。未初始化的目录直接返回 absent，目录保持不存在。该路径兼容已有提交记录，不依赖升级后新增的 manifest 字段。

本地验证覆盖只读文件保持、CLI 未初始化目录、存在/缺失映射、损坏与身份冲突、读失败、后续事件半写，以及 systemd 启动结果未知后的找回。未进行远端部署或真实训练启动验证。
