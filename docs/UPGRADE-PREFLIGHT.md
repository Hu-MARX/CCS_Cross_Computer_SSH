# 只读升级预检与角色版本矩阵

`python -m ccs.upgrade_preflight` 离线读取已经捕获的公共 JSON 请求/回包，
输出 controller、runner CLI、observer daemon、workspace 四个角色的版本及
side-by-side 升级检查表。它不连接服务器，不执行 gateway，不创建数据库，
不安装包，不修改配置，也不切换或重启任何进程。

这是升级流程的第一批：**事实汇总与计划预览，不是部署器**。
后续远端安装、切换及回退仍需独立授权，并遵循公共 CCS Interface 的
dry-run → 精确 `plan_sha256` 确认。输出中的 `preview_sha256` 只是本地预览的
内容摘要，不能作为 gateway 确认，不是认证，也不能证明输入记录真实或仍然新鲜。

## 使用

使用已经安装 CCS 的绝对解释器；不依赖登录 shell 或 Conda 自动激活：

```console
/absolute/ccs/python -m ccs.upgrade_preflight --evidence /absolute/captured-evidence.json --worker-id worker-a --target-version 0.6.1 --artifact-sha256 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
```

示例摘要是占位符，必须替换为目标发布包的真实 SHA-256；程序仅记录调用方
声明的摘要，**尚未校验发布包字节**。可以重复 `--worker-id` 显式选择多个
worker；不提供默认 worker，不会根据负载自行改派。输入 `-` 时从 stdin 读取。
输入上限 4 MiB、256 条观测、64 个显式 worker；输出只写 stdout。

Python 调用使用相同 Interface：

```python
from ccs.upgrade_preflight import upgrade_preview

preview = upgrade_preview(captured_json, worker_ids=["worker-a"],
                          target_version="0.6.1", artifact_sha256=verified_package_sha256)
```

## 捕获文件格式

运行目录中的记录不应提交到公开仓库。以下是缩略格式：

```json
{
  "schema_version": 1,
  "controller": {
    "version": "0.6.1",
    "observed_at": "2026-01-01T00:00:00Z",
    "doctor": {"ok": true, "data": {"interpreter": {"python": [3, 12, 3]}}}
  },
  "observations": [
    {
      "request": {
        "schema_version": 1,
        "request_id": "health-worker-a",
        "actor": {"id": "reader", "kind": "agent"},
        "action": "worker.health",
        "dry_run": false,
        "payload": {"worker_id": "worker-a"}
      },
      "response": {
        "ok": true,
        "data": {
          "request_id": "health-worker-a",
          "action": "worker.health",
          "classification": "read",
          "executed": true,
          "result": {"worker_id": "worker-a"}
        }
      }
    }
  ]
}
```

实际使用时保留原始完整 request/response，不自行填写观测事实。上面的
health 没有接口声明，所以 runner 版本会正确显示 `unknown`，也没有
fleet 观测，所以会显示 `FLEET_OBSERVATION_REQUIRED`。

`controller.version` 来自所选 controller 绝对入口的 `ccsctl --version`；
记录对应入口和捕获时间。doctor 的安装成功或磁盘文件版本**不能代替**这个
观测，也不能证明已经运行的看板进程加载了同一版本。doctor 回包可直接作为
`controller.doctor`，其 Python 元数据只描述该 doctor 选择的解释器。

支持的观测只有以下公共只读动作：

| 观测 | 能说明的角色/事实 | 不能推断的事实 |
|---|---|---|
| `fleet.status` | runner 声明、可见负载、Python；必须在指派工作前查询 | workspace/daemon 同版本；实际 UID、路径访问权限 |
| `worker.health`，显式 `worker_id` | runner CLI 的 `ccs_interface` 与 Python | 正在训练的 Run 使用了此版本 |
| `worker.monitor`，显式 `worker_id` | daemon 最近写出的声明、PID、时间、新鲜度 | 当前 PID 仍存活；陈旧声明仍是当前 daemon 版本 |
| `workspace.execute` 的 inspect/diff/changes | 回包中 workspace 操作的兼容声明 | 其他账号/目录同样有权限 |
| `worker.processes`，显式 `worker_id` | 可见进程数量 | 被截断/失败/缺失的清单意味着空闲 |

workspace 版本依据该请求回包的 `compatibility`，而不是 runner health。
只接受已修复实际 workspace 角色协商的 controller 所捕获的记录；旧 controller
可能检查 runner 而执行 workspace，不能用这种回包证明 workspace 入口版本。
实际账号/root/绝对可执行文件绑定仍须在候选安装验收中确认。

观测按捕获顺序输入，同一角色最后一条生效，包括失败观测。失败不会保留
先前成功版本来伪装健康。daemon 的 `freshness` 原样区分 fresh/stale/unavailable，
即使 stale 声明包含版本，也只能说明历史观测。版本相同只表示字符串相同；
能力逐项保留已有声明，**不从版本推测能力**。输入摘要用于定位记录，不代表
记录现在仍有效；执行前必须刷新所需事实并重新审阅实际动作计划。

## 安全升级与回退

1. **prepare**：校验发布包字节摘要，在实际角色账号的全新版本目录准备安装；
   记录旧入口、源码、环境和活动 Run 的捕获路径。使用一致性数据库备份，
   不能复制一个旧数据库覆盖正在更新的 state。
2. **validate**：保持严格主机密钥检查，确认候选账号/root/绝对入口，再对目标
   动作协商真实声明能力。现有 health 不证明 venv、pip、Conda 或离线缓存可用，
   本预检将这些安装前提明确保留为 unknown；不要猜测并选错安装方案。
3. **switch**：取得另一个具体变更计划的精确确认，仅切换新操作入口及观察器；
   活动训练的原解释器、源码、环境、训练单元均保持不变。busy 和 unknown 都
   不能授权覆盖旧安装；可独立准备候选目录，不表示可以破坏活动运行。
4. **rollback**：停止新提交，恢复旧入口，不恢复旧 state。保留当前操作/audit
   数据库、检查点、未知提交身份和事务日志。先完成或隔离不完整检查点事务，
   再判断旧代码是否能够读取当前 schema；禁止自动重试 unknown 变更。

`status=preview_only`、`executed=false`、`authorization=not_granted` 始终保留。
`deferred_checks` 列出真正部署前仍需完成的检查；没有 `ready_to_apply` 字段，
也没有 `--apply`、自动安装、直接 SSH 或后台修改模式。

## 升级验收记录中的测试计数

分组验证器的 `tests_run` 和 `passed_tests` 按父测试执行计数，不按 subtest
报错条数相减。一个父测试出现五条 subtest error，仍只计一个 `failed_tests`。
`failures`/`errors` 保留原始错误条数；`subtests_run`、`passed_subtests`、
`subtest_failures`、`subtest_errors`、`skipped_subtests` 单独记录子测试结果。

`skipped_tests` 是唯一的跳过父测试执行数，`skipped` 列表仍保留每条原始
跳过证据及原因。类级 fixture 在父测试启动前报错，不从 `passed_tests` 扣减，
而记录到 `fixture_errors`；fixture failure/skip 也单独计数。expected failure
和 unexpected success 分别记录，不伪装成通过。以上修正不改变 required skip、
可选训练依赖或 `--require-linux` 的既有验收门禁。
