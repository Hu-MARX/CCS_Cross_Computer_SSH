# 证据状态 v2

CCS 将运行期证据拆成三个正交事实，避免把部署更新、读取竞争和实际执行输入变化混为一个“证据漂移”告警。

- `execution_integrity`：实际参与 Run 的选定输入。状态为 `no_violation_observed`、`violation_observed` 或 `unknown`。
- `deployment`：快照 Run 所对应的可变原始/部署目录。状态为 `unchanged`、`changed`、`unknown` 或 `not_applicable`。
- `observer`：核验进程本身，独立记录生命周期、`healthy/degraded/failed` 健康度和覆盖范围。

`violation_observed` 和 `changed` 都保留历史；观测器的临时读取竞争不构成内容变化证据，下一次稳定核验可恢复为 `healthy`。若稳定重扫确认摘要不同，再记录到对应的执行或部署范围。

## 告警语义

- `evidence_execution_integrity`：已确认选定执行输入与基线不一致。
- `evidence_deployment_changed`：可变部署目录在 Run 启动后更新；快照 Run 的实际执行副本仍需单独看 `execution_integrity`。
- `evidence_observer_warning`：读取竞争等可恢复核验问题。
- `evidence_monitor_failed`：观察器报错或活跃 Run 的报告超过新鲜度阈值。
- `evidence_drift`：仅为旧版兼容告警，范围可能无法判定。

顶层 `drift_detected`、`issue_count` 和 `issues` 暂时保留给旧调用方，但在 v2 中只映射执行完整性违规。新调用方必须读取三个结构化状态。

## 快照任务和旧告警

快照任务同时核验证据副本、执行快照和仍从原路径读取的哈希数据；原始代码/配置目录作为独立部署范围观察。开发或部署 CCS 不再直接产生执行完整性告警。

旧观察器可能在原始文件被原子替换时记录 `Evidence byte budget exceeded`。Windows 缓存仅在报告明确是快照执行且所有旧问题都是这类读取竞争时，将其兼容解释为“旧观测器读取竞争，未证明执行快照变化”；其他旧告警继续显示为范围无法判定，不作乐观推断。

## 部署边界

本地看板和控制端可以先升级并解释旧报告。`evidence_watch.py` 与 worker 告警分类应在当前长任务结束后再部署到从机，使新启动的 Run 原生生成 v2 报告；不向正在运行的训练进程注入代码，也不改动其快照、Conda 环境或数据。
