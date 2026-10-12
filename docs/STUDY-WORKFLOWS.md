# Study/Stage 持久化工作流

首个工作流内核把多个 `ExperimentSpec`/`Run` 组织成一个可恢复的 `Study`。它是通用 JSON 接口，不依赖 Codex：Codex、脚本、其他 CLI 或后续网页服务都可以调用同一个 `WorkflowEngine.execute(request)`。

## v1 能力

- `StudySpec v1`：研究问题、规划材料、运行次数预算和最多 64 个 Stage。
- 有向无环依赖：依赖不存在、自依赖和环在任何 Run 创建前拒绝。
- 持久状态：Study、Stage、依赖、Run 关联、审批、动作和事件均保存在现有 SQLite 数据库。
- 幂等创建与提交：相同键和相同计划返回原 Study；相同键搭配不同计划被拒绝。每个 Stage Run 使用确定性幂等键。
- 现有 Run 纳管：把已登记且规格匹配的既有 Run 关联到非测试根 Stage，不重启、不停止，也不重复提交训练；纳管来源和兼容升级写入审计记录。
- 阶段决策包：Stage 终态后冻结执行、指标、证据与精确核验的 checkpoint 候选；不自动选择候选，也不构成 test 授权。
- 断线安全：SSH 同步失败只返回 `sync_errors`，不会把 RUNNING Stage 判为 FAILED，也不会启动依赖它的下游。
- 测试门：`test` Stage 必须声明一个静态 checkpoint，或声明来自直接依赖 Stage 的 Artifact Binding；动态绑定由软件核验路径、大小和 SHA-256。之后还须获得绑定当前计划、Recommendation 和产物绑定摘要的明确审批。
- 停止：未开始 Stage 立即取消；已提交 Run 通过既有远端取消协议确认，断线时 Study 保持 `STOPPING`。

## 请求接口

所有请求均可保存为 JSON，并通过以下入口提交：

```powershell
orchestratorctl --host ccs-worker --user mlrunner workflow --request REQUEST.json
```

创建：

```json
{
  "action": "create",
  "idempotency_key": "paper-a-study-v1",
  "study": {
    "schema_version": 1,
    "name": "Ablation study",
    "research_question": "Does module A improve the primary held-out metric?",
    "planning": {
      "hypotheses": ["Module A improves mAP"],
      "statistical_plan": {"primary_metric": "test/mAP"},
      "data_plan": {"test_split_is_held_out": true}
    },
    "budget": {"max_runs": 1},
    "stages": [
      {
        "id": "train",
        "name": "Train candidate",
        "kind": "train",
        "depends_on": [],
        "requires_approval": false,
        "spec": {
          "schema_version": 1,
          "name": "Candidate training",
          "project": {},
          "runtime": {},
          "command": {"argv": ["/absolute/python", "/absolute/train.py"]},
          "task": {"kind": "train", "inputs": []}
        }
      }
    ]
  }
}
```

每个 `spec` 都是现有 `ExperimentSpec v1`。完整结构见 `schemas/study-spec.schema.json`。

其他动作：

```json
{"action":"show","study_id":"study_..."}
{"action":"list","limit":20}
{"action":"reconcile","study_id":"study_..."}
{"action":"adopt_run","study_id":"study_...","stage_id":"train","run_id":"run_...","run_spec_sha256":"64位摘要","idempotency_key":"adopt-train-v1","adopted_by":"操作者标识"}
{"action":"prepare_decision_package","study_id":"study_...","stage_id":"train","checkpoint_directory":"/absolute/workdir/orchestrator_runs/run_.../checkpoints"}
{"action":"recommend","study_id":"study_...","recommendation":{"schema_version":1,"...":"见AGENT-GATEWAY.md"}}
{"action":"approve","study_id":"study_...","stage_id":"test","plan_sha256":"64位摘要","recommendation_id":"rec_...","approved_by":"用户标识"}
{"action":"stop","study_id":"study_...","reason":"停止原因"}
```

`reconcile` 先对账已提交 Run并解析 Artifact Binding，再只启动依赖已成功、最新 Recommendation 为 PROCEED、审批已匹配且预算未耗尽的 test Stage。调用可重复；重复对账不会重复创建远端 Run。`adopt_run` 的完整约束见 [Run 纳管](RUN-ADOPTION.md)，终态事实汇总见 [阶段决策包](STAGE-DECISION-PACKAGES.md)，绑定协议见 [Artifact Binding](ARTIFACT-BINDING.md)，Agent Interface 和信任边界见 [Agent Gateway](AGENT-GATEWAY.md)。

## Nature Skills 的位置

Nature Skills 参与 Study 执行前的规划和执行后的解释，例如文献证据、假设、数据与统计方案、图表计划和论文论证。其输出作为 `planning` 中的版本化材料进入计划摘要。Codex 结合这些材料和运行证据形成科研建议，但建议不是执行授权。最终 held-out `test` 只有在用户批准当前 `plan_sha256` 后，才由工作流内核按确定性规则执行；软件本身不做是否值得测试的科研判断，也不把自然语言建议直接当作训练或删除命令。

## 当前边界和下一片

Artifact Binding v1 只支持 `checkpoint` 角色、直接依赖 Stage 和精确绝对 POSIX 路径；路径模板可使用 `${RUN_ID}`。它刻意不提供含义模糊的“最新 checkpoint”选择。Windows 看板目前只读展示 Study/Stage、产物绑定、Recommendation、审批以及 Run 是工作流提交还是既有任务纳管；不会从网页触发科研决策或执行操作。
