# 自动化科研运行模型

本项目把科研判断与跨机执行分开。上层由研究者、Codex 和 Nature skills 共同形成研究计划、解释证据并写作；底层 CCS 只在两台从机上执行明确任务、托管长运行并返回可核验事实。

```text
研究问题与文献证据
        |
        v
研究契约 -> Study/Stage 计划 -> Nature statistics 预注册
        |                         |
        +-----------+-------------+
                    v
        CCS JSON Agent Interface
          | fleet.status + 显式选择
          | mutation dry-run + plan SHA 确认
          v
       worker-a              worker-b
   RTX 3090 / 24 GB     RTX 5070 Ti / 16 GB
          |                    |
          +------ Run 事实 -----+
                    v
 Stage Decision Package -> 统计分析 -> claim-evidence ledger
                    |
                    v
     图表合同/QA -> Results -> 全文与投稿材料
```

## 接口与责任

CCS 是深模块。调用者只需理解一个版本化 JSON Interface、从机池、操作计划摘要和结果信封；SSH 引号、超时、断线重试、幂等、检查点校验和审计留在模块内部。生产 Adapter 是 OpenSSH，测试 Adapter 是内存/Fake，从机接口声明负责能力协商。

科研层不导入 CCS 的内部实现。它把研究问题、统计契约和预算编译为 StudySpec/ExperimentSpec，再通过 `ccsctl` 提交。CCS 不自动选择从机、不判断模型优劣，也不把运行成功当成科研成功。

## Nature skills 映射

| 阶段 | Skill | 强制产物/门禁 |
|---|---|---|
| 文献与空白 | `nature-literature-pipeline`、按需 `nature-academic-search` | 去重记录、阅读深度、六维评分；自动化只写 raw 层 |
| 实验登记 | `nature-experiment-log` | experiment ID、样品/数据批次、Run ID、原始材料路径、异常记录 |
| 分析设计 | `nature-statistics` | 独立单位、重复、主要终点、效应量、不确定区间、多重比较、排除/缺失规则 |
| 科学图表 | `nature-figure` | 先写结论与证据链；首次实际绘图时选择并固定 Python 或 R；PDF/碰撞/对齐 QA |
| 论文 | `nature-writing` | 一句话论点、术语表、段落职责、claim-evidence map；Results 先写，Abstract 最后 |

目标期刊尚未指定时只应用通用的 Nature-leaning 结构，不套用旗舰 Nature 的具体字数或图表限制。真实绘图尚未开始，因此当前不替用户猜测 Python/R 后端。

## 实验执行协议

1. 填完 `research/config/research-project.json`，`researchctl project ... --require-ready` 必须通过。
2. 先冻结数据划分、基线、主要指标、随机种子、停止/排除规则和资源预算，再创建 Study。
3. 调用 `fleet.status` 获取能力、GPU、磁盘和负载事实；上层显式选择 `worker-a` 或 `worker-b`。
4. 对 `run.submit` 等变更先 dry-run。请求身份、动作、载荷或从机选择改变后，旧 SHA 失效。
5. 每次 Run 形成标准实验日志。失败重试或 checkpoint 恢复是新 Run，不覆盖旧历史。
6. Stage 终态后生成不可变决策包；只有核验过的产物才进入统计分析。
7. 任何 claim 必须记录范围、状态、限制和证据文件 SHA-256。最终 held-out test 仍需 Recommendation 与用户批准。
8. claim ledger 通过后，按实际证据绘图、先写 Results，再完成 Introduction、Discussion、Conclusion、Methods、Title 和 Abstract。

## 当前安全状态

双机配置已迁入 `CCS/config/worker-pool.json`，且不包含密码或私钥内容。服务器控制台提供的 ED25519 指纹已与实际握手密钥逐项匹配，并固定到项目专用 `CCS/config/known_hosts`。研究请求使用 `CCS/scripts/research-ccs.ps1`，私钥仍只从本机用户目录读取，不复制到项目；严格主机密钥校验保持开启。
