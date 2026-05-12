# ChatGPT Pro Output: Fund Philosophy / GitHub Repo Review

状态：ChatGPT Pro output, pending original-source verification  
日期：2026-05-12  
用途：审视 `ai-super-cycle` 作为 source-backed AI Infra research OS / fund research engine 的方法论和仓库工程。

## 总评

这套框架的方向是对的，而且已经不是“AI 主题研究”，而是一个 **source-backed AI Infra research OS** 的雏形。

最强的地方是：从 D0 LLM 需求源头倒推物理约束链条，把 HBM、CoWoS、网络、电力、冷却、测试、材料、融资都看成同一个多层约束系统。AI Infra 应被定义为从 token 需求到算力集群、HBM、网络、封装、材料、数据中心、电力、金融和监管的多层系统，而不是单线产业链。

当前最大工程缺口：

```text
研究哲学强，基金工程弱；
证据规则强，数据契约弱；
研究流程强，组合与风险闭环弱。
```

如果目标是长期 GitHub 仓库化、换电脑可恢复、未来演化成 paper portfolio / fund engine，就必须把“框架”压缩成可执行的数据模型、目录结构、命令、测试、版本规则和公开/私有边界。

## 框架本质

| 概念 | 说明 |
| --- | --- |
| LLM-demand dependency graph | 从 OpenAI、Anthropic、Google / DeepMind 等 D0 需求源头开始，沿训练、推理、算力、内存、封装、网络、电力、冷却、融资做 dependency BFS。 |
| Bottleneck-rent underwriting | 研究谁能因供给慢、认证强、良率难、单位价值量上升而捕获瓶颈租金。 |
| Source-backed falsifiable research system | 所有收入、订单、backlog、CapEx、毛利率、产能、价格和客户关系先回到原始出处。 |
| Infra-capex transmission model | 把 AI 从模型发布 / GPU 订单推进到云 CapEx、RPO、折旧、HBM、CoWoS、网络、电力、散热和 rack 点亮能力。 |
| Repo-native fund research engine skeleton | universe JSONL、SQLite、queue、evidence、reports、docs 已经构成可运行研究仓库雏形。 |

一句话：

```text
传统主题投资是 label-first；本框架是 dependency-first。
传统估值研究是 multiple-first；本框架是 bottleneck-and-evidence-first。
```

## 最大优点

1. 框架自洽：从 D0 需求源头，到 dependency BFS、evidence card、score、反证、queue、report，已经形成完整研究链。
2. 天然可证伪：客户 CapEx 放缓、GPU 供给放开、HBM 过剩、ASIC 替代、推理成本下降、电力并网卡住、供应商切换、毛利率不提升等都能成为失败路径。
3. 长期复用能力强：每个公司结论都可拆成产业链位置、需求证据、供给瓶颈、财务传导、技术替代、反证、评分、下一轮核验。
4. 能避开伪 AI 标的：只说 AI opportunity、普通周期恢复包装成 AI、客户集中但无合同等，都应留在待核验或降级。

## 最大盲区

| 盲区 | 改进方向 |
| --- | --- |
| D0 过于集中 | 从公司名升级为 demand event taxonomy。 |
| BFS 图谱可能静态 | 每条 edge 增加 edge_weight、confidence、time_horizon、substitution_risk、source_status、last_verified_at、failure_trigger。 |
| 反证未阈值化 | 建 red / amber / green 阈值，而不是文字清单。 |
| 估值和拥挤度未工程化 | 增加 expectations risk：implied growth、multiple risk、crowding、liquidity、revision sensitivity。 |
| evidence card 缺审计字段 | 增加 source_quality、source_hash、archived_url、filing_date、period_end、metric_unit、confidence_score、staleness_date、review_status。 |
| fund engine 不完整 | 缺 security master、risk model、portfolio construction、paper ledger、attribution、benchmark、rebalance、FX、liquidity、drawdown limits。 |

## D0 Demand Event Taxonomy

建议 D0 不只写公司名，还要建事件类型：

```text
D0a frontier training run
D0b hyperscaler inference deployment
D0c agentic workflow token expansion
D0d sovereign AI buildout
D0e enterprise private AI deployment
D0f physical AI / robotics / video generation
D0g model efficiency shock
```

这样可以避免把需求源头固定在少数私有 AI lab，而忽略 Meta、xAI、sovereign AI、enterprise inference、AI video、coding agent、robotics 对 infra 的不同拉动方式。

## 推荐新增字段

### Dependency Edge

```text
edge_weight
confidence
time_horizon
substitution_risk
source_status
last_verified_at
failure_trigger
```

### Evidence Card Audit Fields

```text
source_quality: primary / cross-disclosure / secondary / model-generated
source_hash
archived_url
filing_date
period_end
metric_currency
metric_unit
restated_or_not
confidence_score
staleness_date
negative_evidence_found
last_reviewer
review_status
```

### Refutation Dashboard Thresholds

```text
RPO conversion deterioration
backlog cancellation language
gross margin fails to expand despite revenue growth
inventory days spike
capex grows faster than contracted revenue
secured MW delayed beyond target date
interconnection queue slippage
HBM ASP declines while capacity ramps
```

### Expectations Risk

```text
implied growth
multiple expansion / compression risk
consensus crowding
liquidity depth
drawdown sensitivity
earnings revision risk
```

## GitHub Repo Recommendations

推荐仓库名：

```text
ai-super-cycle
```

推荐结构：

```text
ai-super-cycle/
  README.md
  CHANGELOG.md
  CONTRIBUTING.md
  DISCLAIMER.md
  pyproject.toml
  requirements.txt
  Makefile
  .gitignore
  .env.example
  .pre-commit-config.yaml

  docs/
    philosophy.md
    llm-dependency-bfs-framework.md
    research-checklist.md
    source-evidence-template.md
    public-private-boundary.md
    data-security-rules.md
    methodology/

  data/
    schemas/
    seed/
    normalized/
    sqlite/
    raw/                 # gitignored
    private/             # gitignored

  evidence/
    companies/
    themes/
    private/             # gitignored

  queues/
    source_verification/
    alpha_mining/
    pro_agents/

  reports/
    quarterly/

  scripts/

  src/
    ai_super_cycle/

  tests/

  notebooks/
    exploratory/          # gitignored or private
    public_examples/

  .github/
    workflows/
```

## Public / Private Boundary

适合 public：

```text
README.md
docs/philosophy.md
docs/research-checklist.md
docs/llm-dependency-bfs-framework.md
docs/source-evidence-template.md
scripts/*.py
data/schemas/*.json
data/seed/global_universe_sample.jsonl
reports/public_methodology_snapshot.md
evidence/examples/
queues/public_source_verification_queue_sample.md
```

必须 private：

```text
实际 paper portfolio
实际仓位、模拟仓位、调仓记录
watchlist 排名
alpha score 排名
未公开的研究结论
付费数据、券商材料、数据库导出
ChatGPT Pro 会话 URL
浏览器 profile、CDP port、cookie、token
个人路径
API keys
data/ai_infra_universe.sqlite 如果含有内部评分或备注
完整 evidence notes 如果含有主观判断、未核验推论或组合意图
China sleeve 的具体交易映射、流动性和执行记录
```

关键提醒：分支不是数据安全边界。public/private 应该用两个 repo 或 gitignored/private/encrypted data 分离。

## Fund Engine Missing Modules

未来要成为 paper portfolio / fund engine，还缺：

1. security master；
2. source registry；
3. evidence database；
4. scoring model calibration；
5. expectations risk module；
6. paper portfolio ledger；
7. portfolio construction；
8. risk engine；
9. refutation dashboard；
10. attribution；
11. event calendar；
12. CI / automation。

## 推荐口号

英文：

```text
From token demand to bottleneck rent: a source-backed research OS for AI infrastructure.
```

中文：

```text
从 token 需求到瓶颈租金：一个基于原始出处的 AI Infra 研究操作系统。
```

工程化版本：

```text
A reproducible research engine for mapping LLM demand into AI infrastructure bottlenecks, evidence cards, refutation signals, and portfolio-ready research artifacts.
```

## 四条硬约束

```text
No edge, no research.
No primary source, no conclusion.
No refutation, no core pool.
No reproducibility, no repo.
```

这四句话应作为 `ai-super-cycle` 从研究笔记集合升级成可复用、可审计、可恢复、可扩展研究系统的核心准则。

