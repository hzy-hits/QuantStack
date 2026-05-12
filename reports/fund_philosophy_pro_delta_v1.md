# Fund Philosophy Pro Delta v1

日期：2026-05-12  
状态：methodology delta from ChatGPT Pro, pending implementation  
边界：这是方法论和工程化待办，不是投资建议、买卖建议、目标价或仓位建议。

## 结论

Pro 回复没有推翻当前框架，而是给出一个准确定位：

```text
source-backed AI Infra research OS
```

当前项目已经具备研究 OS 雏形，但还没有完整 fund engine。最重要的判断是：

```text
研究哲学强，基金工程弱；
证据规则强，数据契约弱；
研究流程强，组合与风险闭环弱。
```

因此下一步不是继续扩 ticker，而是把已有研究工程化。

## 应采纳的命名

| 名称 | 用法 |
| --- | --- |
| LLM-demand dependency graph | 描述 D0-D5 BFS 图谱 |
| Bottleneck-rent underwriting | 描述寻找瓶颈租金的投资研究逻辑 |
| Source-backed falsifiable research system | 描述原文证据和可证伪性 |
| Infra-capex transmission model | 描述从 LLM demand 到 CapEx / 物理瓶颈的传导 |
| Repo-native fund research engine skeleton | 描述当前 GitHub 仓库化阶段 |

推荐 tagline：

```text
From token demand to bottleneck rent: a source-backed research OS for AI infrastructure.
```

## 需要升级的工程层

| 模块 | 当前状态 | 下一步 |
| --- | --- | --- |
| Data contract | SQLite 表已有，但 schema 和字段契约弱 | 增加 `data/schemas/*.json` |
| Evidence card | Markdown 已有，但审计字段不足 | 增加 source_quality、source_hash、period_end、confidence、review_status |
| Dependency edge | 只有 path/edge 文本 | 增加 edge_weight、confidence、time_horizon、substitution_risk、failure_trigger |
| Refutation | 有反证文字 | 做 red/amber/green 阈值 |
| Portfolio | 有组合哲学 | 建 paper ledger、risk model、attribution、rebalance rules |
| Repo | 有 docs/scripts/reports/evidence | 增加 DISCLAIMER、CHANGELOG、Makefile、tests、public/private boundary |

## D0 需要从公司名升级为事件 taxonomy

当前 D0 是 OpenAI / Anthropic / Google / Meta 等公司。后续应增加事件类型：

| event | 含义 |
| --- | --- |
| D0a frontier training run | frontier model training 对 GPU/HBM/CoWoS 的拉动 |
| D0b hyperscaler inference deployment | 大规模推理上线对 cloud/rack/power/network 的拉动 |
| D0c agentic workflow token expansion | agent loop 拉长推理时间和 token demand |
| D0d sovereign AI buildout | 主权 AI 对本地云、GPU、数据中心、电力的拉动 |
| D0e enterprise private AI deployment | 企业私有 AI 对云、存储、网络、推理优化的拉动 |
| D0f physical AI / robotics / video generation | physical AI/video 对训练、数据和推理负载的拉动 |
| D0g model efficiency shock | 效率提升对硬件需求的反向冲击 |

## 最小工程化待办

### P0

1. 增加 `DISCLAIMER.md`。
2. 增加 `Makefile`，封装 bootstrap / rebuild / verify / reports。
3. 增加 `data/schemas/`，先定义 universe、company、evidence-card、source、score 的 JSON schema。
4. 增加 `tests/test_no_private_data_leak.py`，防止提交个人路径、ChatGPT URL、CDP port、token、cookie、IBKR 数据。
5. 将 README tagline 改成 source-backed research OS。

### P1

1. 增加 `docs/public-private-boundary.md`。
2. 增加 `docs/data-security-rules.md`。
3. 增加 `docs/methodology/refutation-dashboard.md`。
4. 增加 `scripts/validate_evidence_cards.py`。
5. 增加 `scripts/build_refutation_dashboard.py`。

### P2

1. 增加 `src/ai_super_cycle/` 包，把脚本逐步模块化。
2. 增加 paper portfolio ledger schema。
3. 增加 risk engine / attribution / event calendar。
4. 增加 GitHub Actions validate workflow。

## 对 GitHub public/private 的修正

当前推荐仍是先 private repo：`ai-super-cycle`。

如果未来 public，应拆出干净 public 版本，只放：

- 方法论；
- schema；
- 脚本；
- 样例 evidence card；
- sample universe；
- public methodology snapshot。

不应 public：

- ChatGPT Pro 会话 URL；
- 个人路径；
- CDP port / browser profile；
- alpha score 排名；
- paper portfolio；
- 未核验判断；
- 具体仓位或交易记录；
- 付费数据或券商材料。

## 四条仓库规则

建议写入 README / CONTRIBUTING：

```text
No edge, no research.
No primary source, no conclusion.
No refutation, no core pool.
No reproducibility, no repo.
```

## 当前动作

先不重构目录。当前目录已经可工作，下一步应该是小步增强：

1. 新增 `DISCLAIMER.md` 和 `Makefile`。
2. 新增 `data/schemas/` 和 `tests/`。
3. 把 public/private boundary 写清楚。
4. 先做 private GitHub repo，等框架稳定后再导出 public snapshot。

