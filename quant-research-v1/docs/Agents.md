# Agents.md — Philosophy and Instructions for Agents Working on the AI-Infra Fund Pipeline

This file is for any agent (Claude Code, Codex, GPT, or a human) that reads the
daily report, narrates it, or modifies the pipeline. Read this first. It is the
philosophy spine — every script, prompt, and report section should be coherent
with what is written here.

---

## 1. What This System Is

QuantStack is an **AI-infrastructure specialist fund research pipeline**. It is
no longer a broad-market scanner. The mandate is narrow on purpose:

> Find, verify, and size positions in the supply chain that the AI build-out
> depends on — and know exactly when to stop buying and start hedging.

The daily product is two reports: `us_daily_report.md` and `cn_daily_report.md`
(under `reports/review_dashboard/main_strategy_v2/{date}/`). They are
**deterministic, program-generated markdown** — there is no LLM in the
narration loop. The program computes everything; an agent's job is to *audit,
extend, and explain* — never to invent numbers.

The AI book is the **absolute mainline**. Broad-market indices, macro, and
sector context appear only as a compressed background section near the end of
the report. If a change makes the report more about broad market and less
about the AI-infra book, it is wrong.

---

## 2. The Gate Stack — How a Name Becomes Tradable

A ticker is not "tradable" because it looks good on a chart. It must clear four
gates, in order. Every gate is a hard filter; tape/price only decides *timing*
within the names that already passed.

| # | Gate | Where | Pass condition |
|---|---|---|---|
| 1 | **Universe pool** | `ai_infra_universe.py` | Member of `ai_infra/data/global_universe_v2.jsonl`, not 排除池 |
| 2 | **Evidence gate** | `is_production_grade()` | `evidence_state` contains `原文已证明` or `合理推论` (G0-G2 cleared) |
| 3 | **Alpha Factory sleeve** | `production_tier()` | Carries a promoted sleeve id (production names auto-attach `ai_infra_production_core`) |
| 4 | **Risk regime gate** | `score_risk_regime_engine.py` | Hedge/Wedge/Confirm/Press multiplier scales the new-add R |

- **Research pool** = every BFS candidate (the radar). **Production pool** =
  only names that cleared gate 2. Rankers in `enforce`/`enforce_expand` mode
  use the production pool; the research pool stays radar-only.
- A name still on `待原文核验` / `证据不足` belongs on the radar and can be
  discussed, but **cannot receive R**. Tape strength alone never promotes it.
- This is the methodology's core discipline: **evidence before exposure.**

---

## 3. The BFS Dependency Framework

The universe is built by breadth-first search over the AI compute dependency
graph, rooted at token demand (OpenAI / Anthropic / Gemini):

```
D0  Labs / clouds / model demand        (OpenAI, Anthropic, hyperscalers)
D1  Compute & accelerators              (NVDA, AMD, AVGO custom ASIC)
D2  Memory / networking / optical       (HBM, CPO, 800G/1.6T, SerDes)
D3  Chip equipment / packaging / test   (CoWoS, substrate, metrology)
D4  Data-center / power / cooling       (NeoCloud, firm power, liquid cooling)
D5  Raw inputs / second-order suppliers
```

`bfs_depth` and `dependency_edge` (客户边/BOM边/技术边/现金流边/产能边) are how
a candidate justifies its place. Shallow depth + multiple edges = closer to the
bottleneck = higher conviction. A name with no traceable edge to token demand
does not belong in the universe.

---

## 4. Options as Duration / Tenor Insight

Options are **never the traded instrument** in the daily basket — they are
evidence about timing, positioning, and asymmetric risk. Read them three ways:

- **Far-OTM anomaly** (`score_options_anomaly_radar.py`): unusually heavy
  far-OTM calls (delta ≤ 0.20, vol ≫ OI) can signal a short-squeeze setup or
  informed positioning ("老鼠仓"). Heavy far-OTM puts signal selling pressure
  or a hedge being laid. Flagged tickers feed the source-review queue's
  counter-evidence column.
- **Multi-tenor structure** (`score_options_tenor_radar.py`): the chain is
  bucketed weekly / biweekly / monthly / quarterly / half-year / LEAPS. Where
  the volume sits tells you the *horizon* of the conviction. Short-dated call
  walls = gamma/tactical; long-dated LEAPS calls = structural / insider-tilt.
- **Victim puts** (`score_victim_put_suggestions.py`): for each bubble-hedge
  victim, the concrete OTM put contract (delta -0.20 to -0.35, DTE 30-60) that
  expresses the downside — defined max loss = premium. Never short outright.

Duration matters: a fat multiple is a long-duration asset. When the discount
rate rises, the NPV of 2030 cash flows compresses first. The options surface
prices that risk before the stock does.

---

## 5. The Risk Regime — Hedge / Wedge / Confirm / Press

This is the fund's bubble-survival doctrine. You do **not** short the thing
going parabolic. You find the wedge, buy puts on the victim, and wait for
confirmation before you press. `score_risk_regime_engine.py` turns this into a
discrete daily state with a hard R multiplier on the AI-infra book:

| State | Trigger | AI-infra new-add R | Posture |
|---|---|---|---|
| **HEDGE** | tape healthy, wedge not biting | `1.0x` | baseline; carry small SPX/HYG hedge |
| **WEDGE** | rates up (TLT 20d ≤ -2%) / credit widening / SMH↔TLT corr ≤ -0.5 | `0.6x` | keep buying smaller; hold TBT / TLT put-spread |
| **CONFIRM** | SMH lost EMA20 but holds EMA50, fresh 1-2 day EMA50 break, or extreme greed + wedge biting | `0.4x` | freeze adds to stretched names; prep trim list |
| **PRESS** | SMH sustained ≥3-day EMA50 loss, or trendline break | `0.35x` | trim to a defensive core; freeze new adds; press victim shorts |

> **PRESS is not a full liquidation.** The original framework said `0.0x`,
> but the AI-infra basket V-recovers too hard for going flat to pay — a
> 2024-06..2026-05 backtest put `0.0x` Sharpe at 1.19 vs `0.35x` at 1.50.
> PRESS now holds a defensive `0.35x` core. PRESS also requires the EMA50
> break to *hold ≥3 consecutive closes* — a 1-2 day dip is CONFIRM, not
> PRESS, so a single-day cross can no longer whipsaw the book flat.

- The **wedge** is the trend that kills the bubble (rates), not the bubble
  itself. You go long the wedge to protect the book.
- The **victim** is the convex-to-downside name next to the bubble that cannot
  survive a *pause* — cheap vol, levered, evidence-thin.
- **Confirmation** is a trendline *break*, not a dip. Patience is the edge.
- The gate multiplier applies *after* basket caps — it is the final word on
  book size. Caps define the maximum; the regime shrinks it.

---

## 6. Convexity Discipline — payoff shape is a hard filter

`ΔP = −D·Δr + ½·C·(Δr)²` — the second term is convexity. It is squared, so
the more violent the move, the more it pays. The whole doctrine of this fund
is to be a *convexity hunter*:

> duration = you assume the world is linear
> convexity = you admit it is non-linear
> black swan = convexity erupting at the tail

Every expression the system emits is classified (`scripts/lib/convexity.py`):

| Profile | Shape | Examples in this system |
|---|---|---|
| **convex** | bounded loss, non-linear upside | victim puts, wedge put-spreads, LEAPS calls, deep-OTM wings |
| **linear** | symmetric, bounded both ways | a production-basket stock long with a stop |
| **anti_convex** | capped gain, tail ruin — **FORBIDDEN** | selling options, shorting vol, leveraged range-trading |

**Hard rule — `assert_no_anticonvex` blocks the report:** the system NEVER
suggests an anti-convex expression. Selling premium / shorting volatility /
leveraged range-trading wins small and often, then loses everything once on
the tail (XIV 2018, Optionsellers 2020). This is structural suicide, not a
sizing question — it is forbidden as a rule, not discouraged as a preference.
No agent may relax it to "improve carry".

A linear stock long is allowed (the stop bounds it). The aspiration is
convex: limited downside, non-linear upside — long the wedge, puts on the
victim, calls on the wash-out. Convexity is not free: convex assets carry
worse (premium paid, lower yield). You pay the carry to own the tail.

---

## 7. How to Read the Report

Section order is the priority order. AI-infra first, background last.

1. **可交易名单** — the production basket. Each row cleared all four gates.
   Size is in R; entry/stop/target and hedge instrument are explicit.
2. **逐票复核** — per-name review: why it ranked, evidence state, tape.
3. **风控引擎 (Hedge/Wedge/Confirm/Press)** — current regime + R multiplier.
   This gates everything above it.
4. **恐惧贪婪 / 期权异常 / 期权多时段** — sentiment + options timing context.
5. **Bubble Hedge — Wedge/Victim/Confirmation** — the descriptive layers.
6. **AI 证据 / 供应链账本 / 10x 雷达 / 层归因** — the research depth.
7. **Source Review Calendar** — the evidence pipeline feeding the universe.
8. **Benchmark Snapshot / AI Book vs Benchmark** — broad-market *background*.

---

## 8. Program Computes. Agent Narrates.

| Program does | Agent does |
|---|---|
| Fetches data, runs all math | Reads only what is in the payload |
| Applies the four gates | Explains *why* a name passed or was held |
| Sizes R, computes the regime | Interprets what the regime means for the book |
| Flags evidence gaps | Reduces confidence language accordingly |

**Never:**
- Invent, estimate, or round a number not in the payload.
- Recommend buying a name still on `待原文核验`.
- Imply the risk regime is calmer than the gate says.
- Promote a name on tape strength alone.

---

## 9. System Prompt (for programmatic narration)

If feeding the report to an LLM for a second-opinion narrative, prepend:

```
You are the narrative layer for an AI-infrastructure specialist fund's daily
research report. The report is already computed and gated — you explain it,
you do not re-decide it.

Mandate: the AI-infra book is the absolute mainline. Broad market is background.

Hard rules:
- Use only numbers, tickers, dates, and states present in the report.
- A name is tradable only if it cleared all four gates (universe pool →
  evidence gate 原文已证明/合理推论 → Alpha Factory sleeve → risk regime).
  Never imply a 待原文核验 name is tradable.
- The risk regime (HEDGE/WEDGE/CONFIRM/PRESS) gates book size. Never narrate a
  posture looser than the stated regime.
- Options are evidence about timing and asymmetric risk, never the traded
  instrument. Far-OTM anomalies = positioning signal. Tenor = conviction
  horizon. Victim puts = defined-risk downside expression.
- The wedge is rates, not the bubble. Do not suggest shorting the parabola.
- This is research, not financial advice.

Output: (1) regime + book posture in 2 sentences; (2) the production basket —
why each name cleared the gates; (3) the wedge/victim/confirmation read;
(4) evidence gaps and what is still 待原文核验.
```

---

*Companion docs:*
- *`CLAUDE.md` — developer instructions and pipeline architecture*
- *`ai_infra/docs/` — BFS methodology, G0-G4 evidence gates, financials/options methodology*
- *`CLAUDE_HANDOFF.md` — current pipeline state and follow-ups*
