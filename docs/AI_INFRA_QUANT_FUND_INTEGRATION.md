# AI Infra Quant Fund Integration

This is the implementation bridge between the `ai_infra/` research OS and the
quant stack. The goal is to stop broad all-market hunting and run the book as an
AI-infra specialist fund: research universe first, quant timing second,
portfolio hedge third.

## Architecture

1. Research universe:
   `ai_infra/data/global_universe_v2.jsonl` is the upstream universe. BFS depth,
   module, dependency path, source-review state, counterevidence and current
   pool are screening metadata. They are not a direct trade signal.

2. Market ranking:
   US and CN rankers filter and expand candidates through the AI-infra universe.
   A missing price/news/options row should not delete a company from research
   view; it should rank as watch until price, flow, options or evidence improves.

3. Evidence ingestion:
   The pipeline needs separate evidence ledgers for filings, transcripts, news,
   financial statements, backlog, capex, gross margin, free cash flow, debt,
   lease obligations, CDS or credit spreads, options IV/skew/VRP/flow and
   option-leg PnL. A price-only factor may discover motion, but it cannot prove a
   supplier relationship or credit stress.

4. Alpha sleeves:
   - US: `us_theme_cluster_momentum` should rank theme baskets across all AI
     infra symbols, not only the P0 queue.
   - CN: `cn_tape_leadership_continuation` should be the right-side layer for
     AI infra A-shares; oversold sleeves are secondary and only valid inside the
     mandate.
   - Factor Lab: mine within the AI-infra universe first, and use
     `DATA_REQUIREMENTS` for missing non-price datasets rather than inventing
     price proxies.

5. Portfolio ledger:
   Promotion must be measured as long alpha return, beta hedge return, net
   residual return, drawdown, sleeve correlation and layer exposure. A name is
   not money-ready just because the story is right.

## Factor Lab Continuous Mining

> ⚠️ DECOMMISSIONED 2026-06-24 — factor-lab 已退役;以下为历史记录,不反映现状。详见 docs/DECISIONS.md。
> "Continuous mining" 已停止:factor.*/paper.* cron 已删,daily_factors.sh 加 DISABLED 守卫跳过。

Factor Lab now has a dedicated bridge at
`factor-lab/src/autoresearch/ai_infra_context.py`. Autoresearch sessions load
`ai_infra/data/global_universe_v2.jsonl`, prepend a compact BFS/universe summary
to the agent context, and default `FACTOR_LAB_AI_INFRA_ONLY=1` in
`factor-lab/scripts/autoresearch.sh`. The scheduled daily factor lifecycle also
defaults the same mandate in `factor-lab/scripts/daily_factors.sh`, and the
Factor Lab price loaders, daily mining, pipeline export and sleeve-return export
apply the AI-infra universe before any broad-cap fallback filter.

This changes the job of Factor Lab:

- It should continuously search the full local AI-infra universe, not only the
  first 30-row US alpha queue.
- The US queue is a review priority list. It is not the universe boundary.
  Names outside the queue still remain research candidates when they are in
  `global_universe_v2.jsonl` or can be justified as source-review expansion
  candidates.
- Price-only DSL formulas are allowed only for timing discovery: leadership,
  accumulation, relative strength, lifecycle pullback, volatility compression or
  risk reversal. Any supplier/customer, financial, credit, option or power-grid
  claim must be carried in `DATA_REQUIREMENTS` until the relevant dataset exists.
- Passing Factor Lab metrics produces an alpha-sleeve candidate. It does not by
  itself source-confirm a relationship and does not create execution R.

## Data Work To Add

- Financials: revenue by segment, backlog, orders, capex, gross margin, FCF,
  inventory and customer concentration.
- Credit: net debt, lease liabilities, interest expense, maturity wall, CDS or
  credit-spread proxies.
- Source events: official filings, press releases, transcripts, source-linked
  news and reviewed relationship evidence.
- Options: chain bid/ask snapshots, IV rank, skew, VRP, unusual flow, liquidity,
  and bid/ask leg PnL ledger for shadow options.
- Tape: price leadership, volume expansion, relative strength, sector breadth,
  money flow and correlation to AI-infra baskets.

## Promotion Contract

Every promoted sleeve must answer:

- Universe: which `ai_infra` layer and dependency path does this belong to?
- Signal: what price/flow/options/news/financial data moved first?
- Evidence: which claim is source-confirmed, and which remains research-only?
- Payoff: what historical long-only and beta-hedged return does this sleeve
  show after costs?
- Risk: which beta, layer, factor, liquidity, credit and event risks explain the
  position?

If those fields are missing, the row can be ranked watch or source-review queue,
but it should not get execution R.
