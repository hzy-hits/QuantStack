# Report Delivery Contract

This contract keeps research artifacts separate from user-facing daily reports.

## Pipeline Shape

1. Compute layer writes deterministic payloads and ledgers.
   - `main_strategy_v2_backtest.json`
   - `main_strategy_v2_backtest.md`
   - ranker JSON/MD, hedge ledger, option ledger, audit files
2. Final report layer renders market-specific reports.
   - `cn_daily_report.md`
   - `us_daily_report.md`
3. Delivery layer may send only the market-specific final reports.
   - CN mail source must be `cn_daily_report.md`.
   - US mail source must be `us_daily_report.md`.
   - `main_strategy_v2_backtest.md` is an internal kitchen ticket and must never be sent in production.

## Agent/Narrative Role

Agents may rewrite tone, structure, and explanation, but they do not create numbers, tickers, entries, exits, or R sizes.

The final report should read like an analyst wrote it:

- Start from the market tape and theme context.
- For every actionable name, explain why it made the cut from three angles: quant data, news/event risk, and historical evidence.
- Keep watch-only names separate from trades.
- Make uncertainty explicit.
- Do not paste prompt instructions into the final report.

## News And Web Review

US reports may use news, filings, options, and price together. If live web/news review is unavailable, the report must say only what is in the ingested payload.

CN reports treat news as a lagging risk label. The first signal is price, volume, flow, and sector linkage. Consumer names are excluded from the current A-share narrative book unless a future promoted sleeve explicitly changes that rule.

## Delivery Guards

`scripts/send_production_decision_report.py` enforces:

- `--delivery-mode prod --market all` is refused.
- `--market us` can only send a report headed `# 美股量化日报`.
- `--market cn` can only send a report headed `# A股量化日报`.
- Cross-market markers in a final report cause fail-fast before Gmail.
