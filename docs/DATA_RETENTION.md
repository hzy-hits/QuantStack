# Data Retention Policy

## Per-session DuckDB snapshots (`quant-research-v1/data/`)
- Files: `quant_{research,report}_YYYY-MM-DD_{pre,post}.duckdb` — disposable debug/replay copies.
- Policy: keep the most recent **7 days**; older are deleted daily by `data.prune_snapshots`
  (`07:40 Mon-Fri`, runs `ops/prune_snapshots.py --apply --keep-days 7`).
- Canonical DBs (`quant.duckdb`, `quant_report.duckdb`, `quant_cn*.duckdb`,
  `data/strategy_backtest_history.duckdb`, `factor-lab/data/*`) are never pruned.

## Canonical history (future, hot/cold — see portability spec)
- Hot: recent 6-12 months stay in the live DuckDB on the compute host.
- Cold: older months export to partitioned Parquet on the NAS cold lake (lands with
  the Oracle/NAS/Pi phase). Not yet active.

## Component dashboards (`reports/review_dashboard/**/<date>/*.duckdb`)
- Out of scope for the snapshot pruner (may be read for backtest/PIT). Revisit separately.
