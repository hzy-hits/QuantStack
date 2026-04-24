# quant-research-v1 — Snapshot Storage Model

## Why this changed

The US pipeline originally used a single `data/quant.duckdb` file for:

- raw ingestion
- analytics
- report assembly
- weekly/backtest reads

That made iteration and session recovery fragile, especially after adding both `pre` and `post`
daily reports. A single DB plus single payload path meant the current run could overwrite the
other session's intermediate state.

## Database roles

| Role | Config field | Purpose |
|---|---|---|
| `raw` | `data.raw_db_path` | Canonical latest store for ingestion and longitudinal history. |
| `research` | `data.research_db_path` or `data.dev_db_path` | Session working DB used for analytics and targeted fetches. |
| `report` | `data.report_db_path` | Session-specific read-only snapshot for bundle/render/charts/agents. |
| `dev` | `data.dev_db_path` with `use_dev_for_research: true` | Local experiment target that avoids mutating the main research path. |

## Daily flow

For each `date + session`:

1. Ingest/update early data into `raw`
2. Copy `raw -> research_{date}_{session}.duckdb`
3. Run analytics, pass-1 filter, targeted Rust fetch, options, sentiment, HMM in `research`
4. Sync the completed `research` snapshot back into canonical `raw`
5. Promote `research -> report_{date}_{session}.duckdb`
6. Open `report` read-only for pass-2 filter, bundle, charts, and markdown render

This keeps the report path read-only while still preserving a canonical latest DB for weekly
payloads and backtests.

## Session-aware artifacts

- payload: `reports/{date}_payload_{session}.md`
- split payloads:
  - `reports/{date}_payload_macro_{session}.md`
  - `reports/{date}_payload_structural_{session}.md`
  - `reports/{date}_payload_news_{session}.md`
- charts: `reports/charts/{date}/{session}/`
- final report: `reports/{date}_report_zh_{session}.md`

This avoids pre/post overwriting the same files on the same trading date.

## Connection rules

- `connect_write()` is for mutating a DB and keeps the single-writer file lock
- `connect_readonly()` opens DuckDB without taking the extra exclusive file lock
- `connect()` remains as a backwards-compatible alias for `connect_write()`

## Recovery plan

If a session fails mid-run:

1. Inspect `data/quant_research_{date}_{session}.duckdb` if it exists
2. Re-run `scripts/run_daily.py --date YYYY-MM-DD --session pre|post`
3. If only the narrative layer failed, re-use:
   - `reports/{date}_payload_{session}.md`
   - `reports/{date}_payload_*_{session}.md`
4. If you need a clean re-analysis, delete the matching `research/report` session snapshots and rerun

The canonical `raw` DB remains the latest recoverable baseline.
