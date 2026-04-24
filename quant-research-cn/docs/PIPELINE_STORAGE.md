# quant-research-cn — Pipeline Storage and Snapshot Model

## Why this exists

The pipeline used to read, analyze, and render from a single DuckDB file. That worked for
nightly sequential cron runs, but it became a bottleneck for local iteration:

- `analyze` and `render` contended on the same DuckDB file
- partial experiments could overwrite the same working state used by reports
- `shadow_full` was being materialized inside `build_notable_items()`, so `render` was no longer
  a pure reporting step

The current model separates database roles and pushes shortlist pricing into the analysis phase.

## Database roles

| Role | Path config | Purpose |
|---|---|---|
| `raw` | `data.raw_db_path` | Fetch target. Holds ingested market, macro, and reference tables. |
| `research` | `data.research_db_path` or `data.dev_db_path` | Analysis working copy. `enrich` and `analytics` run here. |
| `report` | `data.report_db_path` | Read-only snapshot for `render`, agent payloads, and review. |
| `dev` | `data.dev_db_path` with `use_dev_for_research: true` | Local experiment target that avoids touching the main research snapshot. |

## Command semantics

### `quant-cn fetch`

- Opens `raw`
- Pulls upstream data only
- Does not modify `research` or `report`

### `quant-cn analyze`

Full run:

1. Copy `raw -> research`
2. Run analytics in `research`
3. Materialize shortlist `shadow_full` in `research`
4. Promote `research -> report`

Incremental run with `--module`:

1. Reuse existing `research` if present
2. Else seed `research` from `report`
3. Else fall back to `raw -> research`
4. Run the requested analytics module in `research`
5. Recompute shortlist `shadow_full` in `research`
6. Promote `research -> report`

This keeps partial module runs compatible with report generation without forcing a full pipeline
refresh every time.

### `quant-cn run`

1. Fetch into `raw` unless `--skip-fetch`
2. Copy `raw -> research`
3. Enrich and analyze in `research`
4. Materialize shortlist `shadow_full` in `research`
5. Promote `research -> report`
6. Build notable items and render from `report`

### `quant-cn render`

- Requires an existing `report` snapshot, or promotes an existing `research` snapshot to `report`
- Builds notable items from `report`
- Renders markdown from `report`
- Does not materialize `shadow_full`

`render` is now intended to be report-only. If shortlist shadow pricing is missing, the fix is to
run `analyze` or `run`, not to let rendering mutate analytics tables.

## Shadow option lifecycle

There are two shadow option layers:

### `shadow_fast`

- Cross-sectional
- Runs for the full stock universe during `analytics`
- Writes `shadow_iv_30d`, `shadow_iv_60d`, `shadow_iv_90d`, `downside_stress`

### `shadow_full`

- Shortlist-only
- Uses the notable candidate shortlist, watchlist, and event-driven extra names
- Writes option-like downside metrics such as:
  - `shadow_put_90_3m`
  - `shadow_touch_90_3m`
  - `shadow_floor_1sigma_3m`
  - `shadow_skew_90_3m`

`shadow_full` is materialized during `analyze`/`run`, not during `render`.

## Development workflow

Use `config.dev.yaml` for local experiments:

```bash
cargo run -- --config config.dev.yaml analyze --date 2026-04-14 --module shadow_option
cargo run -- --config config.dev.yaml render --date 2026-04-14
```

Expected behavior:

- market data still reads from the main `raw` database
- analytics writes to `quant_cn_dev.duckdb`
- report rendering reads from `quant_cn_dev_report.duckdb`
- `recipients: []` prevents accidental email sends

## Design rule

`filtering` may rank and select, but it should not backfill analytics during rendering.

If a report needs a metric that is stored in `analytics`, that metric must be materialized earlier
in `analyze` or `run` and then promoted into the report snapshot.
