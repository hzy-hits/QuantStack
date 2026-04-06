# Workflow: Daily Pipeline Run

> Full end-to-end daily pipeline execution and monitoring.

## Trigger
- Cron (盘后 17:00, 盘前 08:30 UTC+8)
- Manual: `./target/release/quant-cn run --date 2026-03-12`

## Steps

### 1. Pre-flight Check
```bash
# Verify config exists and has API keys
test -f config.yaml && grep -q "tushare_token" config.yaml
# Verify DuckDB is accessible
test -f data/quant_cn.duckdb || ./target/release/quant-cn init
```

### 2. Data Fetch (Phase 1)
```bash
./target/release/quant-cn fetch --date $DATE
```
- Tushare: 9 endpoints, ~21k rows, ~96 seconds
- AKShare bridge: optional (gracefully skips if not running)
- Expected: 0 errors in tracing output

### 3. Enrichment (Phase 1.5)
```bash
./target/release/quant-cn enrich --date $DATE
```
- DeepSeek: async concurrent extraction
- Processes un-enriched news/forecasts from last 7 days
- Expected: enriched count > 0 if new announcements exist

### 4. Analytics (Phase 2)
```bash
./target/release/quant-cn analyze --date $DATE
```
- Runs: momentum → announcement → flow → unlock → hmm → macro_gate
- Writes to `analytics` table
- Expected: row count > 0 per module

### 5. Render (Phase 4)
```bash
./target/release/quant-cn render --date $DATE
```
- Generates `reports/{date}_payload.md`
- Contains: market context, notable items, upcoming events

### 6. Agent Narration (Phase 5)
```bash
# Claude reads payload and writes Chinese narrative
claude -p "你是A股量化研究分析师。阅读以下payload并撰写中文研究简报。" < reports/${DATE}_payload.md > reports/${DATE}_report.md
```

### 7. Validation
- [ ] `reports/{date}_payload.md` exists and > 5KB
- [ ] No `ERROR` level logs in output
- [ ] `analytics` table has rows for today's date
- [ ] HMM forecast inserted (check `hmm_forecasts` table)

## Failure Recovery
- If fetch fails: check Tushare token validity, rate limits
- If enrichment fails: check DeepSeek key, may be out of credits
- If analytics fails: likely missing data — re-run fetch first
- Pipeline is idempotent — safe to re-run any phase
