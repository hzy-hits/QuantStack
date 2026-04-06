---
name: debug-pipeline
description: Diagnose pipeline failures — trace errors from logs back to specific modules, API issues, or data problems.
---

# Debug Pipeline Failure

When the pipeline fails or produces unexpected output, follow this diagnostic flow.

## Step 1: Identify the Phase

```
Phase 1 (Fetch)       → API errors, rate limits, network timeouts
Phase 1.5 (Enrich)    → DeepSeek API errors, JSON parse failures
Phase 2 (Analytics)   → Missing data, computation errors, DuckDB write failures
Phase 3 (Filter)      → Empty analytics table, scoring bugs
Phase 4 (Render)      → File I/O errors, template bugs
```

## Step 2: Check Logs

```bash
# Run with debug logging
RUST_LOG=quant_cn=debug ./target/release/quant-cn run --date $DATE 2>&1 | tee logs/debug.log

# Find errors
grep -i "error\|fail\|panic" logs/debug.log
```

## Step 3: Common Issues

### Tushare API Errors
```
code=40203 → token expired or insufficient credits
code=50101 → parameter validation failed (check date format: YYYYMMDD)
code=-2001 → rate limited (increase delay beyond 500ms)
```
Fix: Check `config.yaml` token, verify with `curl https://api.tushare.pro -d '{"api_name":"trade_cal","token":"YOUR_TOKEN","params":{}}'`

### DeepSeek Errors
```
status=401 → invalid API key
status=429 → rate limited (reduce enrichment.concurrency in config)
status=500 → DeepSeek service issue (transient, retry later)
"failed to parse" → LLM returned invalid JSON (logged and skipped, not fatal)
```

### DuckDB Errors
```
"Conversion Error: invalid date" → date format mismatch (need YYYY-MM-DD)
"PRIMARY KEY constraint" → duplicate row (use INSERT OR REPLACE)
"table not found" → run `quant-cn init` first
```

### Analytics Empty Output
```sql
-- Check if upstream data exists
SELECT COUNT(*) FROM prices WHERE trade_date = '$DATE';
-- If 0: re-run fetch
-- If >0: check the specific analytics module for bugs
```

## Step 4: Re-run Specific Phase
```bash
./target/release/quant-cn fetch --date $DATE     # just re-fetch
./target/release/quant-cn enrich --date $DATE    # just re-enrich
./target/release/quant-cn analyze --date $DATE   # just re-analyze
```
Pipeline is idempotent — safe to re-run any phase.
