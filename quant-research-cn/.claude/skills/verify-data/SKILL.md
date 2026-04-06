---
name: verify-data
description: Verify DuckDB data quality after a fetch — check row counts, date ranges, NULL rates, and cross-table consistency.
---

# Verify Data Quality

After running `quant-cn fetch`, verify the data in DuckDB is complete and consistent.

## Queries to Run

```sql
-- 1. Row counts per table
SELECT 'prices' AS tbl, COUNT(*) AS rows FROM prices
UNION ALL SELECT 'daily_basic', COUNT(*) FROM daily_basic
UNION ALL SELECT 'forecast', COUNT(*) FROM forecast
UNION ALL SELECT 'margin_detail', COUNT(*) FROM margin_detail
UNION ALL SELECT 'block_trade', COUNT(*) FROM block_trade
UNION ALL SELECT 'top_list', COUNT(*) FROM top_list
UNION ALL SELECT 'share_unlock', COUNT(*) FROM share_unlock
UNION ALL SELECT 'index_weight', COUNT(*) FROM index_weight;

-- 2. Date freshness
SELECT MAX(trade_date) AS latest FROM prices;
SELECT MAX(trade_date) AS latest FROM daily_basic;

-- 3. Universe coverage
SELECT COUNT(DISTINCT ts_code) AS symbols FROM prices WHERE trade_date = (SELECT MAX(trade_date) FROM prices);

-- 4. NULL rate for critical columns
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) AS null_close,
    SUM(CASE WHEN vol IS NULL THEN 1 ELSE 0 END) AS null_vol
FROM prices WHERE trade_date = (SELECT MAX(trade_date) FROM prices);

-- 5. Cross-table join check
SELECT COUNT(*) AS matched
FROM prices p
JOIN daily_basic d ON p.ts_code = d.ts_code AND p.trade_date = d.trade_date
WHERE p.trade_date = (SELECT MAX(trade_date) FROM prices);
```

## Expected Results
- prices: ~300+ symbols per trading day
- daily_basic: should match prices row count
- forecast: varies (0 is OK if no announcements)
- NULL rate < 1% for close/vol
- Cross-table match rate > 95%

## Red Flags
- 0 rows in prices → Tushare token expired or rate limited
- Large NULL rate → API returned partial data, re-fetch
- date gap > 1 trading day → missed a run
