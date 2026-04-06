# Workflow: Implement an Analytics Module

> Step-by-step workflow for completing a STUB analytics module.

## Pre-requisites
- Read `spec.md` — identify which Axiom this module implements
- Read `CLAUDE.md` — check the Module → Axiom → Data Dependency Map
- Read `analytics/bayes.rs` — understand the shared Beta-Binomial engine

## Steps

### 1. Understand the Data
```sql
-- Check what data is available in DuckDB
SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM prices;
SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM daily_basic;
-- (adjust table name per module)
```

### 2. Read the Existing Stub
- Each module has a `compute(db, cfg, as_of) -> Result<usize>` function
- Helper functions (regime classification, z-score, EWMA) are already implemented
- The TODO comment marks where the main computation loop goes

### 3. Implement the Main Loop
Pattern for all analytics modules:
```rust
pub fn compute(db: &Connection, cfg: &Settings, as_of: NaiveDate) -> Result<usize> {
    // 1. Query raw data from DuckDB
    let mut stmt = db.prepare("SELECT ... FROM ... WHERE trade_date <= ?")?;

    // 2. Compute probabilities using Beta-Binomial or other method
    let posterior = prior.update(wins, losses);

    // 3. Write results to analytics table
    db.execute(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
        params![ts_code, as_of, MODULE_NAME, metric_name, posterior.mean, detail_json],
    )?;

    Ok(count)
}
```

### 4. Add Detail JSON
Every analytics row must include a `detail` JSON with:
```json
{
  "horizon": "5D",
  "conditioning_set": "trending, low_vol",
  "sample_size": 847,
  "ci_lower": 0.58,
  "ci_upper": 0.66,
  "prior": "Beta(2,2)"
}
```
This satisfies constraint C3 from spec.md.

### 5. Test
```bash
cargo test                           # unit tests pass
cargo run -- analyze --date $DATE    # integration: check analytics table
```

### 6. Verify Output
```sql
SELECT * FROM analytics WHERE module = 'MODULE_NAME' AND as_of = '2026-03-12';
```

## Checklist
- [ ] Traces back to exactly one Axiom (spec.md §3)
- [ ] Uses Beta-Binomial from `bayes.rs` (where applicable)
- [ ] Detail JSON has horizon + conditioning_set + sample_size
- [ ] No hardcoded magic numbers — use `cfg.signals.*`
- [ ] `cargo test` passes
- [ ] `cargo clippy` clean
