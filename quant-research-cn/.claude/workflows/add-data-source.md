# Workflow: Add a New Data Source

> How to add a new Tushare endpoint or AKShare bridge route.

## For Tushare Endpoints

### 1. Check API Docs
- See `docs/DATA_SOURCES.md` for the Tushare request/response format
- Verify credit requirements (some endpoints need 2000+ credits)

### 2. Add Schema
In `storage/schema.rs`, add `CREATE TABLE IF NOT EXISTS`:
```rust
CREATE TABLE new_table (
    ts_code    VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    -- fields matching Tushare response
    PRIMARY KEY (ts_code, trade_date)
);
```

### 3. Add Fetcher
In `fetcher/tushare.rs`, follow existing pattern:
```rust
async fn fetch_new_endpoint(db: &Connection, token: &str, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.format("%Y%m%d").to_string();
    let resp = query(token, "api_name", json!({"trade_date": date_str}), "field1,field2").await?;
    // Parse + INSERT, using ts_date() for date conversion
}
```

### 4. Wire Up
- Add call in `fetch_all()` in `fetcher/tushare.rs`
- Add table creation in `storage/schema.rs`

### 5. Test
```bash
cargo run -- fetch --date 2026-03-12
# Check: SELECT COUNT(*) FROM new_table;
```

## For AKShare Bridge Routes

### 1. Add Python Endpoint
In `akshare_bridge.py`:
```python
@app.get("/new_endpoint")
def new_endpoint(param: str):
    df = ak.some_function(param)
    return df.to_dict(orient="records")
```

### 2. Add Rust Client
In `fetcher/akshare.rs`, add async function following existing `fetch_northbound` pattern.

### 3. Non-fatal Failure
AKShare bridge is optional. Always return `Ok(0)` if bridge is not running.

## Checklist
- [ ] Table added to `schema.rs`
- [ ] Date format: `ts_date()` conversion for Tushare dates
- [ ] Rate limit respected (500ms for Tushare, 1s for AKShare)
- [ ] `INSERT OR REPLACE` for idempotency
- [ ] Wired into `fetch_all()`
