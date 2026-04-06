# Data Sources — API Reference

## Tushare Pro API

All endpoints use the same HTTP POST interface:

```
POST https://api.tushare.pro
Content-Type: application/json

{
  "api_name": "<endpoint>",
  "token": "<your_token>",
  "params": { ... },
  "fields": "field1,field2,..."
}

Response: { "code": 0, "data": { "fields": [...], "items": [[...], ...] } }
```

Rate limit: ~200 req/min at 2000-credit tier. Use 500ms delay between calls.

### Endpoints Used (2000-credit tier)

| Endpoint | Credits | Description | Key Fields |
|----------|---------|-------------|------------|
| `daily` | 120 | Daily OHLCV | ts_code, trade_date, open, high, low, close, vol, amount |
| `adj_factor` | 120 | 复权因子 | ts_code, trade_date, adj_factor |
| `daily_basic` | 2000 | PE/PB/换手率/市值 | ts_code, trade_date, pe_ttm, pb, turnover_rate, total_mv, circ_mv |
| `income` | 2000 | 利润表 | ts_code, ann_date, end_date, revenue, n_income, basic_eps |
| `balancesheet` | 2000 | 资产负债表 | ts_code, ann_date, end_date, total_assets, total_liab |
| `cashflow` | 2000 | 现金流量表 | ts_code, ann_date, end_date, n_cashflow_act |
| `forecast` | 1000 | 业绩预告 | ts_code, ann_date, end_date, type, p_change_min/max, net_profit_min/max |
| `express` | 1000 | 业绩快报 | ts_code, ann_date, end_date, revenue, n_income |
| `margin_detail` | 2000 | 融资融券明细 | ts_code, trade_date, rzye, rzmre, rqye |
| `block_trade` | 1000 | 大宗交易 | ts_code, trade_date, price, vol, amount, buyer, seller |
| `top_inst` | 1000 | 龙虎榜机构 | ts_code, trade_date, buy_amount, sell_amount |
| `top_list` | 1000 | 龙虎榜明细 | ts_code, trade_date, buy_amount, sell_amount, broker_name |
| `index_weight` | 500 | 指数成分+权重 | index_code, con_code, trade_date, weight |
| `index_daily` | 120 | 指数行情 | ts_code, trade_date, close, pct_chg |
| `share_float` | 1000 | 限售解禁 | ts_code, float_date, float_share, float_ratio |
| `stk_holdernumber` | 2000 | 股东户数 | ts_code, ann_date, holder_num |

### Tushare Rust Client Pattern

```rust
#[derive(Serialize)]
struct TushareRequest {
    api_name: String,
    token: String,
    params: serde_json::Value,
    fields: String,
}

#[derive(Deserialize)]
struct TushareResponse {
    code: i64,
    data: Option<TushareData>,
    msg: Option<String>,
}

#[derive(Deserialize)]
struct TushareData {
    fields: Vec<String>,
    items: Vec<Vec<serde_json::Value>>,
}
```

## AKShare HTTP Bridge

AKShare is Python-only. We run a lightweight FastAPI server as a sidecar:

```python
# akshare_bridge.py (~50 lines)
from fastapi import FastAPI
import akshare as ak

app = FastAPI()

@app.get("/northbound")
def northbound(start_date: str, end_date: str):
    df = ak.stock_hsgt_north_net_flow_in_em()
    return df.to_dict(orient="records")

@app.get("/unlock/{ts_code}")
def unlock(ts_code: str):
    df = ak.stock_restricted_release_queue_sina(symbol=ts_code)
    return df.to_dict(orient="records")

@app.get("/news/{ts_code}")
def news(ts_code: str):
    df = ak.stock_news_em(symbol=ts_code)
    return df.to_dict(orient="records")

@app.get("/macro/{indicator}")
def macro(indicator: str, start_date: str = ""):
    # dispatch to appropriate ak.macro_* function
    ...
```

Rust calls `http://localhost:8321/northbound?start_date=20260101` via reqwest.

## A-Share Code Format

- Shanghai: `600xxx.SH`, `601xxx.SH`, `603xxx.SH`, `688xxx.SH` (科创板)
- Shenzhen: `000xxx.SZ`, `001xxx.SZ`, `002xxx.SZ`, `300xxx.SZ` (创业板)
- Index: `000001.SH` (上证指数), `000300.SH` (沪深300), `399001.SZ` (深证成指)
- ETF: `510050.SH` (50ETF), `510300.SH` (300ETF)

## Rate Limits

| Source | Limit | Strategy |
|--------|-------|----------|
| Tushare Pro (2000 credits) | ~200 req/min | 500ms delay |
| AKShare (via bridge) | Varies by source | 1s delay (东财), 0.5s (新浪) |
| No API key needed | AKShare is web scraping | Respect source rate limits |
