# quant-research-cn — Design Document

## Philosophy

**The program computes. The agent narrates. No trading signals.**

Same core principle as the US pipeline. Every output is `P(event | conditions)` with explicit
horizon, conditioning set, and sample size. The agent (Claude) reads the computed payload and
writes a narrative report in Chinese. It never touches arithmetic.

## Architecture

```
Cron (2x/day) → Data Ingestion → Analytics → Agent Analysis → Email

                    ┌─────────────────────────────────────┐
                    │           quant-cn (Rust)            │
                    │                                      │
  Tushare Pro ─────►│  fetcher/tushare.rs                  │
  AKShare HTTP ────►│  fetcher/akshare.rs                  │
                    │         │                            │
                    │         ▼                            │
                    │  storage/  ──► DuckDB                │
                    │         │                            │
                    │         ▼                            │
                    │  analytics/                          │
                    │    ├── momentum.rs    (CPT + Bayes)  │
                    │    ├── announcement.rs (业绩预告)     │
                    │    ├── flow.rs        (北向+融资)     │
                    │    ├── hmm.rs         (regime)       │
                    │    ├── unlock.rs      (限售解禁)      │
                    │    ├── bayes.rs       (Beta-Binom)   │
                    │    └── macro_gate.rs  (宏观门控)      │
                    │         │                            │
                    │         ▼                            │
                    │  filtering/notable.rs                │
                    │         │                            │
                    │         ▼                            │
                    │  reporting/render.rs → payload.md    │
                    └─────────────────────────────────────┘
                                  │
                                  ▼
                    claude -p < payload.md → report_zh.md
                                  │
                                  ▼
                            Email (Gmail API)
```

## Key Difference from US Pipeline

The US pipeline is Python + Rust (fetcher only). This pipeline is **pure Rust** — all analytics
computed in Rust, no Python dependency for the core pipeline.

## Data Source Strategy

### Tushare Pro (骨架 — 稳定、结构化)
| Endpoint | Data | Module |
|----------|------|--------|
| `daily` + `adj_factor` | OHLCV + 复权因子 | prices |
| `daily_basic` | PE/PB/换手率/市值 | value_score |
| `income` / `balancesheet` / `cashflow` | 三大报表 | value_score |
| `forecast` | 业绩预告 (预增/预减/扭亏...) | announcement_risk |
| `margin_detail` | 融资融券明细 (个股) | flow_score |
| `block_trade` | 大宗交易 | flow_score |
| `index_weight` | 指数成分+权重 | universe |
| `top_inst` / `top_list` | 龙虎榜 | hot_money |

### AKShare (补充 — 免费、爬虫类)
| Function | Data | Module |
|----------|------|--------|
| `stock_hsgt_north_net_flow_in_em` | 北向资金日度净流入 | flow_score |
| `stock_restricted_release_queue_sina` | 限售解禁日历 | unlock_risk |
| `stock_news_em` | 东财新闻 | news |
| Various macro functions | CPI/PMI/社融/LPR | macro_gate |

### AKShare Integration
AKShare is Python-only. Integration options:
1. **HTTP bridge** (recommended): lightweight FastAPI server wrapping AKShare, Rust calls via reqwest
2. **Direct scraping**: replicate AKShare's HTTP calls in Rust (fragile, needs maintenance)
3. **PyO3 FFI**: embed Python in Rust binary (complex, not worth it)

Recommendation: option 1. A 50-line FastAPI bridge that exposes 5 AKShare endpoints as JSON.

## Pipeline Phases

| Phase | What | Est. Time |
|-------|------|-----------|
| **Universe** | CSI 300/500 constituents + ETFs + watchlist | <5 sec |
| **Prices** | Tushare daily + adj_factor for universe | ~2 min |
| **Fundamentals** | daily_basic + financial statements | ~3 min |
| **Flow data** | 北向 + 融资融券 + 大宗 + 龙虎榜 | ~2 min |
| **Announcements** | 业绩预告 + 限售解禁 | ~1 min |
| **Macro** | CPI/PMI/社融/LPR/Shibor | <30 sec |
| **Analytics** | All probability modules | ~30 sec |
| **Filter** | 2-pass: universe → 120 → 30 | <5 sec |
| **Render** | Markdown payload | <1 sec |
| **Agent** | Claude analysis → Chinese report | ~10 min |
| **Email** | HTML delivery | ~15 sec |
| **Total** | | ~20 min |

## DuckDB Schema

```sql
-- Prices (daily OHLCV with adjustment)
CREATE TABLE prices (
    ts_code     VARCHAR NOT NULL,
    trade_date  DATE NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    pre_close   DOUBLE,
    change      DOUBLE,
    pct_chg     DOUBLE,       -- percent change
    vol         DOUBLE,       -- volume (手)
    amount      DOUBLE,       -- turnover (千元)
    adj_factor  DOUBLE,       -- 复权因子
    PRIMARY KEY (ts_code, trade_date)
);

-- Daily valuation indicators
CREATE TABLE daily_basic (
    ts_code       VARCHAR NOT NULL,
    trade_date    DATE NOT NULL,
    turnover_rate DOUBLE,
    volume_ratio  DOUBLE,
    pe            DOUBLE,
    pe_ttm        DOUBLE,
    pb            DOUBLE,
    ps_ttm        DOUBLE,
    total_mv      DOUBLE,     -- 总市值 (万元)
    circ_mv       DOUBLE,     -- 流通市值 (万元)
    PRIMARY KEY (ts_code, trade_date)
);

-- 业绩预告 (earnings forecast / pre-announcement)
CREATE TABLE forecast (
    ts_code        VARCHAR NOT NULL,
    ann_date       DATE NOT NULL,      -- 公告日期
    end_date       DATE NOT NULL,      -- 报告期
    forecast_type  VARCHAR NOT NULL,   -- 预增/预减/扭亏/首亏/续盈/续亏/略增/略减
    p_change_min   DOUBLE,             -- 预计净利润变动幅度下限 (%)
    p_change_max   DOUBLE,             -- 预计净利润变动幅度上限 (%)
    net_profit_min DOUBLE,             -- 预计净利润下限 (万元)
    net_profit_max DOUBLE,
    summary        VARCHAR,
    PRIMARY KEY (ts_code, ann_date, end_date)
);

-- 融资融券明细 (margin trading detail)
CREATE TABLE margin_detail (
    ts_code    VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    rzye       DOUBLE,       -- 融资余额 (元)
    rzmre      DOUBLE,       -- 融资买入额
    rzche      DOUBLE,       -- 融资偿还额
    rqye       DOUBLE,       -- 融券余额
    rqmcl      DOUBLE,       -- 融券卖出量
    rqchl      DOUBLE,       -- 融券偿还量
    PRIMARY KEY (ts_code, trade_date)
);

-- 北向资金 (northbound capital via Stock Connect)
CREATE TABLE northbound_flow (
    trade_date  DATE NOT NULL,
    buy_amount  DOUBLE,       -- 买入成交额 (亿元)
    sell_amount DOUBLE,       -- 卖出成交额
    net_amount  DOUBLE,       -- 净买入额
    source      VARCHAR,      -- 'sh_connect' | 'sz_connect' | 'total'
    PRIMARY KEY (trade_date, source)
);

-- 大宗交易 (block trades)
CREATE TABLE block_trade (
    ts_code    VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    price      DOUBLE,
    vol        DOUBLE,       -- 成交量 (万股)
    amount     DOUBLE,       -- 成交金额 (万元)
    buyer      VARCHAR,
    seller     VARCHAR,
    premium    DOUBLE,       -- 溢价率 (%) = (price - close) / close × 100
    PRIMARY KEY (ts_code, trade_date, buyer, seller)
);

-- 龙虎榜 (top trader list)
CREATE TABLE top_list (
    ts_code      VARCHAR NOT NULL,
    trade_date   DATE NOT NULL,
    reason       VARCHAR,     -- 上榜原因
    buy_amount   DOUBLE,
    sell_amount  DOUBLE,
    net_amount   DOUBLE,
    broker_name  VARCHAR,     -- 营业部名称
    PRIMARY KEY (ts_code, trade_date, broker_name)
);

-- 限售解禁 (restricted share unlock)
CREATE TABLE share_unlock (
    ts_code      VARCHAR NOT NULL,
    ann_date     DATE,
    float_date   DATE NOT NULL,   -- 解禁日期
    float_share  DOUBLE,          -- 解禁数量 (万股)
    float_ratio  DOUBLE,          -- 解禁比例 (%)
    holder_name  VARCHAR,
    share_type   VARCHAR,         -- 定增/首发/股权激励
    PRIMARY KEY (ts_code, float_date, holder_name)
);

-- Macro indicators
CREATE TABLE macro_cn (
    date        DATE NOT NULL,
    series_id   VARCHAR NOT NULL,
    series_name VARCHAR,
    value       DOUBLE,
    PRIMARY KEY (date, series_id)
);

-- Analytics output (probability computations)
CREATE TABLE analytics (
    ts_code     VARCHAR NOT NULL,
    as_of       DATE NOT NULL,
    module      VARCHAR NOT NULL,   -- momentum / announcement / flow / hmm / unlock
    metric      VARCHAR NOT NULL,   -- trend_prob / p_upside / flow_score / ...
    value       DOUBLE,
    detail      VARCHAR,            -- JSON with full context
    PRIMARY KEY (ts_code, as_of, module, metric)
);

-- HMM forecasts (calibration lifecycle)
CREATE TABLE hmm_forecasts (
    forecast_id VARCHAR NOT NULL PRIMARY KEY,
    as_of       DATE NOT NULL,
    horizon     VARCHAR NOT NULL,   -- '1d'
    p_predicted DOUBLE NOT NULL,
    actual      INTEGER,            -- 1 = positive return, 0 = negative, NULL = pending
    resolved    BOOLEAN DEFAULT FALSE
);

-- Run log
CREATE TABLE run_log (
    run_id    VARCHAR NOT NULL,
    step      VARCHAR NOT NULL,
    status    VARCHAR NOT NULL,
    rows      INTEGER DEFAULT 0,
    detail    VARCHAR,
    ts        TIMESTAMP DEFAULT current_timestamp
);

-- Index constituents
CREATE TABLE index_weight (
    index_code  VARCHAR NOT NULL,
    con_code    VARCHAR NOT NULL,   -- constituent ts_code
    trade_date  DATE NOT NULL,
    weight      DOUBLE,
    PRIMARY KEY (index_code, con_code, trade_date)
);
```

## Cron Schedule (UTC+8)

| Session | Time | Days | Coverage |
|---------|------|------|----------|
| 盘后 | 17:00 | Mon-Fri | 当日收盘数据 |
| 盘前 | 08:30 | Mon-Fri | 隔夜公告 + 北向预测 |

A-share market hours: 09:30-11:30, 13:00-15:00 CST.
Data available on Tushare: ~16:30 for daily, ~18:00 for margin/block.
