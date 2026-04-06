#!/usr/bin/env python3
"""AKShare HTTP bridge for quant-cn Rust pipeline.

Run:
    pip install -r requirements.txt
    python akshare_bridge.py          # default port 8321
    # or: uvicorn akshare_bridge:app --host 0.0.0.0 --port 8321

Provides JSON endpoints wrapping AKShare functions that have no
Tushare equivalent at the 2000-credit tier:
  - /concept_boards   概念板块行情
  - /sector_fund_flow 行业资金流向
  - /stock_news       个股新闻

Design: each endpoint normalises column names to English and returns
a flat JSON array. The Rust client deserialises into typed structs.
If AKShare changes column names, only this bridge needs updating.
"""
import math
import traceback
from datetime import datetime

import akshare as ak
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="AKShare Bridge", version="0.2.0")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _float(v) -> float | None:
    """Safely convert to float, returning None for NaN / non-numeric."""
    try:
        if v is None:
            return None
        f = float(v)
        return None if math.isnan(f) else f
    except (ValueError, TypeError):
        return None


def _int(v) -> int | None:
    try:
        if v is None:
            return None
        f = float(v)
        if math.isnan(f):
            return None
        return int(f)
    except (ValueError, TypeError):
        return None


def _str(v) -> str:
    if v is None:
        return ""
    s = str(v)
    return "" if s == "nan" else s


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "ts": datetime.now().isoformat()}


@app.get("/concept_boards")
def concept_boards():
    """概念板块行情 — board-level metrics from 东方财富.

    Returns list of ~400 concept boards with today's metrics.
    One API call, no per-stock iteration needed.
    """
    try:
        df = ak.stock_board_concept_name_em()
        records = []
        for _, row in df.iterrows():
            records.append({
                "board_name": _str(row.get("板块名称")),
                "board_code": _str(row.get("板块代码")),
                "pct_chg": _float(row.get("涨跌幅")),
                "turnover_rate": _float(row.get("换手率")),
                "total_mv": _float(row.get("总市值")),
                "amount": _float(row.get("成交额")),
                "up_count": _int(row.get("上涨家数")),
                "down_count": _int(row.get("下跌家数")),
                "lead_stock": _str(row.get("领涨股票")),
                "lead_pct": _float(row.get("领涨股票-涨跌幅")),
            })
        return records
    except Exception as e:
        return JSONResponse(
            {"error": str(e), "trace": traceback.format_exc()},
            status_code=500,
        )


@app.get("/sector_fund_flow")
def sector_fund_flow(indicator: str = Query("今日")):
    """行业资金流向排名 from 东方财富.

    indicator: "今日" | "5日" | "10日"
    """
    try:
        df = ak.stock_sector_fund_flow_rank(indicator=indicator)
        cols = list(df.columns)
        records = []
        for _, row in df.iterrows():
            # Columns are positional: 序号, 名称, 涨跌幅, 主力净流入-净额, 主力净流入-净占比,
            # 超大单净流入-净额, 超大单净流入-净占比, 大单净流入-净额, 大单净流入-净占比,
            # 中单净流入-净额, 中单净流入-净占比, 小单净流入-净额, 小单净流入-净占比
            r = list(row)
            records.append({
                "sector_name": _str(r[1]) if len(r) > 1 else "",
                "pct_chg": _float(r[2]) if len(r) > 2 else None,
                "main_net_in": _float(r[3]) if len(r) > 3 else None,
                "main_net_pct": _float(r[4]) if len(r) > 4 else None,
                "super_net_in": _float(r[5]) if len(r) > 5 else None,
                "big_net_in": _float(r[7]) if len(r) > 7 else None,
                "mid_net_in": _float(r[9]) if len(r) > 9 else None,
                "small_net_in": _float(r[11]) if len(r) > 11 else None,
            })
        return records
    except Exception as e:
        return JSONResponse(
            {"error": str(e), "trace": traceback.format_exc()},
            status_code=500,
        )


@app.get("/stock_news")
def stock_news(symbol: str = Query(..., description="Stock code, e.g. 600519")):
    """个股新闻 from 东方财富.

    Returns up to ~20 recent news items for the given stock.
    Content is truncated to 500 chars to keep response size reasonable.
    """
    try:
        df = ak.stock_news_em(symbol=symbol)
        records = []
        for _, row in df.iterrows():
            content = _str(row.get("新闻内容"))
            records.append({
                "title": _str(row.get("新闻标题")),
                "content": content[:500] if content else "",
                "publish_time": _str(row.get("发布时间")),
                "source": _str(row.get("文章来源")),
                "url": _str(row.get("新闻链接")),
            })
        return records
    except Exception as e:
        return JSONResponse(
            {"error": str(e), "trace": traceback.format_exc()},
            status_code=500,
        )


@app.get("/northbound")
def northbound():
    """北向资金 — via stock_hsgt_hist_em (AKShare >=1.18)."""
    try:
        df = ak.stock_hsgt_hist_em(symbol="北向资金")
        records = []
        for _, row in df.tail(10).iterrows():
            records.append({
                "trade_date": str(row.get("日期", ""))[:10],
                "buy_amount": _float(row.get("买入成交额")),
                "sell_amount": _float(row.get("卖出成交额")),
                "net_amount": _float(row.get("当日成交净买额")),
            })
        return records
    except Exception as e:
        return JSONResponse(
            {"error": str(e), "trace": traceback.format_exc()},
            status_code=500,
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8321)
