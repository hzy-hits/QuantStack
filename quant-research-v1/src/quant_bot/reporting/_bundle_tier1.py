"""
Tier 1 market context builder.

Encapsulates the price query, uncertainty tracking, and per-section assembly
that was formerly the 238-line ``_market_context_tier1()`` closure soup.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import duckdb
import structlog

log = structlog.get_logger()


class Tier1Builder:
    """Builds the mandatory Tier 1 market context block per Agents.md contract."""

    # Symbol lists — class-level constants
    MAJOR_INDEX_SYMBOLS = ["SPY", "QQQ", "IWM", "DIA"]
    SECTOR_SYMBOLS = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLU", "XLRE", "XLY", "XLP", "XLB", "XLC"]
    RATE_CREDIT_SYMBOLS = ["TLT", "HYG"]
    COMMODITY_SYMBOLS = ["GLD", "CL=F"]
    VIX_SYMBOL = "^VIX"

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        as_of_str: str,
        universe: dict[str, list[str]],
    ) -> None:
        self.con = con
        self.as_of_str = as_of_str
        self.as_of_date = date.fromisoformat(as_of_str)
        self.universe = universe
        self.sp500_symbols = list(dict.fromkeys(universe.get("sp500", [])))

        self.price_map: dict[str, Any] = {}
        self.uncertainty: dict[str, Any] = {"missing": [], "stale": [], "by_field": {}}

        self._load_prices()

    # ------------------------------------------------------------------
    # Helpers (formerly closures)
    # ------------------------------------------------------------------

    @staticmethod
    def _is_null(v: Any) -> bool:
        return v is None or (isinstance(v, float) and v != v)

    def _f(self, v: Any, d: int | None = None) -> float | None:
        if self._is_null(v):
            return None
        v = float(v)
        return round(v, d) if d is not None else v

    def _pct(self, cur: Any, prev: Any) -> float | None:
        c, p = self._f(cur), self._f(prev)
        if c is None or p in (None, 0.0):
            return None
        return round((c / p - 1.0) * 100.0, 2)

    def _to_date(self, v: Any) -> date | None:
        if self._is_null(v):
            return None
        if hasattr(v, "date"):
            return v.date()
        if isinstance(v, date):
            return v
        return date.fromisoformat(str(v)[:10])

    def _register(
        self,
        path: str,
        *,
        present: bool,
        missing: bool,
        stale: bool,
        source_date: str | None = None,
        reason: str | None = None,
    ) -> None:
        entry: dict[str, Any] = {"present": present, "missing": missing, "stale": stale}
        if source_date:
            entry["source_date"] = source_date
        if reason:
            entry["reason"] = reason
        self.uncertainty["by_field"][path] = entry
        if missing and path not in self.uncertainty["missing"]:
            self.uncertainty["missing"].append(path)
        if stale and path not in self.uncertainty["stale"]:
            self.uncertainty["stale"].append(path)

    def _sym_info(self, sym: str) -> tuple[Any, date | None, bool, bool, str | None]:
        row = self.price_map.get(sym)
        pd_ = self._to_date(row["date"]) if row is not None else None
        lag = (self.as_of_date - pd_).days if pd_ is not None else 999
        missing = lag > 3   # allow weekend/holiday gap (Fri data used on Sat/Mon)
        stale   = lag > 7   # stale only if more than a week old
        return row, pd_, missing, stale, pd_.isoformat() if pd_ else None

    def _above_200ma(self, row: Any) -> bool | None:
        if row is None:
            return None
        n = row.get("n_200")
        sma = self._f(row.get("sma_200"))
        ac  = self._f(row.get("adj_close"))
        if ac is None or sma is None or self._is_null(n) or int(n) < 200:
            return None
        return ac > sma

    # ------------------------------------------------------------------
    # Price loading
    # ------------------------------------------------------------------

    def _load_prices(self) -> None:
        query_symbols = list(dict.fromkeys(
            self.MAJOR_INDEX_SYMBOLS + [self.VIX_SYMBOL] + self.SECTOR_SYMBOLS
            + self.RATE_CREDIT_SYMBOLS + self.COMMODITY_SYMBOLS + self.sp500_symbols
        ))
        if not query_symbols:
            return
        ph = ",".join("?" * len(query_symbols))
        try:
            rows = self.con.execute(f"""
                WITH hist AS (
                    SELECT symbol, date, adj_close,
                           LAG(adj_close,  1) OVER (PARTITION BY symbol ORDER BY date) AS prev_1d,
                           LAG(adj_close,  5) OVER (PARTITION BY symbol ORDER BY date) AS prev_5d,
                           LAG(adj_close, 20) OVER (PARTITION BY symbol ORDER BY date) AS prev_20d,
                           AVG(adj_close) OVER (
                               PARTITION BY symbol ORDER BY date
                               ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                           ) AS sma_200,
                           COUNT(adj_close) OVER (
                               PARTITION BY symbol ORDER BY date
                               ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                           ) AS n_200,
                           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
                    FROM prices_daily
                    WHERE symbol IN ({ph}) AND date <= ?
                )
                SELECT symbol, date, adj_close, prev_1d, prev_5d, prev_20d, sma_200, n_200
                FROM hist WHERE rn = 1
            """, query_symbols + [self.as_of_str]).fetchdf()
            if not rows.empty:
                self.price_map = {str(r["symbol"]): r for _, r in rows.iterrows()}
        except Exception as e:
            log.warning("tier1_price_query_failed", error=str(e))

    # ------------------------------------------------------------------
    # Per-section builders
    # ------------------------------------------------------------------

    def build_indices(self) -> dict[str, Any]:
        major_indices: dict[str, Any] = {}
        for sym in self.MAJOR_INDEX_SYMBOLS:
            row, _, miss, stale, sd = self._sym_info(sym)
            price   = self._f(row["adj_close"], 2) if row is not None else None
            ret_1d  = self._pct(row["adj_close"], row["prev_1d"]) if row is not None else None
            ab200   = self._above_200ma(row)
            major_indices[sym] = {"price": price, "ret_1d_pct": ret_1d, "above_200ma": ab200, "price_date": sd}
            self._register(f"major_indices.{sym}.price",  present=price is not None,  missing=(price is None or miss),  stale=stale, source_date=sd)
            self._register(f"major_indices.{sym}.ret_1d",  present=ret_1d is not None, missing=(ret_1d is None or miss),  stale=stale, source_date=sd)
            self._register(f"major_indices.{sym}.above_200ma", present=ab200 is not None, missing=(ab200 is None or miss), stale=stale, source_date=sd)
        return major_indices

    def build_vix(self) -> dict[str, Any]:
        row, _, miss, stale, sd = self._sym_info(self.VIX_SYMBOL)
        vix_level  = self._f(row["adj_close"], 2) if row is not None else None
        vix_5d     = self._pct(row["adj_close"], row["prev_5d"]) if row is not None else None
        vix_20d    = self._pct(row["adj_close"], row["prev_20d"]) if row is not None else None
        vix        = {"level": vix_level, "change_5d_pct": vix_5d, "change_20d_pct": vix_20d, "price_date": sd}
        self._register("vix.level",        present=vix_level is not None, missing=(vix_level is None or miss), stale=stale, source_date=sd)
        self._register("vix.change_5d",   present=vix_5d is not None,    missing=(vix_5d is None or miss),    stale=stale, source_date=sd)
        self._register("vix.change_20d",  present=vix_20d is not None,   missing=(vix_20d is None or miss),   stale=stale, source_date=sd)
        return vix

    def build_sectors(self) -> dict[str, Any]:
        sectors: dict[str, Any] = {}
        for sym in self.SECTOR_SYMBOLS:
            row, _, miss, stale, sd = self._sym_info(sym)
            ret_1d = self._pct(row["adj_close"], row["prev_1d"]) if row is not None else None
            sectors[sym] = {"ret_1d_pct": ret_1d, "price_date": sd}
            self._register(f"sectors.{sym}.ret_1d", present=ret_1d is not None, missing=(ret_1d is None or miss), stale=stale, source_date=sd)
        return sectors

    def build_rates_credit(self) -> dict[str, Any]:
        rates_credit: dict[str, Any] = {}
        for sym in self.RATE_CREDIT_SYMBOLS:
            row, _, miss, stale, sd = self._sym_info(sym)
            ret_1d = self._pct(row["adj_close"], row["prev_1d"]) if row is not None else None
            ab200  = self._above_200ma(row)
            rates_credit[sym] = {"ret_1d_pct": ret_1d, "above_200ma": ab200, "price_date": sd}
            self._register(f"rates_credit.{sym}.ret_1d",    present=ret_1d is not None, missing=(ret_1d is None or miss), stale=stale, source_date=sd)
            self._register(f"rates_credit.{sym}.above_200ma", present=ab200 is not None, missing=(ab200 is None or miss),  stale=stale, source_date=sd)
        return rates_credit

    def build_commodities(self) -> dict[str, Any]:
        commodities: dict[str, Any] = {}
        for sym in self.COMMODITY_SYMBOLS:
            row, _, miss, stale, sd = self._sym_info(sym)
            ret_1d = self._pct(row["adj_close"], row["prev_1d"]) if row is not None else None
            commodities[sym] = {"ret_1d_pct": ret_1d, "price_date": sd}
            self._register(f"commodities.{sym}.ret_1d", present=ret_1d is not None, missing=(ret_1d is None or miss), stale=stale, source_date=sd)
        return commodities

    def build_breadth(self) -> dict[str, Any]:
        n_above = n_eligible = n_missing = n_stale_b = 0
        for sym in self.sp500_symbols:
            row, _, miss, stale, _ = self._sym_info(sym)
            if miss:
                n_missing += 1
            if stale:
                n_stale_b += 1
            ab200 = self._above_200ma(row)
            if ab200 is None:
                continue
            n_eligible += 1
            if ab200:
                n_above += 1
        breadth_pct = round(n_above / n_eligible * 100.0, 1) if n_eligible else None
        breadth = {
            "sp500_above_200ma_pct": breadth_pct,
            "n_above_200ma": n_above,
            "n_eligible": n_eligible,
            "n_universe": len(self.sp500_symbols),
            "n_missing_on_asof": n_missing,
            "n_stale": n_stale_b,
        }
        self._register("breadth.sp500_above_200ma_pct",
                  present=breadth_pct is not None,
                  missing=(breadth_pct is None or n_missing > 0 or n_eligible < len(self.sp500_symbols)),
                  stale=(n_stale_b > 0))
        return breadth

    def build_polymarket(self) -> list[dict]:
        polymarket_events: list[dict] = []
        poly_latest: date | None = None
        try:
            # Only show events fetched within last 2 days; join previous snapshot for Δ
            poly_rows = self.con.execute("""
                WITH latest AS (
                    -- Deduplicate: keep only the most recent snapshot per market
                    SELECT DISTINCT ON (market_id)
                           market_id, question, p_yes, p_no, volume_usd,
                           end_date, category, fetched_at, fetch_date
                    FROM polymarket_events
                    WHERE p_yes IS NOT NULL
                      AND volume_usd >= 10000
                      AND fetch_date >= CURRENT_DATE - INTERVAL '2 days'
                    ORDER BY market_id, fetch_date DESC
                ),
                prev AS (
                    -- Previous snapshot (before the 2-day window) for Δ
                    SELECT DISTINCT ON (market_id)
                           market_id, p_yes AS prev_p_yes
                    FROM polymarket_events
                    WHERE fetch_date < (SELECT MIN(fetch_date) FROM latest)
                    ORDER BY market_id, fetch_date DESC
                )
                SELECT t.question, t.p_yes, t.p_no, t.volume_usd,
                       t.end_date, t.category, t.fetched_at,
                       p.prev_p_yes,
                       (t.p_yes - p.prev_p_yes) AS p_yes_delta
                FROM latest t
                LEFT JOIN prev p ON t.market_id = p.market_id
                ORDER BY t.volume_usd DESC NULLS LAST
                LIMIT 10
            """).fetchdf()
            if not poly_rows.empty:
                for _, r in poly_rows.iterrows():
                    fd = self._to_date(r.get("fetched_at"))
                    if fd and (poly_latest is None or fd > poly_latest):
                        poly_latest = fd
                    delta = self._f(r.get("p_yes_delta"), 3) if not self._is_null(r.get("p_yes_delta")) else None
                    polymarket_events.append({
                        "question":   r["question"],
                        "p_yes":      self._f(r["p_yes"], 3),
                        "p_no":       self._f(r["p_no"], 3),
                        "p_yes_delta": delta,
                        "volume_usd": round(float(r["volume_usd"])) if not self._is_null(r["volume_usd"]) else None,
                        "end_date":   str(r["end_date"]) if not self._is_null(r["end_date"]) else None,
                        "category":   r["category"],
                        "fetched_at": str(r["fetched_at"]) if not self._is_null(r.get("fetched_at")) else None,
                    })
        except Exception as e:
            log.warning("tier1_polymarket_query_failed", error=str(e))

        poly_stale = poly_latest is not None and (self.as_of_date - poly_latest).days > 2
        self._register("polymarket_events",
                  present=bool(polymarket_events),
                  missing=not polymarket_events,
                  stale=poly_stale,
                  source_date=poly_latest.isoformat() if poly_latest else None)
        return polymarket_events

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def build(self) -> dict:
        """Return the full Tier 1 dict (same structure as old ``_market_context_tier1``)."""
        return {
            "major_indices":     self.build_indices(),
            "vix":               self.build_vix(),
            "sectors":           self.build_sectors(),
            "rates_credit":      self.build_rates_credit(),
            "commodities":       self.build_commodities(),
            "polymarket_events": self.build_polymarket(),
            "breadth":           self.build_breadth(),
            "uncertainty":       self.uncertainty,
        }
