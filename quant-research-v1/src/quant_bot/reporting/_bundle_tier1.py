"""
Tier 1 market context builder.

Encapsulates the price query, uncertainty tracking, and per-section assembly
that was formerly the 238-line ``_market_context_tier1()`` closure soup.
"""
from __future__ import annotations

import json
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

    @staticmethod
    def _scale(value: float | None, low: float, high: float) -> float | None:
        if value is None or high == low:
            return None
        return max(0.0, min(100.0, (float(value) - low) / (high - low) * 100.0))

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

    def _analysis_detail(self, symbol: str, module_name: str) -> dict[str, Any]:
        try:
            row = self.con.execute(
                """
                SELECT details
                FROM analysis_daily
                WHERE symbol = ?
                  AND module_name = ?
                  AND date = (
                      SELECT MAX(date)
                      FROM analysis_daily
                      WHERE symbol = ?
                        AND module_name = ?
                        AND date <= ?
                  )
                """,
                [symbol, module_name, symbol, module_name, self.as_of_str],
            ).fetchone()
        except Exception:
            return {}
        if not row or not row[0]:
            return {}
        try:
            parsed = json.loads(row[0])
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

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

    def build_fear_greed(self, vix: dict[str, Any], breadth: dict[str, Any]) -> dict[str, Any]:
        """Internal market fear/greed composite built only from as-of data.

        This is a deterministic proxy, not CNN's external Fear & Greed index.
        Higher score means risk appetite; lower score means stress/fear.
        """
        spy_row, _, spy_miss, spy_stale, spy_sd = self._sym_info("SPY")
        hyg_row, _, hyg_miss, hyg_stale, hyg_sd = self._sym_info("HYG")
        tlt_row, _, tlt_miss, tlt_stale, tlt_sd = self._sym_info("TLT")

        spy_20d = self._pct(spy_row["adj_close"], spy_row["prev_20d"]) if spy_row is not None else None
        hyg_20d = self._pct(hyg_row["adj_close"], hyg_row["prev_20d"]) if hyg_row is not None else None
        tlt_20d = self._pct(tlt_row["adj_close"], tlt_row["prev_20d"]) if tlt_row is not None else None
        credit_momentum = (
            round(hyg_20d - tlt_20d, 2)
            if hyg_20d is not None and tlt_20d is not None
            else None
        )

        spy_mr = self._analysis_detail("SPY", "mean_reversion")
        spy_rsi = self._f(spy_mr.get("rsi_14"))
        spy_bb_position = self._f(spy_mr.get("bb_position"))

        vix_level = self._f(vix.get("level"))
        vix_change_20d = self._f(vix.get("change_20d_pct"))
        components = {
            "vix_level": self._scale(vix_level, 35.0, 12.0),
            "vix_trend": self._scale(-(vix_change_20d or 0.0) if vix_change_20d is not None else None, -35.0, 35.0),
            "spy_momentum": self._scale(spy_20d, -8.0, 8.0),
            "spy_rsi": self._scale(spy_rsi, 30.0, 70.0),
            "breadth": self._f(breadth.get("sp500_above_200ma_pct")),
            "credit_risk_appetite": self._scale(credit_momentum, -5.0, 5.0),
        }
        weights = {
            "vix_level": 0.25,
            "vix_trend": 0.15,
            "spy_momentum": 0.20,
            "spy_rsi": 0.15,
            "breadth": 0.20,
            "credit_risk_appetite": 0.05,
        }
        usable = [(weights[k], v) for k, v in components.items() if v is not None]
        score = round(sum(w * v for w, v in usable) / sum(w for w, _ in usable), 1) if usable else None
        if score is None:
            label = "unknown"
        elif score < 25:
            label = "extreme_fear"
        elif score < 45:
            label = "fear"
        elif score < 55:
            label = "neutral"
        elif score < 75:
            label = "greed"
        else:
            label = "extreme_greed"

        self._register("fear_greed.score", present=score is not None, missing=score is None, stale=spy_stale or hyg_stale or tlt_stale)
        self._register("fear_greed.vix_level_component", present=components["vix_level"] is not None, missing=components["vix_level"] is None, stale=bool(vix.get("price_date") is None))
        self._register("fear_greed.spy_rsi", present=spy_rsi is not None, missing=spy_rsi is None, stale=False)
        self._register("fear_greed.spy_momentum", present=spy_20d is not None, missing=(spy_20d is None or spy_miss), stale=spy_stale, source_date=spy_sd)
        self._register("fear_greed.credit_risk_appetite", present=credit_momentum is not None, missing=(credit_momentum is None or hyg_miss or tlt_miss), stale=(hyg_stale or tlt_stale), source_date=hyg_sd or tlt_sd)
        return {
            "score": score,
            "label": label,
            "source": "internal_proxy_vix_spy_breadth_credit",
            "components": {k: round(v, 1) if v is not None else None for k, v in components.items()},
            "inputs": {
                "vix_level": vix_level,
                "vix_change_20d_pct": vix_change_20d,
                "spy_20d_pct": spy_20d,
                "spy_rsi_14": spy_rsi,
                "spy_bb_position": spy_bb_position,
                "breadth_above_200ma_pct": breadth.get("sp500_above_200ma_pct"),
                "hyg_20d_pct": hyg_20d,
                "tlt_20d_pct": tlt_20d,
                "credit_risk_appetite_pct": credit_momentum,
            },
            "price_date": spy_sd or vix.get("price_date"),
        }

    def build_polymarket(self) -> list[dict]:
        polymarket_events: list[dict] = []
        poly_latest: date | None = None
        try:
            # Only show snapshots that existed as of the report trade date.  This
            # matters when rerendering a historical report after newer snapshots
            # have landed in the same DuckDB.
            window_start = (self.as_of_date.toordinal() - 2)
            window_start_str = date.fromordinal(window_start).isoformat()
            poly_rows = self.con.execute("""
                WITH eligible AS (
                    SELECT market_id, question, p_yes, p_no, volume_usd,
                           end_date, category, fetched_at, fetch_date,
                           ROW_NUMBER() OVER (
                               PARTITION BY market_id
                               ORDER BY fetch_date DESC, fetched_at DESC NULLS LAST
                           ) AS rn
                    FROM polymarket_events
                    WHERE p_yes IS NOT NULL
                      AND volume_usd >= 10000
                      AND CAST(fetch_date AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                ),
                latest AS (
                    SELECT market_id, question, p_yes, p_no, volume_usd,
                           end_date, category, fetched_at, fetch_date
                    FROM eligible
                    WHERE rn = 1
                )
                SELECT t.question, t.p_yes, t.p_no, t.volume_usd,
                       t.end_date, t.category, t.fetched_at,
                       p.prev_p_yes,
                       (t.p_yes - p.prev_p_yes) AS p_yes_delta
                FROM latest t
                LEFT JOIN LATERAL (
                    SELECT p_yes AS prev_p_yes
                    FROM polymarket_events p
                    WHERE p.market_id = t.market_id
                      AND p.p_yes IS NOT NULL
                      AND CAST(p.fetch_date AS DATE) <= CAST(? AS DATE)
                      AND (
                          CAST(p.fetch_date AS TIMESTAMP) < CAST(t.fetch_date AS TIMESTAMP)
                          OR (
                              CAST(p.fetch_date AS TIMESTAMP) = CAST(t.fetch_date AS TIMESTAMP)
                              AND p.fetched_at IS NOT NULL
                              AND t.fetched_at IS NOT NULL
                              AND p.fetched_at < t.fetched_at
                          )
                      )
                    ORDER BY p.fetch_date DESC, p.fetched_at DESC NULLS LAST
                    LIMIT 1
                ) p ON TRUE
                ORDER BY t.volume_usd DESC NULLS LAST
                LIMIT 10
            """, [window_start_str, self.as_of_str, self.as_of_str]).fetchdf()
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
        vix = self.build_vix()
        breadth = self.build_breadth()
        return {
            "major_indices":     self.build_indices(),
            "vix":               vix,
            "fear_greed":        self.build_fear_greed(vix, breadth),
            "sectors":           self.build_sectors(),
            "rates_credit":      self.build_rates_credit(),
            "commodities":       self.build_commodities(),
            "polymarket_events": self.build_polymarket(),
            "breadth":           breadth,
            "uncertainty":       self.uncertainty,
        }
