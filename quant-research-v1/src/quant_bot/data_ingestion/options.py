"""
Options chain ingestion and analysis via CBOE delayed quotes CDN.

Data source: https://cdn.cboe.com/api/global/delayed_quotes/options/{SYMBOL}.json
  - Free, no API key required, 15-minute delay
  - Full options chain with IV, Greeks, volume, OI, bid/ask
  - iv30 (30-day ATM IV) provided directly

Two levels of output:
  1. options_snapshot — lightweight summary (ATM IV, expected move, P/C ratio)
  2. options_analysis — enhanced forward-looking analysis per symbol/expiry:
     - Lognormal probability cone (1σ/2σ price ranges)
     - IV skew at ~5% OTM moneyness (with liquidity gate)
     - Directional bias signal from skew + P/C volume ratio
     - Liquidity scoring (bid-ask spread, chain width, volume)
     - Unusual activity detection (volume >> open interest)
"""
from __future__ import annotations

import json
import math
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Optional

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import requests
import structlog

log = structlog.get_logger()

# Proxy map: non-optionable symbols → their liquid ETF proxy
OPTIONS_PROXY_MAP: dict[str, str] = {
    "CL=F": "USO",
    "GC=F": "GLD",
    "SI=F": "SLV",
    "NG=F": "UNG",
    "^VIX": "UVXY",
}

CBOE_OPTIONS_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json"
CBOE_TIMEOUT = 8  # seconds per request
CBOE_MAX_WORKERS = max(1, int(os.getenv("CBOE_MAX_WORKERS", "3")))
CBOE_MAX_RETRIES = max(1, int(os.getenv("CBOE_MAX_RETRIES", "3")))
CBOE_BASE_BACKOFF = max(0.5, float(os.getenv("CBOE_BASE_BACKOFF", "2.0")))
CBOE_MAX_BACKOFF = max(CBOE_BASE_BACKOFF, float(os.getenv("CBOE_MAX_BACKOFF", "20.0")))
CBOE_BACKOFF_JITTER = max(0.0, float(os.getenv("CBOE_BACKOFF_JITTER", "0.5")))
CBOE_MIN_REQUEST_INTERVAL = max(
    0.0, float(os.getenv("CBOE_MIN_REQUEST_INTERVAL", "0.5"))
)
CBOE_BLACKLIST_FILE = "data/cboe_blacklist.json"
CBOE_BLACKLIST_DAYS = 30  # skip symbols that returned 403 within this window
_cboe_rate_limit_lock = threading.Lock()
_cboe_next_request_at = 0.0


def is_options_eligible(symbol: str) -> bool:
    """True if symbol can have options fetched directly."""
    if "=" in symbol:   # futures: CL=F, GC=F, etc.
        return False
    if symbol.startswith("^"):  # indices: ^VIX
        return False
    return True


# Liquidity thresholds
MIN_DTE = 2                   # skip very near-term (next-day) expiries
MAX_SPREAD_PCT = 0.50         # 50% bid-ask spread → reject strike
MIN_VOLUME_STRIKE = 5         # min volume to consider a strike liquid
MIN_OI_STRIKE = 10            # min open interest to consider a strike liquid
MONEYNESS_SKEW = 0.05         # 5% OTM for skew computation

# Regex to parse CBOE option symbol: {TICKER}{YYMMDD}{C|P}{STRIKE*1000}
_OPT_RE = re.compile(r"^(.+?)(\d{6})([CP])(\d{8})$")


def _days_to_expiry(expiry_str: str, as_of: date) -> int:
    exp = datetime.strptime(expiry_str, "%Y-%m-%d").date()
    return max((exp - as_of).days, 1)


def _valid_iv(iv) -> Optional[float]:
    """Return IV if it's a valid positive number, else None."""
    if iv is None or (isinstance(iv, float) and (math.isnan(iv) or iv <= 0)):
        return None
    v = float(iv)
    return v if v > 0 else None


def _parse_cboe_option_symbol(opt_sym: str, ticker: str) -> Optional[dict]:
    """Parse CBOE option symbol like AAPL260313C00262500 into components."""
    # Strip the known ticker prefix for reliable parsing
    rest = opt_sym[len(ticker):]
    if len(rest) < 15:
        return None
    try:
        yy = rest[0:2]
        mm = rest[2:4]
        dd = rest[4:6]
        cp = rest[6]
        strike_raw = rest[7:]
        return {
            "expiry": f"20{yy}-{mm}-{dd}",
            "type": "call" if cp == "C" else "put",
            "strike": int(strike_raw) / 1000.0,
        }
    except (ValueError, IndexError):
        return None


def _load_blacklist() -> dict[str, str]:
    """Load {symbol: last_failed_date_iso} from disk."""
    try:
        with open(CBOE_BLACKLIST_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_blacklist(bl: dict[str, str]):
    """Persist blacklist to disk."""
    try:
        with open(CBOE_BLACKLIST_FILE, "w") as f:
            json.dump(bl, f)
    except OSError:
        pass


# Shared set populated during fetch, persisted at the end
_blacklist_additions: dict[str, str] = {}


def _respect_cboe_rate_limit() -> None:
    """Apply a simple process-wide request spacing to avoid CBOE bursts."""
    global _cboe_next_request_at

    if CBOE_MIN_REQUEST_INTERVAL <= 0:
        return

    with _cboe_rate_limit_lock:
        now = time.monotonic()
        wait = _cboe_next_request_at - now
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()
        _cboe_next_request_at = max(_cboe_next_request_at, now) + CBOE_MIN_REQUEST_INTERVAL


def _cboe_retry_delay(resp: requests.Response, attempt: int) -> float:
    """Choose retry delay from Retry-After when present, else short exponential backoff."""
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            delay = float(retry_after)
            if delay > 0:
                return min(delay, CBOE_MAX_BACKOFF)
        except (TypeError, ValueError):
            pass

    delay = min(CBOE_BASE_BACKOFF * (2**attempt), CBOE_MAX_BACKOFF)
    if CBOE_BACKOFF_JITTER > 0:
        delay += random.uniform(0.0, CBOE_BACKOFF_JITTER)
    return round(delay, 3)


def _fetch_cboe_single(symbol: str) -> Optional[dict]:
    """Fetch CBOE delayed quotes for a single symbol. Returns raw JSON or None.

    - 403 = symbol has no options on CBOE (permanent) → blacklist, no retry
    - 429 = rate limit → retry with backoff
    """
    url = CBOE_OPTIONS_URL.format(symbol=symbol)
    for attempt in range(CBOE_MAX_RETRIES):
        try:
            _respect_cboe_rate_limit()
            resp = requests.get(url, timeout=CBOE_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 403:
                _blacklist_additions[symbol] = date.today().isoformat()
                return None
            if resp.status_code == 429 and attempt < CBOE_MAX_RETRIES - 1:
                delay = _cboe_retry_delay(resp, attempt)
                log.warning(
                    "cboe_rate_limited",
                    symbol=symbol,
                    attempt=attempt + 1,
                    delay=delay,
                )
                time.sleep(delay)
                continue
            log.debug("cboe_http_error", symbol=symbol, status=resp.status_code)
            return None
        except Exception as e:
            log.debug("cboe_request_error", symbol=symbol, error=str(e))
            return None
    return None


def _cboe_to_dataframes(
    raw: dict, ticker: str, as_of: date
) -> dict[str, tuple[pd.DataFrame, pd.DataFrame, float]]:
    """
    Parse CBOE JSON into per-expiry (calls_df, puts_df) with yfinance-compatible columns.
    Returns {expiry_str: (calls_df, puts_df, current_price)}.
    """
    data = raw.get("data", raw)
    current_price = data.get("current_price")
    if not current_price or current_price <= 0:
        return {}

    options = data.get("options", [])
    if not options:
        return {}

    # Parse all options into records grouped by expiry
    by_expiry: dict[str, list[dict]] = {}
    for opt in options:
        parsed = _parse_cboe_option_symbol(opt.get("option", ""), ticker)
        if not parsed:
            continue
        expiry = parsed["expiry"]
        by_expiry.setdefault(expiry, []).append({
            "strike": parsed["strike"],
            "opt_type": parsed["type"],
            "bid": opt.get("bid", 0) or 0,
            "ask": opt.get("ask", 0) or 0,
            "volume": opt.get("volume", 0) or 0,
            "openInterest": opt.get("open_interest", 0) or 0,
            "impliedVolatility": opt.get("iv", 0) or 0,
            "delta": opt.get("delta", 0) or 0,
        })

    result = {}
    for expiry_str, records in by_expiry.items():
        df = pd.DataFrame(records)
        calls = df[df["opt_type"] == "call"].copy()
        puts = df[df["opt_type"] == "put"].copy()
        result[expiry_str] = (calls, puts, current_price)

    return result


def _atm_iv(chain_df, current_price: float) -> Optional[float]:
    """Find the closest-to-ATM strike and return its implied vol."""
    if chain_df is None or chain_df.empty:
        return None
    df = chain_df.copy()
    df["dist"] = (df["strike"] - current_price).abs()
    atm_row = df.nsmallest(1, "dist")
    iv = atm_row["impliedVolatility"].iloc[0] if not atm_row.empty else None
    return _valid_iv(iv)


def _compute_iv_skew(
    calls: pd.DataFrame, puts: pd.DataFrame, price: float
) -> Optional[float]:
    """
    IV skew = OTM put IV / OTM call IV at ~5% moneyness.
    >1.0 = market fears downside more; <1.0 = more call demand.
    Returns None if either side lacks liquid strikes at target moneyness.
    """
    target_put_strike = price * (1.0 - MONEYNESS_SKEW)
    target_call_strike = price * (1.0 + MONEYNESS_SKEW)

    def _find_iv_near(df: pd.DataFrame, target: float) -> Optional[float]:
        if df.empty:
            return None
        # Filter to strikes with some liquidity
        liquid = df[
            (df.get("volume", pd.Series(dtype=float)).fillna(0) >= MIN_VOLUME_STRIKE) |
            (df.get("openInterest", pd.Series(dtype=float)).fillna(0) >= MIN_OI_STRIKE)
        ]
        if liquid.empty:
            liquid = df  # fall back to all strikes
        liquid = liquid.copy()
        liquid["dist"] = (liquid["strike"] - target).abs()
        # Accept if within 3% of target
        near = liquid[liquid["dist"] <= price * 0.03]
        if near.empty:
            near = liquid.nsmallest(1, "dist")
        best = near.nsmallest(1, "dist")
        if best.empty:
            return None
        return _valid_iv(best["impliedVolatility"].iloc[0])

    put_iv = _find_iv_near(puts, target_put_strike)
    call_iv = _find_iv_near(calls, target_call_strike)

    if put_iv is None or call_iv is None or call_iv <= 0:
        return None
    return round(put_iv / call_iv, 4)


def _assess_liquidity(
    calls: pd.DataFrame, puts: pd.DataFrame, price: float
) -> tuple[str, int, Optional[float]]:
    """
    Assess chain liquidity near ATM.
    Returns (score_label, chain_width, avg_spread_pct).
    """
    all_opts = pd.concat([calls, puts], ignore_index=True)
    if all_opts.empty:
        return "poor", 0, None

    # Strikes within 10% of current price
    near_atm = all_opts[
        (all_opts["strike"] >= price * 0.90) &
        (all_opts["strike"] <= price * 1.10)
    ]
    chain_width = len(near_atm)

    # Compute bid-ask spread %
    if "bid" in near_atm.columns and "ask" in near_atm.columns:
        valid_quotes = near_atm[
            (near_atm["bid"].fillna(0) > 0) & (near_atm["ask"].fillna(0) > 0)
        ]
        if not valid_quotes.empty:
            mid = (valid_quotes["bid"] + valid_quotes["ask"]) / 2.0
            spread_pct = ((valid_quotes["ask"] - valid_quotes["bid"]) / mid).replace(
                [np.inf, -np.inf], np.nan
            )
            avg_spread = float(spread_pct.mean()) if not spread_pct.isna().all() else None
        else:
            avg_spread = None
    else:
        avg_spread = None

    # Liquidity score
    has_vol = near_atm.get("volume", pd.Series(dtype=float)).fillna(0).sum() > 50
    has_oi = near_atm.get("openInterest", pd.Series(dtype=float)).fillna(0).sum() > 100
    tight_spread = avg_spread is not None and avg_spread < 0.15

    if has_vol and has_oi and tight_spread and chain_width >= 10:
        label = "good"
    elif (has_vol or has_oi) and chain_width >= 4:
        label = "fair"
    else:
        label = "poor"

    return label, chain_width, round(avg_spread, 4) if avg_spread is not None else None


def _find_unusual_activity(
    calls: pd.DataFrame, puts: pd.DataFrame
) -> list[dict]:
    """
    Find strikes where today's volume >> open interest (unusual flow).
    Returns list of {strike, type, volume, open_interest, vol_oi_ratio}.
    """
    unusual = []
    for opt_type, df in [("call", calls), ("put", puts)]:
        if df.empty:
            continue
        for _, row in df.iterrows():
            vol = row.get("volume")
            oi = row.get("openInterest")
            if pd.isna(vol) or pd.isna(oi) or vol is None:
                continue
            vol, oi = int(vol), int(oi) if not pd.isna(oi) else 0
            if vol < 100:  # ignore tiny prints
                continue
            if oi > 0 and vol / oi >= 3.0:
                unusual.append({
                    "strike": float(row["strike"]),
                    "type": opt_type,
                    "volume": vol,
                    "open_interest": oi,
                    "vol_oi_ratio": round(vol / oi, 1),
                })
            elif oi == 0 and vol >= 500:
                unusual.append({
                    "strike": float(row["strike"]),
                    "type": opt_type,
                    "volume": vol,
                    "open_interest": 0,
                    "vol_oi_ratio": None,
                })

    # Sort by volume descending, return top 5
    unusual.sort(key=lambda x: x["volume"], reverse=True)
    return unusual[:5]


def _compute_probability_cone(
    price: float, atm_iv: float, dte: int
) -> dict:
    """
    Lognormal probability cone.
    1σ and 2σ ranges using: S * exp(±σ * sqrt(T))
    where σ is annualized IV (decimal) and T = DTE/252.
    """
    t = dte / 252.0
    sigma_t = atm_iv * math.sqrt(t)

    return {
        "range_68_low": round(price * math.exp(-sigma_t), 2),
        "range_68_high": round(price * math.exp(sigma_t), 2),
        "range_95_low": round(price * math.exp(-2.0 * sigma_t), 2),
        "range_95_high": round(price * math.exp(2.0 * sigma_t), 2),
    }


def _bias_signal(skew: Optional[float], pc_ratio: Optional[float]) -> str:
    """
    Combine IV skew and put/call volume ratio into a directional bias.
    - skew < 0.9 AND pc_ratio < 0.7 → bullish
    - skew > 1.15 AND pc_ratio > 1.3 → bearish
    - otherwise → neutral
    """
    bullish_count = 0
    bearish_count = 0

    if skew is not None:
        if skew < 0.90:
            bullish_count += 1
        elif skew > 1.15:
            bearish_count += 1

    if pc_ratio is not None:
        if pc_ratio < 0.7:
            bullish_count += 1
        elif pc_ratio > 1.3:
            bearish_count += 1

    if bullish_count >= 2:
        return "bullish"
    if bearish_count >= 2:
        return "bearish"
    if bullish_count > bearish_count:
        return "bullish"
    if bearish_count > bullish_count:
        return "bearish"
    return "neutral"


def _process_symbol(
    sym: str, as_of: date, max_expiries: int
) -> tuple[list[dict], list[dict]]:
    """Fetch and process options for a single symbol. Returns (snapshot_records, analysis_records)."""
    snapshot_records = []
    analysis_records = []

    raw = _fetch_cboe_single(sym)
    if raw is None:
        return [], []

    expiry_data = _cboe_to_dataframes(raw, sym, as_of)
    if not expiry_data:
        return [], []

    data = raw.get("data", raw)
    current_price = data.get("current_price", 0)

    # Sort expiries, skip past dates, pick nearest max_expiries
    valid_expiries = []
    for exp_str in sorted(expiry_data.keys()):
        try:
            dte = _days_to_expiry(exp_str, as_of)
            if dte >= 1:
                valid_expiries.append(exp_str)
        except ValueError:
            continue
    valid_expiries = valid_expiries[:max_expiries]

    for exp_str in valid_expiries:
        calls, puts, price = expiry_data[exp_str]
        dte = _days_to_expiry(exp_str, as_of)

        # ATM IV from calls (more liquid near ATM), fallback to puts
        atm_iv = _atm_iv(calls, price)
        if atm_iv is None:
            atm_iv = _atm_iv(puts, price)

        # CBOE iv30 fallback: use top-level iv30 if per-strike ATM IV is 0
        if atm_iv is None:
            iv30 = data.get("iv30")
            if iv30 and float(iv30) > 0:
                atm_iv = float(iv30) / 100.0  # iv30 is in % → decimal

        # Expected move %
        if atm_iv is not None:
            expected_move_pct = atm_iv * math.sqrt(dte / 252.0) * 100.0
        else:
            expected_move_pct = None

        # Put/call volume ratio
        total_call_vol = calls["volume"].sum() if not calls.empty else 0
        total_put_vol = puts["volume"].sum() if not puts.empty else 0
        if total_call_vol and total_call_vol > 0:
            pc_ratio = round(total_put_vol / total_call_vol, 3)
        else:
            pc_ratio = None

        # --- Snapshot record ---
        snapshot_records.append({
            "symbol": sym,
            "as_of": as_of,
            "expiry": exp_str,
            "days_to_exp": dte,
            "atm_iv": round(atm_iv * 100, 2) if atm_iv else None,
            "expected_move_pct": round(expected_move_pct, 2) if expected_move_pct else None,
            "put_call_vol_ratio": pc_ratio,
        })

        # --- Enhanced analysis (skip very short-dated) ---
        if dte < MIN_DTE or atm_iv is None:
            continue

        # Probability cone
        cone = _compute_probability_cone(price, atm_iv, dte)

        # IV skew
        skew = _compute_iv_skew(calls, puts, price)

        # Liquidity assessment
        liq_label, chain_width, avg_spread = _assess_liquidity(
            calls, puts, price
        )

        # Directional bias
        bias = _bias_signal(skew, pc_ratio)

        # Unusual activity
        unusual = _find_unusual_activity(calls, puts)

        analysis_records.append({
            "symbol": sym,
            "as_of": as_of,
            "expiry": exp_str,
            "days_to_exp": dte,
            "current_price": round(price, 2),
            "range_68_low": cone["range_68_low"],
            "range_68_high": cone["range_68_high"],
            "range_95_low": cone["range_95_low"],
            "range_95_high": cone["range_95_high"],
            "atm_iv": round(atm_iv, 4),  # decimal (0.35 = 35%)
            "iv_skew": skew,
            "put_call_vol_ratio": pc_ratio,
            "bias_signal": bias,
            "liquidity_score": liq_label,
            "chain_width": chain_width,
            "avg_spread_pct": avg_spread,
            "unusual_strikes": json.dumps(unusual) if unusual else None,
        })

    return snapshot_records, analysis_records


def fetch_options_snapshot(
    symbols: list[str],
    as_of: date,
    max_expiries: int = 2,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    For each symbol, fetch options chain from CBOE and compute:
    1. Snapshot (lightweight): ATM IV, expected move, P/C ratio
    2. Analysis (enhanced): probability cone, skew, liquidity, unusual activity

    Uses concurrent requests (5 workers) + 403 blacklist for speed.
    Returns (snapshot_df, analysis_df).
    """
    # Filter out blacklisted symbols (403'd within BLACKLIST_DAYS)
    blacklist = _load_blacklist()
    cutoff = (as_of - __import__("datetime").timedelta(days=CBOE_BLACKLIST_DAYS)).isoformat()
    skipped = [s for s in symbols if blacklist.get(s, "") >= cutoff]
    fetch_symbols = [s for s in symbols if s not in blacklist or blacklist.get(s, "") < cutoff]

    if skipped:
        log.info("cboe_blacklist_skip", skipped=len(skipped), fetching=len(fetch_symbols))

    snapshot_records = []
    analysis_records = []
    fetch_failures = 0

    _blacklist_additions.clear()

    with ThreadPoolExecutor(max_workers=CBOE_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_process_symbol, sym, as_of, max_expiries): sym
            for sym in fetch_symbols
        }
        for future in as_completed(futures):
            sym = futures[future]
            try:
                snap, anal = future.result()
                snapshot_records.extend(snap)
                analysis_records.extend(anal)
                if snap:
                    log.info("options_fetched", symbol=sym, source="cboe",
                             expiries=len(snap))
                else:
                    fetch_failures += 1
            except Exception as e:
                fetch_failures += 1
                log.warning("options_fetch_error", symbol=sym, error=str(e))

    # Persist new blacklist entries
    if _blacklist_additions:
        blacklist.update(_blacklist_additions)
        _save_blacklist(blacklist)
        log.info("cboe_blacklist_updated", new_entries=len(_blacklist_additions),
                 total=len(blacklist))

    if fetch_failures > 0:
        total = len(fetch_symbols)
        log.warning("options_fetch_summary", total=total, failures=fetch_failures,
                     source="cboe",
                     success_rate=f"{(total - fetch_failures) / total * 100:.0f}%")

    def _to_df(records):
        if not records:
            return pl.DataFrame()
        return pl.DataFrame(records, infer_schema_length=None).with_columns([pl.col("as_of").cast(pl.Date)])

    return _to_df(snapshot_records), _to_df(analysis_records)


def upsert_options(con: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    if df.is_empty():
        return 0
    con.execute("""
        CREATE TABLE IF NOT EXISTS options_snapshot (
            symbol              VARCHAR NOT NULL,
            as_of               DATE NOT NULL,
            expiry              VARCHAR NOT NULL,
            days_to_exp         INTEGER,
            atm_iv              DOUBLE,      -- annualized IV in %
            expected_move_pct   DOUBLE,      -- market-implied move to expiry in %
            put_call_vol_ratio  DOUBLE,      -- >1 = bearish
            PRIMARY KEY (symbol, as_of, expiry)
        )
    """)
    con.register("opts_updates", df.to_arrow())
    con.execute("""
        INSERT OR REPLACE INTO options_snapshot
        SELECT symbol, as_of, expiry, days_to_exp,
               atm_iv, expected_move_pct, put_call_vol_ratio
        FROM opts_updates
    """)
    con.unregister("opts_updates")
    con.commit()
    return len(df)


def upsert_options_analysis(con: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Store enhanced options analysis in options_analysis table."""
    if df.is_empty():
        return 0
    con.register("opts_analysis_updates", df.to_arrow())
    con.execute("""
        INSERT OR REPLACE INTO options_analysis
        SELECT symbol, as_of, expiry, days_to_exp, current_price,
               range_68_low, range_68_high, range_95_low, range_95_high,
               atm_iv, iv_skew, put_call_vol_ratio, bias_signal,
               liquidity_score, chain_width, avg_spread_pct, unusual_strikes
        FROM opts_analysis_updates
    """)
    con.unregister("opts_analysis_updates")
    con.commit()
    return len(df)
