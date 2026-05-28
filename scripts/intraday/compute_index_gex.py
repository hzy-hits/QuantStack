"""Compute dealer GEX + skew + term structure for cash-settled indices.

Reads latest options_chain_quotes snapshot per symbol, computes per
(symbol, dte_bucket) metrics, persists to index_gex_snapshots.

Dealer GEX model (textbook):
  Assume dealers are short calls (retail buys calls) and long puts
  (retail buys puts). Net dealer gamma per contract:
    call → -OI × gamma × 100 × spot²  (dealer short call → negative gamma)
    put  → +OI × gamma × 100 × spot²  (dealer long put  → positive gamma)
  Net negative → dealers must buy on rallies / sell on dips (amplifier)
  Net positive → dealers sell rallies / buy dips (dampener)

Gamma flip strike: walk through strikes by spot offset; the strike where
running cumulative dealer GEX flips from positive to negative defines the
pivot. Above flip = dampener; below flip = amplifier (in classical
interpretation).

Usage:
  python3 scripts/intraday/compute_index_gex.py [--as-of YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"

# Universe of cash-settled indices we track intraday.
INDEX_SYMBOLS = ["^SPX", "^NDX", "^XSP", "^XND", "^MRUT", "^RUT", "^XEO", "^VIX"]

# DTE bucket definitions (each metric computed once per bucket).
DTE_BUCKETS = [
    ("0DTE", 0, 0),    # same-day (PM-settled)
    ("1DTE", 1, 1),    # next session
    ("WEEK", 2, 9),    # this Friday + next Friday + daily clusters
    ("MONTH", 10, 35), # 1-month
]

# Strike window for GEX computation (% from spot). Beyond this range
# contracts contribute negligibly to spot-pinned gamma exposure.
GEX_STRIKE_WINDOW_PCT = 0.03


def _ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS index_gex_snapshots (
            snapshot_time     TIMESTAMP NOT NULL,
            as_of             DATE NOT NULL,
            symbol            VARCHAR NOT NULL,
            dte_bucket        VARCHAR NOT NULL,
            spot              DOUBLE,
            chain_contracts   INTEGER,
            call_volume_total INTEGER,
            put_volume_total  INTEGER,
            call_oi_total     INTEGER,
            put_oi_total      INTEGER,
            pc_vol_ratio      DOUBLE,
            pc_oi_ratio       DOUBLE,
            net_dealer_gex    DOUBLE,
            gamma_flip_strike DOUBLE,
            atm_call_iv       DOUBLE,
            atm_put_iv        DOUBLE,
            skew_pts          DOUBLE,
            top_oi_call_strike DOUBLE,
            top_oi_call_oi    INTEGER,
            top_oi_put_strike DOUBLE,
            top_oi_put_oi     INTEGER,
            atm_iv            DOUBLE,
            computed_at       TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (snapshot_time, symbol, dte_bucket)
        )
        """
    )


def _per_bucket_metrics(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    as_of_str: str,
    bucket_label: str,
    min_dte: int,
    max_dte: int,
) -> dict[str, Any] | None:
    """Compute one (symbol, dte_bucket) row of metrics from latest snapshot."""
    # Spot price + chain stats
    base = con.execute(
        f"""
        SELECT
          MIN(current_price)             AS spot,
          COUNT(*)                       AS chain_contracts,
          SUM(CASE WHEN option_type='call' THEN COALESCE(volume,0) ELSE 0 END) AS call_vol,
          SUM(CASE WHEN option_type='put'  THEN COALESCE(volume,0) ELSE 0 END) AS put_vol,
          SUM(CASE WHEN option_type='call' THEN COALESCE(open_interest,0) ELSE 0 END) AS call_oi,
          SUM(CASE WHEN option_type='put'  THEN COALESCE(open_interest,0) ELSE 0 END) AS put_oi
        FROM options_chain_quotes
        WHERE symbol = '{symbol}'
          AND as_of  = '{as_of_str}'
          AND days_to_exp BETWEEN {min_dte} AND {max_dte}
        """
    ).fetchone()
    if not base or not base[0]:
        return None
    spot, chain_contracts, call_vol, put_vol, call_oi, put_oi = base
    if not chain_contracts:
        return None

    # Dealer GEX within the strike window
    gex_row = con.execute(
        f"""
        SELECT
          SUM(CASE WHEN option_type='call'
                   THEN -COALESCE(open_interest,0) * COALESCE(gamma,0) * 100 * current_price * current_price
                   ELSE 0 END) AS call_dealer_gex,
          SUM(CASE WHEN option_type='put'
                   THEN  COALESCE(open_interest,0) * COALESCE(gamma,0) * 100 * current_price * current_price
                   ELSE 0 END) AS put_dealer_gex
        FROM options_chain_quotes
        WHERE symbol = '{symbol}'
          AND as_of  = '{as_of_str}'
          AND days_to_exp BETWEEN {min_dte} AND {max_dte}
          AND ABS((strike - current_price) / current_price) <= {GEX_STRIKE_WINDOW_PCT}
          AND gamma IS NOT NULL
        """
    ).fetchone()
    net_gex = (gex_row[0] or 0.0) + (gex_row[1] or 0.0) if gex_row else 0.0

    # Gamma flip strike: cumulative GEX as you walk strikes from low to high;
    # the strike where running sum crosses zero is the flip. If never crosses
    # within the window, leave NULL.
    strikes_gex = con.execute(
        f"""
        SELECT strike,
               SUM(CASE WHEN option_type='call'
                        THEN -COALESCE(open_interest,0) * COALESCE(gamma,0) * 100 * current_price * current_price
                        ELSE 0 END) +
               SUM(CASE WHEN option_type='put'
                        THEN  COALESCE(open_interest,0) * COALESCE(gamma,0) * 100 * current_price * current_price
                        ELSE 0 END) AS strike_gex
        FROM options_chain_quotes
        WHERE symbol = '{symbol}'
          AND as_of  = '{as_of_str}'
          AND days_to_exp BETWEEN {min_dte} AND {max_dte}
          AND ABS((strike - current_price) / current_price) <= {GEX_STRIKE_WINDOW_PCT}
          AND gamma IS NOT NULL
        GROUP BY strike ORDER BY strike
        """
    ).fetchall()
    flip = None
    cum = 0.0
    prev_strike = None
    for strike, sgex in strikes_gex:
        cum_prev = cum
        cum += sgex
        if prev_strike is not None and (cum_prev > 0) != (cum > 0):
            flip = strike
            break
        prev_strike = strike

    # Skew (ATM-ish call IV vs put IV; ±0.5% strike window)
    skew = con.execute(
        f"""
        SELECT
          AVG(CASE WHEN option_type='call' AND implied_volatility > 0 THEN implied_volatility END) AS call_iv,
          AVG(CASE WHEN option_type='put'  AND implied_volatility > 0 THEN implied_volatility END) AS put_iv,
          AVG(CASE WHEN implied_volatility > 0 THEN implied_volatility END) AS atm_iv
        FROM options_chain_quotes
        WHERE symbol = '{symbol}'
          AND as_of  = '{as_of_str}'
          AND days_to_exp BETWEEN {min_dte} AND {max_dte}
          AND ABS((strike - current_price) / current_price) <= 0.005
        """
    ).fetchone()
    call_iv = skew[0] if skew else None
    put_iv = skew[1] if skew else None
    atm_iv = skew[2] if skew else None
    skew_pts = (put_iv - call_iv) if (call_iv and put_iv) else None

    # Top OI strikes (one for calls, one for puts, within ±2% spot)
    top_call = con.execute(
        f"""
        SELECT strike, open_interest FROM options_chain_quotes
        WHERE symbol = '{symbol}' AND as_of = '{as_of_str}'
          AND days_to_exp BETWEEN {min_dte} AND {max_dte}
          AND option_type='call'
          AND ABS((strike - current_price) / current_price) <= 0.02
          AND open_interest > 0
        ORDER BY open_interest DESC LIMIT 1
        """
    ).fetchone()
    top_put = con.execute(
        f"""
        SELECT strike, open_interest FROM options_chain_quotes
        WHERE symbol = '{symbol}' AND as_of = '{as_of_str}'
          AND days_to_exp BETWEEN {min_dte} AND {max_dte}
          AND option_type='put'
          AND ABS((strike - current_price) / current_price) <= 0.02
          AND open_interest > 0
        ORDER BY open_interest DESC LIMIT 1
        """
    ).fetchone()

    pc_vol = (put_vol / call_vol) if call_vol else None
    pc_oi = (put_oi / call_oi) if call_oi else None

    return {
        "spot": spot,
        "chain_contracts": chain_contracts,
        "call_volume_total": call_vol,
        "put_volume_total": put_vol,
        "call_oi_total": call_oi,
        "put_oi_total": put_oi,
        "pc_vol_ratio": pc_vol,
        "pc_oi_ratio": pc_oi,
        "net_dealer_gex": net_gex,
        "gamma_flip_strike": flip,
        "atm_call_iv": call_iv,
        "atm_put_iv": put_iv,
        "skew_pts": skew_pts,
        "top_oi_call_strike": top_call[0] if top_call else None,
        "top_oi_call_oi": top_call[1] if top_call else None,
        "top_oi_put_strike": top_put[0] if top_put else None,
        "top_oi_put_oi": top_put[1] if top_put else None,
        "atm_iv": atm_iv,
    }


def _persist(
    con: duckdb.DuckDBPyConnection,
    snapshot_time: datetime,
    as_of: date,
    symbol: str,
    bucket: str,
    m: dict[str, Any],
) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO index_gex_snapshots (
          snapshot_time, as_of, symbol, dte_bucket,
          spot, chain_contracts,
          call_volume_total, put_volume_total, call_oi_total, put_oi_total,
          pc_vol_ratio, pc_oi_ratio,
          net_dealer_gex, gamma_flip_strike,
          atm_call_iv, atm_put_iv, skew_pts,
          top_oi_call_strike, top_oi_call_oi, top_oi_put_strike, top_oi_put_oi,
          atm_iv
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            snapshot_time, as_of, symbol, bucket,
            m["spot"], m["chain_contracts"],
            m["call_volume_total"], m["put_volume_total"], m["call_oi_total"], m["put_oi_total"],
            m["pc_vol_ratio"], m["pc_oi_ratio"],
            m["net_dealer_gex"], m["gamma_flip_strike"],
            m["atm_call_iv"], m["atm_put_iv"], m["skew_pts"],
            m["top_oi_call_strike"], m["top_oi_call_oi"], m["top_oi_put_strike"], m["top_oi_put_oi"],
            m["atm_iv"],
        ],
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", default=None, help="Override as_of date (default: latest in DB)")
    ap.add_argument("--snapshot-time", default=None, help="Override snapshot timestamp (default: now)")
    args = ap.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))
    _ensure_schema(con)

    snapshot_time = (
        datetime.fromisoformat(args.snapshot_time)
        if args.snapshot_time else datetime.utcnow()
    )

    if args.as_of:
        as_of = date.fromisoformat(args.as_of)
    else:
        latest = con.execute("SELECT MAX(as_of) FROM options_chain_quotes").fetchone()
        if not latest or not latest[0]:
            raise SystemExit("no options_chain_quotes data found")
        as_of = latest[0] if isinstance(latest[0], date) else date.fromisoformat(str(latest[0]))
    as_of_str = as_of.isoformat()

    print(f"=== index_gex compute  as_of={as_of}  snapshot_time={snapshot_time.isoformat()}")
    written = 0
    for symbol in INDEX_SYMBOLS:
        # Skip indices that aren't in the snapshot
        present = con.execute(
            f"SELECT COUNT(*) FROM options_chain_quotes WHERE symbol='{symbol}' AND as_of='{as_of_str}'"
        ).fetchone()[0]
        if not present:
            print(f"  {symbol:7s}  SKIP (no data for {as_of_str})")
            continue
        for bucket_label, min_dte, max_dte in DTE_BUCKETS:
            metrics = _per_bucket_metrics(con, symbol, as_of_str, bucket_label, min_dte, max_dte)
            if not metrics:
                continue
            _persist(con, snapshot_time, as_of, symbol, bucket_label, metrics)
            gex = metrics["net_dealer_gex"]
            flip = metrics["gamma_flip_strike"]
            spot = metrics["spot"]
            print(
                f"  {symbol:7s} {bucket_label:5s}  spot={spot or 0:.2f}  "
                f"GEX={gex/1e12:+.2f}T  flip={flip or 0:.2f}  "
                f"P/C_vol={metrics['pc_vol_ratio'] or 0:.2f}  "
                f"skew={(metrics['skew_pts'] or 0)*100:.1f}pp"
            )
            written += 1
    con.execute("CHECKPOINT")
    con.close()
    print(f"=== wrote {written} (symbol, bucket) rows to index_gex_snapshots")


if __name__ == "__main__":
    main()
