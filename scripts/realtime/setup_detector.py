"""Detect tradeable setups from realtime_quotes state.

A "setup" is a state crossing that warrants LLM attention. Avoids
LLM-noise by only firing when the state genuinely changes.

Setup types covered (V1, conservative):
  - gamma_flip_break:  spot crosses below gamma_flip_strike (SHORT-gamma
                       regime activates; vol amplifier mode)
  - gamma_flip_recover: spot crosses back above gamma_flip_strike
  - skew_spike:        ATM put_iv - call_iv >= 0.30 (extreme put bid)
                       AND was < 0.25 in last 10 min
  - vol_burst:         _last realized 5min stddev > 2x trailing 30min stddev
                       (intraday vol acceleration)
  - vix_spike:         ^VIX up >5% in last 10 min

For each setup, emits a Setup dict:
  {ts, symbol, type, prev_state, current_state, context_metrics}

Usage (daemon):
  python3 scripts/realtime/setup_detector.py --poll-seconds 30

Each detection triggers llm_advisor + notify_telegram via the daemon.py
orchestrator (this module just detects; doesn't fire LLM).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
EVENTS_PATH = STACK_ROOT / "reports" / "intraday" / "setup_events.jsonl"

# Watched indices for setup detection
WATCH_SYMBOLS = ["SPX", "NDX", "XSP", "RUT"]

# Setup thresholds
GAMMA_FLIP_BUFFER_PCT = 0.001   # cross by 0.1% to confirm break
SKEW_SPIKE_THRESHOLD = 0.30     # put-call IV diff (decimal pp)
SKEW_RECENT_BASELINE = 0.25
VOL_BURST_MULTIPLIER = 2.0
VIX_SPIKE_PCT = 0.05            # 5% in 10min


@dataclass
class Setup:
    ts: str
    symbol: str
    type: str
    summary: str
    context: dict[str, Any] = field(default_factory=dict)


def _ensure_events_dir() -> None:
    EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)


def _emit(setup: Setup) -> None:
    _ensure_events_dir()
    with EVENTS_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(setup), default=str) + "\n")


def _latest_field(con: duckdb.DuckDBPyConnection, symbol: str, field: str) -> tuple[datetime, float] | None:
    row = con.execute(
        """
        SELECT ingested_at, value FROM realtime_quotes
        WHERE symbol = ? AND field = ?
        ORDER BY ingested_at DESC LIMIT 1
        """,
        [symbol, field],
    ).fetchone()
    if not row or row[1] is None:
        return None
    return row[0], row[1]


def _prev_field(
    con: duckdb.DuckDBPyConnection, symbol: str, field: str, minutes_back: int
) -> tuple[datetime, float] | None:
    cutoff = datetime.utcnow() - timedelta(minutes=minutes_back)
    row = con.execute(
        """
        SELECT ingested_at, value FROM realtime_quotes
        WHERE symbol = ? AND field = ? AND ingested_at <= ?
        ORDER BY ingested_at DESC LIMIT 1
        """,
        [symbol, field, cutoff],
    ).fetchone()
    if not row or row[1] is None:
        return None
    return row[0], row[1]


def _series(
    con: duckdb.DuckDBPyConnection, symbol: str, field: str, minutes: int
) -> list[float]:
    cutoff = datetime.utcnow() - timedelta(minutes=minutes)
    rows = con.execute(
        """
        SELECT value FROM realtime_quotes
        WHERE symbol = ? AND field = ? AND ingested_at >= ?
          AND value IS NOT NULL
        ORDER BY ingested_at
        """,
        [symbol, field, cutoff],
    ).fetchall()
    return [r[0] for r in rows]


def _stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return var**0.5


def check_gamma_flip(con: duckdb.DuckDBPyConnection, symbol: str) -> Setup | None:
    """spot crosses gamma flip strike from above → below = vol amplifier on."""
    spot = _latest_field(con, symbol, "LAST")
    flip = _latest_field(con, symbol, "FLIP")
    if not spot or not flip:
        return None
    spot_now = spot[1]
    flip_lvl = flip[1]
    # Look 5min back for spot to detect transition
    prev_spot = _prev_field(con, symbol, "LAST", minutes_back=5)
    if not prev_spot:
        return None
    spot_prev = prev_spot[1]
    # Break: was above flip, now below (with buffer)
    if spot_prev > flip_lvl and spot_now < flip_lvl * (1 - GAMMA_FLIP_BUFFER_PCT):
        return Setup(
            ts=datetime.utcnow().isoformat(timespec="seconds"),
            symbol=symbol,
            type="gamma_flip_break",
            summary=f"{symbol} spot {spot_now:.2f} 跌破 gamma flip {flip_lvl:.0f} → vol amplifier 启动",
            context={"spot_prev": spot_prev, "spot_now": spot_now, "flip": flip_lvl},
        )
    # Recovery: was below, now above
    if spot_prev < flip_lvl and spot_now > flip_lvl * (1 + GAMMA_FLIP_BUFFER_PCT):
        return Setup(
            ts=datetime.utcnow().isoformat(timespec="seconds"),
            symbol=symbol,
            type="gamma_flip_recover",
            summary=f"{symbol} spot {spot_now:.2f} 重回 gamma flip {flip_lvl:.0f} 之上 → vol 抑制器恢复",
            context={"spot_prev": spot_prev, "spot_now": spot_now, "flip": flip_lvl},
        )
    return None


def check_skew_spike(con: duckdb.DuckDBPyConnection, symbol: str) -> Setup | None:
    """Put IV - Call IV jumps from baseline to spike threshold."""
    call_iv = _latest_field(con, symbol, "ATM_CALL_IV")
    put_iv = _latest_field(con, symbol, "ATM_PUT_IV")
    if not call_iv or not put_iv:
        return None
    skew_now = put_iv[1] - call_iv[1]
    # Need historical baseline (10 min ago)
    prev_call = _prev_field(con, symbol, "ATM_CALL_IV", minutes_back=10)
    prev_put = _prev_field(con, symbol, "ATM_PUT_IV", minutes_back=10)
    if not prev_call or not prev_put:
        return None
    skew_prev = prev_put[1] - prev_call[1]
    if skew_now >= SKEW_SPIKE_THRESHOLD and skew_prev < SKEW_RECENT_BASELINE:
        return Setup(
            ts=datetime.utcnow().isoformat(timespec="seconds"),
            symbol=symbol,
            type="skew_spike",
            summary=f"{symbol} put-call IV skew {skew_prev*100:+.1f}pp → {skew_now*100:+.1f}pp 急升,下行恐惧",
            context={
                "skew_prev_pp": skew_prev * 100,
                "skew_now_pp": skew_now * 100,
                "put_iv": put_iv[1],
                "call_iv": call_iv[1],
            },
        )
    return None


def check_vol_burst(con: duckdb.DuckDBPyConnection, symbol: str) -> Setup | None:
    """Realized intraday vol accelerates 2x vs trailing baseline."""
    recent = _series(con, symbol, "LAST", minutes=5)
    trailing = _series(con, symbol, "LAST", minutes=30)
    if len(recent) < 5 or len(trailing) < 10:
        return None
    recent_sd = _stddev(recent)
    trailing_sd = _stddev(trailing)
    if trailing_sd <= 0:
        return None
    ratio = recent_sd / trailing_sd
    if ratio >= VOL_BURST_MULTIPLIER and recent_sd > 0.5:  # absolute minimum
        return Setup(
            ts=datetime.utcnow().isoformat(timespec="seconds"),
            symbol=symbol,
            type="vol_burst",
            summary=f"{symbol} 5min 实际波动 {recent_sd:.2f} vs 30min {trailing_sd:.2f},放大 {ratio:.1f}x",
            context={"recent_sd": recent_sd, "trailing_sd": trailing_sd, "ratio": ratio},
        )
    return None


def check_vix_spike(con: duckdb.DuckDBPyConnection) -> Setup | None:
    vix_now = _latest_field(con, "VIX", "LAST")
    vix_10m = _prev_field(con, "VIX", "LAST", minutes_back=10)
    if not vix_now or not vix_10m:
        return None
    pct = (vix_now[1] - vix_10m[1]) / vix_10m[1]
    if pct >= VIX_SPIKE_PCT:
        return Setup(
            ts=datetime.utcnow().isoformat(timespec="seconds"),
            symbol="VIX",
            type="vix_spike",
            summary=f"VIX 10min 上涨 {pct*100:+.1f}% ({vix_10m[1]:.2f} → {vix_now[1]:.2f}),全市场避险加速",
            context={"vix_prev": vix_10m[1], "vix_now": vix_now[1], "pct_change": pct},
        )
    return None


# In-memory dedup so we don't re-fire same setup every poll
_recent_fires: dict[str, datetime] = {}
_DEDUP_WINDOW_MIN = 15


def _should_fire(setup: Setup) -> bool:
    key = f"{setup.symbol}:{setup.type}"
    now = datetime.utcnow()
    last = _recent_fires.get(key)
    if last and (now - last).total_seconds() < _DEDUP_WINDOW_MIN * 60:
        return False
    _recent_fires[key] = now
    return True


def detect_once(con: duckdb.DuckDBPyConnection) -> list[Setup]:
    out: list[Setup] = []
    for sym in WATCH_SYMBOLS:
        for check in (check_gamma_flip, check_skew_spike, check_vol_burst):
            setup = check(con, sym)
            if setup and _should_fire(setup):
                out.append(setup)
                _emit(setup)
    vix = check_vix_spike(con)
    if vix and _should_fire(vix):
        out.append(vix)
        _emit(vix)
    return out


def run(poll_seconds: float) -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH), read_only=True)
    print(f"[setup_detector] watching {WATCH_SYMBOLS} poll_every={poll_seconds}s")
    try:
        while True:
            try:
                setups = detect_once(con)
                for s in setups:
                    print(f"[setup_detector] FIRE {s.type} {s.symbol}: {s.summary}")
            except duckdb.Error as e:
                print(f"[setup_detector] DB error: {e}", file=sys.stderr)
            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        print("[setup_detector] stopped by user")
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--poll-seconds", type=float, default=30.0)
    args = ap.parse_args()
    run(args.poll_seconds)


if __name__ == "__main__":
    main()
