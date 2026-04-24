"""DuckDB connection and schema management."""
from __future__ import annotations

from datetime import date
from pathlib import Path
import shutil

import duckdb


DDL = """
-- Daily OHLCV + adjusted close
CREATE TABLE IF NOT EXISTS prices_daily (
    symbol      VARCHAR NOT NULL,
    date        DATE    NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      BIGINT,
    adj_close   DOUBLE,
    PRIMARY KEY (symbol, date)
);

-- Earnings calendar and EPS surprise history
CREATE TABLE IF NOT EXISTS earnings_calendar (
    symbol          VARCHAR NOT NULL,
    report_date     DATE    NOT NULL,
    fiscal_period   VARCHAR,
    estimate_eps    DOUBLE,
    actual_eps      DOUBLE,
    surprise_pct    DOUBLE,
    PRIMARY KEY (symbol, report_date)
);

-- Daily risk/probability analysis outputs (replaces signals_daily)
CREATE TABLE IF NOT EXISTS analysis_daily (
    symbol          VARCHAR NOT NULL,
    date            DATE    NOT NULL,
    module_name     VARCHAR NOT NULL,   -- 'momentum_risk' | 'earnings_risk'
    -- Probability outputs
    trend_prob      DOUBLE,             -- P(trend continues)
    p_upside        DOUBLE,             -- P(upside event in horizon)
    p_downside      DOUBLE,             -- P(downside event in horizon)
    -- Risk metrics
    daily_risk_usd  DOUBLE,             -- ATR in dollar terms
    expected_move_pct DOUBLE,           -- expected absolute move
    -- Statistical context
    z_score         DOUBLE,
    p_value_raw     DOUBLE,
    p_value_bonf    DOUBLE,             -- Bonferroni-corrected
    strength_bucket VARCHAR,            -- strong|moderate|weak|inconclusive
    regime          VARCHAR,            -- trending|mean_reverting|noisy
    -- Extra fields stored as JSON string (parse in Python)
    details         VARCHAR,
    PRIMARY KEY (symbol, date, module_name)
);

-- Simulated portfolio daily snapshot (exposure tracker)
CREATE TABLE IF NOT EXISTS portfolio_daily (
    date            DATE    NOT NULL,
    symbol          VARCHAR NOT NULL,
    weight          DOUBLE,
    shares          DOUBLE,
    price           DOUBLE,
    market_value    DOUBLE,
    cost_basis      DOUBLE,
    unrealized_pnl  DOUBLE,
    PRIMARY KEY (date, symbol)
);

-- Portfolio NAV curve
CREATE TABLE IF NOT EXISTS portfolio_nav (
    date              DATE PRIMARY KEY,
    total_value       DOUBLE,
    cash              DOUBLE,
    invested          DOUBLE,
    daily_return      DOUBLE,
    cumulative_return DOUBLE,
    drawdown          DOUBLE,
    sharpe_rolling    DOUBLE
);

-- Macro time series (FRED)
CREATE TABLE IF NOT EXISTS macro_daily (
    date        DATE    NOT NULL,
    series_id   VARCHAR NOT NULL,
    series_name VARCHAR,
    value       DOUBLE,
    PRIMARY KEY (date, series_id)
);

-- Resolved probability forecasts for Brier scoring
CREATE TABLE IF NOT EXISTS forecast_outcomes (
    forecast_id     VARCHAR NOT NULL,   -- '{symbol}_{module}_{date}_{horizon}d'
    symbol          VARCHAR,
    module_name     VARCHAR,
    forecast_date   DATE,
    horizon_days    INTEGER,
    resolution_date DATE,
    p_forecast      DOUBLE,
    outcome         INTEGER,            -- 0 or 1
    brier_contrib   DOUBLE,            -- (p - y)^2
    PRIMARY KEY (forecast_id)
);

-- Options-derived forward probability analysis
CREATE TABLE IF NOT EXISTS options_analysis (
    symbol              VARCHAR NOT NULL,
    as_of               DATE NOT NULL,
    expiry              VARCHAR NOT NULL,
    days_to_exp         INTEGER,
    current_price       DOUBLE,
    -- Probability cone (lognormal, from ATM IV)
    range_68_low        DOUBLE,
    range_68_high       DOUBLE,
    range_95_low        DOUBLE,
    range_95_high       DOUBLE,
    -- IV analysis
    atm_iv              DOUBLE,           -- annualized, decimal (0.65 = 65%)
    iv_skew             DOUBLE,           -- OTM put IV / OTM call IV at ~5% moneyness
    -- Directional bias
    put_call_vol_ratio  DOUBLE,
    bias_signal         VARCHAR,          -- 'bullish' | 'bearish' | 'neutral'
    -- Data quality
    liquidity_score     VARCHAR,          -- 'good' | 'fair' | 'poor'
    chain_width         INTEGER,          -- liquid strikes near ATM
    avg_spread_pct      DOUBLE,           -- avg bid-ask spread %
    -- Unusual activity
    unusual_strikes     VARCHAR,          -- JSON array
    PRIMARY KEY (symbol, as_of, expiry)
);

-- S&P 500 / Nasdaq 100 constituent add/remove events
CREATE TABLE IF NOT EXISTS index_changes (
    index_symbol    VARCHAR NOT NULL,
    symbol          VARCHAR NOT NULL,
    change_type     VARCHAR NOT NULL,   -- 'add' | 'remove'
    change_date     DATE NOT NULL,
    fetched_at      TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (index_symbol, symbol, change_date)
);

-- Dividend history (ex-date + cash amount)
CREATE TABLE IF NOT EXISTS dividends (
    symbol      VARCHAR NOT NULL,
    ex_date     DATE    NOT NULL,
    cash_amount DOUBLE  NOT NULL,
    is_special  BOOLEAN DEFAULT FALSE,
    fetched_at  TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (symbol, ex_date)
);

-- Run governance log
CREATE TABLE IF NOT EXISTS run_log (
    run_id      VARCHAR NOT NULL,
    timestamp   TIMESTAMP DEFAULT current_timestamp,
    step        VARCHAR,                -- fetch|analysis|portfolio|report|delivery
    status      VARCHAR,               -- ok|error|partial
    records     INTEGER,
    message     VARCHAR
);

-- US stock symbols master (refreshed weekly from Finnhub)
CREATE TABLE IF NOT EXISTS us_symbols (
    symbol      VARCHAR PRIMARY KEY,
    name        VARCHAR,
    type        VARCHAR,
    exchange    VARCHAR,
    mic         VARCHAR,
    fetched_at  TIMESTAMP DEFAULT current_timestamp
);

-- Company fundamentals (keyed by symbol+date for revision tracking)
CREATE TABLE IF NOT EXISTS company_profile (
    symbol          VARCHAR NOT NULL,
    as_of           DATE NOT NULL,
    company_name    VARCHAR,
    sector          VARCHAR,
    industry        VARCHAR,
    market_cap      DOUBLE,
    pe_ttm          DOUBLE,
    pe_fwd          DOUBLE,
    ps_ratio        DOUBLE,
    pb_ratio        DOUBLE,
    ev_ebitda       DOUBLE,
    roe             DOUBLE,
    fcf_yield       DOUBLE,
    revenue_growth  DOUBLE,
    analyst_target  DOUBLE,
    analyst_count   INTEGER,
    recommendation  DOUBLE,
    PRIMARY KEY (symbol, as_of)
);

-- Options-derived sentiment indicators (VRP + EWMA z-scores)
CREATE TABLE IF NOT EXISTS options_sentiment (
    symbol       VARCHAR NOT NULL,
    as_of        DATE NOT NULL,
    pc_ratio_z   DOUBLE,
    skew_z       DOUBLE,
    vrp          DOUBLE,
    iv_ann       DOUBLE,
    rv_ann       DOUBLE,
    vrp_z        DOUBLE,
    pc_ratio_raw DOUBLE,
    skew_raw     DOUBLE,
    computed_at  TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (symbol, as_of)
);

-- Cointegrated pairs (Engle-Granger within sector)
CREATE TABLE IF NOT EXISTS cointegrated_pairs (
    symbol_a    VARCHAR NOT NULL,
    symbol_b    VARCHAR NOT NULL,
    sector      VARCHAR,
    beta        DOUBLE,
    adf_pvalue  DOUBLE,
    ou_theta    DOUBLE,
    ou_mu       DOUBLE,
    half_life_days DOUBLE,
    spread_zscore  DOUBLE,
    fdr_significant BOOLEAN,
    computed_at DATE NOT NULL,
    PRIMARY KEY (symbol_a, symbol_b, computed_at)
);

-- Granger causality pairs
CREATE TABLE IF NOT EXISTS granger_pairs (
    leader      VARCHAR NOT NULL,
    follower    VARCHAR NOT NULL,
    lag_days    INTEGER,
    f_statistic DOUBLE,
    p_value     DOUBLE,
    fdr_significant BOOLEAN,
    sector      VARCHAR,
    computed_at DATE NOT NULL,
    PRIMARY KEY (leader, follower, computed_at)
);

-- Earnings event study CAR
CREATE TABLE IF NOT EXISTS earnings_car (
    symbol          VARCHAR NOT NULL,
    event_date      DATE NOT NULL,
    car_1d          DOUBLE,
    car_3d          DOUBLE,
    car_5d          DOUBLE,
    car_10d         DOUBLE,
    pre_event_beta  DOUBLE,
    computed_at     DATE NOT NULL,
    PRIMARY KEY (symbol, event_date)
);

-- Kalman-filtered dynamic betas
CREATE TABLE IF NOT EXISTS kalman_betas (
    symbol          VARCHAR NOT NULL,
    beta_current    DOUBLE,
    beta_60d_mean   DOUBLE,
    divergence      DOUBLE,
    beta_std        DOUBLE,
    computed_at     DATE NOT NULL,
    PRIMARY KEY (symbol, computed_at)
);

-- Report-layer decision ledger for postmortem review
CREATE TABLE IF NOT EXISTS report_decisions (
    report_date          DATE NOT NULL,
    session              VARCHAR NOT NULL,
    symbol               VARCHAR NOT NULL,
    selection_status     VARCHAR NOT NULL,   -- selected | ignored
    rank_order           INTEGER,
    report_bucket        VARCHAR,
    signal_direction     VARCHAR,
    signal_confidence    VARCHAR,
    headline_mode        VARCHAR,
    composite_score      DOUBLE,
    report_score         DOUBLE,
    tradability_score    DOUBLE,
    lane_reason          VARCHAR,
    execution_mode       VARCHAR,
    entry_price          DOUBLE,
    reference_price      DOUBLE,
    stop_price           DOUBLE,
    target_price         DOUBLE,
    rr_ratio             DOUBLE,
    expected_move_pct    DOUBLE,
    gap_pct              DOUBLE,
    primary_reason       VARCHAR,
    details_json         VARCHAR,
    created_at           TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (report_date, session, symbol, selection_status)
);

-- Realized outcomes for prior report decisions
CREATE TABLE IF NOT EXISTS report_outcomes (
    report_date              DATE NOT NULL,
    session                  VARCHAR NOT NULL,
    symbol                   VARCHAR NOT NULL,
    selection_status         VARCHAR NOT NULL,
    evaluation_date          DATE NOT NULL,
    next_trade_date          DATE,
    entry_price              DOUBLE,
    reference_price          DOUBLE,
    next_open                DOUBLE,
    next_close               DOUBLE,
    hold_3d_close            DOUBLE,
    next_open_ret_pct        DOUBLE,
    next_close_ret_pct       DOUBLE,
    hold_3d_ret_pct          DOUBLE,
    ref_gap_pct              DOUBLE,
    move_consumed_ratio      DOUBLE,
    execution_slippage_pct   DOUBLE,
    alpha_remaining_pct      DOUBLE,
    data_ready               BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (report_date, session, symbol, selection_status)
);

-- Classification layer for report postmortem + Factor Lab feedback hooks
CREATE TABLE IF NOT EXISTS alpha_postmortem (
    report_date              DATE NOT NULL,
    session                  VARCHAR NOT NULL,
    symbol                   VARCHAR NOT NULL,
    selection_status         VARCHAR NOT NULL,
    evaluation_date          DATE NOT NULL,
    label                    VARCHAR NOT NULL,
    review_note              VARCHAR,
    factor_feedback_action   VARCHAR,
    factor_feedback_weight   DOUBLE,
    best_ret_pct             DOUBLE,
    move_consumed_ratio      DOUBLE,
    execution_slippage_pct   DOUBLE,
    alpha_remaining_pct      DOUBLE,
    PRIMARY KEY (report_date, session, symbol, selection_status)
);
"""


class _ManagedConnection:
    """Thin wrapper that optionally holds a file lock alongside DuckDB.

    Delegates everything to the underlying ``duckdb.DuckDBPyConnection`` so
    callers don't need to change.  The lock is released when ``close()`` is
    called or the wrapper is used as a context-manager.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection, lock_fd=None) -> None:  # noqa: ANN001
        object.__setattr__(self, "_con", con)
        object.__setattr__(self, "_lock_fd", lock_fd)

    # --- context-manager ---------------------------------------------------
    def __enter__(self):  # noqa: ANN204
        return self

    def __exit__(self, *exc):  # noqa: ANN002 ANN204
        self.close()

    def close(self) -> None:
        try:
            self._con.close()
        finally:
            if self._lock_fd is not None:
                self._lock_fd.close()

    # --- transparent proxy -------------------------------------------------
    def __getattr__(self, name: str):  # noqa: ANN204
        return getattr(self._con, name)


def _open_connection(
    db_path: str | Path = "data/quant.duckdb",
    *,
    read_only: bool,
    take_write_lock: bool,
) -> duckdb.DuckDBPyConnection:
    path = Path(db_path)
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)

    lock_fd = None
    if take_write_lock:
        # Writers keep the historical single-writer guard; readers skip it.
        import fcntl

        lock_path = path.with_suffix(".lock")
        lock_fd = open(lock_path, "w")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            lock_fd.close()
            raise RuntimeError(
                f"DuckDB is locked by another process: {path}\n"
                "Another pipeline run may be in progress. Check /tmp/quant-research-pipeline.lock"
            )

    try:
        con = duckdb.connect(str(path), read_only=read_only)
    except Exception:
        if lock_fd is not None:
            lock_fd.close()
        raise
    return _ManagedConnection(con, lock_fd)  # type: ignore[return-value]


def connect_write(db_path: str | Path = "data/quant.duckdb") -> duckdb.DuckDBPyConnection:
    return _open_connection(db_path, read_only=False, take_write_lock=True)


def connect_readonly(db_path: str | Path = "data/quant.duckdb") -> duckdb.DuckDBPyConnection:
    return _open_connection(db_path, read_only=True, take_write_lock=False)


def connect(db_path: str | Path = "data/quant.duckdb") -> duckdb.DuckDBPyConnection:
    return connect_write(db_path)


def init_schema(db_path: str | Path = "data/quant.duckdb") -> None:
    with connect_write(db_path) as con:
        con.execute(DDL)
        con.execute("CHECKPOINT")
        con.commit()
    print(f"Schema initialized at {db_path}")


def snapshot_path(base_path: str | Path, as_of: date | str, session: str) -> Path:
    """Derive a session-aware snapshot filename from a base DuckDB path."""
    path = Path(base_path)
    as_of_str = as_of.isoformat() if isinstance(as_of, date) else str(as_of)
    session = str(session).strip().lower()
    stem = path.stem or "quant"
    suffix = path.suffix or ".duckdb"
    return path.with_name(f"{stem}_{as_of_str}_{session}{suffix}")


def copy_database(src: str | Path, dst: str | Path) -> None:
    src_path = Path(src)
    dst_path = Path(dst)
    if not src_path.exists():
        raise FileNotFoundError(f"database not found: {src_path}")

    dst_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_path, dst_path)

    src_wal = src_path.with_suffix(src_path.suffix + ".wal")
    dst_wal = dst_path.with_suffix(dst_path.suffix + ".wal")
    if src_wal.exists():
        shutil.copy2(src_wal, dst_wal)
    elif dst_wal.exists():
        dst_wal.unlink()


def log_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    step: str,
    status: str,
    records: int = 0,
    message: str = "",
) -> None:
    con.execute(
        "INSERT INTO run_log (run_id, step, status, records, message) VALUES (?,?,?,?,?)",
        [run_id, step, status, records, message],
    )
