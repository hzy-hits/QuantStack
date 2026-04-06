#!/usr/bin/env python3
"""
Main daily pipeline entry point.

Output: a structured Markdown payload file in reports/{date}_payload.md
Feed that file to whichever agent you want (Claude Code, Codex, OpenClaw, etc.)
The agent writes the narrative. The program computes the probabilities.

Usage:
    python scripts/run_daily.py               # normal daily run
    python scripts/run_daily.py --init        # first-time: fetch 2yr history
    python scripts/run_daily.py --date 2026-03-07
    python scripts/run_daily.py --skip-rust   # skip Rust fetcher (debug)

Pipeline:
    1. Build universe (S&P 500 from Wikipedia + ETFs + commodities)
    2. [Rust] Fetch news, macro, SEC 8-K filings, Polymarket, earnings
    3. Fetch prices + options chains (yfinance)
    4. Compute probabilities: momentum regime P, earnings drift P, Bonferroni
    5. Score and filter → top 10-20 notable items
    6. Render structured Markdown payload
    → Feed payload to agent of your choice
"""
from __future__ import annotations

import subprocess
import sys
import uuid
import argparse
from datetime import date, datetime
from pathlib import Path

import structlog

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from quant_bot.config.settings import Settings
from quant_bot.storage.db import connect, init_schema, log_run
from quant_bot.universe.builder import build_universe
from quant_bot.data_ingestion.prices import fetch_and_store_prices
from quant_bot.data_ingestion.symbols import fetch_us_symbols
from quant_bot.data_ingestion.fundamentals import fetch_fundamentals
from quant_bot.data_ingestion.options import (
    fetch_options_snapshot, upsert_options, upsert_options_analysis,
    is_options_eligible, OPTIONS_PROXY_MAP,
)
from quant_bot.analytics.momentum_risk import run_momentum_risk, store_analysis
from quant_bot.analytics.earnings_risk import run_earnings_risk
from quant_bot.analytics.mean_reversion import run_mean_reversion, store_mean_reversion
from quant_bot.analytics.breakout import run_breakout, store_breakout
from quant_bot.analytics.value_score import compute_value_scores
from quant_bot.analytics.variance_premium import compute_vrp, store_vrp
from quant_bot.analytics.sentiment_ewma import compute_sentiment_ewma, store_sentiment
from quant_bot.analytics.covariance import compute_covariance
from quant_bot.analytics.pairs import find_cointegrated_pairs, store_cointegrated_pairs
from quant_bot.analytics.granger import find_granger_leaders, store_granger_pairs
from quant_bot.analytics.event_study import compute_earnings_car, store_earnings_car
from quant_bot.analytics.kalman_beta import compute_kalman_betas, store_kalman_betas
from quant_bot.data_ingestion.dividends import fetch_and_store_dividends
from quant_bot.screens.dividend_dips import run_dyp_screen
from quant_bot.screens.broad_screen import run_broad_screen
from quant_bot.analytics.dividend_dip_score import score_dyp_candidates
from quant_bot.analytics.clustering import compute_clusters
from quant_bot.analytics.risk_params import compute_risk_params
from quant_bot.analytics.contradictions import detect_contradictions
from quant_bot.analytics.scorecard import compute_scorecard
from quant_bot.analytics.news_quality import assess_news_quality
from quant_bot.analytics.portfolio_risk import compute_portfolio_risk
from quant_bot.filtering.notable import build_notable_items
from quant_bot.signals.classify import classify_all
from quant_bot.reporting.bundle import build_report_bundle, compute_options_extremes
from quant_bot.reporting.charts import generate_daily_charts
from quant_bot.reporting.render import render_payload_md

log = structlog.get_logger()


def run_rust_fetcher(cfg: Settings, symbols: list[str], init: bool) -> None:
    """Run Rust fetcher for the given symbols (news, earnings, FRED, SEC, Polymarket).

    Accepts an explicit symbol list instead of deriving from universe —
    callers pass only the filtered candidates (~120) to avoid fetching
    news/earnings for the entire 500+ equity universe.
    """
    binary = Path(__file__).parent.parent / "rust" / "target" / "release" / "quant-fetcher"
    if not binary.exists():
        raise FileNotFoundError(
            f"Rust binary not found: {binary}\n"
            "Build it: cd rust && cargo build --release"
        )

    # Filter out futures and indices (Finnhub doesn't cover them)
    fetch_syms = [s for s in symbols if "=" not in s and not s.startswith("^")]

    sym_args = []
    for s in fetch_syms:
        sym_args += ["--symbols", s]

    cmd = [
        str(binary),
        "--db", str(cfg.db_path_abs),
        "--finnhub-key", cfg.api.finnhub_key,
        "--fred-key", cfg.api.fred_key,
        "all",
        *sym_args,
        *(["--init"] if init else []),
    ]

    log.info("rust_fetcher_start", symbols=len(fetch_syms))
    # Finnhub rate limit: 1.1s/symbol × 2 (news + earnings) + SEC 0.33s/symbol + fixed costs
    # With cross-API parallelism: ~max(Finnhub, SEC+FRED+Polymarket) + margin
    timeout_s = max(1800, len(fetch_syms) * 3 + 300)
    result = subprocess.run(cmd, timeout=timeout_s)
    if result.returncode != 0:
        raise RuntimeError(f"Rust fetcher failed with exit code {result.returncode}")
    log.info("rust_fetcher_done")


def main() -> None:
    parser = argparse.ArgumentParser(description="Quant Research Bot — Probability Pipeline")
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--skip-rust", action="store_true")
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument("--config", type=str, default="config.yaml")
    args = parser.parse_args()

    cfg = Settings.load(args.config)
    run_id = str(uuid.uuid4())[:8]
    if args.date:
        as_of = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        from zoneinfo import ZoneInfo
        as_of = datetime.now(ZoneInfo("America/New_York")).date()

    log.info("pipeline_start", run_id=run_id, as_of=str(as_of))

    init_schema(cfg.db_path_abs)
    con = connect(cfg.db_path_abs)

    try:
        # ── 0a. Fetch/refresh US symbols (weekly, single API call) ─────────
        broad_screen_hits = []
        if cfg.broad_screen.enabled and cfg.api.finnhub_key:
            log.info("step_us_symbols")
            try:
                us_syms = fetch_us_symbols(
                    con, cfg.api.finnhub_key,
                    refresh_days=cfg.data.constituent_refresh_days,
                )
                log_run(con, run_id, "us_symbols", "ok", len(us_syms))
            except Exception as e:
                log.warning("us_symbols_failed", error=str(e))
                log_run(con, run_id, "us_symbols", "error", 0, str(e))
                us_syms = []

            # ── 0b. Broad screen → top N active symbols ───────────────────
            if us_syms:
                log.info("step_broad_screen")
                try:
                    broad_screen_hits = run_broad_screen(
                        con, as_of,
                        top_n=cfg.broad_screen.top_n,
                        min_volume_20d=cfg.broad_screen.min_volume_20d,
                        min_dollar_volume_20d=cfg.broad_screen.min_dollar_volume_20d,
                        min_price=cfg.universe.filters.min_price,
                        return_5d_threshold=cfg.broad_screen.return_5d_threshold,
                        return_20d_threshold=cfg.broad_screen.return_20d_threshold,
                        volume_surge_multiplier=cfg.broad_screen.volume_surge_multiplier,
                    )
                    log_run(con, run_id, "broad_screen", "ok", len(broad_screen_hits))
                except Exception as e:
                    log.warning("broad_screen_failed", error=str(e))
                    log_run(con, run_id, "broad_screen", "error", 0, str(e))
                    broad_screen_hits = []

        # ── 1. Universe ───────────────────────────────────────────────────────
        log.info("step_universe")
        universe = build_universe(
            con,
            scan_sp500=cfg.universe.scan.sp500,
            scan_nasdaq100=cfg.universe.scan.nasdaq100,
            include_sector_etfs=cfg.universe.asset_classes.sector_etfs,
            include_semi_etfs=cfg.universe.asset_classes.semi_etfs,
            include_biotech_etfs=cfg.universe.asset_classes.biotech_etfs,
            include_china_internet_etfs=cfg.universe.asset_classes.china_internet_etfs,
            include_innovation_etfs=cfg.universe.asset_classes.innovation_etfs,
            include_bond_etfs=cfg.universe.asset_classes.bond_etfs,
            include_commodities=cfg.universe.asset_classes.commodities,
            include_international=cfg.universe.asset_classes.international,
            include_volatility=cfg.universe.asset_classes.volatility,
            include_crypto_etfs=cfg.universe.asset_classes.crypto_etfs,
            watchlist=cfg.universe.watchlist,
            constituent_refresh_days=cfg.data.constituent_refresh_days,
            benchmark=cfg.universe.benchmark,
            broad_screen_hits=broad_screen_hits,
        )
        all_symbols = universe["_all"]
        core_symbols = {
            symbol
            for bucket, symbols in universe.items()
            if not bucket.startswith("_") and bucket != "broad_screen"
            for symbol in symbols
        }
        core_symbols.add(cfg.universe.benchmark)
        selection_policy = cfg.selection.model_dump()
        log.info("universe_ready", total=len(all_symbols))
        log_run(con, run_id, "universe", "ok", len(all_symbols))

        # ── 2. Rust fetcher runs AFTER filter pass 1 (see step 5a below) ─────
        # This cuts Finnhub calls from ~500 to ~120 symbols (75% reduction).
        # FRED/Polymarket/index_changes are global and run regardless.

        # ── 2b. Fundamentals (weekly, Finnhub) ──────────────────────────────
        if cfg.fundamentals.enabled and cfg.api.finnhub_key:
            log.info("step_fundamentals", symbols=len(all_symbols))
            try:
                # Fetch for all equity symbols (skip futures, VIX, etc.)
                fund_syms = [
                    s for s in all_symbols
                    if "=" not in s and not s.startswith("^")
                ]
                n_fund = fetch_fundamentals(
                    con, fund_syms, as_of, cfg.api.finnhub_key,
                    refresh_days=cfg.fundamentals.refresh_days,
                )
                log_run(con, run_id, "fundamentals", "ok", n_fund)
            except Exception as e:
                log.warning("fundamentals_failed", error=str(e))
                log_run(con, run_id, "fundamentals", "error", 0, str(e))

        # ── 3. Prices ─────────────────────────────────────────────────────────
        log.info("step_prices", symbols=len(all_symbols))
        n = fetch_and_store_prices(con, all_symbols, init=args.init)
        log_run(con, run_id, "prices", "ok", n)

        # ── 3b. Dividends + DYP screen ───────────────────────────────────────
        dip_cfg = cfg.dip_scanner
        if dip_cfg.enabled:
            sp500_syms = universe.get("sp500", [])
            if sp500_syms:
                log.info("step_dividends", symbols=len(sp500_syms))
                n_div = fetch_and_store_dividends(
                    con, sp500_syms, init=args.init,
                    lookback_years=dip_cfg.lookback_years,
                    special_dividend_multiplier=dip_cfg.special_dividend_multiplier,
                )
                log_run(con, run_id, "dividends", "ok", n_div)

                log.info("step_dyp_screen")
                dyp_results = run_dyp_screen(
                    con, sp500_syms, as_of,
                    dyp_threshold=dip_cfg.dyp_threshold,
                    min_history_days=dip_cfg.min_history_days,
                    lookback_years=dip_cfg.lookback_years,
                    max_results=dip_cfg.max_results,
                )
                log_run(con, run_id, "dyp_screen", "ok", len(dyp_results))

                log.info("step_dyp_scoring")
                dyp_scored = score_dyp_candidates(con, dyp_results, as_of)
                log_run(con, run_id, "dyp_scoring", "ok", len(dyp_scored))
            else:
                dyp_scored = []
        else:
            dyp_scored = []

        # ── 4. Probability computations ───────────────────────────────────────
        # Momentum: P(trend continues | regime, vol_bucket)
        log.info("step_momentum_probabilities")
        mom_df = run_momentum_risk(
            con, all_symbols, as_of,
            momentum_windows=cfg.signals.momentum_windows,
            atr_period=cfg.signals.atr_period,
            ma_filter_window=cfg.signals.ma_filter_window,
        )
        store_analysis(con, mom_df)

        # Earnings: P(upside drift | surprise quintile, regime)
        log.info("step_earnings_probabilities")
        earn_df = run_earnings_risk(
            con, all_symbols, cfg.universe.benchmark, as_of,
            lookback_days=cfg.signals.earnings_lookback_days,
            min_history=cfg.signals.earnings_min_history,
        )
        store_analysis(con, earn_df)
        log_run(con, run_id, "probabilities", "ok", len(mom_df) + len(earn_df))

        # Mean-reversion detection
        log.info("step_mean_reversion")
        try:
            mr_df = run_mean_reversion(con, all_symbols, as_of)
            store_mean_reversion(con, mr_df)
            log_run(con, run_id, "mean_reversion", "ok", len(mr_df))
        except Exception as e:
            log.warning("mean_reversion_failed", error=str(e))
            log_run(con, run_id, "mean_reversion", "error", 0, str(e))

        # Breakout detection
        log.info("step_breakout")
        try:
            bo_df = run_breakout(con, all_symbols, as_of)
            store_breakout(con, bo_df)
            log_run(con, run_id, "breakout", "ok", len(bo_df))
        except Exception as e:
            log.warning("breakout_failed", error=str(e))
            log_run(con, run_id, "breakout", "error", 0, str(e))

        # ── 4b. Value scores (cross-sectional, from fundamentals) ──────────
        if cfg.fundamentals.enabled:
            log.info("step_value_scores")
            try:
                value_scores = compute_value_scores(con, all_symbols, as_of)
                log_run(con, run_id, "value_scores", "ok", len(value_scores))
            except Exception as e:
                log.warning("value_scores_failed", error=str(e))
                log_run(con, run_id, "value_scores", "error", 0, str(e))
                value_scores = {}
        else:
            value_scores = {}

        # ── 4c. Price signal layer ─────────────────────────────────────────
        log.info("step_covariance")
        try:
            cov_result = compute_covariance(con, all_symbols, as_of)
            n_cov = len(cov_result.symbols_aligned) if cov_result else 0
            log_run(con, run_id, "covariance", "ok", n_cov)
        except Exception as e:
            log.warning("covariance_failed", error=str(e))
            log_run(con, run_id, "covariance", "error", 0, str(e))
            cov_result = None

        log.info("step_cointegration")
        try:
            coint_pairs = find_cointegrated_pairs(con, all_symbols, as_of)
            store_cointegrated_pairs(con, coint_pairs, as_of)
            log_run(con, run_id, "cointegration", "ok", len(coint_pairs))
        except Exception as e:
            log.warning("cointegration_failed", error=str(e))
            log_run(con, run_id, "cointegration", "error", 0, str(e))

        log.info("step_granger")
        try:
            granger = find_granger_leaders(con, all_symbols, as_of)
            store_granger_pairs(con, granger, as_of)
            log_run(con, run_id, "granger", "ok", len(granger))
        except Exception as e:
            log.warning("granger_failed", error=str(e))
            log_run(con, run_id, "granger", "error", 0, str(e))

        log.info("step_event_study")
        try:
            cars = compute_earnings_car(con, all_symbols, as_of)
            store_earnings_car(con, cars, as_of)
            log_run(con, run_id, "event_study", "ok", len(cars))
        except Exception as e:
            log.warning("event_study_failed", error=str(e))
            log_run(con, run_id, "event_study", "error", 0, str(e))

        log.info("step_kalman_beta")
        try:
            kalman = compute_kalman_betas(con, all_symbols, as_of)
            store_kalman_betas(con, kalman, as_of)
            log_run(con, run_id, "kalman_beta", "ok", len(kalman))
        except Exception as e:
            log.warning("kalman_beta_failed", error=str(e))
            log_run(con, run_id, "kalman_beta", "error", 0, str(e))

        # ── 5. Two-pass notable filter with targeted options fetch ──────────
        # Pass 1: rank full universe WITHOUT options → candidate pool
        log.info("step_filter_pass1_candidates")
        candidates = build_notable_items(
            con, as_of, all_symbols,
            benchmark=cfg.universe.benchmark,
            max_items=120,
            core_symbols=core_symbols,
            selection_policy=selection_policy,
        )
        candidate_syms = [c["symbol"] for c in candidates]

        # ── 5a. Rust fetcher — only for filtered candidates ──────────────
        # Fetches news, earnings, FRED, SEC, Polymarket for ~120 candidates
        # instead of the full 500+ equity universe. Non-fatal: stale data
        # from previous runs is acceptable for report generation.
        if not args.skip_rust:
            con.close()  # DuckDB single-writer: release before Rust subprocess
            try:
                run_rust_fetcher(cfg, candidate_syms, args.init)
                con = connect(cfg.db_path_abs)
                log_run(con, run_id, "rust_fetch", "ok", len(candidate_syms))
            except Exception as e:
                log.warning("rust_fetcher_failed_nonfatal", error=str(e))
                con = connect(cfg.db_path_abs)
                log_run(con, run_id, "rust_fetch", "error", 0, str(e))

        # Build options fetch list: eligible candidates + all non-equity
        # optionable universe members (ETFs like USO, UVXY, GLD, etc.)
        non_equity_optionable = [
            s for s in all_symbols
            if is_options_eligible(s) and s not in set(universe.get("equities", []))
        ]
        # Also resolve proxies for ineligible candidates
        proxy_syms = [
            OPTIONS_PROXY_MAP[s] for s in candidate_syms
            if not is_options_eligible(s) and s in OPTIONS_PROXY_MAP
        ]
        fetch_syms = sorted(set(
            [s for s in candidate_syms if is_options_eligible(s)]
            + non_equity_optionable
            + proxy_syms
        ))
        log.info("step_options_targeted", fetch_count=len(fetch_syms),
                 candidates=len(candidate_syms))

        snapshot_df, analysis_df = fetch_options_snapshot(fetch_syms, as_of, max_expiries=2)
        n = upsert_options(con, snapshot_df)
        n2 = upsert_options_analysis(con, analysis_df)
        log_run(con, run_id, "options", "ok", n, f"snapshot={n} analysis={n2}")

        # ── 5c. Options sentiment signals (VRP + EWMA) ──────────────
        log.info("step_vrp")
        vrp_results = compute_vrp(con, all_symbols, as_of)
        store_vrp(con, vrp_results, as_of)

        log.info("step_sentiment_ewma")
        ewma_results = compute_sentiment_ewma(con, all_symbols, as_of)
        store_sentiment(con, ewma_results, as_of)
        log_run(con, run_id, "sentiment", "ok", len(vrp_results) + len(ewma_results))

        # Pass 2: re-rank candidate pool WITH options → final top 20
        # Ensure benchmark is in the list (needed for cross-asset scoring)
        pass2_syms = candidate_syms if cfg.universe.benchmark in candidate_syms \
            else candidate_syms + [cfg.universe.benchmark]
        log.info("step_filter_pass2_final")
        notable = build_notable_items(
            con, as_of, pass2_syms,
            benchmark=cfg.universe.benchmark,
            max_items=cfg.output.max_notable_items,
            core_symbols=core_symbols,
            selection_policy=selection_policy,
        )
        log_run(con, run_id, "filter", "ok", len(notable))

        # ── 7. Bundle ─────────────────────────────────────────────────────────
        bundle = build_report_bundle(
            con, as_of, notable,
            benchmark=cfg.universe.benchmark,
            universe=universe,
            dividend_dip_screen=dyp_scored,
        )

        # ── 7b. Signal classification ────────────────────────────────────────
        # Classify after bundling so we have market context for macro gating.
        # notable_items are mutable dicts — signal is added in-place.
        log.info("step_signal_classification")
        classify_all(bundle["notable_items"], bundle.get("market_context"))
        log.info(
            "notable_items_classified",
            total=len(bundle["notable_items"]),
            high=len([x for x in bundle["notable_items"] if (x.get("signal") or {}).get("confidence") == "HIGH"]),
            moderate=len([x for x in bundle["notable_items"] if (x.get("signal") or {}).get("confidence") == "MODERATE"]),
            low=len([x for x in bundle["notable_items"] if (x.get("signal") or {}).get("confidence") in ("LOW", "NO_SIGNAL", None)]),
            core=len([x for x in bundle["notable_items"] if x.get("report_bucket") == "core"]),
            event_tape=len([x for x in bundle["notable_items"] if x.get("report_bucket") == "event_tape"]),
            appendix=len([x for x in bundle["notable_items"] if x.get("report_bucket") == "appendix"]),
        )

        bundle["options_extremes"] = compute_options_extremes(bundle["notable_items"])

        # ── Phase 4: Algorithmic pre-processing ─────────────────────────────
        # Clustering (needs covariance result from Phase 2)
        log.info("step_clustering")
        clusters_result = None
        try:
            if cov_result is not None:
                clusters_result = compute_clusters(
                    cov_result.corr_matrix, cov_result.symbols_aligned,
                )
                log_run(con, run_id, "clustering", "ok", clusters_result["n_independent_bets"])
            else:
                log.info("clustering_skipped_no_covariance")
        except Exception as e:
            log.warning("clustering_failed", error=str(e))
            log_run(con, run_id, "clustering", "error", 0, str(e))

        # Risk params for HIGH and MODERATE items
        log.info("step_risk_params")
        n_risk = 0
        for item in bundle["notable_items"]:
            sig_conf = (item.get("signal") or {}).get("confidence")
            if sig_conf in ("HIGH", "MODERATE"):
                item["risk_params"] = compute_risk_params(item)
                if item["risk_params"]:
                    n_risk += 1
        log_run(con, run_id, "risk_params", "ok", n_risk)

        # Contradictions
        log.info("step_contradictions")
        for item in bundle["notable_items"]:
            item["contradictions"] = detect_contradictions(
                item, value_scores.get(item["symbol"]),
            )
        log_run(con, run_id, "contradictions", "ok", len(bundle["notable_items"]))

        # Scorecard
        log.info("step_scorecard")
        try:
            scorecard = compute_scorecard(con, as_of)
            bundle["scorecard"] = scorecard
            log_run(con, run_id, "scorecard", "ok",
                     scorecard.get("momentum_accuracy", {}).get("calls", 0))
        except Exception as e:
            log.warning("scorecard_failed", error=str(e))
            log_run(con, run_id, "scorecard", "error", 0, str(e))
            bundle["scorecard"] = {}

        # News quality
        log.info("step_news_quality")
        try:
            shared_catalysts = assess_news_quality(bundle["notable_items"], as_of)
            bundle["shared_catalysts"] = shared_catalysts
            log_run(con, run_id, "news_quality", "ok", len(shared_catalysts))
        except Exception as e:
            log.warning("news_quality_failed", error=str(e))
            log_run(con, run_id, "news_quality", "error", 0, str(e))
            bundle["shared_catalysts"] = []

        # Portfolio risk
        log.info("step_portfolio_risk")
        try:
            high_items = [
                i for i in bundle["notable_items"]
                if (i.get("signal") or {}).get("confidence") == "HIGH"
            ]
            portfolio_risk = compute_portfolio_risk(high_items, clusters_result)
            bundle["portfolio_risk"] = portfolio_risk
            bundle["clusters"] = clusters_result
            log_run(con, run_id, "portfolio_risk", "ok", len(high_items))
        except Exception as e:
            log.warning("portfolio_risk_failed", error=str(e))
            log_run(con, run_id, "portfolio_risk", "error", 0, str(e))
            bundle["portfolio_risk"] = {}
            bundle["clusters"] = None

        # ── Phase 5: HMM market regime overlay ───────────────────────────────
        try:
            from quant_bot.analytics.hmm_regime import (
                fit_hmm_regime,
                record_hmm_forecast,
                resolve_hmm_forecasts,
                compute_hmm_calibration,
            )
            log.info("step_hmm_regime")

            # Resolve past HMM forecasts before fitting new one
            try:
                n_resolved = resolve_hmm_forecasts(con, as_of)
                if n_resolved:
                    log.info("hmm_forecasts_resolved", count=n_resolved)
            except Exception as e:
                log.warning("hmm_resolve_failed", error=str(e))

            hmm_result = fit_hmm_regime(con, as_of)
            if hmm_result:
                # Record forecast for tomorrow
                try:
                    record_hmm_forecast(
                        con, as_of, hmm_result["p_ret_positive_tomorrow"]
                    )
                except Exception as e:
                    log.warning("hmm_record_failed", error=str(e))

                # Attach calibration metrics to bundle
                try:
                    cal = compute_hmm_calibration(con, as_of)
                    hmm_result["calibration"] = cal
                except Exception as e:
                    log.warning("hmm_calibration_failed", error=str(e))

                bundle["hmm_regime"] = hmm_result
                log_run(con, run_id, "hmm_regime", "ok", 1)
            else:
                log.warning("hmm_regime_skipped")
        except ImportError:
            log.warning("hmmlearn_not_installed")
        except Exception as e:
            log.warning("hmm_regime_failed", error=str(e))
            log_run(con, run_id, "hmm_regime", "error", 0, str(e))

        # ── 8. Generate charts ───────────────────────────────────────────────
        charts_dir = Path("reports") / "charts" / str(as_of)
        log.info("step_charts", output_dir=str(charts_dir))
        chart_paths = generate_daily_charts(bundle, charts_dir, con=con, as_of=as_of)
        log_run(con, run_id, "charts", "ok", len(chart_paths))

        # ── 9. Render raw Markdown payload ────────────────────────────────────
        # This is the program's output. Feed it to whichever agent you want.
        payload_path = Path("reports") / f"{as_of}_payload.md"
        render_payload_md(bundle, payload_path, chart_paths=chart_paths)
        log_run(con, run_id, "render", "ok", 1, str(payload_path))

        log.info("pipeline_complete", run_id=run_id, payload=str(payload_path))
        print(f"\n✓ Payload ready: {payload_path}")
        print(f"  Charts: {charts_dir}/ ({len(chart_paths)} files)")
        print(f"  Feed to agent: claude < {payload_path}")
        print(f"  Or: codex '{payload_path}'")
        print(f"  Or open in any LLM chat\n")

    except Exception as e:
        log.error("pipeline_failed", run_id=run_id, error=str(e))
        try:
            # Connection may be closed if Rust fetcher step failed
            log_run(con, run_id, "pipeline", "error", 0, str(e))
        except Exception:
            try:
                con = connect(cfg.db_path_abs)
                log_run(con, run_id, "pipeline", "error", 0, str(e))
            except Exception:
                pass  # Best-effort logging
        raise
    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
