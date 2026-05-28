"""Intraday index options analysis (refresh + GEX + dashboard).

Designed for ^SPX / ^NDX / ^XSP / ^XND / ^MRUT / ^RUT / ^XEO / ^VIX
cash-settled index options. CBOE CDN data is delayed ~15min, so
"intraday" here means a 30-min snapshot cadence during 9:30 ET - 16:00 ET
trading window, not real-time. For true real-time you'd need OPRA / Polygon
/ IBKR feeds.

Main entry points:
  refresh_index_chains.py — fetches the 8 indices' chains, writes
    options_chain_quotes (timestamped) and trims to a manageable strike
    window around spot.
  compute_gex.py — computes dealer net gamma exposure per (symbol,
    dte_bucket) plus skew + term structure metrics; writes
    index_gex_snapshots history table.
  render_dashboard.py — outputs reports/intraday/<date>/
    index_dashboard_<HHMM>.md for human review + agent narrator
    consumption.
"""
