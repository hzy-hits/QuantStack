"""Asset / theme bucket classification."""
from __future__ import annotations


# ── Symbol sets ──────────────────────────────────────────────────────────────

BROAD_EQUITY_SYMBOLS = {"SPY", "QQQ", "IWM", "DIA", "EEM", "EFA", "FXI", "VWO"}
GROWTH_ETFS = {"XLK", "XLC", "XLY"}
DEFENSIVE_ETFS = {"XLU", "XLP", "XLV"}
ENERGY_SYMBOLS = {"XLE", "USO", "CL=F", "NG=F"}
DEFENSE_SYMBOLS = {"RTX", "LMT", "NOC", "GD", "LHX", "HWM", "BA"}
DURATION_SYMBOLS = {"TLT", "IEF", "SHY"}
CREDIT_SYMBOLS = {"HYG", "LQD"}
GOLD_SYMBOLS = {"GLD", "GC=F", "SLV", "SI=F"}
VOL_SYMBOLS = {"^VIX", "UVXY"}

# Cross-asset divergence: map individual stocks to their reference assets
# When the stock diverges from ALL reference assets, confidence is downgraded.
CROSS_ASSET_REF_MAP: dict[str, list[str]] = {}

# Build from existing sector sets — energy stocks reference CL=F + XLE
_ENERGY_STOCKS = {"OXY", "XOM", "CVX", "COP", "HAL", "SLB", "EOG", "PXD", "DVN", "MPC", "VLO", "PSX", "CF"}
for _s in _ENERGY_STOCKS:
    CROSS_ASSET_REF_MAP[_s] = ["CL=F", "XLE"]

_GOLD_STOCKS = {"NEM", "GOLD", "FNV", "AEM", "WPM"}
for _s in _GOLD_STOCKS:
    CROSS_ASSET_REF_MAP[_s] = ["GC=F", "GLD"]

_DEFENSE_STOCK_REFS = {s: ["ITA", "XLI"] for s in DEFENSE_SYMBOLS}  # ITA = iShares US Aerospace & Defense ETF
CROSS_ASSET_REF_MAP.update(_DEFENSE_STOCK_REFS)


def _asset_bucket_for_item(item: dict) -> str:
    """Resolve asset/theme bucket from symbol or sector metadata."""
    symbol = item.get("symbol", "")
    sector = (item.get("sector") or "").strip().lower()

    if symbol in VOL_SYMBOLS:
        return "vol"
    if symbol in DURATION_SYMBOLS:
        return "duration"
    if symbol in CREDIT_SYMBOLS:
        return "credit"
    if symbol in GOLD_SYMBOLS:
        return "gold"
    if symbol in ENERGY_SYMBOLS or sector == "energy":
        return "energy"
    if symbol in DEFENSE_SYMBOLS:
        return "defense"
    if symbol in DEFENSIVE_ETFS or sector in {"utilities", "consumer staples", "health care"}:
        return "defensive"
    if symbol in GROWTH_ETFS or sector in {"information technology", "communication services"}:
        return "growth"
    if symbol in BROAD_EQUITY_SYMBOLS:
        return "broad_equity"
    return "broad_equity"
