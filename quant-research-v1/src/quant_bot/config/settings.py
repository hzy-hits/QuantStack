from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class ApiConfig(BaseModel):
    finnhub_key: str = ""
    fred_key: str = ""
    anthropic_key: str = ""
    sec_user_agent: str = "quant-research-v1 user@example.com"


class RuntimeConfig(BaseModel):
    timezone: str = "America/New_York"
    random_seed: int = 42


class ScanConfig(BaseModel):
    sp500: bool = True
    nasdaq100: bool = False
    russell2000: bool = False


class AssetClassConfig(BaseModel):
    sector_etfs: bool = True
    semi_etfs: bool = True
    biotech_etfs: bool = True
    china_internet_etfs: bool = True
    innovation_etfs: bool = True
    bond_etfs: bool = True
    commodities: bool = True
    international: bool = True
    volatility: bool = True
    crypto_etfs: bool = False


class FiltersConfig(BaseModel):
    min_avg_volume_shares: int = 500_000
    min_price: float = 5.0


class UniverseConfig(BaseModel):
    benchmark: str = "SPY"
    scan: ScanConfig = Field(default_factory=ScanConfig)
    asset_classes: AssetClassConfig = Field(default_factory=AssetClassConfig)
    watchlist: list[str] = Field(default_factory=list)
    filters: FiltersConfig = Field(default_factory=FiltersConfig)


class OutputConfig(BaseModel):
    max_notable_items: int = 20
    min_notable_items: int = 8


class SelectionConfig(BaseModel):
    core_max_items: int = 18
    tactical_continuation_max_items: int = 2
    event_tape_max_items: int = 6
    appendix_max_items: int = 6
    core_min_market_cap_musd: float = 2_000.0
    core_min_price: float = 5.0
    core_min_dollar_volume_20d: float = 20_000_000.0


class DataConfig(BaseModel):
    # Legacy single-DB fallback used by older callers.
    db_path: str = "data/quant.duckdb"
    raw_db_path: str = ""
    research_db_path: str = ""
    report_db_path: str = ""
    dev_db_path: str = ""
    use_dev_for_research: bool = False
    constituent_refresh_days: int = 7


class SignalsConfig(BaseModel):
    momentum_windows: list[int] = [20, 60]
    atr_period: int = 14
    ma_filter_window: int = 20
    earnings_min_history: int = 4
    earnings_lookback_days: int = 730


class FredConfig(BaseModel):
    # Dict of series_id -> human-readable label
    series: dict[str, str] = Field(default_factory=lambda: {
        "FEDFUNDS":     "Fed Funds Rate (%)",
        "DGS10":        "10Y Treasury Yield (%)",
        "BAMLH0A0HYM2": "HY Credit Spread",
        "VIXCLS":       "VIX — Market Fear Index",
        "T10Y2Y":       "10Y-2Y Yield Spread (recession indicator)",
        "UNRATE":       "Unemployment Rate (%)",
        "CPIAUCSL":     "CPI (inflation proxy)",
    })

    @property
    def series_ids(self) -> list[str]:
        return list(self.series.keys())


class DipScannerConfig(BaseModel):
    enabled: bool = True
    dyp_threshold: int = 70
    min_history_days: int = 730
    lookback_years: int = 5
    special_dividend_multiplier: float = 3.0
    max_results: int = 30


class BroadScreenConfig(BaseModel):
    enabled: bool = True
    top_n: int = 200
    min_volume_20d: int = 100_000
    min_dollar_volume_20d: float = 20_000_000.0
    return_5d_threshold: float = 10.0
    return_20d_threshold: float = 20.0
    volume_surge_multiplier: float = 3.0


class FundamentalsConfig(BaseModel):
    enabled: bool = True
    refresh_days: int = 7


class ReportingConfig(BaseModel):
    anthropic_model: str = "claude-sonnet-4-6"
    anthropic_temperature: float = 0.15
    max_tokens: int = 3000
    recipients: list[str] = Field(default_factory=list)


class Settings(BaseModel):
    api: ApiConfig = Field(default_factory=ApiConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    universe: UniverseConfig = Field(default_factory=UniverseConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    selection: SelectionConfig = Field(default_factory=SelectionConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    signals: SignalsConfig = Field(default_factory=SignalsConfig)
    fred: FredConfig = Field(default_factory=FredConfig)
    reporting: ReportingConfig = Field(default_factory=ReportingConfig)
    dip_scanner: DipScannerConfig = Field(default_factory=DipScannerConfig)
    broad_screen: BroadScreenConfig = Field(default_factory=BroadScreenConfig)
    fundamentals: FundamentalsConfig = Field(default_factory=FundamentalsConfig)

    @classmethod
    def load(cls, path: str | Path = "config.yaml") -> "Settings":
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(
                f"Config file not found: {path}\n"
                f"Copy config.example.yaml to config.yaml and fill in your API keys."
            )
        with open(path) as f:
            raw: dict[str, Any] = yaml.safe_load(f) or {}
        return cls.model_validate(raw)

    @property
    def db_path_abs(self) -> Path:
        return Path(self.data.db_path)

    @property
    def raw_db_path_abs(self) -> Path:
        return Path(self.data.raw_db_path or self.data.db_path)

    @property
    def research_db_path_abs(self) -> Path:
        return Path(self.data.research_db_path or self.data.db_path)

    @property
    def report_db_path_abs(self) -> Path:
        return Path(self.data.report_db_path or self.data.research_db_path or self.data.db_path)

    @property
    def dev_db_path_abs(self) -> Path:
        return Path(self.data.dev_db_path or self.data.research_db_path or self.data.db_path)

    @property
    def active_research_db_path_abs(self) -> Path:
        if self.data.use_dev_for_research:
            return self.dev_db_path_abs
        return self.research_db_path_abs
