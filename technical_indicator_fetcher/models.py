from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .config import DEFAULT_MINIMUM_BARS, DEFAULT_RECOMMENDED_BARS

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd


@dataclass(slots=True)
class FetcherOptions:
    indicator_profile: str = "core"
    pattern_profile: str = "major_only"
    output_format: str = "yaml"
    lookback_bars: int = 10
    include_history: bool = False
    price_adjustment: bool = True
    yaml_version: str = "1.0"
    emit_yaml_comments: bool = True
    json_indent: int = 2
    volatility_regime_window: int = 50
    obv_direction_window: int = 5
    pattern_recent_window: int = 10
    strict: bool = False
    minimum_bars: int = DEFAULT_MINIMUM_BARS
    recommended_bars: int = DEFAULT_RECOMMENDED_BARS


@dataclass(slots=True)
class WarningEntry:
    code: str
    message: str
    severity: str = "warning"

    def as_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
        }


@dataclass(slots=True)
class NormalizedOHLCV:
    dataframe: pd.DataFrame
    open_arr: np.ndarray
    high_arr: np.ndarray
    low_arr: np.ndarray
    close_arr: np.ndarray
    volume_arr: np.ndarray
    bars_used: int
    adjusted: bool
    candle_status: str
    latest_timestamp: pd.Timestamp
    warnings: list[WarningEntry] = field(default_factory=list)


@dataclass(slots=True)
class BatchRequest:
    symbol: str
    timeframe: str
    ohlcv: pd.DataFrame
    as_of: str | pd.Timestamp
    options: FetcherOptions | None = None
