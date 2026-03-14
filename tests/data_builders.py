from __future__ import annotations

import numpy as np
import pandas as pd


def build_trending_ohlcv(
    *,
    periods: int = 260,
    direction: str = "up",
    start: float = 100.0,
    step: float = 0.6,
) -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=periods, freq="D")
    base = pd.Series(np.arange(periods), dtype="float64")
    close = start + (base * step if direction == "up" else -(base * step))
    return pd.DataFrame(
        {
            "timestamp": index,
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": 1000 + base * 10,
        }
    )


def build_high_volatility_ohlcv(*, periods: int = 260) -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=periods, freq="D")
    base = np.arange(periods, dtype="float64")
    close = 120 + np.sin(base / 3.0) * 6 + np.where(base % 2 == 0, 5.5, -5.0)
    return pd.DataFrame(
        {
            "timestamp": index,
            "open": close - np.where(base % 2 == 0, 1.5, -1.0),
            "high": close + 4.5,
            "low": close - 4.0,
            "close": close,
            "volume": 2000 + (base % 7) * 120,
        }
    )


def build_volume_missing_ohlcv(*, periods: int = 260) -> pd.DataFrame:
    frame = build_trending_ohlcv(periods=periods)
    frame.loc[periods - 3 :, "volume"] = np.nan
    return frame


def build_unclosed_ohlcv(*, periods: int = 260) -> pd.DataFrame:
    return build_trending_ohlcv(periods=periods)
