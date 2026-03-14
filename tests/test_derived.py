import pandas as pd

from technical_indicator_fetcher.derived import build_derived_states
from technical_indicator_fetcher.models import FetcherOptions
from technical_indicator_fetcher.preprocess import normalize_ohlcv


def _build_base_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=6, freq="D"),
            "open": [99, 100, 101, 102, 103, 104],
            "high": [101, 102, 103, 104, 105, 106],
            "low": [98, 99, 100, 101, 102, 103],
            "close": [100, 101, 102, 103, 104, 105],
            "volume": [1000, 1100, 1200, 1300, 1400, 1500],
        }
    )


def test_build_derived_states_maps_boundary_values_and_unknowns() -> None:
    normalized = normalize_ohlcv(
        _build_base_ohlcv(),
        as_of="2026-01-06",
        options=FetcherOptions(minimum_bars=5, recommended_bars=6),
    )
    raw = {
        "sma_20": 100.0,
        "sma_50": 106.0,
        "sma_200": None,
        "adx_14": 25.0,
        "plus_di_14": 10.0,
        "minus_di_14": 20.0,
        "sar": 110.0,
        "rsi_14": 30.0,
        "macd": {"macd": -1.0, "signal": 1.0, "hist": 0.0},
        "stoch": {"slowk": 80.0, "slowd": 70.0},
        "bbands_20_2": {"upper": 110.0, "middle": 103.0, "lower": 95.0},
        "mfi_14": 20.0,
        "adosc": -5.0,
    }
    history = {
        "atr_14": pd.Series([1.0, 2.0, 3.0, 4.0, 5.0]),
        "natr_14": pd.Series([5.0, 4.0, 3.0, 2.0, 1.0]),
        "obv": pd.Series([1.0, 2.0, 3.0, 4.0, 3.0, 2.0]),
    }

    derived = build_derived_states(
        raw,
        history,
        normalized,
        options=FetcherOptions(volatility_regime_window=5, obv_direction_window=5),
    )

    assert derived["trend"]["close_vs_sma20"] == "above"
    assert derived["trend"]["close_vs_sma50"] == "below"
    assert derived["trend"]["close_vs_sma200"] == "unknown"
    assert derived["trend"]["adx_state"] == "strong_trend"
    assert derived["trend"]["di_direction"] == "bearish"
    assert derived["trend"]["sar_position"] == "above_price"
    assert derived["momentum"]["rsi_state"] == "oversold"
    assert derived["momentum"]["macd_vs_signal"] == "below"
    assert derived["momentum"]["macd_histogram_sign"] == "zero"
    assert derived["momentum"]["stochastic_state"] == "upper_zone"
    assert derived["volatility"]["bbands_position"] == "inside_upper_half"
    assert derived["volatility"]["atr_regime"] == "high"
    assert derived["volatility"]["natr_regime"] == "low"
    assert derived["volume"]["obv_direction_5"] == "up"
    assert derived["volume"]["mfi_state"] == "oversold"
    assert derived["volume"]["adosc_sign"] == "negative"
