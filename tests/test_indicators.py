import pandas as pd

from technical_indicator_fetcher.indicators import compute_core_indicators
from technical_indicator_fetcher.models import FetcherOptions
from technical_indicator_fetcher.preprocess import normalize_ohlcv
from tests.data_builders import build_trending_ohlcv, build_volume_missing_ohlcv


def test_compute_core_indicators_returns_latest_values_for_core_profile() -> None:
    normalized = normalize_ohlcv(build_trending_ohlcv(step=0.8), as_of="2025-09-17")

    raw, history, warnings = compute_core_indicators(normalized)

    assert warnings == []
    assert raw["sma_20"] is not None
    assert raw["sma_50"] is not None
    assert raw["sma_200"] is not None
    assert raw["macd"]["macd"] is not None
    assert raw["bbands_20_2"]["upper"] is not None
    assert raw["rsi_14"] is not None
    assert raw["obv"] is not None
    assert history["atr_14"].iloc[-1] == raw["atr_14"]


def test_compute_core_indicators_converts_nan_latest_values_to_none() -> None:
    normalized = normalize_ohlcv(
        build_trending_ohlcv(periods=30, step=0.8),
        as_of="2025-01-30",
        options=FetcherOptions(minimum_bars=20, recommended_bars=30),
    )

    raw, _, _ = compute_core_indicators(normalized)

    assert raw["sma_200"] is None


def test_compute_core_indicators_warns_only_for_volume_indicators_when_volume_is_missing() -> None:
    normalized = normalize_ohlcv(
        build_volume_missing_ohlcv(),
        as_of="2025-09-17",
        options=FetcherOptions(minimum_bars=200, recommended_bars=260),
    )

    raw, _, warnings = compute_core_indicators(normalized)

    assert [warning.code for warning in warnings] == ["volume_indicator_compute_failed"]
    assert raw["sma_20"] is not None
    assert raw["rsi_14"] is not None
    assert raw["obv"] is None
    assert raw["mfi_14"] is None
    assert raw["ad"] is None
    assert raw["adosc"] is None


def test_compute_core_indicators_handles_insufficient_bars_without_crashing() -> None:
    normalized = normalize_ohlcv(
        build_trending_ohlcv(periods=30, step=0.8),
        as_of="2025-01-30",
        options=FetcherOptions(minimum_bars=20, recommended_bars=30),
    )

    raw, history, warnings = compute_core_indicators(normalized)

    assert warnings == []
    assert raw["sma_200"] is None
    assert raw["rsi_14"] is not None
    assert history["sma_200"].isna().all()
