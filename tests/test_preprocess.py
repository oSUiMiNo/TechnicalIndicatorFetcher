import pandas as pd
import pytest

from technical_indicator_fetcher.exceptions import OHLCVValidationError
from technical_indicator_fetcher.models import FetcherOptions
from technical_indicator_fetcher.preprocess import normalize_ohlcv
from tests.data_builders import build_volume_missing_ohlcv


def _build_valid_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": pd.date_range("2026-01-01", periods=4, freq="D"),
            "Open": [100, 101, 102, 103],
            "High": [101, 102, 103, 104],
            "Low": [99, 100, 101, 102],
            "Close": [100.5, 101.5, 102.5, 103.5],
            "Volume": [1000, 1100, 1200, 1300],
        }
    )


def test_normalize_ohlcv_builds_normalized_frame_and_arrays() -> None:
    frame = _build_valid_ohlcv()
    frame.attrs["price_adjusted"] = True
    frame["is_closed"] = [True, True, True, True]

    result = normalize_ohlcv(frame, as_of="2026-01-04")

    assert list(result.dataframe.columns) == ["open", "high", "low", "close", "volume", "is_closed"]
    assert result.dataframe.index.name == "timestamp"
    assert result.bars_used == 4
    assert result.adjusted is True
    assert result.candle_status == "closed"
    assert result.close_arr.tolist() == [100.5, 101.5, 102.5, 103.5]
    assert result.warnings[0].code == "insufficient_bars"


def test_normalize_ohlcv_warns_when_bars_are_below_minimum() -> None:
    frame = _build_valid_ohlcv()

    result = normalize_ohlcv(
        frame,
        as_of="2026-01-04",
        options=FetcherOptions(minimum_bars=5, recommended_bars=8),
    )

    assert [warning.code for warning in result.warnings] == ["insufficient_bars"]


def test_normalize_ohlcv_warns_for_unclosed_latest_candle() -> None:
    frame = _build_valid_ohlcv()

    result = normalize_ohlcv(frame, as_of="2026-01-05")

    assert result.candle_status == "unclosed"
    assert [warning.code for warning in result.warnings] == [
        "insufficient_bars",
        "unclosed_latest_candle",
    ]


def test_normalize_ohlcv_warns_when_adjusted_prices_are_unavailable() -> None:
    frame = _build_valid_ohlcv()
    frame.attrs["adjusted"] = False

    result = normalize_ohlcv(
        frame,
        as_of="2026-01-04",
        options=FetcherOptions(price_adjustment=True),
    )

    assert [warning.code for warning in result.warnings] == [
        "insufficient_bars",
        "adjusted_price_unavailable",
    ]


def test_normalize_ohlcv_strict_rejects_unclosed_latest_candle() -> None:
    frame = _build_valid_ohlcv()

    with pytest.raises(OHLCVValidationError, match="latest candle"):
        normalize_ohlcv(
            frame,
            as_of="2026-01-05",
            options=FetcherOptions(strict=True),
        )


def test_normalize_ohlcv_strict_rejects_unadjusted_input_when_adjustment_is_required() -> None:
    frame = _build_valid_ohlcv()
    frame.attrs["adjusted"] = False

    with pytest.raises(OHLCVValidationError, match="price_adjustment"):
        normalize_ohlcv(
            frame,
            as_of="2026-01-04",
            options=FetcherOptions(strict=True, price_adjustment=True),
        )


def test_normalize_ohlcv_warns_when_volume_contains_null_values() -> None:
    result = normalize_ohlcv(
        build_volume_missing_ohlcv(),
        as_of="2025-09-17",
        options=FetcherOptions(minimum_bars=200, recommended_bars=260),
    )

    assert [warning.code for warning in result.warnings] == ["missing_volume"]
    assert pd.isna(result.volume_arr[-1])
