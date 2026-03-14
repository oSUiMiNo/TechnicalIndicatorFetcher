import pandas as pd
import pytest

from technical_indicator_fetcher.exceptions import OHLCVValidationError
from technical_indicator_fetcher.validator import validate_ohlcv


def _build_valid_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=3, freq="D"),
            "open": [100, 101, 102],
            "high": [101, 102, 103],
            "low": [99, 100, 101],
            "close": [100.5, 101.5, 102.5],
            "volume": [1000, 1100, 1200],
        }
    )


def test_validate_ohlcv_accepts_timestamp_column() -> None:
    validated = validate_ohlcv(_build_valid_ohlcv())

    assert list(validated.columns) == ["timestamp", "open", "high", "low", "close", "volume"]


def test_validate_ohlcv_rejects_missing_required_column() -> None:
    frame = _build_valid_ohlcv().drop(columns=["volume"])

    with pytest.raises(OHLCVValidationError, match="missing required columns"):
        validate_ohlcv(frame)


def test_validate_ohlcv_rejects_empty_frame() -> None:
    frame = _build_valid_ohlcv().iloc[0:0]

    with pytest.raises(OHLCVValidationError, match="must not be empty"):
        validate_ohlcv(frame)


def test_validate_ohlcv_rejects_unsorted_timestamps() -> None:
    frame = _build_valid_ohlcv().iloc[[1, 0, 2]].reset_index(drop=True)

    with pytest.raises(OHLCVValidationError, match="sorted in ascending order"):
        validate_ohlcv(frame)


def test_validate_ohlcv_rejects_duplicate_timestamps() -> None:
    frame = _build_valid_ohlcv()
    frame.loc[2, "timestamp"] = frame.loc[1, "timestamp"]

    with pytest.raises(OHLCVValidationError, match="duplicate timestamps"):
        validate_ohlcv(frame)

