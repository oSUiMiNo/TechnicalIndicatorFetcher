from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from .exceptions import OHLCVValidationError

REQUIRED_COLUMNS = ("open", "high", "low", "close", "volume")
PRICE_COLUMNS = ("open", "high", "low", "close")
TIMESTAMP_CANDIDATES = ("timestamp", "datetime", "date")


def _normalized_columns(columns: Iterable[object]) -> list[str]:
    return [str(column).strip().lower().replace(" ", "_") for column in columns]


def _resolve_timestamp_source(df: pd.DataFrame) -> pd.Series | pd.Index:
    if isinstance(df.index, pd.DatetimeIndex):
        return df.index

    lowered = _normalized_columns(df.columns)
    for candidate in TIMESTAMP_CANDIDATES:
        if candidate in lowered:
            source_column = df.columns[lowered.index(candidate)]
            return df[source_column]

    raise OHLCVValidationError(
        "ohlcv must have a DatetimeIndex or a timestamp-like column"
    )


def validate_ohlcv(ohlcv: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(ohlcv, pd.DataFrame):
        raise OHLCVValidationError("ohlcv must be a pandas DataFrame")

    if ohlcv.empty:
        raise OHLCVValidationError("ohlcv must not be empty")

    lowered_columns = _normalized_columns(ohlcv.columns)
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in lowered_columns]
    if missing_columns:
        raise OHLCVValidationError(
            f"ohlcv is missing required columns: {', '.join(missing_columns)}"
        )

    timestamps = pd.to_datetime(_resolve_timestamp_source(ohlcv), errors="coerce")
    if pd.isna(timestamps).any():
        raise OHLCVValidationError("ohlcv contains invalid timestamps")
    timestamp_index = pd.DatetimeIndex(timestamps)
    if timestamp_index.has_duplicates:
        raise OHLCVValidationError("ohlcv contains duplicate timestamps")
    if not timestamp_index.is_monotonic_increasing:
        raise OHLCVValidationError("ohlcv timestamps must be sorted in ascending order")

    for column_name in PRICE_COLUMNS:
        source_column = ohlcv.columns[lowered_columns.index(column_name)]
        numeric_series = pd.to_numeric(ohlcv[source_column], errors="coerce")
        if numeric_series.isna().any():
            raise OHLCVValidationError(
                f"ohlcv column '{column_name}' must be numeric and contain no null values"
            )

    volume_column = ohlcv.columns[lowered_columns.index("volume")]
    volume_series = pd.to_numeric(ohlcv[volume_column], errors="coerce")
    invalid_volume = volume_series.isna() & ohlcv[volume_column].notna()
    if invalid_volume.any():
        raise OHLCVValidationError(
            "ohlcv column 'volume' must be numeric when a value is provided"
        )

    return ohlcv.copy()
