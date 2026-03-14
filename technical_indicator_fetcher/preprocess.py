from __future__ import annotations

from datetime import datetime

import pandas as pd

from .config import VALID_CANDLE_STATUSES
from .exceptions import OHLCVValidationError
from .models import FetcherOptions, NormalizedOHLCV, WarningEntry
from .validator import REQUIRED_COLUMNS, TIMESTAMP_CANDIDATES, validate_ohlcv


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [
        str(column).strip().lower().replace(" ", "_") for column in normalized.columns
    ]
    return normalized


def _normalize_index(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.index, pd.DatetimeIndex):
        normalized = df.copy()
        normalized.index = pd.to_datetime(normalized.index, errors="raise")
        normalized.index.name = "timestamp"
        return normalized

    for candidate in TIMESTAMP_CANDIDATES:
        if candidate in df.columns:
            normalized = df.copy()
            normalized.index = pd.to_datetime(normalized.pop(candidate), errors="raise")
            normalized.index.name = "timestamp"
            return normalized

    raise OHLCVValidationError(
        "ohlcv must have a DatetimeIndex or a timestamp-like column"
    )


def _coerce_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for column in REQUIRED_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="raise")
    return normalized


def _normalize_as_of(as_of: str | datetime | pd.Timestamp | None) -> pd.Timestamp | None:
    if as_of is None:
        return None

    timestamp = pd.Timestamp(as_of)
    if pd.isna(timestamp):
        raise OHLCVValidationError("as_of must be a valid timestamp")
    return timestamp


def _infer_adjusted(
    df: pd.DataFrame, options: FetcherOptions
) -> tuple[bool, WarningEntry | None]:
    explicit_adjusted: bool | None = None
    for attr_key in ("adjusted", "price_adjusted"):
        attr_value = df.attrs.get(attr_key)
        if isinstance(attr_value, bool):
            explicit_adjusted = attr_value
            break

    if explicit_adjusted is None and "adjusted" in df.columns and not df["adjusted"].empty:
        value = df["adjusted"].iloc[-1]
        if pd.notna(value):
            explicit_adjusted = bool(value)

    if explicit_adjusted is None and "adj_close" in df.columns:
        explicit_adjusted = True

    adjusted = options.price_adjustment if explicit_adjusted is None else explicit_adjusted
    warning = None
    if options.price_adjustment and explicit_adjusted is False:
        warning = WarningEntry(
            code="adjusted_price_unavailable",
            message="price_adjustment was requested but input indicates unadjusted prices",
        )

    return adjusted, warning


def _infer_candle_status(
    df: pd.DataFrame, as_of: pd.Timestamp | None, latest_timestamp: pd.Timestamp
) -> tuple[str, WarningEntry | None]:
    if "candle_status" in df.columns and not df["candle_status"].empty:
        raw_value = str(df["candle_status"].iloc[-1]).strip().lower()
        if raw_value not in VALID_CANDLE_STATUSES:
            raise OHLCVValidationError(
                "candle_status must be either 'closed' or 'unclosed'"
            )
        warning = None
        if raw_value == "unclosed":
            warning = WarningEntry(
                code="unclosed_latest_candle",
                message="latest candle is marked as unclosed",
            )
        return raw_value, warning

    if "is_closed" in df.columns and not df["is_closed"].empty:
        is_closed = bool(df["is_closed"].iloc[-1])
        if is_closed:
            return "closed", None
        return (
            "unclosed",
            WarningEntry(
                code="unclosed_latest_candle",
                message="latest candle is marked as unclosed",
            ),
        )

    if as_of is None or latest_timestamp == as_of:
        return "closed", None

    return (
        "unclosed",
        WarningEntry(
            code="unclosed_latest_candle",
            message="latest candle timestamp does not match as_of",
        ),
    )


def _build_bar_warning(bars_used: int, options: FetcherOptions) -> WarningEntry | None:
    if bars_used >= options.minimum_bars:
        return None

    return WarningEntry(
        code="insufficient_bars",
        message=(
            f"received {bars_used} bars, but at least {options.minimum_bars} are "
            f"required and {options.recommended_bars} are recommended"
        ),
    )


def _build_volume_warning(df: pd.DataFrame) -> WarningEntry | None:
    if not df["volume"].isna().any():
        return None

    return WarningEntry(
        code="missing_volume",
        message="volume column contains null values and volume indicators will be skipped",
    )


def normalize_ohlcv(
    ohlcv: pd.DataFrame,
    *,
    as_of: str | datetime | pd.Timestamp | None = None,
    options: FetcherOptions | None = None,
) -> NormalizedOHLCV:
    resolved_options = options or FetcherOptions()
    validated = validate_ohlcv(ohlcv)
    normalized = _normalize_columns(validated)
    normalized = _normalize_index(normalized)
    normalized = _coerce_required_columns(normalized)

    latest_timestamp = normalized.index[-1]
    resolved_as_of = _normalize_as_of(as_of)
    candle_status, candle_warning = _infer_candle_status(
        normalized, resolved_as_of, latest_timestamp
    )
    adjusted, adjusted_warning = _infer_adjusted(normalized, resolved_options)
    bars_used = len(normalized)

    warnings: list[WarningEntry] = []
    bar_warning = _build_bar_warning(bars_used, resolved_options)
    if bar_warning is not None:
        warnings.append(bar_warning)
    volume_warning = _build_volume_warning(normalized)
    if volume_warning is not None:
        warnings.append(volume_warning)
    if candle_warning is not None:
        if resolved_options.strict:
            raise OHLCVValidationError(candle_warning.message)
        warnings.append(candle_warning)
    if adjusted_warning is not None:
        if resolved_options.strict:
            raise OHLCVValidationError(adjusted_warning.message)
        warnings.append(adjusted_warning)

    return NormalizedOHLCV(
        dataframe=normalized,
        open_arr=normalized["open"].to_numpy(dtype="float64", copy=True),
        high_arr=normalized["high"].to_numpy(dtype="float64", copy=True),
        low_arr=normalized["low"].to_numpy(dtype="float64", copy=True),
        close_arr=normalized["close"].to_numpy(dtype="float64", copy=True),
        volume_arr=normalized["volume"].to_numpy(dtype="float64", copy=True),
        bars_used=bars_used,
        adjusted=adjusted,
        candle_status=candle_status,
        latest_timestamp=latest_timestamp,
        warnings=warnings,
    )
