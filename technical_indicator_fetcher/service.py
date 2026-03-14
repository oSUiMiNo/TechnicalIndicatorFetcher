from __future__ import annotations

from datetime import datetime
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from .derived import build_derived_states
from .indicators import compute_indicators
from .models import BatchRequest, FetcherOptions, WarningEntry
from .patterns import compute_candlestick_patterns
from .preprocess import normalize_ohlcv
from .serializer import serialize_to_json, serialize_to_yaml


def _normalize_timestamp(value: str | datetime | pd.Timestamp) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.isoformat()
    return timestamp.isoformat()


def _safe_float(value: float | int | None) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _build_data_summary(normalized) -> dict[str, Any]:
    latest_open = _safe_float(normalized.open_arr[-1]) if normalized.open_arr.size else None
    latest_high = _safe_float(normalized.high_arr[-1]) if normalized.high_arr.size else None
    latest_low = _safe_float(normalized.low_arr[-1]) if normalized.low_arr.size else None
    latest_close = _safe_float(normalized.close_arr[-1]) if normalized.close_arr.size else None
    latest_volume = _safe_float(normalized.volume_arr[-1]) if normalized.volume_arr.size else None
    return {
        "bars_used": normalized.bars_used,
        "latest_open": latest_open,
        "latest_high": latest_high,
        "latest_low": latest_low,
        "latest_close": latest_close,
        "latest_volume": latest_volume,
        "adjusted": normalized.adjusted,
        "candle_status": normalized.candle_status,
    }


def build_output_snapshot(
    *,
    symbol: str,
    timeframe: str,
    normalized,
    raw: dict[str, Any],
    derived: dict[str, dict[str, str]],
    history: dict[str, Any] | None,
    candlestick_patterns: dict[str, Any],
    warnings: list[WarningEntry],
    as_of: str | datetime | pd.Timestamp,
    options: FetcherOptions,
) -> dict[str, Any]:
    indicators: dict[str, Any] = {
        "raw": raw,
        "derived": derived,
    }
    if history is not None:
        indicators["history"] = history

    return {
        "schema_version": options.yaml_version,
        "generated_at": _normalize_timestamp(as_of),
        "symbol": symbol,
        "timeframe": timeframe,
        "as_of": _normalize_timestamp(as_of),
        "data_summary": _build_data_summary(normalized),
        "indicators": indicators,
        "candlestick_patterns": candlestick_patterns,
        "warnings": [warning.as_dict() for warning in warnings],
    }


def _serialize_snapshot(snapshot: dict[str, Any], options: FetcherOptions) -> str:
    if options.output_format == "yaml":
        return serialize_to_yaml(snapshot, emit_comments=options.emit_yaml_comments)
    if options.output_format == "json":
        return serialize_to_json(snapshot, indent=options.json_indent)
    raise ValueError("output_format must be either 'yaml' or 'json'")


def _resolve_timeframe_as_of(
    timeframe: str,
    as_of: str | datetime | pd.Timestamp | Mapping[str, str | datetime | pd.Timestamp],
) -> str | datetime | pd.Timestamp:
    if isinstance(as_of, Mapping):
        if timeframe not in as_of:
            raise KeyError(f"missing as_of for timeframe '{timeframe}'")
        return as_of[timeframe]
    return as_of


def _resolve_timeframe_options(
    timeframe: str,
    options: FetcherOptions | Mapping[str, FetcherOptions] | None,
) -> FetcherOptions | None:
    if isinstance(options, Mapping):
        return options.get(timeframe)
    return options


def technical_indicator_fetcher(
    symbol: str,
    timeframe: str,
    ohlcv: pd.DataFrame,
    as_of: str | datetime | pd.Timestamp,
    options: FetcherOptions | None = None,
    return_dict: bool = False,
) -> str | dict[str, Any]:
    resolved_options = options or FetcherOptions()
    normalized = normalize_ohlcv(ohlcv, as_of=as_of, options=resolved_options)
    raw, history_context, history_payload, indicator_warnings = compute_indicators(
        normalized,
        options=resolved_options,
    )
    candlestick_patterns, pattern_warnings = compute_candlestick_patterns(
        normalized,
        options=resolved_options,
    )
    derived = build_derived_states(
        raw,
        history_context,
        normalized,
        options=resolved_options,
    )
    snapshot = build_output_snapshot(
        symbol=symbol,
        timeframe=timeframe,
        normalized=normalized,
        raw=raw,
        derived=derived,
        history=history_payload,
        candlestick_patterns=candlestick_patterns,
        warnings=[*normalized.warnings, *indicator_warnings, *pattern_warnings],
        as_of=as_of,
        options=resolved_options,
    )
    if return_dict:
        return snapshot
    return _serialize_snapshot(snapshot, resolved_options)


def multi_timeframe_indicator_fetcher(
    symbol: str,
    ohlcv_by_timeframe: Mapping[str, pd.DataFrame],
    as_of: str | datetime | pd.Timestamp | Mapping[str, str | datetime | pd.Timestamp],
    options: FetcherOptions | Mapping[str, FetcherOptions] | None = None,
    return_dict: bool = False,
) -> dict[str, Any]:
    return {
        timeframe: technical_indicator_fetcher(
            symbol=symbol,
            timeframe=timeframe,
            ohlcv=ohlcv,
            as_of=_resolve_timeframe_as_of(timeframe, as_of),
            options=_resolve_timeframe_options(timeframe, options),
            return_dict=return_dict,
        )
        for timeframe, ohlcv in ohlcv_by_timeframe.items()
    }


def batch_technical_indicator_fetcher(
    requests: Sequence[BatchRequest],
    *,
    return_dict: bool = False,
) -> list[str | dict[str, Any]]:
    return [
        technical_indicator_fetcher(
            symbol=request.symbol,
            timeframe=request.timeframe,
            ohlcv=request.ohlcv,
            as_of=request.as_of,
            options=request.options,
            return_dict=return_dict,
        )
        for request in requests
    ]
