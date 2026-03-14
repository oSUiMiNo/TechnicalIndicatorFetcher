from __future__ import annotations

from typing import Any

import pandas as pd

from .config import DEFAULT_THRESHOLDS
from .models import FetcherOptions, NormalizedOHLCV


def _compare_values(left: float | None, right: float | None) -> str:
    if left is None or right is None:
        return "unknown"
    if left > right:
        return "above"
    if left < right:
        return "below"
    return "equal"


def _extract_nested(raw: dict[str, Any], key: str, nested_key: str) -> float | None:
    payload = raw.get(key)
    if not isinstance(payload, dict):
        return None
    value = payload.get(nested_key)
    if value is None:
        return None
    return float(value)


def _adx_state(adx: float | None) -> str:
    if adx is None:
        return "unknown"
    if adx < DEFAULT_THRESHOLDS["adx_trend_present"]:
        return "weak_trend"
    if adx < DEFAULT_THRESHOLDS["adx_strong_trend"]:
        return "trend_present"
    return "strong_trend"


def _di_direction(plus_di: float | None, minus_di: float | None) -> str:
    if plus_di is None or minus_di is None:
        return "unknown"
    if plus_di > minus_di:
        return "bullish"
    if plus_di < minus_di:
        return "bearish"
    return "neutral"


def _sar_position(sar: float | None, close: float | None) -> str:
    if sar is None or close is None:
        return "unknown"
    if sar < close:
        return "below_price"
    if sar > close:
        return "above_price"
    return "equal"


def _rsi_state(rsi: float | None) -> str:
    if rsi is None:
        return "unknown"
    if rsi >= DEFAULT_THRESHOLDS["rsi_overbought"]:
        return "overbought"
    if rsi <= DEFAULT_THRESHOLDS["rsi_oversold"]:
        return "oversold"
    return "neutral"


def _macd_histogram_sign(hist: float | None) -> str:
    if hist is None:
        return "unknown"
    if hist > 0:
        return "positive"
    if hist < 0:
        return "negative"
    return "zero"


def _stochastic_state(slowk: float | None) -> str:
    if slowk is None:
        return "unknown"
    if slowk >= DEFAULT_THRESHOLDS["stochastic_upper_zone"]:
        return "upper_zone"
    if slowk <= DEFAULT_THRESHOLDS["stochastic_lower_zone"]:
        return "lower_zone"
    return "middle_zone"


def _bbands_position(
    close: float | None, upper: float | None, middle: float | None, lower: float | None
) -> str:
    if close is None or upper is None or middle is None or lower is None:
        return "unknown"
    if close > upper:
        return "above_upper"
    if close < lower:
        return "below_lower"
    if middle <= close <= upper:
        return "inside_upper_half"
    return "inside_lower_half"


def _percent_rank_state(series: pd.Series, window: int) -> str:
    tail = series.dropna().iloc[-window:]
    if tail.empty or len(tail) < window:
        return "unknown"
    latest = float(tail.iloc[-1])
    rank = float((tail <= latest).sum()) / float(len(tail))
    if rank >= DEFAULT_THRESHOLDS["volatility_high_percentile"]:
        return "high"
    if rank <= DEFAULT_THRESHOLDS["volatility_low_percentile"]:
        return "low"
    return "normal"


def _obv_direction(series: pd.Series, window: int) -> str:
    tail = series.dropna()
    if len(tail) <= window:
        return "unknown"
    latest = float(tail.iloc[-1])
    previous = float(tail.iloc[-(window + 1)])
    if latest > previous:
        return "up"
    if latest < previous:
        return "down"
    return "flat"


def _mfi_state(mfi: float | None) -> str:
    if mfi is None:
        return "unknown"
    if mfi >= DEFAULT_THRESHOLDS["mfi_overbought"]:
        return "overbought"
    if mfi <= DEFAULT_THRESHOLDS["mfi_oversold"]:
        return "oversold"
    return "neutral"


def _adosc_sign(adosc: float | None) -> str:
    if adosc is None:
        return "unknown"
    if adosc > 0:
        return "positive"
    if adosc < 0:
        return "negative"
    return "zero"


def build_derived_states(
    raw: dict[str, Any],
    history_context: dict[str, pd.Series],
    normalized: NormalizedOHLCV,
    *,
    options: FetcherOptions | None = None,
) -> dict[str, dict[str, str]]:
    resolved_options = options or FetcherOptions()
    latest_close = float(normalized.close_arr[-1]) if normalized.close_arr.size else None

    macd_value = _extract_nested(raw, "macd", "macd")
    macd_signal = _extract_nested(raw, "macd", "signal")
    macd_hist = _extract_nested(raw, "macd", "hist")
    bb_upper = _extract_nested(raw, "bbands_20_2", "upper")
    bb_middle = _extract_nested(raw, "bbands_20_2", "middle")
    bb_lower = _extract_nested(raw, "bbands_20_2", "lower")
    slowk = _extract_nested(raw, "stoch", "slowk")

    return {
        "trend": {
            "close_vs_sma20": _compare_values(latest_close, raw.get("sma_20")),
            "close_vs_sma50": _compare_values(latest_close, raw.get("sma_50")),
            "close_vs_sma200": _compare_values(latest_close, raw.get("sma_200")),
            "sma20_vs_sma50": _compare_values(raw.get("sma_20"), raw.get("sma_50")),
            "sma50_vs_sma200": _compare_values(raw.get("sma_50"), raw.get("sma_200")),
            "adx_state": _adx_state(raw.get("adx_14")),
            "di_direction": _di_direction(raw.get("plus_di_14"), raw.get("minus_di_14")),
            "sar_position": _sar_position(raw.get("sar"), latest_close),
        },
        "momentum": {
            "rsi_state": _rsi_state(raw.get("rsi_14")),
            "macd_vs_signal": _compare_values(macd_value, macd_signal),
            "macd_histogram_sign": _macd_histogram_sign(macd_hist),
            "stochastic_state": _stochastic_state(slowk),
        },
        "volatility": {
            "bbands_position": _bbands_position(latest_close, bb_upper, bb_middle, bb_lower),
            "atr_regime": _percent_rank_state(
                history_context.get("atr_14", pd.Series(dtype="float64")),
                resolved_options.volatility_regime_window,
            ),
            "natr_regime": _percent_rank_state(
                history_context.get("natr_14", pd.Series(dtype="float64")),
                resolved_options.volatility_regime_window,
            ),
        },
        "volume": {
            "obv_direction_5": _obv_direction(
                history_context.get("obv", pd.Series(dtype="float64")),
                resolved_options.obv_direction_window,
            ),
            "mfi_state": _mfi_state(raw.get("mfi_14")),
            "adosc_sign": _adosc_sign(raw.get("adosc")),
        },
    }
