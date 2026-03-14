from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from .config import FULL_CANDLE_PATTERNS, MAJOR_CANDLE_PATTERNS
from .exceptions import PatternComputationError
from .models import FetcherOptions, NormalizedOHLCV, WarningEntry

PatternDetector = Callable[[pd.Series, pd.Series, pd.Series, pd.Series, int], int]


def _score_to_state(score: int) -> str:
    if score > 0:
        return "bullish"
    if score < 0:
        return "bearish"
    return "neutral"


def _body(open_: float, close: float) -> float:
    return abs(close - open_)


def _candle_range(high: float, low: float) -> float:
    return max(high - low, 0.0)


def _upper_shadow(open_: float, high: float, close: float) -> float:
    return max(high - max(open_, close), 0.0)


def _lower_shadow(open_: float, low: float, close: float) -> float:
    return max(min(open_, close) - low, 0.0)


def _prior_change(close: pd.Series, index: int, lookback: int = 3) -> float:
    if index <= 0:
        return 0.0
    start = max(0, index - lookback)
    return float(close.iloc[index - 1] - close.iloc[start])


def _is_bullish(open_: float, close: float) -> bool:
    return close > open_


def _is_bearish(open_: float, close: float) -> bool:
    return close < open_


def _detect_doji(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    candle_range = _candle_range(float(high.iloc[index]), float(low.iloc[index]))
    if candle_range == 0:
        return 0
    body = _body(float(open_.iloc[index]), float(close.iloc[index]))
    return 100 if body <= candle_range * 0.1 else 0


def _is_hammer_shape(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> bool:
    open_value = float(open_.iloc[index])
    high_value = float(high.iloc[index])
    low_value = float(low.iloc[index])
    close_value = float(close.iloc[index])
    candle_range = _candle_range(high_value, low_value)
    if candle_range == 0:
        return False
    body = _body(open_value, close_value)
    lower_shadow = _lower_shadow(open_value, low_value, close_value)
    upper_shadow = _upper_shadow(open_value, high_value, close_value)
    if lower_shadow < max(body * 2.0, candle_range * 0.35):
        return False
    if upper_shadow > max(body, candle_range * 0.15):
        return False
    if max(open_value, close_value) < high_value - (candle_range * 0.35):
        return False
    return True


def _is_inverted_hammer_shape(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> bool:
    open_value = float(open_.iloc[index])
    high_value = float(high.iloc[index])
    low_value = float(low.iloc[index])
    close_value = float(close.iloc[index])
    candle_range = _candle_range(high_value, low_value)
    if candle_range == 0:
        return False
    body = _body(open_value, close_value)
    lower_shadow = _lower_shadow(open_value, low_value, close_value)
    upper_shadow = _upper_shadow(open_value, high_value, close_value)
    if upper_shadow < max(body * 2.0, candle_range * 0.35):
        return False
    if lower_shadow > max(body, candle_range * 0.15):
        return False
    if min(open_value, close_value) > low_value + (candle_range * 0.35):
        return False
    return True


def _detect_hammer(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if _prior_change(close, index) >= 0:
        return 0
    return 100 if _is_hammer_shape(open_, high, low, close, index) else 0


def _detect_inverted_hammer(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if _prior_change(close, index) >= 0:
        return 0
    return 100 if _is_inverted_hammer_shape(open_, high, low, close, index) else 0


def _detect_hanging_man(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if _prior_change(close, index) <= 0:
        return 0
    return -100 if _is_hammer_shape(open_, high, low, close, index) else 0


def _detect_shooting_star(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if _prior_change(close, index) <= 0:
        return 0
    return -100 if _is_inverted_hammer_shape(open_, high, low, close, index) else 0


def _detect_engulfing(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if index < 1:
        return 0
    prev_open = float(open_.iloc[index - 1])
    prev_close = float(close.iloc[index - 1])
    current_open = float(open_.iloc[index])
    current_close = float(close.iloc[index])
    prev_body = _body(prev_open, prev_close)
    current_body = _body(current_open, current_close)
    if (
        _is_bearish(prev_open, prev_close)
        and _is_bullish(current_open, current_close)
        and current_open <= prev_close
        and current_close >= prev_open
        and current_body >= prev_body
    ):
        return 100
    if (
        _is_bullish(prev_open, prev_close)
        and _is_bearish(current_open, current_close)
        and current_open >= prev_close
        and current_close <= prev_open
        and current_body >= prev_body
    ):
        return -100
    return 0


def _detect_harami(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if index < 1:
        return 0
    prev_open = float(open_.iloc[index - 1])
    prev_close = float(close.iloc[index - 1])
    current_open = float(open_.iloc[index])
    current_close = float(close.iloc[index])
    prev_top = max(prev_open, prev_close)
    prev_bottom = min(prev_open, prev_close)
    current_top = max(current_open, current_close)
    current_bottom = min(current_open, current_close)
    if (
        _is_bearish(prev_open, prev_close)
        and _is_bullish(current_open, current_close)
        and current_top <= prev_top
        and current_bottom >= prev_bottom
        and _body(current_open, current_close) < _body(prev_open, prev_close)
    ):
        return 100
    if (
        _is_bullish(prev_open, prev_close)
        and _is_bearish(current_open, current_close)
        and current_top <= prev_top
        and current_bottom >= prev_bottom
        and _body(current_open, current_close) < _body(prev_open, prev_close)
    ):
        return -100
    return 0


def _detect_piercing(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if index < 1:
        return 0
    prev_open = float(open_.iloc[index - 1])
    prev_close = float(close.iloc[index - 1])
    current_open = float(open_.iloc[index])
    current_close = float(close.iloc[index])
    midpoint = (prev_open + prev_close) / 2.0
    if (
        _is_bearish(prev_open, prev_close)
        and _is_bullish(current_open, current_close)
        and current_open < prev_close
        and midpoint < current_close < prev_open
    ):
        return 100
    return 0


def _detect_dark_cloud_cover(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if index < 1:
        return 0
    prev_open = float(open_.iloc[index - 1])
    prev_close = float(close.iloc[index - 1])
    current_open = float(open_.iloc[index])
    current_close = float(close.iloc[index])
    midpoint = (prev_open + prev_close) / 2.0
    if (
        _is_bullish(prev_open, prev_close)
        and _is_bearish(current_open, current_close)
        and current_open > prev_close
        and prev_open < current_close < midpoint
    ):
        return -100
    return 0


def _detect_morning_star(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if index < 2:
        return 0
    first_open = float(open_.iloc[index - 2])
    first_close = float(close.iloc[index - 2])
    second_open = float(open_.iloc[index - 1])
    second_close = float(close.iloc[index - 1])
    third_open = float(open_.iloc[index])
    third_close = float(close.iloc[index])
    first_body = _body(first_open, first_close)
    second_body = _body(second_open, second_close)
    midpoint = (first_open + first_close) / 2.0
    if (
        _is_bearish(first_open, first_close)
        and second_body <= max(first_body * 0.6, 1e-9)
        and _is_bullish(third_open, third_close)
        and third_close > midpoint
    ):
        return 100
    return 0


def _detect_evening_star(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if index < 2:
        return 0
    first_open = float(open_.iloc[index - 2])
    first_close = float(close.iloc[index - 2])
    second_open = float(open_.iloc[index - 1])
    second_close = float(close.iloc[index - 1])
    third_open = float(open_.iloc[index])
    third_close = float(close.iloc[index])
    first_body = _body(first_open, first_close)
    second_body = _body(second_open, second_close)
    midpoint = (first_open + first_close) / 2.0
    if (
        _is_bullish(first_open, first_close)
        and second_body <= max(first_body * 0.6, 1e-9)
        and _is_bearish(third_open, third_close)
        and third_close < midpoint
    ):
        return -100
    return 0


def _detect_3white_soldiers(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if index < 2:
        return 0
    opens = [float(open_.iloc[index - offset]) for offset in (2, 1, 0)]
    closes = [float(close.iloc[index - offset]) for offset in (2, 1, 0)]
    if not all(_is_bullish(open_value, close_value) for open_value, close_value in zip(opens, closes, strict=True)):
        return 0
    if not (closes[0] < closes[1] < closes[2]):
        return 0
    if not (opens[1] >= opens[0] and opens[2] >= opens[1]):
        return 0
    return 100


def _detect_3black_crows(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if index < 2:
        return 0
    opens = [float(open_.iloc[index - offset]) for offset in (2, 1, 0)]
    closes = [float(close.iloc[index - offset]) for offset in (2, 1, 0)]
    if not all(_is_bearish(open_value, close_value) for open_value, close_value in zip(opens, closes, strict=True)):
        return 0
    if not (closes[0] > closes[1] > closes[2]):
        return 0
    if not (opens[1] <= opens[0] and opens[2] <= opens[1]):
        return 0
    return -100


def _detect_marubozu(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    open_value = float(open_.iloc[index])
    high_value = float(high.iloc[index])
    low_value = float(low.iloc[index])
    close_value = float(close.iloc[index])
    candle_range = _candle_range(high_value, low_value)
    if candle_range == 0:
        return 0
    body = _body(open_value, close_value)
    upper_shadow = _upper_shadow(open_value, high_value, close_value)
    lower_shadow = _lower_shadow(open_value, low_value, close_value)
    if body < candle_range * 0.9:
        return 0
    if max(upper_shadow, lower_shadow) > candle_range * 0.05:
        return 0
    if close_value > open_value:
        return 100
    if close_value < open_value:
        return -100
    return 0


def _detect_spinning_top(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    open_value = float(open_.iloc[index])
    high_value = float(high.iloc[index])
    low_value = float(low.iloc[index])
    close_value = float(close.iloc[index])
    candle_range = _candle_range(high_value, low_value)
    if candle_range == 0:
        return 0
    body = _body(open_value, close_value)
    upper_shadow = _upper_shadow(open_value, high_value, close_value)
    lower_shadow = _lower_shadow(open_value, low_value, close_value)
    if body > candle_range * 0.25:
        return 0
    if upper_shadow < body or lower_shadow < body:
        return 0
    if close_value > open_value:
        return 100
    if close_value < open_value:
        return -100
    return 0


def _detect_long_legged_doji(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    open_value = float(open_.iloc[index])
    high_value = float(high.iloc[index])
    low_value = float(low.iloc[index])
    close_value = float(close.iloc[index])
    candle_range = _candle_range(high_value, low_value)
    if candle_range == 0:
        return 0
    body = _body(open_value, close_value)
    upper_shadow = _upper_shadow(open_value, high_value, close_value)
    lower_shadow = _lower_shadow(open_value, low_value, close_value)
    if body > candle_range * 0.1:
        return 0
    if upper_shadow < candle_range * 0.3 or lower_shadow < candle_range * 0.3:
        return 0
    return 100


def _detect_belt_hold(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    open_value = float(open_.iloc[index])
    high_value = float(high.iloc[index])
    low_value = float(low.iloc[index])
    close_value = float(close.iloc[index])
    candle_range = _candle_range(high_value, low_value)
    if candle_range == 0:
        return 0
    body = _body(open_value, close_value)
    if body < candle_range * 0.7:
        return 0
    upper_shadow = _upper_shadow(open_value, high_value, close_value)
    lower_shadow = _lower_shadow(open_value, low_value, close_value)
    if _prior_change(close, index) < 0 and _is_bullish(open_value, close_value):
        return 100 if lower_shadow <= candle_range * 0.05 else 0
    if _prior_change(close, index) > 0 and _is_bearish(open_value, close_value):
        return -100 if upper_shadow <= candle_range * 0.05 else 0
    return 0


def _detect_kicking(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if index < 1:
        return 0
    previous_score = _detect_marubozu(open_, high, low, close, index - 1)
    current_score = _detect_marubozu(open_, high, low, close, index)
    if previous_score == -100 and current_score == 100:
        if float(low.iloc[index]) > float(high.iloc[index - 1]):
            return 100
    if previous_score == 100 and current_score == -100:
        if float(high.iloc[index]) < float(low.iloc[index - 1]):
            return -100
    return 0


def _detect_takuri(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    if _prior_change(close, index) >= 0:
        return 0
    open_value = float(open_.iloc[index])
    high_value = float(high.iloc[index])
    low_value = float(low.iloc[index])
    close_value = float(close.iloc[index])
    candle_range = _candle_range(high_value, low_value)
    if candle_range == 0:
        return 0
    body = _body(open_value, close_value)
    lower_shadow = _lower_shadow(open_value, low_value, close_value)
    upper_shadow = _upper_shadow(open_value, high_value, close_value)
    if lower_shadow >= max(body * 3.0, candle_range * 0.6) and upper_shadow <= candle_range * 0.1:
        return 100
    return 0


def _detect_rickshaw_man(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, index: int
) -> int:
    open_value = float(open_.iloc[index])
    high_value = float(high.iloc[index])
    low_value = float(low.iloc[index])
    close_value = float(close.iloc[index])
    candle_range = _candle_range(high_value, low_value)
    if candle_range == 0:
        return 0
    body = _body(open_value, close_value)
    upper_shadow = _upper_shadow(open_value, high_value, close_value)
    lower_shadow = _lower_shadow(open_value, low_value, close_value)
    if body > candle_range * 0.1:
        return 0
    if upper_shadow < candle_range * 0.25 or lower_shadow < candle_range * 0.25:
        return 0
    shadow_gap = abs(upper_shadow - lower_shadow)
    if shadow_gap > candle_range * 0.2:
        return 0
    return 100


PATTERN_DETECTORS: dict[str, PatternDetector] = {
    "cdl_doji": _detect_doji,
    "cdl_hammer": _detect_hammer,
    "cdl_inverted_hammer": _detect_inverted_hammer,
    "cdl_hanging_man": _detect_hanging_man,
    "cdl_shooting_star": _detect_shooting_star,
    "cdl_engulfing": _detect_engulfing,
    "cdl_harami": _detect_harami,
    "cdl_piercing": _detect_piercing,
    "cdl_dark_cloud_cover": _detect_dark_cloud_cover,
    "cdl_morning_star": _detect_morning_star,
    "cdl_evening_star": _detect_evening_star,
    "cdl_3white_soldiers": _detect_3white_soldiers,
    "cdl_3black_crows": _detect_3black_crows,
    "cdl_marubozu": _detect_marubozu,
    "cdl_spinning_top": _detect_spinning_top,
    "cdl_long_legged_doji": _detect_long_legged_doji,
    "cdl_belt_hold": _detect_belt_hold,
    "cdl_kicking": _detect_kicking,
    "cdl_takuri": _detect_takuri,
    "cdl_rickshaw_man": _detect_rickshaw_man,
}


def _build_pattern_series(
    detector: PatternDetector,
    open_: pd.Series,
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
) -> pd.Series:
    return pd.Series(
        [detector(open_, high, low, close, index) for index in range(len(close))],
        index=close.index,
        dtype="int64",
    )


def compute_candlestick_patterns(
    normalized: NormalizedOHLCV,
    *,
    options: FetcherOptions | None = None,
) -> tuple[dict[str, object], list[WarningEntry]]:
    resolved_options = options or FetcherOptions()
    if resolved_options.pattern_profile == "major_only":
        pattern_names = MAJOR_CANDLE_PATTERNS
    elif resolved_options.pattern_profile == "full":
        pattern_names = FULL_CANDLE_PATTERNS
    else:
        raise PatternComputationError(
            "pattern_profile must be either 'major_only' or 'full'"
        )

    open_ = pd.Series(normalized.open_arr, dtype="float64")
    high = pd.Series(normalized.high_arr, dtype="float64")
    low = pd.Series(normalized.low_arr, dtype="float64")
    close = pd.Series(normalized.close_arr, dtype="float64")

    latest_bar: dict[str, dict[str, int | str | bool]] = {}
    recent_hits: list[dict[str, int | str]] = []
    warnings: list[WarningEntry] = []
    recent_window = max(min(resolved_options.pattern_recent_window, len(close)), 0)
    start_index = len(close) - recent_window

    for pattern_name in pattern_names:
        detector = PATTERN_DETECTORS[pattern_name]
        try:
            scores = _build_pattern_series(detector, open_, high, low, close)
        except Exception as error:
            if resolved_options.strict:
                raise PatternComputationError(
                    f"failed to compute candlestick pattern '{pattern_name}'"
                ) from error
            warnings.append(
                WarningEntry(
                    code="pattern_compute_failed",
                    message=f"failed to compute candlestick pattern '{pattern_name}'",
                )
            )
            scores = pd.Series(0, index=close.index, dtype="int64")

        latest_score = int(scores.iloc[-1]) if not scores.empty else 0
        latest_bar[pattern_name] = {
            "score": latest_score,
            "state": _score_to_state(latest_score),
            "detected_on_latest_bar": latest_score != 0,
        }

        if recent_window == 0:
            continue

        for index in range(start_index, len(scores)):
            score = int(scores.iloc[index])
            if score == 0:
                continue
            recent_hits.append(
                {
                    "pattern": pattern_name,
                    "score": score,
                    "state": _score_to_state(score),
                    "bars_ago": len(scores) - 1 - index,
                }
            )

    recent_hits.sort(key=lambda hit: (int(hit["bars_ago"]), str(hit["pattern"])))

    return {
        "latest_bar": latest_bar,
        "recent_hits": recent_hits,
    }, warnings
