from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from .config import DEFAULT_WINDOWS
from .exceptions import IndicatorComputationError
from .models import FetcherOptions, NormalizedOHLCV, WarningEntry


def _as_series(values: np.ndarray) -> pd.Series:
    return pd.Series(values.astype("float64", copy=False))


def _wilder_smoothing(series: pd.Series, period: int) -> pd.Series:
    values = series.to_numpy(dtype="float64", copy=False)
    result = np.full(len(values), np.nan, dtype="float64")
    if len(values) < period:
        return pd.Series(result, index=series.index, dtype="float64")

    initial = np.nansum(values[:period])
    result[period - 1] = initial
    for index in range(period, len(values)):
        result[index] = result[index - 1] - (result[index - 1] / period) + values[index]
    return pd.Series(result, index=series.index, dtype="float64")


def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False, min_periods=period).mean()


def _rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    average_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    average_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = average_gain / average_loss
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.where(average_loss != 0, 100.0)
    rsi = rsi.where(~((average_gain == 0) & (average_loss == 0)), 50.0)
    return rsi


def _true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    previous_close = close.shift(1)
    components = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    )
    return components.max(axis=1)


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    tr = _true_range(high, low, close)
    smoothed = _wilder_smoothing(tr, period)
    smoothed.iloc[: period - 1] = np.nan
    return smoothed / period


def _adx_components(
    high: pd.Series, low: pd.Series, close: pd.Series, period: int
) -> tuple[pd.Series, pd.Series, pd.Series]:
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=high.index,
        dtype="float64",
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=high.index,
        dtype="float64",
    )
    atr = _atr(high, low, close, period)
    plus_di = 100 * (_wilder_smoothing(plus_dm, period) / period) / atr
    minus_di = 100 * (_wilder_smoothing(minus_dm, period) / period) / atr
    denominator = plus_di + minus_di
    dx = 100 * (plus_di - minus_di).abs() / denominator
    dx = dx.where(denominator != 0)
    adx = dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    return adx, plus_di, minus_di


def _stochastic(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    fastk_period: int,
    slowk_period: int,
    slowd_period: int,
) -> tuple[pd.Series, pd.Series]:
    lowest_low = low.rolling(window=fastk_period, min_periods=fastk_period).min()
    highest_high = high.rolling(window=fastk_period, min_periods=fastk_period).max()
    denominator = highest_high - lowest_low
    fastk = 100 * (close - lowest_low) / denominator
    fastk = fastk.where(denominator != 0)
    slowk = fastk.rolling(window=slowk_period, min_periods=slowk_period).mean()
    slowd = slowk.rolling(window=slowd_period, min_periods=slowd_period).mean()
    return slowk, slowd


def _willr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    lowest_low = low.rolling(window=period, min_periods=period).min()
    highest_high = high.rolling(window=period, min_periods=period).max()
    denominator = highest_high - lowest_low
    willr = -100 * (highest_high - close) / denominator
    return willr.where(denominator != 0)


def _cci(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    typical_price = (high + low + close) / 3
    moving_average = typical_price.rolling(window=period, min_periods=period).mean()
    mean_deviation = typical_price.rolling(window=period, min_periods=period).apply(
        lambda values: np.mean(np.abs(values - values.mean())),
        raw=True,
    )
    denominator = 0.015 * mean_deviation
    cci = (typical_price - moving_average) / denominator
    return cci.where(denominator != 0)


def _roc(close: pd.Series, period: int) -> pd.Series:
    previous = close.shift(period)
    roc = 100 * (close - previous) / previous
    return roc.where(previous != 0)


def _bbands(close: pd.Series, period: int, deviations: float) -> tuple[pd.Series, pd.Series, pd.Series]:
    middle = close.rolling(window=period, min_periods=period).mean()
    std = close.rolling(window=period, min_periods=period).std(ddof=0)
    upper = middle + (std * deviations)
    lower = middle - (std * deviations)
    return upper, middle, lower


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = close.diff().fillna(0.0)
    signed_volume = np.where(direction > 0, volume, np.where(direction < 0, -volume, 0.0))
    return pd.Series(signed_volume, index=close.index, dtype="float64").cumsum()


def _mfi(
    high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int
) -> pd.Series:
    typical_price = (high + low + close) / 3
    raw_money_flow = typical_price * volume
    price_change = typical_price.diff()
    positive_flow = raw_money_flow.where(price_change > 0, 0.0)
    negative_flow = raw_money_flow.where(price_change < 0, 0.0).abs()
    positive_sum = positive_flow.rolling(window=period, min_periods=period).sum()
    negative_sum = negative_flow.rolling(window=period, min_periods=period).sum()
    ratio = positive_sum / negative_sum
    mfi = 100 - (100 / (1 + ratio))
    mfi = mfi.where(negative_sum != 0, 100.0)
    mfi = mfi.where(~((positive_sum == 0) & (negative_sum == 0)), 50.0)
    return mfi


def _ad(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> pd.Series:
    denominator = high - low
    multiplier = ((close - low) - (high - close)) / denominator
    multiplier = multiplier.where(denominator != 0, 0.0)
    money_flow_volume = multiplier * volume
    return money_flow_volume.cumsum()


def _adosc(ad_line: pd.Series, fast_period: int = 3, slow_period: int = 10) -> pd.Series:
    fast = ad_line.ewm(span=fast_period, adjust=False, min_periods=fast_period).mean()
    slow = ad_line.ewm(span=slow_period, adjust=False, min_periods=slow_period).mean()
    return fast - slow


def _sar(high: pd.Series, low: pd.Series, acceleration: float = 0.02, maximum: float = 0.2) -> pd.Series:
    if high.empty:
        return pd.Series(dtype="float64")

    result = np.full(len(high), np.nan, dtype="float64")
    if len(high) == 1:
        result[0] = low.iloc[0]
        return pd.Series(result, index=high.index, dtype="float64")

    long_position = high.iloc[1] >= high.iloc[0]
    extreme_point = high.iloc[0] if long_position else low.iloc[0]
    sar = low.iloc[0] if long_position else high.iloc[0]
    af = acceleration
    result[0] = sar

    for index in range(1, len(high)):
        sar = sar + af * (extreme_point - sar)
        if long_position:
            sar = min(sar, low.iloc[index - 1])
            if index > 1:
                sar = min(sar, low.iloc[index - 2])
            if low.iloc[index] < sar:
                long_position = False
                sar = extreme_point
                extreme_point = low.iloc[index]
                af = acceleration
            else:
                if high.iloc[index] > extreme_point:
                    extreme_point = high.iloc[index]
                    af = min(af + acceleration, maximum)
        else:
            sar = max(sar, high.iloc[index - 1])
            if index > 1:
                sar = max(sar, high.iloc[index - 2])
            if high.iloc[index] > sar:
                long_position = True
                sar = extreme_point
                extreme_point = high.iloc[index]
                af = acceleration
            else:
                if low.iloc[index] < extreme_point:
                    extreme_point = low.iloc[index]
                    af = min(af + acceleration, maximum)
        result[index] = sar

    return pd.Series(result, index=high.index, dtype="float64")


def _latest_scalar(series: pd.Series) -> float | None:
    if series.empty:
        return None
    value = series.iloc[-1]
    if pd.isna(value):
        return None
    return float(value)


def _history_payload(series: pd.Series, lookback: int) -> list[float | None]:
    window = series.iloc[-lookback:] if lookback > 0 else series.iloc[0:0]
    payload: list[float | None] = []
    for value in window.tolist():
        if pd.isna(value):
            payload.append(None)
        else:
            payload.append(float(value))
    return payload


def _warning_or_raise(
    *,
    options: FetcherOptions,
    code: str,
    message: str,
    warnings: list[WarningEntry],
    error: Exception,
) -> None:
    if options.strict:
        raise IndicatorComputationError(message) from error
    warnings.append(WarningEntry(code=code, message=message))


def _nan_series(length: int) -> pd.Series:
    return pd.Series(np.nan, index=range(length), dtype="float64")


def _aroon(high: pd.Series, low: pd.Series, period: int) -> tuple[pd.Series, pd.Series]:
    def _aroon_up(values: np.ndarray) -> float:
        periods_since_high = len(values) - 1 - int(np.argmax(values))
        return 100.0 * (period - periods_since_high) / period

    def _aroon_down(values: np.ndarray) -> float:
        periods_since_low = len(values) - 1 - int(np.argmin(values))
        return 100.0 * (period - periods_since_low) / period

    up = high.rolling(window=period, min_periods=period).apply(_aroon_up, raw=True)
    down = low.rolling(window=period, min_periods=period).apply(_aroon_down, raw=True)
    return up, down


def _ppo(
    close: pd.Series, fast_period: int, slow_period: int, signal_period: int
) -> tuple[pd.Series, pd.Series, pd.Series]:
    fast = _ema(close, fast_period)
    slow = _ema(close, slow_period)
    ppo = 100 * (fast - slow) / slow.replace(0, np.nan)
    signal = ppo.ewm(span=signal_period, adjust=False, min_periods=signal_period).mean()
    hist = ppo - signal
    return ppo, signal, hist


def _trix(close: pd.Series, period: int) -> pd.Series:
    ema1 = _ema(close, period)
    ema2 = _ema(ema1, period)
    ema3 = _ema(ema2, period)
    previous = ema3.shift(1)
    trix = 100 * (ema3 - previous) / previous
    return trix.where(previous != 0)


def _kama(
    close: pd.Series, period: int, fast_period: int, slow_period: int
) -> pd.Series:
    if close.empty:
        return pd.Series(dtype="float64")

    change = (close - close.shift(period)).abs()
    volatility = close.diff().abs().rolling(window=period, min_periods=period).sum()
    efficiency_ratio = change / volatility.replace(0, np.nan)
    fast_constant = 2 / (fast_period + 1)
    slow_constant = 2 / (slow_period + 1)
    smoothing_constant = (efficiency_ratio * (fast_constant - slow_constant) + slow_constant) ** 2

    kama = pd.Series(np.nan, index=close.index, dtype="float64")
    if len(close) < period:
        return kama

    kama.iloc[period - 1] = close.iloc[:period].mean()
    for index in range(period, len(close)):
        previous = kama.iloc[index - 1]
        if pd.isna(previous):
            previous = close.iloc[index - 1]
        factor = smoothing_constant.iloc[index]
        if pd.isna(factor):
            kama.iloc[index] = previous
            continue
        kama.iloc[index] = previous + factor * (close.iloc[index] - previous)
    return kama


def _linearreg_slope(close: pd.Series, period: int) -> pd.Series:
    x = np.arange(period, dtype="float64")
    x_mean = x.mean()
    denominator = np.square(x - x_mean).sum()
    return close.rolling(window=period, min_periods=period).apply(
        lambda values: float(np.dot(x - x_mean, values - values.mean()) / denominator),
        raw=True,
    )


def _mom(close: pd.Series, period: int) -> pd.Series:
    return close - close.shift(period)


def _cmo(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gains = delta.clip(lower=0.0)
    losses = -delta.clip(upper=0.0)
    gain_sum = gains.rolling(window=period, min_periods=period).sum()
    loss_sum = losses.rolling(window=period, min_periods=period).sum()
    denominator = gain_sum + loss_sum
    cmo = 100 * (gain_sum - loss_sum) / denominator
    return cmo.where(denominator != 0, 0.0)


def _all_leaf_values_none(payload: dict[str, Any]) -> bool:
    for value in payload.values():
        if isinstance(value, dict):
            if not _all_leaf_values_none(value):
                return False
            continue
        if value is not None:
            return False
    return True


def _merge_raw(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    merged.update(extra)
    return merged


def _merge_history(base: dict[str, pd.Series], extra: dict[str, pd.Series]) -> dict[str, pd.Series]:
    merged = dict(base)
    merged.update(extra)
    return merged


def _history_payload_tree(tree: Mapping[str, Any], series_map: dict[str, pd.Series], lookback: int) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in tree.items():
        if isinstance(value, Mapping):
            payload[key] = _history_payload_tree(value, series_map, lookback)
            continue
        payload[key] = _history_payload(series_map[value], lookback)
    return payload


def _core_history_tree() -> dict[str, Any]:
    return {
        "sma_20": "sma_20",
        "sma_50": "sma_50",
        "sma_200": "sma_200",
        "ema_20": "ema_20",
        "ema_50": "ema_50",
        "macd": {
            "macd": "macd",
            "signal": "macd_signal",
            "hist": "macd_hist",
        },
        "adx_14": "adx_14",
        "plus_di_14": "plus_di_14",
        "minus_di_14": "minus_di_14",
        "sar": "sar",
        "rsi_14": "rsi_14",
        "stoch": {
            "slowk": "stoch_slowk",
            "slowd": "stoch_slowd",
        },
        "willr_14": "willr_14",
        "cci_20": "cci_20",
        "roc_10": "roc_10",
        "atr_14": "atr_14",
        "natr_14": "natr_14",
        "bbands_20_2": {
            "upper": "bbands_20_2_upper",
            "middle": "bbands_20_2_middle",
            "lower": "bbands_20_2_lower",
        },
        "obv": "obv",
        "mfi_14": "mfi_14",
        "ad": "ad",
        "adosc": "adosc",
    }


def _extended_history_tree() -> dict[str, Any]:
    return {
        "aroon_25": {
            "up": "aroon_25_up",
            "down": "aroon_25_down",
        },
        "aroonosc_25": "aroonosc_25",
        "ppo_12_26_9": {
            "ppo": "ppo_12_26_9",
            "signal": "ppo_12_26_9_signal",
            "hist": "ppo_12_26_9_hist",
        },
        "trix_30": "trix_30",
        "kama_10": "kama_10",
        "linearreg_slope_14": "linearreg_slope_14",
        "mom_10": "mom_10",
        "cmo_14": "cmo_14",
    }


def compute_core_indicators(
    normalized: NormalizedOHLCV,
    *,
    options: FetcherOptions | None = None,
) -> tuple[dict[str, Any], dict[str, pd.Series], list[WarningEntry]]:
    resolved_options = options or FetcherOptions()
    warnings: list[WarningEntry] = []
    close = _as_series(normalized.close_arr)
    high = _as_series(normalized.high_arr)
    low = _as_series(normalized.low_arr)
    open_ = _as_series(normalized.open_arr)
    volume = _as_series(normalized.volume_arr)

    try:
        sma_20 = close.rolling(
            window=DEFAULT_WINDOWS["sma_fast"], min_periods=DEFAULT_WINDOWS["sma_fast"]
        ).mean()
        sma_50 = close.rolling(
            window=DEFAULT_WINDOWS["sma_medium"], min_periods=DEFAULT_WINDOWS["sma_medium"]
        ).mean()
        sma_200 = close.rolling(
            window=DEFAULT_WINDOWS["sma_slow"], min_periods=DEFAULT_WINDOWS["sma_slow"]
        ).mean()
        ema_20 = _ema(close, DEFAULT_WINDOWS["sma_fast"])
        ema_50 = _ema(close, DEFAULT_WINDOWS["sma_medium"])
        ema_fast = _ema(close, DEFAULT_WINDOWS["ema_fast"])
        ema_slow = _ema(close, DEFAULT_WINDOWS["ema_slow"])
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(
            span=DEFAULT_WINDOWS["ema_signal"],
            adjust=False,
            min_periods=DEFAULT_WINDOWS["ema_signal"],
        ).mean()
        macd_hist = macd_line - signal_line
        adx, plus_di, minus_di = _adx_components(high, low, close, DEFAULT_WINDOWS["adx"])
        sar = _sar(high, low)
        rsi = _rsi(close, DEFAULT_WINDOWS["rsi"])
        slowk, slowd = _stochastic(
            high,
            low,
            close,
            DEFAULT_WINDOWS["stochastic"],
            DEFAULT_WINDOWS["stochastic_slowk"],
            DEFAULT_WINDOWS["stochastic_slowd"],
        )
        willr = _willr(high, low, close, DEFAULT_WINDOWS["willr"])
        cci = _cci(high, low, close, DEFAULT_WINDOWS["cci"])
        roc = _roc(close, DEFAULT_WINDOWS["roc"])
        atr = _atr(high, low, close, DEFAULT_WINDOWS["atr"])
        natr = 100 * atr / close.replace(0, np.nan)
        bb_upper, bb_middle, bb_lower = _bbands(close, DEFAULT_WINDOWS["bbands"], 2.0)
    except Exception as error:
        _warning_or_raise(
            options=resolved_options,
            code="price_indicator_compute_failed",
            message="failed to compute core price indicators",
            warnings=warnings,
            error=error,
        )
        sma_20 = _nan_series(len(close))
        sma_50 = _nan_series(len(close))
        sma_200 = _nan_series(len(close))
        ema_20 = _nan_series(len(close))
        ema_50 = _nan_series(len(close))
        macd_line = _nan_series(len(close))
        signal_line = _nan_series(len(close))
        macd_hist = _nan_series(len(close))
        adx = _nan_series(len(close))
        plus_di = _nan_series(len(close))
        minus_di = _nan_series(len(close))
        sar = _nan_series(len(close))
        rsi = _nan_series(len(close))
        slowk = _nan_series(len(close))
        slowd = _nan_series(len(close))
        willr = _nan_series(len(close))
        cci = _nan_series(len(close))
        roc = _nan_series(len(close))
        atr = _nan_series(len(close))
        natr = _nan_series(len(close))
        bb_upper = _nan_series(len(close))
        bb_middle = _nan_series(len(close))
        bb_lower = _nan_series(len(close))

    ad_line = pd.Series(np.nan, index=close.index, dtype="float64")
    adosc = pd.Series(np.nan, index=close.index, dtype="float64")
    obv = pd.Series(np.nan, index=close.index, dtype="float64")
    mfi = pd.Series(np.nan, index=close.index, dtype="float64")

    try:
        if volume.isna().any():
            raise ValueError("volume series contains null values")
        obv = _obv(close, volume)
        mfi = _mfi(high, low, close, volume, DEFAULT_WINDOWS["mfi"])
        ad_line = _ad(high, low, close, volume)
        adosc = _adosc(ad_line)
    except Exception as error:
        _warning_or_raise(
            options=resolved_options,
            code="volume_indicator_compute_failed",
            message="failed to compute volume indicators",
            warnings=warnings,
            error=error,
        )

    raw: dict[str, Any] = {
        "sma_20": _latest_scalar(sma_20),
        "sma_50": _latest_scalar(sma_50),
        "sma_200": _latest_scalar(sma_200),
        "ema_20": _latest_scalar(ema_20),
        "ema_50": _latest_scalar(ema_50),
        "macd": {
            "macd": _latest_scalar(macd_line),
            "signal": _latest_scalar(signal_line),
            "hist": _latest_scalar(macd_hist),
        },
        "adx_14": _latest_scalar(adx),
        "plus_di_14": _latest_scalar(plus_di),
        "minus_di_14": _latest_scalar(minus_di),
        "sar": _latest_scalar(sar),
        "rsi_14": _latest_scalar(rsi),
        "stoch": {
            "slowk": _latest_scalar(slowk),
            "slowd": _latest_scalar(slowd),
        },
        "willr_14": _latest_scalar(willr),
        "cci_20": _latest_scalar(cci),
        "roc_10": _latest_scalar(roc),
        "atr_14": _latest_scalar(atr),
        "natr_14": _latest_scalar(natr),
        "bbands_20_2": {
            "upper": _latest_scalar(bb_upper),
            "middle": _latest_scalar(bb_middle),
            "lower": _latest_scalar(bb_lower),
        },
        "obv": _latest_scalar(obv),
        "mfi_14": _latest_scalar(mfi),
        "ad": _latest_scalar(ad_line),
        "adosc": _latest_scalar(adosc),
    }

    history = {
        "close": close,
        "open": open_,
        "high": high,
        "low": low,
        "volume": volume,
        "sma_20": sma_20,
        "sma_50": sma_50,
        "sma_200": sma_200,
        "ema_20": ema_20,
        "ema_50": ema_50,
        "atr_14": atr,
        "natr_14": natr,
        "obv": obv,
        "adx_14": adx,
        "plus_di_14": plus_di,
        "minus_di_14": minus_di,
        "sar": sar,
        "rsi_14": rsi,
        "macd": macd_line,
        "macd_signal": signal_line,
        "macd_hist": macd_hist,
        "stoch_slowk": slowk,
        "stoch_slowd": slowd,
        "willr_14": willr,
        "cci_20": cci,
        "roc_10": roc,
        "bbands_20_2_upper": bb_upper,
        "bbands_20_2_middle": bb_middle,
        "bbands_20_2_lower": bb_lower,
        "mfi_14": mfi,
        "ad": ad_line,
        "adosc": adosc,
    }

    if _all_leaf_values_none(raw):
        raise IndicatorComputationError("failed to compute all core indicators")

    return raw, history, warnings


def compute_extended_indicators(
    normalized: NormalizedOHLCV,
    *,
    options: FetcherOptions | None = None,
) -> tuple[dict[str, Any], dict[str, pd.Series], list[WarningEntry]]:
    resolved_options = options or FetcherOptions()
    warnings: list[WarningEntry] = []
    close = _as_series(normalized.close_arr)
    high = _as_series(normalized.high_arr)
    low = _as_series(normalized.low_arr)

    try:
        aroon_up, aroon_down = _aroon(high, low, DEFAULT_WINDOWS["aroon"])
        aroonosc = aroon_up - aroon_down
        ppo, ppo_signal, ppo_hist = _ppo(
            close,
            DEFAULT_WINDOWS["ppo_fast"],
            DEFAULT_WINDOWS["ppo_slow"],
            DEFAULT_WINDOWS["ppo_signal"],
        )
        trix = _trix(close, DEFAULT_WINDOWS["trix"])
        kama = _kama(
            close,
            DEFAULT_WINDOWS["kama"],
            DEFAULT_WINDOWS["kama_fast"],
            DEFAULT_WINDOWS["kama_slow"],
        )
        linearreg_slope = _linearreg_slope(close, DEFAULT_WINDOWS["linearreg_slope"])
        mom = _mom(close, DEFAULT_WINDOWS["mom"])
        cmo = _cmo(close, DEFAULT_WINDOWS["cmo"])
    except Exception as error:
        _warning_or_raise(
            options=resolved_options,
            code="extended_indicator_compute_failed",
            message="failed to compute extended indicators",
            warnings=warnings,
            error=error,
        )
        aroon_up = _nan_series(len(close))
        aroon_down = _nan_series(len(close))
        aroonosc = _nan_series(len(close))
        ppo = _nan_series(len(close))
        ppo_signal = _nan_series(len(close))
        ppo_hist = _nan_series(len(close))
        trix = _nan_series(len(close))
        kama = _nan_series(len(close))
        linearreg_slope = _nan_series(len(close))
        mom = _nan_series(len(close))
        cmo = _nan_series(len(close))

    raw: dict[str, Any] = {
        "aroon_25": {
            "up": _latest_scalar(aroon_up),
            "down": _latest_scalar(aroon_down),
        },
        "aroonosc_25": _latest_scalar(aroonosc),
        "ppo_12_26_9": {
            "ppo": _latest_scalar(ppo),
            "signal": _latest_scalar(ppo_signal),
            "hist": _latest_scalar(ppo_hist),
        },
        "trix_30": _latest_scalar(trix),
        "kama_10": _latest_scalar(kama),
        "linearreg_slope_14": _latest_scalar(linearreg_slope),
        "mom_10": _latest_scalar(mom),
        "cmo_14": _latest_scalar(cmo),
    }

    history = {
        "aroon_25_up": aroon_up,
        "aroon_25_down": aroon_down,
        "aroonosc_25": aroonosc,
        "ppo_12_26_9": ppo,
        "ppo_12_26_9_signal": ppo_signal,
        "ppo_12_26_9_hist": ppo_hist,
        "trix_30": trix,
        "kama_10": kama,
        "linearreg_slope_14": linearreg_slope,
        "mom_10": mom,
        "cmo_14": cmo,
    }

    return raw, history, warnings


def compute_indicators(
    normalized: NormalizedOHLCV,
    *,
    options: FetcherOptions | None = None,
) -> tuple[dict[str, Any], dict[str, pd.Series], dict[str, Any] | None, list[WarningEntry]]:
    resolved_options = options or FetcherOptions()
    if resolved_options.indicator_profile not in {"core", "extended"}:
        raise IndicatorComputationError(
            "indicator_profile must be either 'core' or 'extended'"
        )

    raw, history_context, warnings = compute_core_indicators(
        normalized,
        options=resolved_options,
    )
    if resolved_options.indicator_profile == "extended":
        extended_raw, extended_history, extended_warnings = compute_extended_indicators(
            normalized,
            options=resolved_options,
        )
        raw = _merge_raw(raw, extended_raw)
        history_context = _merge_history(history_context, extended_history)
        warnings.extend(extended_warnings)

    history_payload = None
    if resolved_options.include_history:
        history_tree = _core_history_tree()
        if resolved_options.indicator_profile == "extended":
            history_tree = _merge_raw(history_tree, _extended_history_tree())
        history_payload = _history_payload_tree(
            history_tree,
            history_context,
            resolved_options.lookback_bars,
        )

    return raw, history_context, history_payload, warnings
