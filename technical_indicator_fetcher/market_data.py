from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from .exceptions import MarketDataFetchError
from .models import FetcherOptions


def _import_yfinance():
    try:
        import yfinance as yf
    except ModuleNotFoundError as error:
        raise MarketDataFetchError(
            "yfinance is required for market data fetching"
        ) from error
    return yf


def fetch_ohlcv_with_yfinance(
    symbol: str,
    *,
    period: str = "1y",
    interval: str = "1d",
    auto_adjust: bool = True,
    prepost: bool = False,
) -> pd.DataFrame:
    yf = _import_yfinance()

    try:
        history = yf.Ticker(symbol).history(
            period=period,
            interval=interval,
            auto_adjust=auto_adjust,
            prepost=prepost,
        )
    except Exception as error:
        raise MarketDataFetchError(
            f"failed to fetch OHLCV for symbol '{symbol}' via yfinance"
        ) from error

    if history.empty:
        raise MarketDataFetchError(
            f"yfinance returned no OHLCV rows for symbol '{symbol}'"
        )

    history = history.copy()
    history.attrs["adjusted"] = auto_adjust
    return history


def fetch_and_run_with_yfinance(
    symbol: str,
    timeframe: str,
    *,
    period: str = "1y",
    interval: str = "1d",
    auto_adjust: bool = True,
    prepost: bool = False,
    as_of: str | datetime | pd.Timestamp | None = None,
    options: FetcherOptions | None = None,
    return_dict: bool = False,
) -> str | dict[str, Any]:
    from .service import technical_indicator_fetcher

    history = fetch_ohlcv_with_yfinance(
        symbol,
        period=period,
        interval=interval,
        auto_adjust=auto_adjust,
        prepost=prepost,
    )
    resolved_as_of = history.index[-1] if as_of is None else as_of
    return technical_indicator_fetcher(
        symbol=symbol,
        timeframe=timeframe,
        ohlcv=history,
        as_of=resolved_as_of,
        options=options,
        return_dict=return_dict,
    )
