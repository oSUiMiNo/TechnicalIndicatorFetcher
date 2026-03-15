from __future__ import annotations

import sys
import types

import pandas as pd
import pytest

from technical_indicator_fetcher import (
    FetcherOptions,
    MarketDataFetchError,
    fetch_and_run_with_yfinance,
    fetch_ohlcv_with_yfinance,
)


def _install_fake_yfinance(monkeypatch: pytest.MonkeyPatch, frame: pd.DataFrame) -> None:
    class FakeTicker:
        def __init__(self, symbol: str) -> None:
            self.symbol = symbol

        def history(self, **_: object) -> pd.DataFrame:
            return frame.copy()

    fake_module = types.SimpleNamespace(Ticker=FakeTicker)
    monkeypatch.setitem(sys.modules, "yfinance", fake_module)


def _build_history() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Open": [100.0, 101.0, 102.0],
            "High": [101.0, 102.0, 103.0],
            "Low": [99.0, 100.0, 101.0],
            "Close": [100.5, 101.5, 102.5],
            "Volume": [1000, 1100, 1200],
        },
        index=pd.date_range("2025-01-01", periods=3, freq="D"),
    )


def test_fetch_ohlcv_with_yfinance_returns_dataframe_with_adjusted_attr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_history()
    _install_fake_yfinance(monkeypatch, frame)

    history = fetch_ohlcv_with_yfinance("NVDA", period="1mo", interval="1d", auto_adjust=True)

    assert list(history.columns) == ["Open", "High", "Low", "Close", "Volume"]
    assert history.attrs["adjusted"] is True


def test_fetch_ohlcv_with_yfinance_rejects_empty_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_yfinance(monkeypatch, pd.DataFrame())

    with pytest.raises(MarketDataFetchError, match="returned no OHLCV rows"):
        fetch_ohlcv_with_yfinance("NVDA")


def test_fetch_and_run_with_yfinance_returns_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Open": [100 + index * 0.5 for index in range(260)],
            "High": [101 + index * 0.5 for index in range(260)],
            "Low": [99 + index * 0.5 for index in range(260)],
            "Close": [100.5 + index * 0.5 for index in range(260)],
            "Volume": [1000 + index * 10 for index in range(260)],
        },
        index=pd.date_range("2025-01-01", periods=260, freq="D"),
    )
    _install_fake_yfinance(monkeypatch, frame)

    snapshot = fetch_and_run_with_yfinance(
        "NVDA",
        timeframe="1d",
        period="1y",
        interval="1d",
        options=FetcherOptions(indicator_profile="extended", pattern_profile="full"),
        return_dict=True,
    )

    assert snapshot["symbol"] == "NVDA"
    assert snapshot["timeframe"] == "1d"
    assert snapshot["data_summary"]["bars_used"] == 260
    assert "cdl_marubozu" in snapshot["candlestick_patterns"]["latest_bar"]
