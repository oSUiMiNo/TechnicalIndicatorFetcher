from __future__ import annotations

import os

import pytest

from technical_indicator_fetcher import FetcherOptions, fetch_and_run_with_yfinance


def test_real_data_snapshot_from_yfinance() -> None:
    if os.environ.get("RUN_REAL_DATA_TEST") != "1":
        pytest.skip("RUN_REAL_DATA_TEST=1 のときだけ実データ結合テストを実行する")

    snapshot = fetch_and_run_with_yfinance(
        symbol="AAPL",
        timeframe="1d",
        period="1y",
        interval="1d",
        auto_adjust=True,
        options=FetcherOptions(indicator_profile="extended", pattern_profile="full"),
        return_dict=True,
    )

    assert snapshot["symbol"] == "AAPL"
    assert snapshot["timeframe"] == "1d"
    assert snapshot["data_summary"]["bars_used"] > 0
    assert "raw" in snapshot["indicators"]
    assert "derived" in snapshot["indicators"]
    assert "latest_bar" in snapshot["candlestick_patterns"]
