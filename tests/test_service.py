import json

import pandas as pd
import pytest

from technical_indicator_fetcher.exceptions import IndicatorComputationError, OHLCVValidationError
from technical_indicator_fetcher import (
    BatchRequest,
    FetcherOptions,
    batch_technical_indicator_fetcher,
    multi_timeframe_indicator_fetcher,
    technical_indicator_fetcher,
)
from tests.data_builders import (
    build_high_volatility_ohlcv,
    build_trending_ohlcv,
    build_unclosed_ohlcv,
    build_volume_missing_ohlcv,
)


def test_technical_indicator_fetcher_returns_snapshot_dict() -> None:
    snapshot = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_trending_ohlcv(),
        as_of="2025-09-17",
        return_dict=True,
    )

    assert snapshot["schema_version"] == "1.0"
    assert snapshot["symbol"] == "AAPL"
    assert snapshot["timeframe"] == "1d"
    assert snapshot["warnings"] == []
    assert snapshot["data_summary"]["bars_used"] == 260
    assert snapshot["data_summary"]["latest_open"] is not None
    assert snapshot["data_summary"]["latest_high"] is not None
    assert snapshot["data_summary"]["latest_low"] is not None
    assert snapshot["indicators"]["raw"]["sma_20"] is not None
    assert snapshot["indicators"]["derived"]["trend"]["close_vs_sma20"] == "above"
    assert "candlestick_patterns" in snapshot
    assert "latest_bar" in snapshot["candlestick_patterns"]
    assert "recent_hits" in snapshot["candlestick_patterns"]


def test_technical_indicator_fetcher_returns_yaml_by_default() -> None:
    yaml_text = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_trending_ohlcv(),
        as_of="2025-09-17",
        options=FetcherOptions(),
    )

    assert isinstance(yaml_text, str)
    assert yaml_text.startswith("# technical_indicator_fetcher の出力\n")
    assert 'schema_version: "1.0"\n' in yaml_text
    assert 'symbol: "AAPL"\n' in yaml_text
    assert "warnings: []\n" in yaml_text


def test_technical_indicator_fetcher_can_disable_yaml_comments() -> None:
    yaml_text = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_trending_ohlcv(),
        as_of="2025-09-17",
        options=FetcherOptions(emit_yaml_comments=False),
    )

    assert not yaml_text.startswith("#")
    assert "\n# " not in yaml_text


def test_technical_indicator_fetcher_includes_normalization_warnings() -> None:
    snapshot = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_trending_ohlcv(periods=30),
        as_of="2025-01-30",
        options=FetcherOptions(minimum_bars=50, recommended_bars=60),
        return_dict=True,
    )

    assert [warning["code"] for warning in snapshot["warnings"]] == ["insufficient_bars"]


def test_technical_indicator_fetcher_is_deterministic_for_same_input() -> None:
    first = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_trending_ohlcv(),
        as_of="2025-09-17",
        return_dict=True,
    )
    second = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_trending_ohlcv(),
        as_of="2025-09-17",
        return_dict=True,
    )

    assert first == second


def test_technical_indicator_fetcher_yaml_is_deterministic_for_same_input() -> None:
    first = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_trending_ohlcv(),
        as_of="2025-09-17",
    )
    second = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_trending_ohlcv(),
        as_of="2025-09-17",
    )

    assert first == second


def test_technical_indicator_fetcher_supports_extended_indicator_profile() -> None:
    snapshot = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_trending_ohlcv(),
        as_of="2025-09-17",
        options=FetcherOptions(indicator_profile="extended"),
        return_dict=True,
    )

    raw = snapshot["indicators"]["raw"]

    assert raw["sma_20"] is not None
    assert raw["aroon_25"]["up"] is not None
    assert raw["ppo_12_26_9"]["ppo"] is not None
    assert raw["trix_30"] is not None


def test_technical_indicator_fetcher_can_include_indicator_history() -> None:
    snapshot = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_trending_ohlcv(),
        as_of="2025-09-17",
        options=FetcherOptions(
            indicator_profile="extended",
            include_history=True,
            lookback_bars=4,
        ),
        return_dict=True,
    )

    history = snapshot["indicators"]["history"]

    assert "rsi_14_history" not in snapshot["indicators"]["raw"]
    assert len(history["rsi_14"]) == 4
    assert len(history["macd"]["macd"]) == 4
    assert len(history["ppo_12_26_9"]["ppo"]) == 4


def test_technical_indicator_fetcher_rejects_invalid_indicator_profile() -> None:
    with pytest.raises(IndicatorComputationError, match="indicator_profile"):
        technical_indicator_fetcher(
            symbol="AAPL",
            timeframe="1d",
            ohlcv=build_trending_ohlcv(),
            as_of="2025-09-17",
            options=FetcherOptions(indicator_profile="unsupported"),
            return_dict=True,
        )


def test_technical_indicator_fetcher_strict_rejects_unclosed_input() -> None:
    with pytest.raises(OHLCVValidationError, match="latest candle"):
        technical_indicator_fetcher(
            symbol="AAPL",
            timeframe="1d",
            ohlcv=build_unclosed_ohlcv(periods=30),
            as_of="2025-02-01",
            options=FetcherOptions(strict=True),
            return_dict=True,
        )


def test_technical_indicator_fetcher_includes_missing_volume_and_volume_indicator_warnings() -> None:
    snapshot = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_volume_missing_ohlcv(),
        as_of="2025-09-17",
        return_dict=True,
    )

    assert [warning["code"] for warning in snapshot["warnings"]] == [
        "missing_volume",
        "volume_indicator_compute_failed",
    ]
    assert snapshot["indicators"]["raw"]["sma_20"] is not None
    assert snapshot["indicators"]["raw"]["obv"] is None
    assert snapshot["data_summary"]["latest_volume"] is None


def test_technical_indicator_fetcher_can_return_json() -> None:
    json_text = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_trending_ohlcv(),
        as_of="2025-09-17",
        options=FetcherOptions(output_format="json"),
    )

    payload = json.loads(json_text)

    assert payload["symbol"] == "AAPL"
    assert payload["timeframe"] == "1d"
    assert payload["warnings"] == []


def test_technical_indicator_fetcher_supports_full_pattern_profile() -> None:
    snapshot = technical_indicator_fetcher(
        symbol="AAPL",
        timeframe="1d",
        ohlcv=build_high_volatility_ohlcv(),
        as_of="2025-09-17",
        options=FetcherOptions(pattern_profile="full"),
        return_dict=True,
    )

    assert "cdl_marubozu" in snapshot["candlestick_patterns"]["latest_bar"]
    assert "cdl_rickshaw_man" in snapshot["candlestick_patterns"]["latest_bar"]


def test_multi_timeframe_indicator_fetcher_returns_results_per_timeframe() -> None:
    snapshots = multi_timeframe_indicator_fetcher(
        symbol="AAPL",
        ohlcv_by_timeframe={
            "1d": build_trending_ohlcv(),
            "1wk": build_trending_ohlcv(direction="down"),
        },
        as_of={
            "1d": "2025-09-17",
            "1wk": "2025-09-17",
        },
        return_dict=True,
    )

    assert set(snapshots) == {"1d", "1wk"}
    assert snapshots["1d"]["timeframe"] == "1d"
    assert snapshots["1wk"]["timeframe"] == "1wk"
    assert snapshots["1wk"]["indicators"]["derived"]["trend"]["close_vs_sma20"] == "below"


def test_batch_technical_indicator_fetcher_returns_results_in_request_order() -> None:
    results = batch_technical_indicator_fetcher(
        [
            BatchRequest(
                symbol="AAPL",
                timeframe="1d",
                ohlcv=build_trending_ohlcv(),
                as_of="2025-09-17",
            ),
            BatchRequest(
                symbol="MSFT",
                timeframe="1d",
                ohlcv=build_high_volatility_ohlcv(),
                as_of="2025-09-17",
                options=FetcherOptions(output_format="json"),
            ),
        ],
        return_dict=False,
    )

    assert '"symbol": "AAPL"' not in results[0]
    assert "schema_version" in results[0]
    assert json.loads(results[1])["symbol"] == "MSFT"
