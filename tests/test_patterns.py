import pandas as pd

from technical_indicator_fetcher.models import FetcherOptions
from technical_indicator_fetcher.patterns import compute_candlestick_patterns
from technical_indicator_fetcher.preprocess import normalize_ohlcv


def _build_pattern_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-02-01", periods=6, freq="D"),
            "open": [10.5, 10.4, 9.95, 10.2, 11.3, 10.6],
            "high": [10.7, 10.5, 10.05, 10.8, 11.4, 11.7],
            "low": [10.0, 9.9, 9.3, 10.1, 10.7, 10.5],
            "close": [10.4, 10.0, 10.0, 10.6, 10.8, 11.6],
            "volume": [1000, 1010, 1020, 1030, 1040, 1050],
        }
    )


def test_compute_candlestick_patterns_builds_latest_bar_and_recent_hits() -> None:
    normalized = normalize_ohlcv(
        _build_pattern_ohlcv(),
        as_of="2026-02-06",
        options=FetcherOptions(minimum_bars=5, recommended_bars=6),
    )

    patterns, warnings = compute_candlestick_patterns(
        normalized,
        options=FetcherOptions(
            minimum_bars=5,
            recommended_bars=6,
            pattern_recent_window=6,
        ),
    )

    assert warnings == []
    assert patterns["latest_bar"]["cdl_engulfing"] == {
        "score": 100,
        "state": "bullish",
        "detected_on_latest_bar": True,
    }
    assert patterns["latest_bar"]["cdl_morning_star"] == {
        "score": 0,
        "state": "neutral",
        "detected_on_latest_bar": False,
    }
    assert patterns["recent_hits"][0] == {
        "pattern": "cdl_engulfing",
        "score": 100,
        "state": "bullish",
        "bars_ago": 0,
    }
    assert any(
        hit["pattern"] == "cdl_hammer" and hit["bars_ago"] == 3
        for hit in patterns["recent_hits"]
    )
    assert [hit["bars_ago"] for hit in patterns["recent_hits"]] == sorted(
        hit["bars_ago"] for hit in patterns["recent_hits"]
    )


def test_compute_candlestick_patterns_supports_full_profile() -> None:
    normalized = normalize_ohlcv(
        _build_pattern_ohlcv(),
        as_of="2026-02-06",
        options=FetcherOptions(minimum_bars=5, recommended_bars=6),
    )

    patterns, warnings = compute_candlestick_patterns(
        normalized,
        options=FetcherOptions(
            minimum_bars=5,
            recommended_bars=6,
            pattern_profile="full",
        ),
    )

    assert warnings == []
    assert "cdl_marubozu" in patterns["latest_bar"]
    assert "cdl_kicking" in patterns["latest_bar"]
