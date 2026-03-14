from __future__ import annotations

import argparse

import pandas as pd

from technical_indicator_fetcher import FetcherOptions, technical_indicator_fetcher


def build_sample_ohlcv(periods: int = 260) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=periods, freq="D"),
            "open": [100 + i * 0.5 for i in range(periods)],
            "high": [101 + i * 0.5 for i in range(periods)],
            "low": [99 + i * 0.5 for i in range(periods)],
            "close": [100.5 + i * 0.5 for i in range(periods)],
            "volume": [1000 + i * 10 for i in range(periods)],
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--timeframe", default="1d")
    parser.add_argument("--as-of", dest="as_of", default="2025-09-17")
    parser.add_argument("--format", choices=("yaml", "json", "dict"), default="yaml")
    parser.add_argument("--indicator-profile", choices=("core", "extended"), default="core")
    parser.add_argument("--pattern-profile", choices=("major_only", "full"), default="major_only")
    parser.add_argument("--include-history", action="store_true")
    args = parser.parse_args()

    options = FetcherOptions(
        output_format="json" if args.format == "json" else "yaml",
        indicator_profile=args.indicator_profile,
        pattern_profile=args.pattern_profile,
        include_history=args.include_history,
    )

    result = technical_indicator_fetcher(
        symbol=args.symbol,
        timeframe=args.timeframe,
        ohlcv=build_sample_ohlcv(),
        as_of=args.as_of,
        options=options,
        return_dict=args.format == "dict",
    )
    print(result)


if __name__ == "__main__":
    main()
