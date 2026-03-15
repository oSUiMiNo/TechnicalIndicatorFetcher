from __future__ import annotations

import argparse

from technical_indicator_fetcher import FetcherOptions, MarketDataFetchError, fetch_and_run_with_yfinance


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--timeframe", default="1d")
    parser.add_argument("--period", default="1y")
    parser.add_argument("--interval", default="1d")
    parser.add_argument("--format", choices=("yaml", "json", "dict"), default="yaml")
    parser.add_argument("--indicator-profile", choices=("core", "extended"), default="core")
    parser.add_argument("--pattern-profile", choices=("major_only", "full"), default="major_only")
    parser.add_argument("--include-history", action="store_true")
    parser.add_argument("--no-adjust", action="store_true")
    return parser.parse_args()

def _print_dict(payload: dict) -> None:
    import json

    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> None:
    args = _parse_args()
    options = FetcherOptions(
        output_format="json" if args.format == "json" else "yaml",
        indicator_profile=args.indicator_profile,
        pattern_profile=args.pattern_profile,
        include_history=args.include_history,
        price_adjustment=not args.no_adjust,
    )
    try:
        result = fetch_and_run_with_yfinance(
            symbol=args.symbol,
            timeframe=args.timeframe,
            period=args.period,
            interval=args.interval,
            auto_adjust=not args.no_adjust,
            options=options,
            return_dict=args.format == "dict",
        )
    except MarketDataFetchError as error:
        raise SystemExit(str(error)) from error
    if args.format == "dict":
        _print_dict(result)
        return
    print(result)


if __name__ == "__main__":
    main()
