import json
import shutil
import subprocess

import pytest

from technical_indicator_fetcher.serializer import serialize_to_yaml


@pytest.fixture
def sample_snapshot() -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "generated_at": "2025-09-17T00:00:00",
        "symbol": "AAPL",
        "timeframe": "1d",
        "as_of": "2025-09-17T00:00:00",
        "data_summary": {
            "bars_used": 260,
            "latest_open": 100.0,
            "latest_high": 101.0,
            "latest_low": 99.0,
            "latest_close": 100.5,
            "latest_volume": 12345.0,
            "adjusted": True,
            "candle_status": "closed",
        },
        "indicators": {
            "raw": {
                "sma_20": 98.1,
                "macd": {
                    "macd": 1.2,
                    "signal": 0.8,
                    "hist": 0.4,
                },
            },
            "derived": {
                "trend": {
                    "close_vs_sma20": "above",
                },
            },
        },
        "candlestick_patterns": {
            "latest_bar": {
                "cdl_engulfing": {
                    "score": 100,
                    "state": "bullish",
                    "detected_on_latest_bar": True,
                },
            },
            "recent_hits": [
                {
                    "pattern": "cdl_engulfing",
                    "score": 100,
                    "state": "bullish",
                    "bars_ago": 0,
                }
            ],
        },
        "warnings": [
            {
                "code": "insufficient_bars",
                "message": "need more bars",
                "severity": "warning",
            }
        ],
    }


def test_serialize_to_yaml_keeps_top_level_order_and_section_comments(
    sample_snapshot: dict[str, object],
) -> None:
    yaml_text = serialize_to_yaml(sample_snapshot)

    schema_index = yaml_text.index('schema_version: "1.0"')
    generated_index = yaml_text.index('generated_at: "2025-09-17T00:00:00"')
    symbol_index = yaml_text.index('symbol: "AAPL"')
    timeframe_index = yaml_text.index('timeframe: "1d"')
    as_of_index = yaml_text.index('as_of: "2025-09-17T00:00:00"')
    data_summary_index = yaml_text.index("data_summary:")
    indicators_index = yaml_text.index("indicators:")
    candlestick_index = yaml_text.index("candlestick_patterns:")
    warnings_index = yaml_text.index("warnings:")

    assert yaml_text.startswith("# technical_indicator_fetcher の出力\n")
    assert '# 出力スキーマのバージョン\nschema_version: "1.0"\n' in yaml_text
    assert '  # 指標計算に使ったバー本数\n  bars_used: 260\n' in yaml_text
    assert "    # MACD 本体と signal と hist\n" in yaml_text
    assert '      # MACD の signal ライン\n      signal: 0.8\n' in yaml_text
    assert '      # 終値が SMA20 より上か下か\n      close_vs_sma20: "above"\n' in yaml_text
    assert '      # 最新バーでそのパターンが出たかどうか\n      detected_on_latest_bar: true\n' in yaml_text
    assert '    # warning の識別コード\n    code: "insufficient_bars"\n' in yaml_text
    assert "# データ要約\n" in yaml_text
    assert "  # 生の指標値\n" in yaml_text
    assert "  # 機械判定済みの状態ラベル\n" in yaml_text
    assert "# ローソク足パターン\n" in yaml_text
    assert schema_index < generated_index < symbol_index < timeframe_index < as_of_index
    assert as_of_index < data_summary_index < indicators_index < candlestick_index < warnings_index


def test_serialize_to_yaml_can_omit_comments(sample_snapshot: dict[str, object]) -> None:
    yaml_text = serialize_to_yaml(sample_snapshot, emit_comments=False)

    assert "#" not in yaml_text
    assert 'schema_version: "1.0"\n' in yaml_text
    assert "warnings:\n" in yaml_text


def test_serialize_to_yaml_round_trips_via_system_yaml(
    sample_snapshot: dict[str, object],
) -> None:
    python3 = shutil.which("python3")
    if python3 is None:
        pytest.skip("python3 is not available")

    command = [
        python3,
        "-c",
        (
            "import json, sys\n"
            "import yaml\n"
            "print(json.dumps(yaml.safe_load(sys.stdin.read()), ensure_ascii=False))\n"
        ),
    ]
    yaml_text = serialize_to_yaml(sample_snapshot, emit_comments=False)
    completed = subprocess.run(
        command,
        input=yaml_text,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        pytest.skip("system python3 does not provide yaml")

    parsed = json.loads(completed.stdout)

    assert parsed == sample_snapshot
