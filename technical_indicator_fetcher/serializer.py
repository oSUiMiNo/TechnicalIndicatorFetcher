from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from typing import Any

from .exceptions import SerializationError

SECTION_COMMENTS: dict[tuple[str, ...], tuple[str, ...]] = {
    (): (
        "technical_indicator_fetcher の出力",
        "上位エージェントがそのまま読みやすいように主要セクションへコメントを付けている",
    ),
    ("data_summary",): ("データ要約",),
    ("indicators", "raw"): ("生の指標値",),
    ("indicators", "derived"): ("機械判定済みの状態ラベル",),
    ("indicators", "history"): ("直近の履歴系列",),
    ("candlestick_patterns",): ("ローソク足パターン",),
}

FIELD_COMMENTS: dict[tuple[str, ...], tuple[str, ...]] = {
    ("schema_version",): ("出力スキーマのバージョン",),
    ("generated_at",): ("この出力を生成した時刻",),
    ("symbol",): ("分析対象の銘柄コード",),
    ("timeframe",): ("分析に使った時間足",),
    ("as_of",): ("どの時点までのバーを反映しているか",),
    ("data_summary", "bars_used"): ("指標計算に使ったバー本数",),
    ("data_summary", "latest_open"): ("最新バーの始値",),
    ("data_summary", "latest_high"): ("最新バーの高値",),
    ("data_summary", "latest_low"): ("最新バーの安値",),
    ("data_summary", "latest_close"): ("最新バーの終値",),
    ("data_summary", "latest_volume"): ("最新バーの出来高",),
    ("data_summary", "adjusted"): ("調整後価格を使ったかどうか",),
    ("data_summary", "candle_status"): ("最新バーが確定足か未確定足か",),
    ("indicators", "raw", "sma_20"): ("終値の20本単純移動平均",),
    ("indicators", "raw", "sma_50"): ("終値の50本単純移動平均",),
    ("indicators", "raw", "sma_200"): ("終値の200本単純移動平均",),
    ("indicators", "raw", "ema_20"): ("終値の20本指数移動平均",),
    ("indicators", "raw", "ema_50"): ("終値の50本指数移動平均",),
    ("indicators", "raw", "macd"): ("MACD 本体と signal と hist",),
    ("indicators", "raw", "macd", "macd"): ("MACD ライン",),
    ("indicators", "raw", "macd", "signal"): ("MACD の signal ライン",),
    ("indicators", "raw", "macd", "hist"): ("MACD と signal の差",),
    ("indicators", "raw", "adx_14"): ("14本 ADX。トレンドの強さを見る",),
    ("indicators", "raw", "plus_di_14"): ("14本 +DI。上方向の勢い",),
    ("indicators", "raw", "minus_di_14"): ("14本 -DI。下方向の勢い",),
    ("indicators", "raw", "sar"): ("パラボリック SAR",),
    ("indicators", "raw", "rsi_14"): ("14本 RSI",),
    ("indicators", "raw", "stoch"): ("ストキャスティクス",),
    ("indicators", "raw", "stoch", "slowk"): ("ストキャスティクスの %K",),
    ("indicators", "raw", "stoch", "slowd"): ("ストキャスティクスの %D",),
    ("indicators", "raw", "willr_14"): ("14本 Williams %R",),
    ("indicators", "raw", "cci_20"): ("20本 CCI",),
    ("indicators", "raw", "roc_10"): ("10本 ROC。価格変化率",),
    ("indicators", "raw", "atr_14"): ("14本 ATR。値幅の大きさ",),
    ("indicators", "raw", "natr_14"): ("14本 NATR。ATR を価格比で正規化した値",),
    ("indicators", "raw", "bbands_20_2"): ("20本ボリンジャーバンド",),
    ("indicators", "raw", "bbands_20_2", "upper"): ("ボリンジャーバンド上限",),
    ("indicators", "raw", "bbands_20_2", "middle"): ("ボリンジャーバンド中央線",),
    ("indicators", "raw", "bbands_20_2", "lower"): ("ボリンジャーバンド下限",),
    ("indicators", "raw", "obv"): ("OBV。出来高の累積方向",),
    ("indicators", "raw", "mfi_14"): ("14本 MFI。出来高込みの資金流入感",),
    ("indicators", "raw", "ad"): ("A/D ライン。累積的な資金流入",),
    ("indicators", "raw", "adosc"): ("A/D オシレーター",),
    ("indicators", "raw", "aroon_25"): ("25本 Aroon の up と down",),
    ("indicators", "raw", "aroonosc_25"): ("25本 Aroon Oscillator",),
    ("indicators", "raw", "ppo_12_26_9"): ("PPO 本体と signal と hist",),
    ("indicators", "raw", "trix_30"): ("30本 TRIX",),
    ("indicators", "raw", "kama_10"): ("10本 KAMA",),
    ("indicators", "raw", "linearreg_slope_14"): ("14本線形回帰の傾き",),
    ("indicators", "raw", "mom_10"): ("10本 Momentum",),
    ("indicators", "raw", "cmo_14"): ("14本 CMO",),
    ("indicators", "derived", "trend"): ("トレンド系の機械判定",),
    ("indicators", "derived", "trend", "close_vs_sma20"): ("終値が SMA20 より上か下か",),
    ("indicators", "derived", "trend", "close_vs_sma50"): ("終値が SMA50 より上か下か",),
    ("indicators", "derived", "trend", "close_vs_sma200"): ("終値が SMA200 より上か下か",),
    ("indicators", "derived", "trend", "sma20_vs_sma50"): ("SMA20 が SMA50 より上か下か",),
    ("indicators", "derived", "trend", "sma50_vs_sma200"): ("SMA50 が SMA200 より上か下か",),
    ("indicators", "derived", "trend", "adx_state"): ("ADX を弱い / ある / 強いトレンドへ変換した状態",),
    ("indicators", "derived", "trend", "di_direction"): ("+DI と -DI のどちらが優勢か",),
    ("indicators", "derived", "trend", "sar_position"): ("SAR が価格の上か下か",),
    ("indicators", "derived", "momentum"): ("モメンタム系の機械判定",),
    ("indicators", "derived", "momentum", "rsi_state"): ("RSI を買われすぎ / 売られすぎへ変換した状態",),
    ("indicators", "derived", "momentum", "macd_vs_signal"): ("MACD が signal より上か下か",),
    ("indicators", "derived", "momentum", "macd_histogram_sign"): ("MACD hist の符号",),
    ("indicators", "derived", "momentum", "stochastic_state"): ("ストキャスティクスの位置状態",),
    ("indicators", "derived", "volatility"): ("ボラティリティ系の機械判定",),
    ("indicators", "derived", "volatility", "bbands_position"): ("終値がボリンジャーバンドのどこにいるか",),
    ("indicators", "derived", "volatility", "atr_regime"): ("ATR を相対評価した状態",),
    ("indicators", "derived", "volatility", "natr_regime"): ("NATR を相対評価した状態",),
    ("indicators", "derived", "volume"): ("出来高系の機械判定",),
    ("indicators", "derived", "volume", "obv_direction_5"): ("OBV が5本前より上か下か",),
    ("indicators", "derived", "volume", "mfi_state"): ("MFI を買われすぎ / 売られすぎへ変換した状態",),
    ("indicators", "derived", "volume", "adosc_sign"): ("A/D オシレーターの符号",),
    ("indicators", "history"): ("直近数本の履歴。include_history=True のときだけ出る",),
    ("candlestick_patterns", "latest_bar"): ("最新バー単体のパターン判定",),
    ("candlestick_patterns", "latest_bar", "*"): ("各パターンの最新バー判定",),
    ("candlestick_patterns", "latest_bar", "*", "score"): ("パターン強度。正は強気、負は弱気、0は未検出",),
    ("candlestick_patterns", "latest_bar", "*", "state"): ("score を bullish / bearish / neutral に変換した状態",),
    ("candlestick_patterns", "latest_bar", "*", "detected_on_latest_bar"): ("最新バーでそのパターンが出たかどうか",),
    ("candlestick_patterns", "recent_hits"): ("直近 window 本で検出されたパターン一覧",),
    ("candlestick_patterns", "recent_hits", "[]", "pattern"): ("検出されたパターン名",),
    ("candlestick_patterns", "recent_hits", "[]", "score"): ("パターン強度。正は強気、負は弱気、0はなし",),
    ("candlestick_patterns", "recent_hits", "[]", "state"): ("score を bullish / bearish / neutral に変換した状態",),
    ("candlestick_patterns", "recent_hits", "[]", "bars_ago"): ("最新バーから何本前か。0 が最新",),
    ("warnings",): ("継続可能な注意事項一覧",),
    ("warnings", "[]", "code"): ("warning の識別コード",),
    ("warnings", "[]", "message"): ("warning の説明文",),
    ("warnings", "[]", "severity"): ("warning の重要度",),
}


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _format_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise SerializationError("cannot serialize non-finite float values to YAML")
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    raise SerializationError(f"unsupported scalar type for YAML serialization: {type(value).__name__}")


def _append_comments(lines: list[str], indent: int, comments: Sequence[str]) -> None:
    if lines and lines[-1] != "" and not lines[-1].endswith(":"):
        lines.append("")
    prefix = " " * indent
    for comment in comments:
        lines.append(f"{prefix}# {comment}")


def _comments_for_path(path: tuple[str, ...]) -> Sequence[str] | None:
    comments = SECTION_COMMENTS.get(path) or FIELD_COMMENTS.get(path)
    if comments is not None:
        return comments

    wildcard_path = tuple("*" if index == len(path) - 1 else part for index, part in enumerate(path))
    comments = SECTION_COMMENTS.get(wildcard_path) or FIELD_COMMENTS.get(wildcard_path)
    if comments is not None:
        return comments

    wildcard_path = tuple("*" if index == len(path) - 2 else part for index, part in enumerate(path))
    comments = SECTION_COMMENTS.get(wildcard_path) or FIELD_COMMENTS.get(wildcard_path)
    return comments


def _dump_mapping(
    mapping: Mapping[str, Any],
    *,
    indent: int,
    path: tuple[str, ...],
    emit_comments: bool,
    lines: list[str],
) -> None:
    prefix = " " * indent
    for key, value in mapping.items():
        key_path = (*path, str(key))
        comments = _comments_for_path(key_path)
        if emit_comments and comments:
            _append_comments(lines, indent, comments)
        if isinstance(value, Mapping):
            if value:
                lines.append(f"{prefix}{key}:")
                _dump_mapping(value, indent=indent + 2, path=key_path, emit_comments=emit_comments, lines=lines)
            else:
                lines.append(f"{prefix}{key}: {{}}")
            continue
        if _is_sequence(value):
            if value:
                lines.append(f"{prefix}{key}:")
                _dump_sequence(value, indent=indent + 2, path=key_path, emit_comments=emit_comments, lines=lines)
            else:
                lines.append(f"{prefix}{key}: []")
            continue
        lines.append(f"{prefix}{key}: {_format_scalar(value)}")


def _dump_sequence(
    values: Sequence[Any],
    *,
    indent: int,
    path: tuple[str, ...],
    emit_comments: bool,
    lines: list[str],
) -> None:
    prefix = " " * indent
    for value in values:
        if isinstance(value, Mapping):
            if value:
                lines.append(f"{prefix}-")
                _dump_mapping(
                    value,
                    indent=indent + 2,
                    path=(*path, "[]"),
                    emit_comments=emit_comments,
                    lines=lines,
                )
            else:
                lines.append(f"{prefix}- {{}}")
            continue
        if _is_sequence(value):
            if value:
                lines.append(f"{prefix}-")
                _dump_sequence(
                    value,
                    indent=indent + 2,
                    path=(*path, "[]"),
                    emit_comments=emit_comments,
                    lines=lines,
                )
            else:
                lines.append(f"{prefix}- []")
            continue
        lines.append(f"{prefix}- {_format_scalar(value)}")


def serialize_to_yaml(snapshot: Mapping[str, Any], *, emit_comments: bool = True) -> str:
    lines: list[str] = []
    if emit_comments:
        _append_comments(lines, 0, SECTION_COMMENTS[()])
    _dump_mapping(snapshot, indent=0, path=(), emit_comments=emit_comments, lines=lines)
    return "\n".join(lines) + "\n"


def serialize_to_json(snapshot: Mapping[str, Any], *, indent: int = 2) -> str:
    try:
        return json.dumps(
            snapshot,
            ensure_ascii=False,
            indent=indent,
            allow_nan=False,
        ) + "\n"
    except (TypeError, ValueError) as error:
        raise SerializationError("cannot serialize snapshot to JSON") from error
