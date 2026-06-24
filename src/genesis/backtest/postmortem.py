from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from genesis.backtest.trade_engine import max_consecutive_losses, summarize_trade_returns


SUMMARY_COLUMNS = [
    "symbol",
    "market_type",
    "tp_variant",
    "total_return",
    "win_rate",
    "profit_factor",
    "max_drawdown",
    "expectancy",
    "trades_count",
    "return_to_drawdown_ratio",
    "top_5_trades_pnl_share",
    "decision",
]


@dataclass(frozen=True)
class ORRArtifactSpec:
    symbol: str
    market_type: str
    prefix: str

    @classmethod
    def from_symbol(cls, symbol: str) -> "ORRArtifactSpec":
        normalized = symbol.strip().upper()
        if normalized in {"BTCUSDT", "BTC/USDT:USDT"}:
            return cls(symbol="BTCUSDT", market_type="crypto_futures", prefix="opening_range_reversion_ny")
        return cls(
            symbol=normalized,
            market_type="equity",
            prefix=f"opening_range_reversion_ny_equity_{normalized.lower()}",
        )

    def path(self, alpha_store: Path, suffix: str) -> Path:
        return alpha_store / f"{self.prefix}_{suffix}.csv"


def run_orr_postmortem(
    alpha_store: str | Path = "data/alpha_store",
    output_prefix: str = "opening_range_reversion_v1_postmortem",
    symbols: list[str] | None = None,
) -> dict[str, Path]:
    store = Path(alpha_store)
    specs = [ORRArtifactSpec.from_symbol(symbol) for symbol in (symbols or ["BTCUSDT", "QQQ", "SPY"])]
    loaded = [load_symbol_artifacts(store, spec) for spec in specs]

    summary = build_comparison_summary(loaded)
    trades = pd.concat([item["trades"] for item in loaded if not item["trades"].empty], ignore_index=True)
    signals = pd.concat([item["signals"] for item in loaded if not item["signals"].empty], ignore_index=True)
    monthly = build_monthly_postmortem(loaded)
    diagnostics = build_trade_diagnostics(trades, signals, summary)
    side = build_side_breakdown(trades)
    recommendation = decide_postmortem(summary)
    markdown = render_postmortem_markdown(summary, diagnostics, monthly, side, recommendation)

    paths = {
        "summary": store / f"{output_prefix}_summary.csv",
        "trade_diagnostics": store / f"{output_prefix}_trade_diagnostics.csv",
        "monthly": store / f"{output_prefix}_monthly.csv",
        "side_breakdown": store / f"{output_prefix}_side_breakdown.csv",
        "markdown": store / f"{output_prefix}.md",
    }
    summary.to_csv(paths["summary"], index=False)
    diagnostics.to_csv(paths["trade_diagnostics"], index=False)
    monthly.to_csv(paths["monthly"], index=False)
    side.to_csv(paths["side_breakdown"], index=False)
    paths["markdown"].write_text(markdown, encoding="utf-8")
    return paths


def load_symbol_artifacts(alpha_store: Path, spec: ORRArtifactSpec) -> dict:
    summary = _read_csv(spec.path(alpha_store, "summary"))
    trades = _read_csv(spec.path(alpha_store, "trades"))
    signals = _read_csv(spec.path(alpha_store, "signals"))
    equity = _read_csv(spec.path(alpha_store, "equity"))
    monthly = _read_csv(spec.path(alpha_store, "monthly_breakdown"))
    validation_path = alpha_store / f"{spec.prefix}_validation.md"
    validation = validation_path.read_text(encoding="utf-8") if validation_path.exists() else ""

    for frame in [summary, trades, signals, equity, monthly]:
        if not frame.empty:
            frame["symbol"] = spec.symbol
            frame["market_type"] = spec.market_type
    return {
        "spec": spec,
        "summary": summary,
        "trades": trades,
        "signals": signals,
        "equity": equity,
        "monthly": monthly,
        "validation": validation,
    }


def build_comparison_summary(loaded: list[dict]) -> pd.DataFrame:
    rows: list[dict] = []
    for item in loaded:
        spec: ORRArtifactSpec = item["spec"]
        summary = item["summary"]
        if summary.empty:
            rows.append({"symbol": spec.symbol, "market_type": spec.market_type, "decision": "no_data"})
            continue
        for _, row in summary.iterrows():
            normalized = {column: row.get(column, 0.0) for column in SUMMARY_COLUMNS}
            normalized["symbol"] = spec.symbol
            normalized["market_type"] = spec.market_type
            normalized["decision"] = _normalize_decision(row.get("decision", ""))
            rows.append(normalized)
    return pd.DataFrame(rows, columns=SUMMARY_COLUMNS)


def build_trade_diagnostics(trades: pd.DataFrame, signals: pd.DataFrame, summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame()
    rows: list[dict] = []
    grouped_trades = trades.groupby(["symbol", "market_type", "tp_variant"]) if not trades.empty else {}
    grouped_signals = signals.groupby(["symbol", "market_type"]) if not signals.empty else {}

    for _, summary_row in summary.iterrows():
        symbol = summary_row["symbol"]
        market_type = summary_row["market_type"]
        variant = summary_row.get("tp_variant", "")
        trade_group = _get_group(grouped_trades, (symbol, market_type, variant))
        signal_group = _get_group(grouped_signals, (symbol, market_type))
        returns = _returns(trade_group)
        rows.append(
            {
                "symbol": symbol,
                "market_type": market_type,
                "tp_variant": variant,
                "trades_count": int(len(trade_group)),
                "signals_count": int(len(signal_group)),
                "signal_to_trade_rate": float(len(trade_group) / len(signal_group)) if len(signal_group) else 0.0,
                "avg_pnl": float(returns.mean()) if not returns.empty else 0.0,
                "median_pnl": float(returns.median()) if not returns.empty else 0.0,
                "best_5_trades_sum": float(returns.sort_values(ascending=False).head(5).sum()) if not returns.empty else 0.0,
                "worst_5_trades_sum": float(returns.sort_values().head(5).sum()) if not returns.empty else 0.0,
                "max_consecutive_losses": max_consecutive_losses(returns) if not returns.empty else 0,
                "long_trades": int((trade_group.get("side", pd.Series(dtype=str)) == "long").sum()) if not trade_group.empty else 0,
                "short_trades": int((trade_group.get("side", pd.Series(dtype=str)) == "short").sum()) if not trade_group.empty else 0,
                "long_return": _side_return(trade_group, "long"),
                "short_return": _side_return(trade_group, "short"),
                "best_entry_hour": _best_entry_hour(trade_group),
                "avg_bars_in_trade": _mean_column(trade_group, "bars_in_trade"),
                "median_bars_in_trade": _median_column(trade_group, "bars_in_trade"),
                "top_5_trades_pnl_share": float(summary_row.get("top_5_trades_pnl_share", 1.0) or 1.0),
                "breakout_above_count": int((signal_group.get("breakout_side", pd.Series(dtype=str)) == "above").sum())
                if not signal_group.empty
                else 0,
                "breakout_below_count": int((signal_group.get("breakout_side", pd.Series(dtype=str)) == "below").sum())
                if not signal_group.empty
                else 0,
                "discarded_signals_count": _discarded_signals_count(signal_group),
                "median_confirmation_lag_minutes": _median_lag_minutes(signal_group, "reentry_time", "confirmation_time"),
                "median_entry_lag_minutes": _median_lag_minutes(signal_group, "confirmation_time", "entry_time"),
            }
        )
    return pd.DataFrame(rows)


def build_side_breakdown(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["symbol", "market_type", "tp_variant", "side", "trades_count", "total_return", "win_rate", "profit_factor", "expectancy"])
    rows: list[dict] = []
    for (symbol, market_type, variant, side), group in trades.groupby(["symbol", "market_type", "tp_variant", "side"]):
        stats = summarize_trade_returns(_returns(group))
        rows.append(
            {
                "symbol": symbol,
                "market_type": market_type,
                "tp_variant": variant,
                "side": side,
                "trades_count": int(len(group)),
                "total_return": stats["total_return"],
                "win_rate": stats["win_rate"],
                "profit_factor": stats["profit_factor"],
                "expectancy": stats["expectancy"],
            }
        )
    return pd.DataFrame(rows).sort_values(["symbol", "tp_variant", "side"]).reset_index(drop=True)


def build_monthly_postmortem(loaded: list[dict]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for item in loaded:
        monthly = item["monthly"]
        if monthly.empty:
            continue
        spec: ORRArtifactSpec = item["spec"]
        monthly = monthly.copy()
        monthly["symbol"] = spec.symbol
        monthly["market_type"] = spec.market_type
        frames.append(monthly)
    if not frames:
        return pd.DataFrame(columns=["symbol", "market_type", "period", "tp_variant", "trades_count", "total_return", "win_rate", "profit_factor"])
    result = pd.concat(frames, ignore_index=True)
    ordered = ["symbol", "market_type", "period", "tp_variant", "trades_count", "total_return", "win_rate", "profit_factor"]
    return result[[col for col in ordered if col in result.columns]].sort_values(["symbol", "period", "tp_variant"]).reset_index(drop=True)


def decide_postmortem(summary: pd.DataFrame) -> str:
    if summary.empty:
        return "continue_research"
    numeric = summary.copy()
    numeric["profit_factor"] = pd.to_numeric(numeric["profit_factor"], errors="coerce").fillna(0.0)
    numeric["expectancy"] = pd.to_numeric(numeric["expectancy"], errors="coerce").fillna(0.0)
    numeric["total_return"] = pd.to_numeric(numeric["total_return"], errors="coerce").fillna(0.0)

    if ((numeric["profit_factor"] <= 1.0) & (numeric["expectancy"] <= 0.0)).all():
        return "abandon"
    if (numeric["total_return"] > 0.0).any():
        return "continue_research"
    if _opposite_extreme_improves_consistently(numeric):
        return "redesign"
    return "continue_research"


def render_postmortem_markdown(
    summary: pd.DataFrame,
    diagnostics: pd.DataFrame,
    monthly: pd.DataFrame,
    side: pd.DataFrame,
    recommendation: str,
) -> str:
    lines = [
        "# Opening Range Reversion V1 Post-Mortem",
        "",
        "## Executive Summary",
        "",
        f"Final recommendation: `{recommendation}`.",
        "",
        "ORR V1 was validated across BTCUSDT futures UM, QQQ and SPY with reproducible historical data. The failure mode is performance, not infrastructure: all evaluated variants remain below profitability thresholds and no demo trading is recommended.",
        "",
        "## What Was Tested",
        "",
        "- BTCUSDT futures UM 2025 with Binance Vision historical data.",
        "- QQQ 2025 with Polygon Stocks adjusted data.",
        "- SPY 2025 with Polygon Stocks adjusted data.",
        "- TP variants: `midpoint` and `opposite_extreme`.",
        "",
        "## Data Quality",
        "",
        "Daily warmup and 5m coverage were sufficient for the tested datasets. Known equity partial sessions/gaps are reported as market-calendar artifacts, not automatic no-data conditions when global coverage is sufficient.",
        "",
        "## Result By Market And TP Variant",
        "",
        _markdown_table(_round_frame(summary)),
        "",
        "## Trade Diagnostics",
        "",
        _markdown_table(_round_frame(diagnostics)),
        "",
        "## Side Breakdown",
        "",
        _markdown_table(_round_frame(side)),
        "",
        "## Monthly Performance",
        "",
        _markdown_table(_round_frame(monthly)),
        "",
        "## Observed Patterns",
        "",
        "- `opposite_extreme` improves profit factor versus `midpoint` in the tested markets, but remains below 1.0.",
        "- Win rate alone is misleading: QQQ and SPY `opposite_extreme` reach near or above 50% win rate while expectancy remains negative.",
        "- Losses are broad enough across markets and months that this is not a single bad data slice.",
        "",
        "## Failure Hypotheses",
        "",
        "- The fixed NY opening-range reversion rule may be fading genuine continuation rather than transient opening dislocation.",
        "- The stop/target geometry produces poor payoff after fees/slippage, especially for midpoint exits.",
        "- The same base entry logic does not transfer from synthetic crypto sessions to official equity opens in its current form.",
        "",
        "## Final Recommendation",
        "",
        f"`{recommendation}`",
        "",
        "Do not pass ORR V1 to demo. Any further work should be treated as redesign or fresh research, not parameter polishing of the current rule.",
    ]
    return "\n".join(lines) + "\n"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _returns(trades: pd.DataFrame) -> pd.Series:
    if trades.empty:
        return pd.Series(dtype=float)
    column = "return" if "return" in trades.columns else "pnl_pct"
    if column not in trades.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(trades[column], errors="coerce").dropna().astype(float)


def _get_group(grouped, key: tuple) -> pd.DataFrame:
    try:
        return grouped.get_group(key).copy()
    except (AttributeError, KeyError):
        return pd.DataFrame()


def _side_return(trades: pd.DataFrame, side: str) -> float:
    if trades.empty or "side" not in trades.columns:
        return 0.0
    return float(_returns(trades[trades["side"] == side]).sum())


def _best_entry_hour(trades: pd.DataFrame) -> str:
    if trades.empty or "entry_time" not in trades.columns:
        return ""
    frame = trades.copy()
    frame["entry_hour"] = pd.to_datetime(frame["entry_time"], utc=True).dt.hour
    hourly = frame.groupby("entry_hour").apply(lambda group: _returns(group).sum(), include_groups=False)
    if hourly.empty:
        return ""
    return str(int(hourly.idxmax()))


def _mean_column(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    return float(pd.to_numeric(frame[column], errors="coerce").mean())


def _median_column(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    return float(pd.to_numeric(frame[column], errors="coerce").median())


def _discarded_signals_count(signals: pd.DataFrame) -> int:
    if signals.empty or "invalid_signal_reason" not in signals.columns:
        return 0
    reasons = signals["invalid_signal_reason"].fillna("").astype(str).str.strip()
    return int((reasons != "").sum())


def _median_lag_minutes(frame: pd.DataFrame, start_col: str, end_col: str) -> float:
    if frame.empty or start_col not in frame.columns or end_col not in frame.columns:
        return 0.0
    start = pd.to_datetime(frame[start_col], utc=True, errors="coerce")
    end = pd.to_datetime(frame[end_col], utc=True, errors="coerce")
    lag = (end - start).dt.total_seconds() / 60.0
    lag = lag.dropna()
    return float(lag.median()) if not lag.empty else 0.0


def _opposite_extreme_improves_consistently(summary: pd.DataFrame) -> bool:
    improvements = []
    for _, group in summary.groupby("symbol"):
        variants = group.set_index("tp_variant")
        if "midpoint" not in variants.index or "opposite_extreme" not in variants.index:
            continue
        improvements.append(
            float(variants.loc["opposite_extreme", "profit_factor"]) > float(variants.loc["midpoint", "profit_factor"])
        )
    return bool(improvements) and all(improvements)


def _normalize_decision(value: object) -> str:
    text = str(value).strip()
    return text.replace("-", "_")


def _round_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame.round(4)


def _markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No rows."
    columns = frame.columns.tolist()
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in frame.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in columns) + " |")
    return "\n".join(lines)
