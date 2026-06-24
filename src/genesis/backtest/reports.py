from __future__ import annotations

from pathlib import Path

import pandas as pd

from genesis.backtest.trade_engine import max_consecutive_losses, summarize_trade_returns


def build_summary(
    trades: pd.DataFrame,
    signals: pd.DataFrame,
    invalid_sessions: pd.DataFrame,
    total_sessions: int,
    valid_sessions: int,
    data_quality: dict | None = None,
) -> pd.DataFrame:
    rows: list[dict] = []
    variants = sorted(trades["tp_variant"].dropna().unique().tolist()) if not trades.empty else ["midpoint", "opposite_extreme"]
    signals_count = int(len(signals))
    invalid_count = (
        int(invalid_sessions["session_id"].nunique())
        if not invalid_sessions.empty and "session_id" in invalid_sessions.columns
        else int(len(invalid_sessions))
    )
    data_quality = {} if data_quality is None else dict(data_quality)
    invalid_sessions_ratio = float(invalid_count / total_sessions) if total_sessions else 0.0

    for variant in variants:
        variant_trades = trades[trades["tp_variant"] == variant].copy() if not trades.empty else pd.DataFrame()
        returns = variant_trades["return"].astype(float) if not variant_trades.empty else pd.Series(dtype=float)
        base = summarize_trade_returns(returns)
        trades_count = int(len(variant_trades))
        return_to_drawdown = _return_to_drawdown_ratio(base["total_return"], base["max_drawdown"])
        top_5_share = _top_5_trades_pnl_share(returns)
        top_5_net_share = _top_5_net_pnl_share(returns)
        top_month_share = _top_month_return_share(variant_trades)
        row = {
            "tp_variant": variant,
            **base,
            "return_to_drawdown_ratio": return_to_drawdown,
            "top_5_trades_pnl_share": top_5_share,
            "top_5_net_pnl_share": top_5_net_share,
            "top_month_return_share": top_month_share,
            "avg_rr": float(variant_trades["rr"].mean()) if trades_count else 0.0,
            "profit_per_trade": float(returns.mean()) if trades_count else 0.0,
            "max_consecutive_losses": max_consecutive_losses(returns) if trades_count else 0,
            "time_in_market": float(variant_trades["bars_in_trade"].sum()) if trades_count else 0.0,
            "avg_bars_in_trade": float(variant_trades["bars_in_trade"].mean()) if trades_count else 0.0,
            "median_bars_in_trade": float(variant_trades["bars_in_trade"].median()) if trades_count else 0.0,
            "total_sessions": int(total_sessions),
            "valid_sessions": int(valid_sessions),
            "invalid_sessions": invalid_count,
            "invalid_sessions_ratio": invalid_sessions_ratio,
            "signals_count": signals_count,
            "trades_count": trades_count,
            "signal_to_trade_rate": float(trades_count / signals_count) if signals_count else 0.0,
        }
        row.update(data_quality)
        row["decision"] = decision_for_variant(row)
        rows.append(row)
    return pd.DataFrame(rows)


def _return_to_drawdown_ratio(total_return: float, max_drawdown: float) -> float:
    if max_drawdown == 0:
        return float("inf") if total_return > 0 else 0.0
    return float(total_return / abs(max_drawdown))


def _top_5_trades_pnl_share(returns: pd.Series) -> float:
    positive = returns[returns > 0].sort_values(ascending=False)
    if positive.empty:
        return 1.0
    return float(positive.head(5).sum() / positive.sum())


def _top_5_net_pnl_share(returns: pd.Series) -> float:
    net = float(returns.sum())
    if net <= 0:
        return 0.0
    return float(returns.sort_values(ascending=False).head(5).sum() / net)


def _top_month_return_share(trades: pd.DataFrame) -> float:
    if trades.empty or "entry_time" not in trades.columns:
        return 1.0
    frame = trades.copy()
    frame["entry_time"] = pd.to_datetime(frame["entry_time"], utc=True).dt.tz_convert(None)
    frame["period"] = frame["entry_time"].dt.to_period("M").astype(str)
    monthly = frame.groupby("period")["return"].sum()
    positive = monthly[monthly > 0]
    if positive.empty:
        return 1.0
    return float(positive.max() / positive.sum())


def decision_for_variant(row: dict | pd.Series) -> str:
    if row.get("decision_profile") == "market_fit":
        return market_fit_decision_for_variant(row)
    coverage = float(row.get("coverage_ratio_5m", 1.0) or 0.0)
    invalid_ratio = float(row.get("invalid_sessions_ratio", 0.0) or 0.0)
    daily_warmup_ok = row.get("daily_warmup_ok", True)
    if coverage < 0.50 or invalid_ratio > 0.50 or not bool(daily_warmup_ok):
        return "no-go"

    hard_no_go = (
        float(row.get("expectancy", 0.0) or 0.0) <= 0
        or float(row.get("profit_factor", 0.0) or 0.0) <= 1.0
    )
    if hard_no_go:
        return "no-go"

    demo_ready = (
        int(row.get("trades_count", 0) or 0) >= 50
        and float(row.get("total_return", 0.0) or 0.0) > 0
        and float(row.get("expectancy", 0.0) or 0.0) > 0
        and float(row.get("profit_factor", 0.0) or 0.0) > 1.15
        and float(row.get("return_to_drawdown_ratio", 0.0) or 0.0) >= 0.5
        and float(row.get("top_5_trades_pnl_share", 1.0) or 1.0) <= 0.60
        and coverage >= 0.90
        and invalid_ratio <= 0.30
    )
    if demo_ready:
        return "demo"
    return "continue"


def market_fit_decision_for_variant(row: dict | pd.Series) -> str:
    coverage = float(row.get("coverage_ratio_5m", 0.0) or 0.0)
    daily_warmup_ok = bool(row.get("daily_warmup_ok", False))
    if coverage < 0.90 or not daily_warmup_ok:
        return "no_data"
    trades_count = int(row.get("trades_count", 0) or 0)
    if trades_count == 0:
        return "no_data"
    expectancy = float(row.get("expectancy", float("nan")))
    profit_factor = float(row.get("profit_factor", float("nan")))
    if pd.isna(expectancy) or pd.isna(profit_factor):
        return "no_data"
    if expectancy <= 0 or profit_factor <= 1.0:
        return "no_go"
    candidate = (
        trades_count >= 50
        and expectancy > 0
        and profit_factor > 1.15
        and float(row.get("return_to_drawdown_ratio", 0.0) or 0.0) >= 0.5
        and float(row.get("top_5_trades_pnl_share", 1.0) or 1.0) <= 0.60
        and float(row.get("top_month_return_share", 1.0) or 1.0) <= 0.60
    )
    return "candidate_for_validation" if candidate else "continue_research"


def build_time_breakdown(trades: pd.DataFrame, freq: str) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["period", "tp_variant", "trades_count", "total_return", "win_rate", "profit_factor"])
    frame = trades.copy()
    frame["entry_time"] = pd.to_datetime(frame["entry_time"], utc=True).dt.tz_convert(None)
    frame["period"] = frame["entry_time"].dt.to_period(freq).astype(str)
    rows: list[dict] = []
    for (period, variant), group in frame.groupby(["period", "tp_variant"]):
        summary = summarize_trade_returns(group["return"].astype(float))
        rows.append(
            {
                "period": period,
                "tp_variant": variant,
                "trades_count": int(len(group)),
                "total_return": summary["total_return"],
                "win_rate": summary["win_rate"],
                "profit_factor": summary["profit_factor"],
            }
        )
    return pd.DataFrame(rows).sort_values(["period", "tp_variant"]).reset_index(drop=True)


def recommendation(summary: pd.DataFrame) -> str:
    if summary.empty:
        return "no-go"
    best = summary.sort_values(["profit_factor", "expectancy", "total_return"], ascending=False).iloc[0]
    return str(best.get("decision", decision_for_variant(best)))


def _markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return ""
    columns = frame.columns.tolist()
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in frame.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in columns) + " |")
    return "\n".join(lines)


def render_validation_markdown(
    summary: pd.DataFrame,
    invalid_sessions: pd.DataFrame,
    output_prefix: str,
    monthly_breakdown: pd.DataFrame | None = None,
    gaps: pd.DataFrame | None = None,
) -> str:
    monthly_breakdown = pd.DataFrame() if monthly_breakdown is None else monthly_breakdown
    gaps = pd.DataFrame() if gaps is None else gaps
    lines = [
        f"# Opening Range Reversion Validation - {output_prefix}",
        "",
        "## Summary",
        "",
    ]
    if summary.empty:
        lines.append("No summary rows were generated.")
    else:
        display_cols = [
            "tp_variant",
            "symbol",
            "total_return",
            "win_rate",
            "profit_factor",
            "decision",
            "max_drawdown",
            "return_to_drawdown_ratio",
            "expectancy",
            "top_5_trades_pnl_share",
            "top_month_return_share",
            "avg_bars_in_trade",
            "median_bars_in_trade",
            "coverage_ratio_5m",
            "partial_regular_sessions",
            "invalid_sessions_ratio",
            "signals_count",
            "trades_count",
            "signal_to_trade_rate",
        ]
        available = [col for col in display_cols if col in summary.columns]
        lines.append(_markdown_table(summary[available].round(4)))
        best = summary.sort_values(["profit_factor", "expectancy", "total_return"], ascending=False).iloc[0]
        lines.extend(
            [
                "",
                "## Decision Inputs",
                "",
                f"- best_tp_variant: {best['tp_variant']}",
                f"- symbol: {best.get('symbol', '')}",
                f"- decision: {best.get('decision', decision_for_variant(best))}",
                f"- coverage_ratio_5m: {best.get('coverage_ratio_5m', '')}",
                f"- partial_regular_sessions: {best.get('partial_regular_sessions', '')}",
                f"- invalid_sessions_ratio: {best.get('invalid_sessions_ratio', '')}",
                f"- top_5_trades_pnl_share: {best.get('top_5_trades_pnl_share', '')}",
                f"- top_5_net_pnl_share: {best.get('top_5_net_pnl_share', '')}",
                f"- top_month_return_share: {best.get('top_month_return_share', '')}",
                f"- adjusted_data: {best.get('adjusted_data', '')}",
                f"- ny_gap_count_gt_60m: {best.get('ny_gap_count_gt_60m', '')}",
            ]
        )

    lines.extend(["", "## Monthly Performance", ""])
    if monthly_breakdown.empty:
        lines.append("No monthly performance rows.")
    else:
        lines.append(_markdown_table(monthly_breakdown.round(4)))

    lines.extend(["", "## Invalid Sessions", ""])
    if invalid_sessions.empty:
        lines.append("No invalid sessions.")
    else:
        counts = invalid_sessions["invalid_reason"].value_counts().rename_axis("invalid_reason").reset_index(name="count")
        lines.append(_markdown_table(counts))

    lines.extend(["", "## Relevant 5m Gaps", ""])
    if gaps.empty:
        lines.append("No gaps above 60 minutes inside the NY validation window.")
    else:
        gap_cols = [col for col in ["session_date", "gap_start", "gap_end", "gap_minutes"] if col in gaps.columns]
        lines.append(_markdown_table(gaps[gap_cols].head(20)))

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Reports prioritize `or_size_vs_daily_atr`; `or_size_vs_atr` is only a compatibility alias.",
            "- TP variants are evaluated from the same base signal and entry.",
            "",
            "## Recommendation",
            "",
            recommendation(summary),
        ]
    )
    return "\n".join(lines) + "\n"


def write_report_artifacts(
    output_dir: Path,
    output_prefix: str,
    summary: pd.DataFrame,
    signals: pd.DataFrame,
    trades: pd.DataFrame,
    equity: pd.DataFrame,
    invalid_sessions: pd.DataFrame,
    gaps: pd.DataFrame | None = None,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    gaps = pd.DataFrame() if gaps is None else gaps
    daily = build_time_breakdown(trades, "D")
    weekly = build_time_breakdown(trades, "W")
    monthly = build_time_breakdown(trades, "M")
    markdown = render_validation_markdown(summary, invalid_sessions, output_prefix, monthly_breakdown=monthly, gaps=gaps)

    paths = {
        "summary": output_dir / f"{output_prefix}_summary.csv",
        "signals": output_dir / f"{output_prefix}_signals.csv",
        "trades": output_dir / f"{output_prefix}_trades.csv",
        "equity": output_dir / f"{output_prefix}_equity.csv",
        "invalid_sessions": output_dir / f"{output_prefix}_invalid_sessions.csv",
        "daily_breakdown": output_dir / f"{output_prefix}_daily_breakdown.csv",
        "weekly_breakdown": output_dir / f"{output_prefix}_weekly_breakdown.csv",
        "monthly_breakdown": output_dir / f"{output_prefix}_monthly_breakdown.csv",
        "gaps": output_dir / f"{output_prefix}_gaps.csv",
        "validation": output_dir / f"{output_prefix}_validation.md",
    }
    summary.to_csv(paths["summary"], index=False)
    signals.to_csv(paths["signals"], index=False)
    trades.to_csv(paths["trades"], index=False)
    equity.to_csv(paths["equity"], index=False)
    invalid_sessions.to_csv(paths["invalid_sessions"], index=False)
    daily.to_csv(paths["daily_breakdown"], index=False)
    weekly.to_csv(paths["weekly_breakdown"], index=False)
    monthly.to_csv(paths["monthly_breakdown"], index=False)
    gaps.to_csv(paths["gaps"], index=False)
    paths["validation"].write_text(markdown, encoding="utf-8")
    return paths
