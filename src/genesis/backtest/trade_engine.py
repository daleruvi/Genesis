from __future__ import annotations

import numpy as np
import pandas as pd

from genesis.features.sessions import hhmm_to_minutes
from genesis.strategies.opening_range_reversion import OpeningRangeReversionConfig


TP_VARIANTS = {
    "midpoint": "tp_midpoint",
    "opposite_extreme": "tp_opposite_extreme",
}


def apply_slippage(price: float, side: str, event: str, slippage_rate: float) -> float:
    if side == "long" and event == "entry":
        return price * (1.0 + slippage_rate)
    if side == "long" and event == "exit":
        return price * (1.0 - slippage_rate)
    if side == "short" and event == "entry":
        return price * (1.0 - slippage_rate)
    if side == "short" and event == "exit":
        return price * (1.0 + slippage_rate)
    raise ValueError(f"Unsupported side/event: {side}/{event}")


def _trade_return(side: str, entry_fill: float, exit_fill: float, fee_rate: float) -> float:
    if side == "long":
        gross = (exit_fill / entry_fill) - 1.0
    else:
        gross = (entry_fill / exit_fill) - 1.0
    return gross - (2.0 * fee_rate)


def _exit_hit(row: pd.Series, side: str, stop: float, tp: float) -> tuple[str | None, float | None]:
    high = float(row["high"])
    low = float(row["low"])
    if side == "long":
        if low <= stop:
            return "stop", stop
        if high >= tp:
            return "take_profit", tp
    else:
        if high >= stop:
            return "stop", stop
        if low <= tp:
            return "take_profit", tp
    return None, None


def simulate_trade_variant(
    signal: pd.Series,
    session_bars: pd.DataFrame,
    tp_variant: str,
    config: OpeningRangeReversionConfig,
) -> dict:
    side = signal["side"]
    entry_time = pd.Timestamp(signal["entry_time"])
    entry_price = float(signal["entry_price"])
    stop = float(signal["stop"])
    tp = float(signal[TP_VARIANTS[tp_variant]])
    exit_limit = hhmm_to_minutes(config.time_exit_at)

    entry_fill = apply_slippage(entry_price, side, "entry", config.slippage_rate)
    bars = session_bars[session_bars["timestamp_utc"] >= entry_time].sort_values("timestamp_utc").reset_index(drop=True)
    if bars.empty:
        raise ValueError(f"No bars available at or after entry_time={entry_time}.")

    exit_reason = "time_exit"
    exit_price = float(bars.iloc[-1]["close"])
    exit_time = bars.iloc[-1]["timestamp_utc"]
    bars_in_trade = int(len(bars))
    last_before_exit = bars.iloc[0]

    for idx, row in bars.iterrows():
        minute = int(row["local_minutes"])
        if minute > exit_limit:
            exit_price = float(last_before_exit["close"])
            exit_time = last_before_exit["timestamp_utc"]
            bars_in_trade = int(idx)
            break

        reason, hit_price = _exit_hit(row, side, stop, tp)
        if reason is not None:
            exit_reason = reason
            exit_price = float(hit_price)
            exit_time = row["timestamp_utc"]
            bars_in_trade = int(idx + 1)
            break

        last_before_exit = row
        if minute == exit_limit:
            exit_price = float(row["close"])
            exit_time = row["timestamp_utc"]
            bars_in_trade = int(idx + 1)
            break

    exit_fill = apply_slippage(exit_price, side, "exit", config.slippage_rate)
    net_return = _trade_return(side, entry_fill, exit_fill, config.fee_rate)
    risk_per_unit = abs(entry_fill - apply_slippage(stop, side, "exit", config.slippage_rate))
    pnl_per_unit = (exit_fill - entry_fill) if side == "long" else (entry_fill - exit_fill)
    rr = pnl_per_unit / risk_per_unit if risk_per_unit > 0 else 0.0

    trade = signal.to_dict()
    trade.update(
        {
            "tp_variant": tp_variant,
            "tp": tp,
            "entry_fill": entry_fill,
            "exit_time": exit_time,
            "exit_price": exit_price,
            "exit_fill": exit_fill,
            "exit_reason": exit_reason,
            "return": net_return,
            "pnl_pct": net_return,
            "rr": rr,
            "fee_rate": config.fee_rate,
            "slippage_rate": config.slippage_rate,
            "bars_in_trade": bars_in_trade,
        }
    )
    return trade


def backtest_signals(
    signals: pd.DataFrame,
    strategy_frame: pd.DataFrame,
    config: OpeningRangeReversionConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    trades: list[dict] = []
    if signals.empty:
        return pd.DataFrame(), pd.DataFrame(
            [{"tp_variant": variant, "equity": config.initial_equity, "trade_number": 0} for variant in TP_VARIANTS]
        )

    grouped = {session_id: group for session_id, group in strategy_frame.groupby("session_id")}
    for _, signal in signals.iterrows():
        session_bars = grouped.get(signal["session_id"])
        if session_bars is None:
            continue
        for variant in TP_VARIANTS:
            trades.append(simulate_trade_variant(signal, session_bars, variant, config))

    trades_df = pd.DataFrame(trades)
    equity_rows: list[dict] = []
    for variant in TP_VARIANTS:
        equity = float(config.initial_equity)
        equity_rows.append({"tp_variant": variant, "trade_number": 0, "equity": equity, "return": 0.0})
        variant_trades = trades_df[trades_df["tp_variant"] == variant] if not trades_df.empty else pd.DataFrame()
        for trade_number, (_, trade) in enumerate(variant_trades.iterrows(), start=1):
            equity *= 1.0 + float(trade["return"])
            equity_rows.append(
                {
                    "tp_variant": variant,
                    "trade_number": trade_number,
                    "timestamp": trade["exit_time"],
                    "equity": equity,
                    "return": float(trade["return"]),
                }
            )
    return trades_df, pd.DataFrame(equity_rows)


def max_consecutive_losses(returns: pd.Series) -> int:
    max_losses = 0
    current = 0
    for value in returns.fillna(0.0):
        if value < 0:
            current += 1
            max_losses = max(max_losses, current)
        else:
            current = 0
    return int(max_losses)


def profit_factor_from_trades(returns: pd.Series) -> float:
    gains = returns[returns > 0].sum()
    losses = -returns[returns < 0].sum()
    if losses == 0:
        return float("inf") if gains > 0 else 0.0
    return float(gains / losses)


def summarize_trade_returns(returns: pd.Series, periods_per_year: int = 252) -> dict:
    returns = returns.fillna(0.0)
    if returns.empty:
        return {
            "total_return": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "max_drawdown": 0.0,
            "sharpe": 0.0,
            "expectancy": 0.0,
        }
    curve = (1.0 + returns).cumprod()
    peak = curve.cummax()
    drawdown = (curve / peak) - 1.0
    std = returns.std()
    return {
        "total_return": float(curve.iloc[-1] - 1.0),
        "win_rate": float((returns > 0).mean()),
        "profit_factor": profit_factor_from_trades(returns),
        "max_drawdown": float(drawdown.min()),
        "sharpe": 0.0 if std == 0 or np.isnan(std) else float((returns.mean() / std) * np.sqrt(periods_per_year)),
        "expectancy": float(returns.mean()),
    }
