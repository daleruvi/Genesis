import argparse

import numpy as np
import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.backtest.performance import PerformanceAnalyzer
from genesis.config.settings import (
    ALPHA_STORE_DIR,
    DAY_MTF_D1_LIMIT,
    DAY_MTF_COVERAGE_MODE,
    DAY_MTF_DAILY_BIAS_MODE,
    DAY_MTF_END_DATE,
    DAY_MTF_BALANCED_RET_COL,
    DAY_MTF_BALANCED_THRESHOLD,
    DAY_MTF_ENTRY_WINDOW_BARS,
    DAY_MTF_FEE_RATE,
    DAY_MTF_H1_LIMIT,
    DAY_MTF_INITIAL_EQUITY_USDT,
    DAY_MTF_LOOKBACK_EXTREME,
    DAY_MTF_LOOKBACK_STRUCTURE,
    DAY_MTF_M5_BREAKOUT_LOOKBACK,
    DAY_MTF_M5_TRIGGER_MODE,
    DAY_MTF_M5_LIMIT,
    DAY_MTF_MIN_M5_COVERAGE_RATIO,
    DAY_MTF_CHUNK_LIMIT,
    DAY_MTF_MAX_HOLD_BARS,
    DAY_MTF_MIN_IMPULSE_PCT,
    DAY_MTF_MIN_RISK_DISTANCE_PCT,
    DAY_MTF_OUTPUT_PREFIX,
    DAY_MTF_RISK_PER_TRADE,
    DAY_MTF_RR,
    DAY_MTF_SLIPPAGE_RATE,
    DAY_MTF_START_DATE,
    DAY_MTF_SYMBOL,
    DAY_MTF_ZONE_TOLERANCE_PCT,
    ensure_data_dirs,
)
from genesis.data.market_data_loader import MarketDataLoader


def section(title):
    print(f"\n=== {title} ===")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backtest day-trading MTF reversal strategy (D1 context -> H1 setup -> M5 trigger)."
    )
    parser.add_argument("--symbol", default=DAY_MTF_SYMBOL, help="Public market symbol for OHLCV.")
    parser.add_argument("--d1-limit", type=int, default=DAY_MTF_D1_LIMIT, help="D1 bars to fetch.")
    parser.add_argument("--h1-limit", type=int, default=DAY_MTF_H1_LIMIT, help="H1 bars to fetch.")
    parser.add_argument("--m5-limit", type=int, default=DAY_MTF_M5_LIMIT, help="M5 bars to fetch.")
    parser.add_argument("--d1-path", default="", help="Optional parquet path for D1 data.")
    parser.add_argument("--h1-path", default="", help="Optional parquet path for H1 data.")
    parser.add_argument("--m5-path", default="", help="Optional parquet path for M5 data.")
    parser.add_argument(
        "--start-date",
        default=DAY_MTF_START_DATE,
        help="Inclusive UTC start date for range mode (for example 2025-01-01).",
    )
    parser.add_argument(
        "--end-date",
        default=DAY_MTF_END_DATE,
        help="Exclusive UTC end date for range mode (for example 2026-01-01).",
    )
    parser.add_argument("--chunk-limit", type=int, default=DAY_MTF_CHUNK_LIMIT, help="Per-call chunk size in range mode.")
    parser.add_argument(
        "--coverage-mode",
        choices=["warn", "error", "clip"],
        default=DAY_MTF_COVERAGE_MODE,
        help="How to handle partial timeframe coverage for requested range.",
    )
    parser.add_argument(
        "--min-m5-coverage-ratio",
        type=float,
        default=DAY_MTF_MIN_M5_COVERAGE_RATIO,
        help="Minimum available M5 ratio vs expected bars for requested range (0 disables).",
    )
    parser.add_argument("--initial-equity-usdt", type=float, default=DAY_MTF_INITIAL_EQUITY_USDT)
    parser.add_argument("--risk-per-trade", type=float, default=DAY_MTF_RISK_PER_TRADE, help="Fraction of equity risked per trade.")
    parser.add_argument("--rr", type=float, default=DAY_MTF_RR, help="Risk/reward target (suggested 0.8 - 1.6).")
    parser.add_argument("--fee-rate", type=float, default=DAY_MTF_FEE_RATE)
    parser.add_argument("--slippage-rate", type=float, default=DAY_MTF_SLIPPAGE_RATE)
    parser.add_argument("--max-hold-bars", type=int, default=DAY_MTF_MAX_HOLD_BARS, help="Time stop in M5 bars.")
    parser.add_argument("--lookback-extreme", type=int, default=DAY_MTF_LOOKBACK_EXTREME, help="D1 rolling lookback for extremes.")
    parser.add_argument("--lookback-structure", type=int, default=DAY_MTF_LOOKBACK_STRUCTURE, help="H1 structure lookback.")
    parser.add_argument("--zone-tolerance-pct", type=float, default=DAY_MTF_ZONE_TOLERANCE_PCT, help="Tolerance around fib 0.618 and EMA50.")
    parser.add_argument(
        "--daily-bias-mode",
        choices=["strict", "relaxed", "off", "balanced"],
        default=DAY_MTF_DAILY_BIAS_MODE,
        help="How strict daily context should be.",
    )
    parser.add_argument(
        "--balanced-ret-col",
        choices=["ret_1", "ret_3"],
        default=DAY_MTF_BALANCED_RET_COL,
        help="Daily return column used by balanced mode.",
    )
    parser.add_argument(
        "--balanced-threshold",
        type=float,
        default=DAY_MTF_BALANCED_THRESHOLD,
        help="Symmetric threshold for balanced mode bias assignment.",
    )
    parser.add_argument(
        "--entry-window-bars",
        type=int,
        default=DAY_MTF_ENTRY_WINDOW_BARS,
        help="Allow M5 trigger up to N bars after H1 zone appears.",
    )
    parser.add_argument(
        "--m5-trigger-mode",
        choices=["ema_cross", "ema_or_breakout"],
        default=DAY_MTF_M5_TRIGGER_MODE,
        help="M5 trigger style.",
    )
    parser.add_argument(
        "--m5-breakout-lookback",
        type=int,
        default=DAY_MTF_M5_BREAKOUT_LOOKBACK,
        help="Lookback bars for M5 breakout trigger.",
    )
    parser.add_argument(
        "--min-risk-distance-pct",
        type=float,
        default=DAY_MTF_MIN_RISK_DISTANCE_PCT,
        help="Minimum entry-to-stop distance as pct of entry price.",
    )
    parser.add_argument(
        "--min-impulse-pct",
        type=float,
        default=DAY_MTF_MIN_IMPULSE_PCT,
        help="Minimum H1 impulse strength required for entry.",
    )
    parser.add_argument("--output-prefix", default=DAY_MTF_OUTPUT_PREFIX)
    return parser.parse_args()


def fetch_ohlcv(
    loader: MarketDataLoader,
    symbol: str,
    timeframe: str,
    limit: int,
    start_date: str = "",
    end_date: str = "",
    chunk_limit: int = 1000,
) -> pd.DataFrame:
    if start_date:
        df = loader.fetch_ohlcv_range(
            symbol=symbol,
            timeframe=timeframe,
            start=start_date,
            end=end_date or None,
            limit_per_call=chunk_limit,
        )
    else:
        df = loader.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit)
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def load_parquet_ohlcv(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()
    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} missing OHLCV columns: {sorted(missing)}")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values("timestamp").reset_index(drop=True)


def timeframe_to_timedelta(timeframe: str) -> pd.Timedelta:
    tf = str(timeframe).strip().lower()
    amount = int(tf[:-1])
    unit = tf[-1]
    if unit == "m":
        return pd.Timedelta(minutes=amount)
    if unit == "h":
        return pd.Timedelta(hours=amount)
    if unit == "d":
        return pd.Timedelta(days=amount)
    raise ValueError(f"Unsupported timeframe for coverage checks: {timeframe}")


def describe_coverage(df: pd.DataFrame, timeframe: str) -> dict:
    if df.empty:
        return {"timeframe": timeframe, "rows": 0, "start": pd.NaT, "end": pd.NaT, "end_exclusive": pd.NaT}
    step = timeframe_to_timedelta(timeframe)
    start = pd.to_datetime(df["timestamp"].min())
    end = pd.to_datetime(df["timestamp"].max())
    return {
        "timeframe": timeframe,
        "rows": int(len(df)),
        "start": start,
        "end": end,
        "end_exclusive": end + step,
    }


def _format_ts(value) -> str:
    return "-" if pd.isna(value) else str(pd.Timestamp(value))


def validate_and_align_coverage(
    d1: pd.DataFrame,
    h1: pd.DataFrame,
    m5: pd.DataFrame,
    args,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    coverage = pd.DataFrame(
        [
            describe_coverage(d1, "1d"),
            describe_coverage(h1, "1h"),
            describe_coverage(m5, "5m"),
        ]
    )
    printable = coverage.copy()
    printable["start"] = printable["start"].map(_format_ts)
    printable["end"] = printable["end"].map(_format_ts)
    printable["end_exclusive"] = printable["end_exclusive"].map(_format_ts)
    print("\nCoverage by timeframe:")
    print(printable[["timeframe", "rows", "start", "end", "end_exclusive"]])

    if m5.empty:
        raise ValueError("M5 dataset is empty; cannot run day-MTF backtest.")

    requested_start = pd.to_datetime(args.start_date) if args.start_date else None
    requested_end = pd.to_datetime(args.end_date) if args.end_date else None

    violations = []
    for row in coverage.itertuples(index=False):
        tf = row.timeframe
        if row.rows == 0:
            violations.append(f"{tf}: empty dataset")
            continue
        if requested_start is not None and pd.Timestamp(row.start) > requested_start:
            violations.append(f"{tf}: starts at {row.start} (requested {requested_start})")
        if requested_end is not None and pd.Timestamp(row.end_exclusive) < requested_end:
            violations.append(f"{tf}: ends at {row.end_exclusive} (requested {requested_end})")

    if requested_start is not None and requested_end is not None:
        expected_m5 = int((requested_end - requested_start) / pd.Timedelta(minutes=5))
        if expected_m5 > 0:
            m5_ratio = len(m5) / expected_m5
            print(f"M5 coverage ratio vs requested window: {m5_ratio:.4f} ({len(m5)}/{expected_m5})")
            if args.min_m5_coverage_ratio > 0 and m5_ratio < args.min_m5_coverage_ratio:
                violations.append(
                    f"m5 coverage ratio {m5_ratio:.4f} is below min {args.min_m5_coverage_ratio:.4f}"
                )

    if violations and args.coverage_mode == "error":
        raise ValueError("Coverage validation failed:\n- " + "\n- ".join(violations))
    if violations and args.coverage_mode == "warn":
        print("\n[WARN] Partial coverage detected:")
        for item in violations:
            print(f"- {item}")

    if args.coverage_mode == "clip":
        starts = [pd.Timestamp(v) for v in coverage["start"].tolist() if not pd.isna(v)]
        ends = [pd.Timestamp(v) for v in coverage["end_exclusive"].tolist() if not pd.isna(v)]
        clip_start = max(starts)
        clip_end = min(ends)
        if requested_start is not None:
            clip_start = max(clip_start, requested_start)
        if requested_end is not None:
            clip_end = min(clip_end, requested_end)
        if clip_start >= clip_end:
            raise ValueError(f"Invalid clip window after coverage alignment: {clip_start} >= {clip_end}")
        d1 = d1[(d1["timestamp"] >= clip_start) & (d1["timestamp"] < clip_end)].copy()
        h1 = h1[(h1["timestamp"] >= clip_start) & (h1["timestamp"] < clip_end)].copy()
        m5 = m5[(m5["timestamp"] >= clip_start) & (m5["timestamp"] < clip_end)].copy()
        print(f"\nCoverage mode=clip applied: {clip_start} -> {clip_end}")
        print(f"Rows after clip: D1={len(d1)} H1={len(h1)} M5={len(m5)}")
        if d1.empty or h1.empty or m5.empty:
            raise ValueError("At least one timeframe became empty after clip; aborting.")

    return d1, h1, m5, coverage


def build_daily_context(
    d1: pd.DataFrame,
    lookback: int,
    mode: str = "strict",
    balanced_ret_col: str = "ret_1",
    balanced_threshold: float = 0.0,
) -> pd.DataFrame:
    frame = d1.copy()
    frame["ret_1"] = frame["close"].pct_change(1)
    frame["ret_3"] = frame["close"].pct_change(3)
    frame["range"] = frame["high"] - frame["low"]
    frame["body"] = (frame["close"] - frame["open"]).abs()

    rolling_low = frame["low"].rolling(lookback).min()
    rolling_high = frame["high"].rolling(lookback).max()
    near_support = frame["close"] <= rolling_low * 1.01
    near_resistance = frame["close"] >= rolling_high * 0.99
    near_support_relaxed = frame["close"] <= rolling_low * 1.03
    near_resistance_relaxed = frame["close"] >= rolling_high * 0.97

    accel = frame["ret_3"].abs() > frame["ret_3"].abs().rolling(lookback).quantile(0.6)
    decel = (frame["body"] < frame["body"].shift(1)) & (frame["range"] < frame["range"].shift(1))
    bullish_reversal = (frame["close"] > frame["open"]) & (frame["close"].shift(1) < frame["open"].shift(1))
    bearish_reversal = (frame["close"] < frame["open"]) & (frame["close"].shift(1) > frame["open"].shift(1))

    strict_long = near_support & accel & decel & bullish_reversal
    strict_short = near_resistance & accel & decel & bearish_reversal
    relaxed_long = near_support_relaxed & (
        bullish_reversal
        | (decel & (frame["ret_3"] < 0))
        | (accel & (frame["ret_3"] < 0))
    )
    relaxed_short = near_resistance_relaxed & (
        bearish_reversal
        | (decel & (frame["ret_3"] > 0))
        | (accel & (frame["ret_3"] > 0))
    )

    frame["daily_bias"] = "flat"
    if mode == "off":
        # Truly disable the extreme filter: assign bias from short daily momentum sign.
        frame.loc[frame["ret_3"] >= 0, "daily_bias"] = "long"
        frame.loc[frame["ret_3"] < 0, "daily_bias"] = "short"
    elif mode == "balanced":
        ret_col = balanced_ret_col if balanced_ret_col in frame.columns else "ret_1"
        threshold = abs(float(balanced_threshold))
        frame.loc[frame[ret_col] > threshold, "daily_bias"] = "long"
        frame.loc[frame[ret_col] < -threshold, "daily_bias"] = "short"
    elif mode == "relaxed":
        frame.loc[relaxed_long, "daily_bias"] = "long"
        frame.loc[relaxed_short, "daily_bias"] = "short"
    else:
        frame.loc[strict_long, "daily_bias"] = "long"
        frame.loc[strict_short, "daily_bias"] = "short"
    return frame[["timestamp", "daily_bias"]].copy()


def build_h1_setups(h1: pd.DataFrame, lookback: int, zone_tolerance_pct: float) -> pd.DataFrame:
    frame = h1.copy()
    frame["ema50"] = frame["close"].ewm(span=50, adjust=False).mean()
    frame["roll_high"] = frame["high"].rolling(lookback).max().shift(1)
    frame["roll_low"] = frame["low"].rolling(lookback).min().shift(1)
    frame["impulse_long"] = frame["close"] > frame["roll_high"]
    frame["impulse_short"] = frame["close"] < frame["roll_low"]

    frame["swing_low"] = frame["low"].rolling(lookback).min().shift(1)
    frame["swing_high"] = frame["high"].rolling(lookback).max().shift(1)

    long_range = (frame["high"] - frame["swing_low"]).replace(0, np.nan)
    short_range = (frame["swing_high"] - frame["low"]).replace(0, np.nan)
    frame["impulse_strength_long_pct"] = (long_range / frame["close"]).replace([np.inf, -np.inf], np.nan)
    frame["impulse_strength_short_pct"] = (short_range / frame["close"]).replace([np.inf, -np.inf], np.nan)

    frame["fib0618_long"] = frame["high"] - (long_range * 0.618)
    frame["fib075_long"] = frame["high"] - (long_range * 0.75)
    frame["fib0618_short"] = frame["low"] + (short_range * 0.618)
    frame["fib075_short"] = frame["low"] + (short_range * 0.75)

    frame["zone_long"] = (
        frame["impulse_long"]
        & ((frame["close"] - frame["fib0618_long"]).abs() / frame["close"] <= zone_tolerance_pct)
        & ((frame["close"] - frame["ema50"]).abs() / frame["close"] <= zone_tolerance_pct)
    )
    frame["zone_short"] = (
        frame["impulse_short"]
        & ((frame["close"] - frame["fib0618_short"]).abs() / frame["close"] <= zone_tolerance_pct)
        & ((frame["close"] - frame["ema50"]).abs() / frame["close"] <= zone_tolerance_pct)
    )

    return frame[
        [
            "timestamp",
            "zone_long",
            "zone_short",
            "fib075_long",
            "fib075_short",
            "impulse_strength_long_pct",
            "impulse_strength_short_pct",
        ]
    ].copy()


def join_mtf(
    m5: pd.DataFrame,
    d1_ctx: pd.DataFrame,
    h1_setup: pd.DataFrame,
    entry_window_bars: int,
    m5_trigger_mode: str,
    m5_breakout_lookback: int,
) -> pd.DataFrame:
    frame = m5.copy()
    frame["date"] = frame["timestamp"].dt.floor("D")
    d1_map = d1_ctx.copy()
    d1_map["date"] = d1_map["timestamp"].dt.floor("D")
    frame = frame.merge(d1_map[["date", "daily_bias"]], on="date", how="left")

    h1 = h1_setup.sort_values("timestamp")
    frame = pd.merge_asof(
        frame.sort_values("timestamp"),
        h1,
        on="timestamp",
        direction="backward",
    )
    frame["daily_bias"] = frame["daily_bias"].fillna("flat")
    frame["ema20_m5"] = frame["close"].ewm(span=20, adjust=False).mean()
    ema_long = (frame["close"] > frame["ema20_m5"]) & (frame["close"].shift(1) <= frame["ema20_m5"].shift(1))
    ema_short = (frame["close"] < frame["ema20_m5"]) & (frame["close"].shift(1) >= frame["ema20_m5"].shift(1))
    lb = max(2, int(m5_breakout_lookback))
    breakout_long = frame["close"] > frame["high"].rolling(lb).max().shift(1)
    breakout_short = frame["close"] < frame["low"].rolling(lb).min().shift(1)
    if m5_trigger_mode == "ema_or_breakout":
        frame["entry_long_m5"] = (ema_long | breakout_long).fillna(False)
        frame["entry_short_m5"] = (ema_short | breakout_short).fillna(False)
    else:
        frame["entry_long_m5"] = ema_long.fillna(False)
        frame["entry_short_m5"] = ema_short.fillna(False)
    window = max(1, int(entry_window_bars))
    frame["zone_long_recent"] = frame["zone_long"].fillna(False).astype(int).rolling(window, min_periods=1).max().astype(bool)
    frame["zone_short_recent"] = frame["zone_short"].fillna(False).astype(int).rolling(window, min_periods=1).max().astype(bool)
    return frame


def backtest(frame: pd.DataFrame, args) -> tuple[pd.DataFrame, pd.DataFrame, dict, dict]:
    fee_and_slippage = args.fee_rate + args.slippage_rate
    analyzer = PerformanceAnalyzer(periods_per_year=365 * 24 * 12)

    equity = float(args.initial_equity_usdt)
    position = None
    equity_curve = []
    strategy_returns = []
    benchmark_returns = []
    turnover = []
    trades = []

    for i in range(1, len(frame)):
        row_prev = frame.iloc[i - 1]
        row = frame.iloc[i]
        ret = (float(row["close"]) / float(row_prev["close"])) - 1.0
        benchmark_returns.append(ret)

        step_turnover = 0.0
        step_return = 0.0

        if position is not None:
            direction = 1.0 if position["side"] == "long" else -1.0
            step_return += direction * position["exposure"] * ret
            hold_bars = i - position["entry_idx"]
            stop_hit = (row["low"] <= position["stop"]) if position["side"] == "long" else (row["high"] >= position["stop"])
            tp_hit = (row["high"] >= position["tp"]) if position["side"] == "long" else (row["low"] <= position["tp"])
            time_hit = hold_bars >= args.max_hold_bars

            if stop_hit or tp_hit or time_hit:
                exit_price = position["stop"] if stop_hit else (position["tp"] if tp_hit else float(row["close"]))
                pnl = (exit_price / position["entry_price"] - 1.0) * direction * position["exposure"] * position["entry_equity"]
                reason = "stop_loss" if stop_hit else ("take_profit" if tp_hit else "time_stop")
                trades.append(
                    {
                        "entry_time": position["entry_time"],
                        "exit_time": row["timestamp"],
                        "side": position["side"],
                        "entry_price": position["entry_price"],
                        "exit_price": exit_price,
                        "stop": position["stop"],
                        "tp": position["tp"],
                        "exposure": position["exposure"],
                        "pnl_usdt": pnl,
                        "exit_reason": reason,
                    }
                )
                step_turnover += position["exposure"]
                position = None

        if position is None:
            long_setup = row.get("daily_bias") == "long" and bool(row.get("zone_long_recent")) and bool(row.get("entry_long_m5"))
            short_setup = row.get("daily_bias") == "short" and bool(row.get("zone_short_recent")) and bool(row.get("entry_short_m5"))
            if long_setup or short_setup:
                side = "long" if long_setup else "short"
                entry_price = float(row["close"])
                stop = float(row["fib075_long"]) if side == "long" else float(row["fib075_short"])
                if not np.isfinite(stop):
                    stop = entry_price * (0.995 if side == "long" else 1.005)
                risk_distance = abs(entry_price - stop)
                if risk_distance > 0:
                    risk_distance_pct = risk_distance / max(entry_price, 1e-8)
                    impulse_pct = float(
                        row.get("impulse_strength_long_pct") if side == "long" else row.get("impulse_strength_short_pct")
                    )
                    if not np.isfinite(impulse_pct):
                        impulse_pct = 0.0
                    passes_quality = (
                        risk_distance_pct >= args.min_risk_distance_pct
                        and impulse_pct >= args.min_impulse_pct
                    )
                    if passes_quality:
                        risk_budget = equity * args.risk_per_trade
                        notional = risk_budget / (risk_distance / entry_price)
                        exposure = min(max(notional / max(equity, 1e-8), 0.0), 1.0)
                        tp = entry_price + (risk_distance * args.rr) if side == "long" else entry_price - (risk_distance * args.rr)
                        position = {
                            "side": side,
                            "entry_time": row["timestamp"],
                            "entry_idx": i,
                            "entry_price": entry_price,
                            "entry_equity": equity,
                            "stop": stop,
                            "tp": tp,
                            "exposure": exposure,
                            "risk_distance_pct": risk_distance_pct,
                            "impulse_pct": impulse_pct,
                        }
                        step_turnover += exposure

        cost = step_turnover * fee_and_slippage
        step_return -= cost
        equity *= (1.0 + step_return)

        strategy_returns.append(step_return)
        turnover.append(step_turnover)
        equity_curve.append(equity)

    results = pd.DataFrame(
        {
            "timestamp": frame["timestamp"].iloc[1:].values,
            "close": frame["close"].iloc[1:].values,
            "strategy_returns": strategy_returns,
            "benchmark_returns": benchmark_returns,
            "turnover": turnover,
            "equity_usdt": equity_curve,
        }
    )
    trades_df = pd.DataFrame(trades)
    strat_summary = analyzer.summarize(results["strategy_returns"], turnover=results["turnover"])
    bench_summary = analyzer.summarize(results["benchmark_returns"])
    return results, trades_df, strat_summary, bench_summary


def build_signal_diagnostics(frame: pd.DataFrame, args) -> pd.DataFrame:
    total = len(frame)
    daily_long = frame["daily_bias"] == "long"
    daily_short = frame["daily_bias"] == "short"
    zone_long = frame["zone_long"].fillna(False).astype(bool)
    zone_short = frame["zone_short"].fillna(False).astype(bool)
    zone_long_recent = frame.get("zone_long_recent", pd.Series(False, index=frame.index)).fillna(False).astype(bool)
    zone_short_recent = frame.get("zone_short_recent", pd.Series(False, index=frame.index)).fillna(False).astype(bool)
    m5_long = frame["entry_long_m5"].fillna(False).astype(bool)
    m5_short = frame["entry_short_m5"].fillna(False).astype(bool)
    long_risk_distance_pct = ((frame["close"] - frame["fib075_long"]).abs() / frame["close"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    short_risk_distance_pct = ((frame["close"] - frame["fib075_short"]).abs() / frame["close"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    impulse_long_pct = frame.get("impulse_strength_long_pct", pd.Series(0.0, index=frame.index)).fillna(0.0)
    impulse_short_pct = frame.get("impulse_strength_short_pct", pd.Series(0.0, index=frame.index)).fillna(0.0)

    raw_long = daily_long & zone_long_recent & m5_long
    raw_short = daily_short & zone_short_recent & m5_short
    quality_long = raw_long & (long_risk_distance_pct >= args.min_risk_distance_pct) & (impulse_long_pct >= args.min_impulse_pct)
    quality_short = raw_short & (short_risk_distance_pct >= args.min_risk_distance_pct) & (impulse_short_pct >= args.min_impulse_pct)

    funnel = [
        {"metric": "rows_total", "count": int(total), "ratio": 1.0 if total > 0 else 0.0},
        {"metric": "daily_long", "count": int(daily_long.sum()), "ratio": float(daily_long.mean()) if total > 0 else 0.0},
        {"metric": "daily_short", "count": int(daily_short.sum()), "ratio": float(daily_short.mean()) if total > 0 else 0.0},
        {"metric": "zone_long", "count": int(zone_long.sum()), "ratio": float(zone_long.mean()) if total > 0 else 0.0},
        {"metric": "zone_short", "count": int(zone_short.sum()), "ratio": float(zone_short.mean()) if total > 0 else 0.0},
        {
            "metric": "daily_long_and_zone_long",
            "count": int((daily_long & zone_long).sum()),
            "ratio": float((daily_long & zone_long).mean()) if total > 0 else 0.0,
        },
        {
            "metric": "daily_short_and_zone_short",
            "count": int((daily_short & zone_short).sum()),
            "ratio": float((daily_short & zone_short).mean()) if total > 0 else 0.0,
        },
        {
            "metric": "daily_long_and_zone_long_recent",
            "count": int((daily_long & zone_long_recent).sum()),
            "ratio": float((daily_long & zone_long_recent).mean()) if total > 0 else 0.0,
        },
        {
            "metric": "daily_short_and_zone_short_recent",
            "count": int((daily_short & zone_short_recent).sum()),
            "ratio": float((daily_short & zone_short_recent).mean()) if total > 0 else 0.0,
        },
        {"metric": "m5_entry_long", "count": int(m5_long.sum()), "ratio": float(m5_long.mean()) if total > 0 else 0.0},
        {"metric": "m5_entry_short", "count": int(m5_short.sum()), "ratio": float(m5_short.mean()) if total > 0 else 0.0},
        {
            "metric": "final_long_candidates_raw",
            "count": int(raw_long.sum()),
            "ratio": float(raw_long.mean()) if total > 0 else 0.0,
        },
        {
            "metric": "final_short_candidates_raw",
            "count": int(raw_short.sum()),
            "ratio": float(raw_short.mean()) if total > 0 else 0.0,
        },
        {
            "metric": "quality_long_candidates",
            "count": int(quality_long.sum()),
            "ratio": float(quality_long.mean()) if total > 0 else 0.0,
        },
        {
            "metric": "quality_short_candidates",
            "count": int(quality_short.sum()),
            "ratio": float(quality_short.mean()) if total > 0 else 0.0,
        },
    ]
    return pd.DataFrame(funnel)


def main():
    args = parse_args()
    ensure_data_dirs()
    loader = MarketDataLoader()

    section("Step 1/7 - Load MTF OHLCV")
    use_local = bool(args.d1_path and args.h1_path and args.m5_path)
    if use_local:
        print(f"Local dataset mode: d1={args.d1_path} h1={args.h1_path} m5={args.m5_path}")
        d1 = load_parquet_ohlcv(args.d1_path)
        h1 = load_parquet_ohlcv(args.h1_path)
        m5 = load_parquet_ohlcv(args.m5_path)
    else:
        if args.start_date:
            print(f"Range mode: {args.start_date} -> {args.end_date or 'latest'} (chunk_limit={args.chunk_limit})")
        d1 = fetch_ohlcv(loader, args.symbol, "1d", args.d1_limit, args.start_date, args.end_date, args.chunk_limit)
        h1 = fetch_ohlcv(loader, args.symbol, "1h", args.h1_limit, args.start_date, args.end_date, args.chunk_limit)
        m5 = fetch_ohlcv(loader, args.symbol, "5m", args.m5_limit, args.start_date, args.end_date, args.chunk_limit)
    print(f"D1 rows: {len(d1)} | H1 rows: {len(h1)} | M5 rows: {len(m5)}")

    section("Step 2/7 - Validate coverage")
    print(f"Coverage mode: {args.coverage_mode}")
    if args.min_m5_coverage_ratio > 0:
        print(f"Min M5 coverage ratio: {args.min_m5_coverage_ratio}")
    d1, h1, m5, _ = validate_and_align_coverage(d1, h1, m5, args)

    section("Step 3/7 - Build strategy signals")
    print(f"Daily bias mode: {args.daily_bias_mode}")
    if args.daily_bias_mode == "balanced":
        print(f"Balanced settings: ret_col={args.balanced_ret_col}, threshold={args.balanced_threshold}")
    print(f"Entry window bars: {args.entry_window_bars}")
    print(f"M5 trigger mode: {args.m5_trigger_mode} (breakout lookback={args.m5_breakout_lookback})")
    print(f"Quality filters: min_risk_distance_pct={args.min_risk_distance_pct}, min_impulse_pct={args.min_impulse_pct}")
    d1_ctx = build_daily_context(
        d1,
        args.lookback_extreme,
        mode=args.daily_bias_mode,
        balanced_ret_col=args.balanced_ret_col,
        balanced_threshold=args.balanced_threshold,
    )
    h1_setup = build_h1_setups(h1, args.lookback_structure, args.zone_tolerance_pct)
    frame = join_mtf(
        m5,
        d1_ctx,
        h1_setup,
        entry_window_bars=args.entry_window_bars,
        m5_trigger_mode=args.m5_trigger_mode,
        m5_breakout_lookback=args.m5_breakout_lookback,
    ).dropna(subset=["close"]).reset_index(drop=True)
    print(f"Backtest rows (M5 merged): {len(frame)}")
    diagnostics_df = build_signal_diagnostics(frame, args)
    print("\nSignal funnel diagnostics:")
    print(diagnostics_df.round(4))

    section("Step 4/7 - Run execution emulation backtest")
    results, trades_df, strat_summary, bench_summary = backtest(frame, args)

    section("Step 5/7 - Build outputs")
    summary_df = pd.DataFrame(
        [
            {"name": "strategy", **strat_summary},
            {"name": "benchmark", **bench_summary},
        ]
    )

    section("Step 6/7 - Save artifacts")
    summary_path = ALPHA_STORE_DIR / f"{args.output_prefix}_summary.csv"
    equity_path = ALPHA_STORE_DIR / f"{args.output_prefix}_equity.csv"
    trades_path = ALPHA_STORE_DIR / f"{args.output_prefix}_trades.csv"
    diagnostics_path = ALPHA_STORE_DIR / f"{args.output_prefix}_diagnostics.csv"
    summary_df.to_csv(summary_path, index=False)
    results.to_csv(equity_path, index=False)
    trades_df.to_csv(trades_path, index=False)
    diagnostics_df.to_csv(diagnostics_path, index=False)
    print(f"summary -> {summary_path}")
    print(f"equity -> {equity_path}")
    print(f"trades -> {trades_path}")
    print(f"diagnostics -> {diagnostics_path}")

    section("Step 7/7 - Print headline metrics")
    print(summary_df.round(4))
    print(f"Total trades: {len(trades_df)}")


if __name__ == "__main__":
    main()
