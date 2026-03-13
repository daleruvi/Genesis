import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.alpha.alpha_ensemble import AlphaEnsemble
from genesis.alpha.alpha_generator import AlphaGenerator
from genesis.backtest.performance import PerformanceAnalyzer
from genesis.config.settings import (
    ALPHA_STORE_DIR,
    DEMO_LOOP_OHLCV_LIMIT,
    DEMO_LOOP_SIGNAL_THRESHOLD,
    DEMO_LOOP_TIMEFRAME,
    DEMO_LOOP_DATA_SYMBOL,
    ENSEMBLE_MAX_TEST_TURNOVER,
    ENSEMBLE_MIN_TEST_PROFIT_FACTOR,
    ENSEMBLE_MIN_TEST_SHARPE,
    RAW_DATA_DIR,
    TRADING_STOP_LOSS_PCT,
    TRADING_TAKE_PROFIT_PCT,
    TRADING_STYLE,
    SIZING_DRAWDOWN_CUTOFF,
    SIZING_DRAWDOWN_FLOOR_SCALE,
    SIZING_MAX_MULTIPLIER,
    SIZING_MIN_CONVICTION_SCALE,
    SIZING_MIN_MULTIPLIER,
    SIZING_TARGET_ANNUAL_VOL,
    ensure_data_dirs,
)
from genesis.data.market_data_loader import MarketDataLoader
from genesis.features.feature_engineering import FeatureEngineer
from genesis.llm.regime_detector import RegimeDetector
from genesis.portfolio.position_sizing import PositionSizer
from genesis.portfolio.risk_manager import RiskManager


def section(title):
    print(f"\n=== {title} ===")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backtest swing that emulates GENESIS decision->risk->execution flow for BTC/USDT."
    )
    parser.add_argument("--symbol", default=DEMO_LOOP_DATA_SYMBOL, help="Market data symbol (spot format).")
    parser.add_argument("--timeframe", default=DEMO_LOOP_TIMEFRAME, help="OHLCV timeframe (for example: 4h, 1h).")
    parser.add_argument("--limit", type=int, default=DEMO_LOOP_OHLCV_LIMIT, help="OHLCV bars when fetching from BingX.")
    parser.add_argument(
        "--dataset-path",
        default=str(RAW_DATA_DIR / "btc_4h.parquet"),
        help="Parquet with OHLCV columns timestamp/open/high/low/close/volume.",
    )
    parser.add_argument(
        "--refresh-data",
        action="store_true",
        help="Fetch fresh OHLCV from BingX and overwrite --dataset-path.",
    )
    parser.add_argument("--initial-equity-usdt", type=float, default=10000.0, help="Initial equity for the simulation.")
    parser.add_argument("--base-notional-usdt", type=float, default=100.0, help="Base notional before dynamic sizing.")
    parser.add_argument("--max-position-usdt", type=float, default=200.0, help="Max allowed notional per position.")
    parser.add_argument("--max-open-positions", type=int, default=1, help="Max open positions (1 for single-symbol swing).")
    parser.add_argument("--signal-threshold", type=float, default=DEMO_LOOP_SIGNAL_THRESHOLD, help="Ensemble conviction threshold.")
    parser.add_argument("--fee-rate", type=float, default=0.0005, help="Transaction fee rate per turnover.")
    parser.add_argument("--slippage-rate", type=float, default=0.0005, help="Slippage rate per turnover.")
    parser.add_argument(
        "--stop-loss-pct",
        type=float,
        default=TRADING_STOP_LOSS_PCT,
        help="Stop loss threshold (for example 0.02 = 2%%).",
    )
    parser.add_argument("--take-profit-pct", type=float, default=TRADING_TAKE_PROFIT_PCT, help="Take profit threshold.")
    parser.add_argument("--time-stop-bars", type=int, default=12, help="Max bars to hold a position.")
    parser.add_argument("--drawdown-window", type=int, default=20, help="Window to compute recent drawdown for sizing.")
    parser.add_argument("--max-trades-per-day", type=int, default=8, help="Daily max turnover events.")
    parser.add_argument("--max-daily-loss-usdt", type=float, default=300.0, help="Daily loss kill switch (absolute USDT).")
    parser.add_argument("--train-ratio", type=float, default=0.7, help="Train/test split for OOS reporting.")
    parser.add_argument("--output-prefix", default="swing_genesis", help="Prefix for output files inside data/alpha_store.")
    return parser.parse_args()


def load_or_fetch_ohlcv(args) -> pd.DataFrame:
    dataset_path = Path(args.dataset_path)
    if args.refresh_data:
        loader = MarketDataLoader()
        fetched = loader.fetch_ohlcv(symbol=args.symbol, timeframe=args.timeframe, limit=args.limit)
        dataset_path.parent.mkdir(parents=True, exist_ok=True)
        fetched.to_parquet(dataset_path, engine="pyarrow")
        print(f"dataset refreshed -> {dataset_path}")

    if not dataset_path.exists():
        raise FileNotFoundError(
            f"Dataset not found: {dataset_path}. Use --refresh-data or run scripts/run_data_pipeline.py first."
        )

    df = pd.read_parquet(dataset_path).copy()
    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Dataset missing required columns: {sorted(missing)}")
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False)
    return df


def load_research_inputs():
    rankings_path = ALPHA_STORE_DIR / "selected_alpha_rankings.csv"
    regime_path = ALPHA_STORE_DIR / "alpha_regime_performance.csv"
    missing = [str(path) for path in (rankings_path, regime_path) if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing alpha research artifacts. Run scripts/run_alpha_research.py and "
            f"scripts/run_regime_analysis.py first. Missing: {missing}"
        )
    return pd.read_csv(rankings_path), pd.read_csv(regime_path)


def build_features_and_alphas(ohlcv: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    features = (
        FeatureEngineer(ohlcv)
        .returns()
        .volatility()
        .momentum()
        .volume_features()
        .build()
        .reset_index(drop=True)
    )
    alpha_frame = pd.DataFrame(AlphaGenerator(features).generate_default_alphas()).reindex(features.index)
    joined = pd.concat([features, alpha_frame], axis=1).dropna().reset_index(drop=True)
    feature_cols = features.columns.tolist()
    alpha_cols = alpha_frame.columns.tolist()
    return joined[feature_cols].copy(), joined[alpha_cols].copy()


def split_label(index: int, total: int, train_ratio: float) -> str:
    split_idx = max(1, int(total * train_ratio))
    return "train" if index < split_idx else "test"


def main():
    args = parse_args()
    ensure_data_dirs()
    section("Step 1/7 - Load dataset")
    print(f"Trading style profile: {TRADING_STYLE}")
    ohlcv = load_or_fetch_ohlcv(args)
    print(f"Loaded rows: {len(ohlcv)}")
    print(f"Source: {args.dataset_path}")

    section("Step 2/7 - Build features, alphas, and regimes")
    features, alphas = build_features_and_alphas(ohlcv)
    rankings, regime_performance = load_research_inputs()
    regimes = RegimeDetector().detect(features)["regime"].reset_index(drop=True)
    print(f"Usable rows after warmup/dropna: {len(features)}")
    print(f"Alphas generated: {alphas.shape[1]}")

    section("Step 3/7 - Configure ensemble, sizing, and risk")
    ensemble = AlphaEnsemble(
        rankings=rankings,
        regime_performance=regime_performance,
        signal_threshold=args.signal_threshold,
        min_test_sharpe=ENSEMBLE_MIN_TEST_SHARPE,
        min_test_profit_factor=ENSEMBLE_MIN_TEST_PROFIT_FACTOR,
        max_test_turnover=ENSEMBLE_MAX_TEST_TURNOVER,
    )
    sizer = PositionSizer(
        target_annual_vol=SIZING_TARGET_ANNUAL_VOL,
        min_multiplier=SIZING_MIN_MULTIPLIER,
        max_multiplier=SIZING_MAX_MULTIPLIER,
        min_conviction_scale=SIZING_MIN_CONVICTION_SCALE,
        drawdown_cutoff=SIZING_DRAWDOWN_CUTOFF,
        drawdown_floor_scale=SIZING_DRAWDOWN_FLOOR_SCALE,
    )
    risk = RiskManager(
        max_position_usdt=args.max_position_usdt,
        max_open_positions=args.max_open_positions,
    )
    print(
        f"Ensemble filters: sharpe>{ENSEMBLE_MIN_TEST_SHARPE}, "
        f"profit_factor>{ENSEMBLE_MIN_TEST_PROFIT_FACTOR}, turnover<={ENSEMBLE_MAX_TEST_TURNOVER}"
    )
    print(f"Eligible alphas: {len(ensemble.filtered_rankings)}")

    section("Step 4/7 - Run swing backtest loop with execution emulation")
    close = features["close"].astype(float).reset_index(drop=True)
    timestamps = pd.to_datetime(features["timestamp"]).reset_index(drop=True)
    n = len(features)
    if n < 60:
        raise ValueError("Not enough rows after feature warmup. Use more OHLCV data.")

    periods_per_year = 365 * max(int(24 / max(1, int(args.timeframe.replace('h', '') or 4))), 1) if args.timeframe.endswith("h") else 365 * 6
    analyzer = PerformanceAnalyzer(periods_per_year=periods_per_year)

    strategy_returns = pd.Series(0.0, index=features.index, dtype="float64")
    turnover_series = pd.Series(0.0, index=features.index, dtype="float64")
    exposure_series = pd.Series(0.0, index=features.index, dtype="float64")
    signal_series = pd.Series("flat", index=features.index, dtype="object")
    decision_regime = pd.Series("unknown", index=features.index, dtype="object")
    decision_conviction = pd.Series(0.0, index=features.index, dtype="float64")
    reason_series = pd.Series("init", index=features.index, dtype="object")

    equity = float(args.initial_equity_usdt)
    equity_curve = [equity]
    exposure = 0.0
    position_side = None
    entry_price = None
    entry_bar = None
    current_date = None
    day_start_equity = equity
    trades_today = 0
    fee_and_slippage = args.fee_rate + args.slippage_rate
    trades_log = []

    for i in range(1, n):
        price_prev = float(close.iloc[i - 1])
        price_now = float(close.iloc[i])
        ts_now = timestamps.iloc[i]
        date_now = ts_now.date()
        if current_date != date_now:
            current_date = date_now
            day_start_equity = equity
            trades_today = 0

        asset_return = (price_now / price_prev) - 1.0 if price_prev > 0 else 0.0
        desired_exposure = exposure
        reason = "hold"

        if exposure != 0.0 and position_side is not None and entry_price is not None:
            pnl_since_entry = ((price_now / entry_price) - 1.0) * (1.0 if position_side == "long" else -1.0)
            held_bars = i - int(entry_bar)
            if pnl_since_entry <= -abs(args.stop_loss_pct):
                desired_exposure = 0.0
                reason = "stop_loss"
            elif pnl_since_entry >= abs(args.take_profit_pct):
                desired_exposure = 0.0
                reason = "take_profit"
            elif held_bars >= args.time_stop_bars:
                desired_exposure = 0.0
                reason = "time_stop"

        daily_pnl = equity - day_start_equity
        kill_switch = daily_pnl <= -abs(args.max_daily_loss_usdt)
        if kill_switch:
            desired_exposure = 0.0
            reason = "daily_kill_switch"

        latest_alphas = alphas.iloc[i]
        regime = str(regimes.iloc[i])
        decision = ensemble.decide(latest_alphas=latest_alphas, regime=regime)
        signal = decision["signal"]

        recent_window = close.iloc[max(0, i - args.drawdown_window + 1): i + 1]
        recent_drawdown = (recent_window / recent_window.cummax() - 1.0).min()
        vol = float(features.iloc[i]["volatility_20"])
        realized_annual_vol = vol * (periods_per_year ** 0.5)
        sizing = sizer.size_notional(
            base_notional_usdt=args.base_notional_usdt,
            conviction=decision["conviction"],
            realized_annual_vol=realized_annual_vol,
            recent_drawdown=float(recent_drawdown),
        )
        requested_usdt = sizing["final_notional_usdt"]
        target_usdt = risk.normalize_target_usdt(requested_usdt)
        target_exposure_abs = min(1.0, target_usdt / max(equity, 1e-8))

        active_positions = []
        if exposure != 0.0:
            active_positions.append({"contracts": 1.0, "side": position_side})
        can_open = risk.can_open_new_position(active_positions, requested_usdt)["allowed"]

        if reason in {"hold", "daily_kill_switch"}:
            if signal == "flat":
                desired_exposure = 0.0
                if reason == "hold":
                    reason = "signal_flat"
            elif signal in {"long", "short"} and not kill_switch:
                target_side = "long" if signal == "long" else "short"
                target_exposure = target_exposure_abs if signal == "long" else -target_exposure_abs
                if exposure == 0.0:
                    if can_open and trades_today < args.max_trades_per_day:
                        desired_exposure = target_exposure
                        reason = "open"
                    else:
                        desired_exposure = 0.0
                        reason = "blocked_risk_or_limits"
                elif position_side == target_side:
                    desired_exposure = target_exposure
                    reason = "rebalance_hold"
                else:
                    if trades_today < args.max_trades_per_day:
                        desired_exposure = target_exposure
                        reason = "reverse"
                    else:
                        desired_exposure = 0.0
                        reason = "blocked_daily_trade_limit"

        turnover = abs(desired_exposure - exposure)
        step_return = (exposure * asset_return) - (turnover * fee_and_slippage)
        equity *= (1.0 + step_return)

        if desired_exposure != exposure:
            trades_today += 1
            if desired_exposure == 0.0:
                trades_log.append(
                    {
                        "timestamp": ts_now,
                        "event": "close",
                        "side": position_side,
                        "price": price_now,
                        "equity_usdt": equity,
                        "reason": reason,
                    }
                )
                position_side = None
                entry_price = None
                entry_bar = None
            else:
                new_side = "long" if desired_exposure > 0 else "short"
                if exposure != 0.0 and position_side is not None:
                    trades_log.append(
                        {
                            "timestamp": ts_now,
                            "event": "close",
                            "side": position_side,
                            "price": price_now,
                            "equity_usdt": equity,
                            "reason": reason,
                        }
                    )
                trades_log.append(
                    {
                        "timestamp": ts_now,
                        "event": "open",
                        "side": new_side,
                        "price": price_now,
                        "equity_usdt": equity,
                        "target_notional_usdt": target_usdt,
                        "reason": reason,
                    }
                )
                position_side = new_side
                entry_price = price_now
                entry_bar = i

        exposure = desired_exposure
        strategy_returns.iloc[i] = step_return
        turnover_series.iloc[i] = turnover
        exposure_series.iloc[i] = exposure
        signal_series.iloc[i] = signal
        decision_regime.iloc[i] = regime
        decision_conviction.iloc[i] = float(decision["conviction"])
        reason_series.iloc[i] = reason
        equity_curve.append(equity)

    section("Step 5/7 - Build performance reports")
    benchmark_returns = close.pct_change().fillna(0.0)
    summary_strategy = analyzer.summarize(strategy_returns, turnover=turnover_series)
    summary_benchmark = analyzer.summarize(benchmark_returns)

    split_mask = [split_label(i, n, args.train_ratio) for i in range(n)]
    split_frame = pd.DataFrame(
        {
            "split": split_mask,
            "strategy_returns": strategy_returns,
            "benchmark_returns": benchmark_returns,
            "turnover": turnover_series,
        }
    )
    split_rows = []
    for split_name, group in split_frame.groupby("split"):
        row = analyzer.summarize(group["strategy_returns"], turnover=group["turnover"])
        row["name"] = f"{split_name}_strategy"
        split_rows.append(row)
        bmk = analyzer.summarize(group["benchmark_returns"])
        bmk["name"] = f"{split_name}_benchmark"
        split_rows.append(bmk)
    split_summary = pd.DataFrame(split_rows)

    regime_frame = pd.DataFrame(
        {
            "regime": decision_regime,
            "strategy_returns": strategy_returns,
            "turnover": turnover_series,
        }
    )
    regime_rows = []
    for regime_name, group in regime_frame.groupby("regime"):
        row = analyzer.summarize(group["strategy_returns"], turnover=group["turnover"])
        row["regime"] = regime_name
        row["observations"] = int(len(group))
        regime_rows.append(row)
    regime_summary = pd.DataFrame(regime_rows).sort_values("regime").reset_index(drop=True)

    section("Step 6/7 - Save artifacts")
    output_prefix = args.output_prefix
    summary_path = ALPHA_STORE_DIR / f"{output_prefix}_summary.csv"
    split_path = ALPHA_STORE_DIR / f"{output_prefix}_split_summary.csv"
    regime_path = ALPHA_STORE_DIR / f"{output_prefix}_by_regime.csv"
    equity_path = ALPHA_STORE_DIR / f"{output_prefix}_equity.csv"
    trades_path = ALPHA_STORE_DIR / f"{output_prefix}_trades.csv"

    summary_df = pd.DataFrame(
        [
            {"name": "strategy", **summary_strategy},
            {"name": "benchmark", **summary_benchmark},
        ]
    )
    summary_df.to_csv(summary_path, index=False)
    split_summary.to_csv(split_path, index=False)
    regime_summary.to_csv(regime_path, index=False)

    equity_df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "close": close,
            "equity_usdt": equity_curve,
            "strategy_returns": strategy_returns,
            "benchmark_returns": benchmark_returns,
            "turnover": turnover_series,
            "exposure": exposure_series,
            "signal": signal_series,
            "regime": decision_regime,
            "conviction": decision_conviction,
            "reason": reason_series,
        }
    )
    equity_df.to_csv(equity_path, index=False)
    trades_df = pd.DataFrame(trades_log)
    if not trades_df.empty:
        trades_df.to_csv(trades_path, index=False)
    else:
        pd.DataFrame(columns=["timestamp", "event", "side", "price", "equity_usdt", "reason"]).to_csv(trades_path, index=False)

    section("Step 7/7 - Print headline metrics")
    print("Strategy summary:")
    print(summary_df.round(4))
    print("\nSplit summary:")
    print(split_summary.round(4))
    print(f"\nArtifacts saved:")
    print(f"- {summary_path}")
    print(f"- {split_path}")
    print(f"- {regime_path}")
    print(f"- {equity_path}")
    print(f"- {trades_path}")


if __name__ == "__main__":
    main()
