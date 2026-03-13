import argparse

import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.backtest.engine import BacktestEngine
from genesis.backtest.performance import PerformanceAnalyzer
from genesis.config.settings import ALPHA_STORE_DIR, FEATURE_STORE_DIR


def section(title):
    print(f"\n=== {title} ===")


def summarize_frame(analyzer, name, returns, turnover=None):
    summary = analyzer.summarize(returns, turnover=turnover)
    summary["name"] = name
    return summary


def calibrate_quantile_engine(train_df, alpha_name, engine, analyzer):
    candidates = [(0.3, 0.7), (0.25, 0.75), (0.2, 0.8), (0.4, 0.6)]
    best = None

    for lower_q, upper_q in candidates:
        positions = engine.signal_to_positions(
            train_df[alpha_name],
            mode="quantile",
            lower_q=lower_q,
            upper_q=upper_q,
        )
        results = engine.run(train_df["close"], positions)
        sharpe = analyzer.sharpe_ratio(results["strategy_returns"])
        candidate = {"lower_q": lower_q, "upper_q": upper_q, "sharpe": sharpe}
        if best is None or candidate["sharpe"] > best["sharpe"]:
            best = candidate
    return best


def main():
    parser = argparse.ArgumentParser(description="Run a basic alpha backtest with simple OOS split.")
    parser.add_argument("--alpha", default="alpha_1", help="Alpha column to backtest.")
    parser.add_argument("--train-ratio", type=float, default=0.7, help="Train split ratio.")
    parser.add_argument("--fee-rate", type=float, default=0.0005, help="Per-turnover fee rate.")
    parser.add_argument("--slippage-rate", type=float, default=0.0005, help="Per-turnover slippage rate.")
    parser.add_argument("--mode", choices=["sign", "quantile"], default="quantile", help="Signal conversion mode.")
    args = parser.parse_args()

    section("Step 1/5 - Load feature and alpha datasets")
    features = pd.read_parquet(FEATURE_STORE_DIR / "btc_features_4h.parquet")
    alphas = pd.read_parquet(ALPHA_STORE_DIR / "generated_alphas.parquet")
    df = features.join(alphas, how="inner").dropna()

    if args.alpha not in df.columns:
        raise ValueError(f"Alpha `{args.alpha}` not found in dataset.")

    engine = BacktestEngine(fee_rate=args.fee_rate, slippage_rate=args.slippage_rate)
    analyzer = PerformanceAnalyzer()
    train_df, test_df = engine.train_test_split(df, train_ratio=args.train_ratio)

    quantiles = None
    section("Step 2/5 - Split train/test and calibrate signal conversion")
    if args.mode == "quantile":
        quantiles = calibrate_quantile_engine(train_df, args.alpha, engine, analyzer)
        print("Selected quantile thresholds from train split:")
        print(quantiles)

    def build_positions(frame):
        if args.mode == "sign":
            return engine.signal_to_positions(frame[args.alpha], mode="sign")
        return engine.signal_to_positions(
            frame[args.alpha],
            mode="quantile",
            lower_q=quantiles["lower_q"],
            upper_q=quantiles["upper_q"],
        )

    section("Step 3/5 - Build positions and run backtests")
    train_positions = build_positions(train_df)
    test_positions = build_positions(test_df)
    full_positions = pd.concat([train_positions, test_positions]).reindex(df.index)

    train_results = engine.run(train_df["close"], train_positions)
    test_results = engine.run(test_df["close"], test_positions)
    full_results = engine.run(df["close"], full_positions)

    section("Step 4/5 - Compute performance summaries")
    summary_rows = [
        summarize_frame(analyzer, "train_strategy", train_results["strategy_returns"], train_results["turnover"]),
        summarize_frame(analyzer, "test_strategy", test_results["strategy_returns"], test_results["turnover"]),
        summarize_frame(analyzer, "full_strategy", full_results["strategy_returns"], full_results["turnover"]),
        summarize_frame(analyzer, "train_benchmark", train_results["benchmark_returns"]),
        summarize_frame(analyzer, "test_benchmark", test_results["benchmark_returns"]),
        summarize_frame(analyzer, "full_benchmark", full_results["benchmark_returns"]),
    ]

    summary_df = pd.DataFrame(summary_rows)
    section("Step 5/5 - Display key outputs")
    print("\nBacktest summary:")
    print(summary_df.round(4))

    print("\nLast rows of full backtest:")
    print(full_results.tail())


if __name__ == "__main__":
    main()
