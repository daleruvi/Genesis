import argparse
import itertools
import subprocess
import sys
from pathlib import Path

import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.config.settings import ALPHA_STORE_DIR, RAW_DATA_DIR, ensure_data_dirs


def section(title):
    print(f"\n=== {title} ===")


def parse_float_list(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def parse_int_list(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run parameter grid for scripts/run_backtest_genesis_swing.py and build one comparison table."
    )
    parser.add_argument("--dataset-path", default=str(RAW_DATA_DIR / "btc_4h.parquet"))
    parser.add_argument("--timeframe", default="4h")
    parser.add_argument("--base-notional-usdt", default="80,100,120")
    parser.add_argument("--signal-threshold", default="0.10,0.15,0.20")
    parser.add_argument("--stop-loss-pct", default="0.015,0.02,0.025")
    parser.add_argument("--take-profit-pct", default="0.03,0.04,0.05")
    parser.add_argument("--time-stop-bars", default="8,12,16")
    parser.add_argument("--max-position-usdt", type=float, default=200.0)
    parser.add_argument("--max-trades-per-day", type=int, default=8)
    parser.add_argument("--max-daily-loss-usdt", type=float, default=300.0)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument("--slippage-rate", type=float, default=0.0005)
    parser.add_argument("--train-ratio", type=float, default=0.7)
    parser.add_argument("--output-prefix", default="swing_grid")
    return parser.parse_args()


def main():
    args = parse_args()
    ensure_data_dirs()

    notionals = parse_float_list(args.base_notional_usdt)
    thresholds = parse_float_list(args.signal_threshold)
    stop_losses = parse_float_list(args.stop_loss_pct)
    take_profits = parse_float_list(args.take_profit_pct)
    time_stops = parse_int_list(args.time_stop_bars)

    grid = list(itertools.product(notionals, thresholds, stop_losses, take_profits, time_stops))
    if not grid:
        raise ValueError("Empty grid. Provide at least one value for each search dimension.")

    section("Step 1/4 - Build parameter grid")
    print(f"Total combinations: {len(grid)}")
    print(
        {
            "notionals": notionals,
            "signal_thresholds": thresholds,
            "stop_losses": stop_losses,
            "take_profits": take_profits,
            "time_stop_bars": time_stops,
        }
    )

    results = []
    script_path = Path(__file__).resolve().parent / "run_backtest_genesis_swing.py"

    section("Step 2/4 - Execute swing backtests")
    for idx, (notional, threshold, stop_loss, take_profit, time_stop) in enumerate(grid, start=1):
        run_prefix = f"{args.output_prefix}_{idx:03d}"
        cmd = [
            sys.executable,
            str(script_path),
            "--dataset-path",
            args.dataset_path,
            "--timeframe",
            args.timeframe,
            "--base-notional-usdt",
            str(notional),
            "--signal-threshold",
            str(threshold),
            "--max-position-usdt",
            str(args.max_position_usdt),
            "--stop-loss-pct",
            str(stop_loss),
            "--take-profit-pct",
            str(take_profit),
            "--time-stop-bars",
            str(time_stop),
            "--max-trades-per-day",
            str(args.max_trades_per_day),
            "--max-daily-loss-usdt",
            str(args.max_daily_loss_usdt),
            "--fee-rate",
            str(args.fee_rate),
            "--slippage-rate",
            str(args.slippage_rate),
            "--train-ratio",
            str(args.train_ratio),
            "--output-prefix",
            run_prefix,
        ]
        print(
            f"[{idx}/{len(grid)}] "
            f"notional={notional}, threshold={threshold}, stop={stop_loss}, tp={take_profit}, tstop={time_stop}"
        )
        completed = subprocess.run(cmd, capture_output=True, text=True)
        if completed.returncode != 0:
            print("  -> failed")
            print(completed.stderr.strip() or completed.stdout.strip())
            continue

        summary_path = ALPHA_STORE_DIR / f"{run_prefix}_summary.csv"
        if not summary_path.exists():
            print("  -> missing summary artifact, skipping")
            continue

        summary_df = pd.read_csv(summary_path)
        strategy_row = summary_df.loc[summary_df["name"] == "strategy"]
        if strategy_row.empty:
            print("  -> strategy row missing, skipping")
            continue
        row = strategy_row.iloc[0].to_dict()
        row.update(
            {
                "run_id": run_prefix,
                "base_notional_usdt": notional,
                "signal_threshold": threshold,
                "stop_loss_pct": stop_loss,
                "take_profit_pct": take_profit,
                "time_stop_bars": time_stop,
            }
        )
        results.append(row)
        print("  -> ok")

    section("Step 3/4 - Consolidate and rank results")
    if not results:
        raise RuntimeError("No successful backtests in grid search.")

    comparison = pd.DataFrame(results)
    comparison["score"] = (
        comparison["sharpe"].fillna(0.0) * 0.5
        + comparison["profit_factor"].fillna(0.0) * 0.3
        + comparison["total_return"].fillna(0.0) * 0.2
    )
    comparison = comparison.sort_values(
        ["score", "sharpe", "profit_factor", "total_return"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    comparison.insert(0, "rank", comparison.index + 1)

    output_path = ALPHA_STORE_DIR / f"{args.output_prefix}_comparison.csv"
    comparison.to_csv(output_path, index=False)

    section("Step 4/4 - Print top runs")
    cols = [
        "rank",
        "run_id",
        "score",
        "sharpe",
        "profit_factor",
        "total_return",
        "max_drawdown",
        "total_turnover",
        "base_notional_usdt",
        "signal_threshold",
        "stop_loss_pct",
        "take_profit_pct",
        "time_stop_bars",
    ]
    print(comparison.loc[:, cols].head(10).round(4))
    print(f"\ncomparison saved -> {output_path}")


if __name__ == "__main__":
    main()
