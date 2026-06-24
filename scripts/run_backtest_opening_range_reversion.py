import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.backtest.reports import build_summary, write_report_artifacts
from genesis.backtest.trade_engine import backtest_signals
from genesis.backtest.validation import (
    coverage_ratio_5m,
    daily_warmup_status,
    equity_regular_session_coverage,
    relevant_5m_gaps,
)
from genesis.config.settings import ALPHA_STORE_DIR, ensure_data_dirs
from genesis.strategies.opening_range_reversion import (
    build_strategy_frame,
    generate_opening_range_signals,
    load_strategy_config,
)


DEFAULT_CONFIG = "config/strategies/opening_range_reversion.yaml"


def parse_args():
    parser = argparse.ArgumentParser(description="Backtest Opening Range Reversion for synthetic NY session.")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--dataset-path")
    parser.add_argument("--daily-dataset-path")
    parser.add_argument("--entry-start")
    parser.add_argument("--entry-end")
    parser.add_argument("--time-exit-at")
    parser.add_argument("--atr-mode")
    parser.add_argument("--daily-atr-period", type=int)
    parser.add_argument("--atr-buffer-mult", type=float)
    parser.add_argument("--min-or-size-vs-atr", type=float)
    parser.add_argument("--max-or-size-vs-atr", type=float)
    parser.add_argument("--min-daily-atr-pct", type=float)
    parser.add_argument("--trend-filter", choices=["ema", "none"])
    parser.add_argument("--ema-period", type=int)
    parser.add_argument("--output-prefix")
    parser.add_argument("--symbol")
    return parser.parse_args()


def load_ohlcv(path: str, name: str) -> pd.DataFrame:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"{name} not found: {file_path}")
    return pd.read_parquet(file_path).copy()


def main() -> int:
    args = parse_args()
    ensure_data_dirs()
    overrides = {
        "dataset_path": args.dataset_path,
        "daily_dataset_path": args.daily_dataset_path,
        "entry_start": args.entry_start,
        "entry_end": args.entry_end,
        "time_exit_at": args.time_exit_at,
        "atr_mode": args.atr_mode,
        "daily_atr_period": args.daily_atr_period,
        "atr_buffer_mult": args.atr_buffer_mult,
        "min_or_size_vs_atr": args.min_or_size_vs_atr,
        "max_or_size_vs_atr": args.max_or_size_vs_atr,
        "min_daily_atr_pct": args.min_daily_atr_pct,
        "trend_filter": args.trend_filter,
        "ema_period": args.ema_period,
        "output_prefix": args.output_prefix,
        "symbol": args.symbol,
    }
    config = load_strategy_config(args.config, overrides=overrides)

    print("=== Opening Range Reversion Backtest ===")
    print(f"Config: {args.config}")
    print(f"Intraday dataset: {config.dataset_path}")
    print(f"Daily dataset: {config.daily_dataset_path}")
    print(f"Session: {config.session} {config.session_start} {config.timezone}")
    print(f"Entry window: {config.entry_start} -> {config.entry_end}; time exit: {config.time_exit_at}")

    intraday = load_ohlcv(config.dataset_path, "intraday dataset")
    daily = load_ohlcv(config.daily_dataset_path, "daily dataset")
    strategy_frame, invalid_sessions = build_strategy_frame(intraday, daily, config)
    signals, invalid_signals = generate_opening_range_signals(strategy_frame, config)
    if not invalid_signals.empty:
        invalid_signals_as_sessions = invalid_signals[["session_id", "session", "session_date", "invalid_signal_reason"]].rename(
            columns={"invalid_signal_reason": "invalid_reason"}
        )
        invalid_sessions = pd.concat([invalid_sessions, invalid_signals_as_sessions], ignore_index=True)

    trades, equity = backtest_signals(signals, strategy_frame, config)
    valid_sessions = int(strategy_frame["session_id"].nunique()) if not strategy_frame.empty else 0
    invalid_session_count = int(invalid_sessions["session_id"].nunique()) if not invalid_sessions.empty else 0
    total_sessions = valid_sessions + invalid_session_count
    first_session_date = None if strategy_frame.empty else str(strategy_frame["session_date"].min())
    if config.session == "ny_equity":
        coverage = equity_regular_session_coverage(
            intraday,
            daily,
            timezone=config.timezone,
            session_start=config.session_start,
            session_end=config.time_exit_at,
        )
    else:
        coverage = coverage_ratio_5m(intraday)
    quality = {
        "symbol": config.symbol,
        "adjusted_data": bool(config.data_adjusted),
        "decision_profile": config.decision_profile,
        **coverage,
        **daily_warmup_status(daily, first_session_date, warmup_days=config.daily_atr_period),
    }
    gaps = relevant_5m_gaps(
        intraday,
        timezone=config.timezone,
        entry_start=config.entry_start,
        time_exit_at=config.time_exit_at,
    )
    quality["ny_gap_count_gt_60m"] = int(len(gaps))
    summary = build_summary(
        trades=trades,
        signals=signals,
        invalid_sessions=invalid_sessions,
        total_sessions=total_sessions,
        valid_sessions=valid_sessions,
        data_quality=quality,
    )
    paths = write_report_artifacts(
        output_dir=ALPHA_STORE_DIR,
        output_prefix=config.output_prefix,
        summary=summary,
        signals=signals,
        trades=trades,
        equity=equity,
        invalid_sessions=invalid_sessions,
        gaps=gaps,
    )

    print("\nSummary:")
    print(summary.round(4))
    print("\nArtifacts:")
    for name, path in paths.items():
        print(f"{name}: {path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, ValueError) as exc:
        print(f"[FAIL] {exc}")
        raise SystemExit(1)
