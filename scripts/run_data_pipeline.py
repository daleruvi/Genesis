import argparse
import re

import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.config.settings import (
    DATA_PIPELINE_CHUNK_LIMIT,
    DATA_PIPELINE_END_DATE,
    DATA_PIPELINE_LIMIT,
    DATA_PIPELINE_OUTPUT_NAME,
    DATA_PIPELINE_PARTITION,
    DATA_PIPELINE_START_DATE,
    DATA_PIPELINE_SYMBOL,
    DATA_PIPELINE_TIMEFRAMES,
    RAW_DATA_DIR,
    ensure_data_dirs,
)
from genesis.data.dataset_builder import DatasetBuilder
from genesis.data.market_data_loader import MarketDataLoader


def section(title):
    print(f"\n=== {title} ===")


def warn(msg: str):
    print(f"[WARN] {msg}")


def parse_args():
    parser = argparse.ArgumentParser(description="Run raw data pipeline with optional historical range pagination.")
    parser.add_argument("--symbol", default=DATA_PIPELINE_SYMBOL, help="Market symbol (for example BTC/USDT).")
    parser.add_argument(
        "--timeframes",
        nargs="+",
        default=[DATA_PIPELINE_TIMEFRAMES],
        help="Timeframes as space-separated values or a quoted comma list (for example '1d,1h,5m').",
    )
    parser.add_argument("--limit", type=int, default=DATA_PIPELINE_LIMIT, help="Single-call limit when start_date is empty.")
    parser.add_argument(
        "--start-date",
        default=DATA_PIPELINE_START_DATE,
        help="Inclusive UTC start date (for example 2025-01-01). Enables paginated range mode.",
    )
    parser.add_argument("--end-date", default=DATA_PIPELINE_END_DATE, help="Exclusive UTC end date (for example 2026-01-01).")
    parser.add_argument("--chunk-limit", type=int, default=DATA_PIPELINE_CHUNK_LIMIT, help="Per-request chunk size in range mode.")
    parser.add_argument(
        "--partition",
        choices=["none", "monthly"],
        default=DATA_PIPELINE_PARTITION,
        help="Partition range downloads to improve exchange compatibility.",
    )
    parser.add_argument(
        "--combine-partitions",
        action="store_true",
        help="When partitioning, also save one combined dataset per timeframe.",
    )
    parser.add_argument(
        "--save-empty",
        action="store_true",
        help="Save empty datasets too (useful for auditing unavailable windows).",
    )
    parser.add_argument(
        "--output-name",
        default=DATA_PIPELINE_OUTPUT_NAME,
        help="Dataset base name for single timeframe mode (default keeps backward compatibility: btc_4h).",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on first timeframe error. By default, errors are logged and pipeline continues.",
    )
    return parser.parse_args()


def parse_timeframes(values) -> list[str]:
    if isinstance(values, str):
        candidates = [values]
    else:
        candidates = list(values)

    parsed = []
    for item in candidates:
        if not isinstance(item, str):
            raise ValueError(
                "Invalid timeframe argument received from shell. "
                "In PowerShell, wrap timeframes in quotes. "
                "Example: --timeframes \"1d,1h,5m\""
            )
        parts = [part.strip() for part in str(item).split(",") if part.strip()]
        parsed.extend(parts)

    for tf in parsed:
        if not re.fullmatch(r"\d+[mhdw]", tf.lower()):
            raise ValueError(
                f"Invalid timeframe value: {tf}. "
                "Use values like 5m, 1h, 1d and in PowerShell wrap comma lists in quotes."
            )
    return parsed


def sanitize_symbol(symbol: str) -> str:
    safe = symbol.lower().replace("/", "_").replace(":", "_")
    safe = re.sub(r"[^a-z0-9_]+", "_", safe)
    return re.sub(r"_+", "_", safe).strip("_")


def build_dataset_name(symbol: str, timeframe: str, start_date: str, end_date: str) -> str:
    name = f"{sanitize_symbol(symbol)}_{timeframe}"
    if start_date:
        start_tag = pd.to_datetime(start_date).strftime("%Y%m%d")
        end_tag = pd.to_datetime(end_date).strftime("%Y%m%d") if end_date else "latest"
        name = f"{name}_{start_tag}_{end_tag}"
    return name


def monthly_windows(start_date: str, end_date: str) -> list[tuple[pd.Timestamp, pd.Timestamp, str]]:
    if not start_date or not end_date:
        raise ValueError("Monthly partition requires both --start-date and --end-date.")
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    if start_ts >= end_ts:
        raise ValueError(f"Invalid range: start-date ({start_ts}) must be before end-date ({end_ts}).")

    windows = []
    cursor = start_ts
    while cursor < end_ts:
        month_end = (cursor + pd.offsets.MonthBegin(1)).normalize()
        if month_end <= cursor:
            month_end = cursor + pd.offsets.DateOffset(months=1)
        window_end = min(month_end, end_ts)
        label = cursor.strftime("%Y%m")
        windows.append((cursor, window_end, label))
        cursor = window_end
    return windows


def select_dataset_name(args, timeframes: list[str], timeframe: str, start_date: str, end_date: str, suffix: str | None = None) -> str:
    if len(timeframes) == 1:
        if suffix:
            if args.output_name != DATA_PIPELINE_OUTPUT_NAME:
                return f"{args.output_name}_{suffix}"
            return build_dataset_name(args.symbol, timeframe, start_date, end_date)
        dataset_name = args.output_name
        if args.output_name == DATA_PIPELINE_OUTPUT_NAME and args.symbol != "BTC/USDT":
            dataset_name = build_dataset_name(args.symbol, timeframe, start_date, end_date)
        if args.output_name == DATA_PIPELINE_OUTPUT_NAME and timeframe != "4h":
            dataset_name = build_dataset_name(args.symbol, timeframe, start_date, end_date)
        return dataset_name
    return build_dataset_name(args.symbol, timeframe, start_date, end_date)


def report_partial_coverage(df: pd.DataFrame, start_date: str, timeframe: str):
    if not start_date or df.empty:
        return
    requested_start = pd.to_datetime(start_date)
    actual_start = pd.to_datetime(df["timestamp"].min())
    if actual_start > requested_start:
        warn(
            f"{timeframe}: coverage starts at {actual_start} (requested {requested_start}). "
            "Dataset is partial for the requested range."
        )


def main():
    args = parse_args()
    ensure_data_dirs()
    timeframes = parse_timeframes(args.timeframes)
    if not timeframes:
        raise ValueError("At least one timeframe is required.")

    section("Step 1/3 - Load market data from BingX")
    loader = MarketDataLoader()
    builder = DatasetBuilder(RAW_DATA_DIR)
    saved_paths = []

    for timeframe in timeframes:
        if args.start_date and args.partition == "monthly":
            windows = monthly_windows(args.start_date, args.end_date)
            combined_chunks = []
            for window_start, window_end, month_label in windows:
                try:
                    df_month = loader.fetch_ohlcv_range(
                        symbol=args.symbol,
                        timeframe=timeframe,
                        start=str(window_start),
                        end=str(window_end),
                        limit_per_call=args.chunk_limit,
                    )
                except Exception as exc:
                    warn(
                        f"{timeframe} {month_label}: failed to download "
                        f"({type(exc).__name__}) {exc}"
                    )
                    if args.fail_fast:
                        raise
                    continue

                if df_month.empty:
                    warn(
                        f"{timeframe} {month_label}: 0 rows returned for {args.symbol}. "
                        "The exchange may not expose this historical depth for this window."
                    )
                    if not args.save_empty:
                        continue

                dataset_name = select_dataset_name(
                    args,
                    timeframes=timeframes,
                    timeframe=timeframe,
                    start_date=str(window_start),
                    end_date=str(window_end),
                    suffix=month_label,
                )
                dataset_path = builder.save_dataset(df_month, dataset_name)
                saved_paths.append(dataset_path)
                if not df_month.empty:
                    print(
                        f"{timeframe} {month_label}: {len(df_month)} rows | "
                        f"{df_month['timestamp'].min()} -> {df_month['timestamp'].max()} | saved as {dataset_name}"
                    )
                    combined_chunks.append(df_month)

            if args.combine_partitions and combined_chunks:
                combined_df = (
                    pd.concat(combined_chunks, ignore_index=True)
                    .drop_duplicates(subset=["timestamp"])
                    .sort_values("timestamp")
                    .reset_index(drop=True)
                )
                combined_name = select_dataset_name(
                    args,
                    timeframes=timeframes,
                    timeframe=timeframe,
                    start_date=args.start_date,
                    end_date=args.end_date,
                )
                combined_path = builder.save_dataset(combined_df, combined_name)
                saved_paths.append(combined_path)
                print(
                    f"{timeframe} combined: {len(combined_df)} rows | "
                    f"{combined_df['timestamp'].min()} -> {combined_df['timestamp'].max()} | saved as {combined_name}"
                )
                report_partial_coverage(combined_df, args.start_date, timeframe)
            elif args.combine_partitions and not combined_chunks:
                warn(f"{timeframe}: no non-empty partitions to combine.")
        else:
            try:
                if args.start_date:
                    df = loader.fetch_ohlcv_range(
                        symbol=args.symbol,
                        timeframe=timeframe,
                        start=args.start_date,
                        end=args.end_date or None,
                        limit_per_call=args.chunk_limit,
                    )
                else:
                    df = loader.fetch_ohlcv(
                        symbol=args.symbol,
                        timeframe=timeframe,
                        limit=args.limit,
                    )
            except Exception as exc:
                warn(f"{timeframe}: failed to download ({type(exc).__name__}) {exc}")
                if args.fail_fast:
                    raise
                continue

            dataset_name = select_dataset_name(
                args,
                timeframes=timeframes,
                timeframe=timeframe,
                start_date=args.start_date,
                end_date=args.end_date,
            )

            dataset_path = builder.save_dataset(df, dataset_name)
            saved_paths.append(dataset_path)
            if df.empty:
                warn(
                    f"{timeframe}: 0 rows returned for {args.symbol}. "
                    "The exchange may not expose this historical depth for this timeframe."
                )
            else:
                print(f"{timeframe}: {len(df)} rows | {df['timestamp'].min()} -> {df['timestamp'].max()} | saved as {dataset_name}")
                report_partial_coverage(df, args.start_date, timeframe)

    section("Step 3/3 - Preview saved dataset")
    if not saved_paths:
        warn("No datasets were saved.")
        return
    for path in saved_paths:
        saved_df = pd.read_parquet(path)
        print(f"\n{path.name}")
        print(saved_df.head(3))
        print(saved_df.tail(3))


if __name__ == "__main__":
    main()
