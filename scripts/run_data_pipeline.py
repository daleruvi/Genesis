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
from genesis.backtest.validation import coverage_ratio_5m, equity_regular_session_coverage, relevant_5m_gaps
from genesis.data.dataset_builder import DatasetBuilder
from genesis.data.market_data_loader import MarketDataLoader
from genesis.data.providers.binance_vision import BinanceVisionProvider
from genesis.data.providers.polygon import PolygonStocksProvider, sanitize_polygon_url


def section(title):
    print(f"\n=== {title} ===")


def warn(msg: str):
    print(f"[WARN] {msg}")


def parse_args():
    parser = argparse.ArgumentParser(description="Run raw data pipeline with optional historical range pagination.")
    parser.add_argument("--symbol", default=DATA_PIPELINE_SYMBOL, help="Market symbol (for example BTC/USDT).")
    parser.add_argument(
        "--provider",
        choices=["bingx", "binance_vision", "polygon"],
        default="bingx",
        help="Market data provider.",
    )
    parser.add_argument(
        "--market-type",
        choices=["futures_um", "spot", "stocks"],
        default="futures_um",
        help="Provider market type. Used by binance_vision; ignored by bingx.",
    )
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


def build_loader(provider: str, market_type: str):
    if provider == "binance_vision":
        return BinanceVisionProvider(market_type=market_type)
    if provider == "polygon":
        return PolygonStocksProvider(market_type=market_type)
    return MarketDataLoader()


def report_5m_quality(
    df: pd.DataFrame,
    start_date: str,
    end_date: str,
    label: str,
    provider: str = "bingx",
    market_type: str = "futures_um",
):
    if not start_date or not end_date or df.empty:
        return
    if provider == "polygon" and market_type == "stocks":
        quality = equity_regular_session_coverage(df, daily=None)
        ratio = quality["regular_session_coverage_ratio"]
        print(
            f"{label}: regular_session_coverage_ratio={ratio:.4f} "
            f"({quality['observed_regular_bars']}/{quality['expected_regular_bars']} regular-session bars)"
        )
        print(
            f"{label}: total_regular_sessions_detected={quality['total_regular_sessions_detected']} "
            f"complete_regular_sessions={quality['complete_regular_sessions']} "
            f"partial_regular_sessions={quality['partial_regular_sessions']} "
            f"extended_hours_bars_count={quality['extended_hours_bars_count']}"
        )
        if quality["coverage_is_provisional"]:
            warn(
                f"{label}: denominator derived from detected intraday sessions; "
                "fully missing sessions cannot be detected."
            )
    else:
        quality = coverage_ratio_5m(df, start=start_date, end=end_date)
        ratio = quality["coverage_ratio_5m"]
        print(
            f"{label}: coverage_ratio_5m={ratio:.4f} "
            f"({quality['observed_5m_bars']}/{quality['expected_5m_bars']} bars)"
        )
    if ratio < 0.50:
        print(f"[FAIL] {label}: coverage_ratio_5m < 0.50; dataset is insufficient for empirical validation.")
    elif ratio < 0.90:
        warn(f"{label}: coverage_ratio_5m < 0.90; validation decision is capped at continue.")

    gaps = relevant_5m_gaps(df, min_gap_minutes=60)
    if gaps.empty:
        print(f"{label}: no gaps > 60 minutes inside NY validation window.")
    else:
        warn(f"{label}: {len(gaps)} gap(s) > 60 minutes inside NY validation window.")
        print(gaps.head(10).to_string(index=False))


def save_and_report(
    builder: DatasetBuilder,
    df: pd.DataFrame,
    dataset_name: str,
    timeframe: str,
    start_date: str,
    end_date: str,
    provider: str = "bingx",
    market_type: str = "futures_um",
):
    dataset_path = builder.save_dataset(df, dataset_name)
    if not df.empty:
        report_partial_coverage(df, start_date, timeframe)
        if timeframe.lower() == "5m":
            report_5m_quality(df, start_date, end_date, dataset_name, provider=provider, market_type=market_type)
    return dataset_path


def main():
    args = parse_args()
    ensure_data_dirs()
    timeframes = parse_timeframes(args.timeframes)
    if not timeframes:
        raise ValueError("At least one timeframe is required.")

    section(f"Step 1/3 - Load market data from {args.provider}")
    loader = build_loader(args.provider, args.market_type)
    if args.provider == "polygon":
        rate_limit = getattr(loader, "rate_limit", None)
        if rate_limit is not None:
            print(f"Polygon plan detected: {rate_limit.plan}")
            print(f"effective rate limit: {rate_limit.effective_calls_per_minute} calls/min")
            print(f"sleep between requests: {rate_limit.sleep_seconds:g}s")
        print("adjusted=true")
    builder = DatasetBuilder(RAW_DATA_DIR)
    saved_paths = []
    failed_partitions = []

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
                    detail = sanitize_polygon_url(str(exc)) if args.provider == "polygon" else str(exc)
                    warn(
                        f"{timeframe} {month_label}: failed to download "
                        f"({type(exc).__name__}) {detail}"
                    )
                    if args.fail_fast:
                        raise RuntimeError(detail) from None
                    failed_partitions.append(
                        {
                            "timeframe": timeframe,
                            "partition": month_label,
                            "reason": detail,
                        }
                    )
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
                dataset_path = save_and_report(
                    builder,
                    df_month,
                    dataset_name,
                    timeframe,
                    str(window_start),
                    str(window_end),
                    provider=args.provider,
                    market_type=args.market_type,
                )
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
                combined_path = save_and_report(
                    builder,
                    combined_df,
                    combined_name,
                    timeframe,
                    args.start_date,
                    args.end_date,
                    provider=args.provider,
                    market_type=args.market_type,
                )
                saved_paths.append(combined_path)
                print(
                    f"{timeframe} combined: {len(combined_df)} rows | "
                    f"{combined_df['timestamp'].min()} -> {combined_df['timestamp'].max()} | saved as {combined_name}"
                )
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
                detail = sanitize_polygon_url(str(exc)) if args.provider == "polygon" else str(exc)
                warn(f"{timeframe}: failed to download ({type(exc).__name__}) {detail}")
                if args.fail_fast:
                    raise RuntimeError(detail) from None
                continue

            dataset_name = select_dataset_name(
                args,
                timeframes=timeframes,
                timeframe=timeframe,
                start_date=args.start_date,
                end_date=args.end_date,
            )

            dataset_path = save_and_report(
                builder,
                df,
                dataset_name,
                timeframe,
                args.start_date,
                args.end_date,
                provider=args.provider,
                market_type=args.market_type,
            )
            saved_paths.append(dataset_path)
            if df.empty:
                warn(
                    f"{timeframe}: 0 rows returned for {args.symbol}. "
                    "The exchange may not expose this historical depth for this timeframe."
                )
            else:
                print(f"{timeframe}: {len(df)} rows | {df['timestamp'].min()} -> {df['timestamp'].max()} | saved as {dataset_name}")

    section("Step 3/3 - Preview saved dataset")
    if not saved_paths:
        warn("No datasets were saved.")
        return
    if failed_partitions:
        warn(f"failed_partition count: {len(failed_partitions)}")
        for item in failed_partitions:
            warn(f"failed_partition {item['timeframe']} {item['partition']}: {item['reason']}")
    for path in saved_paths:
        saved_df = pd.read_parquet(path)
        print(f"\n{path.name}")
        print(saved_df.head(3))
        print(saved_df.tail(3))


if __name__ == "__main__":
    main()
