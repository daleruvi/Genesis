from __future__ import annotations

import numpy as np
import pandas as pd

from genesis.features.sessions import SessionSpec, assign_synthetic_session, hhmm_to_minutes


REQUIRED_OHLCV_COLUMNS = {"timestamp", "open", "high", "low", "close", "volume"}


def validate_ohlcv(df: pd.DataFrame, name: str = "dataset") -> None:
    missing = REQUIRED_OHLCV_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"{name} missing OHLCV columns: {sorted(missing)}")


def calculate_daily_atr(daily_df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    validate_ohlcv(daily_df, "daily dataset")
    frame = daily_df.copy()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.sort_values("timestamp_utc").reset_index(drop=True)
    frame["daily_date"] = frame["timestamp_utc"].dt.strftime("%Y-%m-%d")

    prev_close = frame["close"].astype(float).shift(1)
    tr = pd.concat(
        [
            frame["high"].astype(float) - frame["low"].astype(float),
            (frame["high"].astype(float) - prev_close).abs(),
            (frame["low"].astype(float) - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    frame["atr_raw"] = tr.rolling(period).mean()
    frame["daily_atr"] = frame["atr_raw"].shift(1)
    frame["daily_close"] = frame["close"].astype(float).shift(1)
    frame["daily_atr_pct"] = frame["daily_atr"] / frame["daily_close"]
    frame["atr_observation_count"] = frame["atr_raw"].rolling(1).count().cumsum()
    return frame[["daily_date", "daily_atr", "daily_close", "daily_atr_pct", "atr_observation_count"]]


def add_intraday_ema(df: pd.DataFrame, period: int = 50) -> pd.DataFrame:
    frame = df.copy()
    frame[f"ema_{period}"] = frame["close"].astype(float).ewm(span=period, adjust=False).mean()
    return frame


def build_opening_range_dataset(
    intraday_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    session_spec: SessionSpec,
    opening_range_minutes: int = 15,
    daily_atr_period: int = 14,
    min_or_size_vs_atr: float = 0.05,
    max_or_size_vs_atr: float = 0.60,
    min_daily_atr_pct: float = 0.015,
    ema_period: int = 50,
    gap_filter_enabled: bool = False,
    min_abs_gap_pct: float | None = None,
    max_abs_gap_pct: float | None = None,
    opening_volume_filter_enabled: bool = False,
    min_opening_volume_vs_avg: float | None = None,
    opening_volume_avg_sessions: int = 20,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    validate_ohlcv(intraday_df, "intraday dataset")
    frame = assign_synthetic_session(intraday_df, session_spec)
    frame = add_intraday_ema(frame, ema_period)
    daily_atr = calculate_daily_atr(daily_df, period=daily_atr_period).set_index("daily_date")

    session_start_minute = hhmm_to_minutes(session_spec.session_start)
    or_end_minute = session_start_minute + int(opening_range_minutes)
    valid_rows = []
    invalid_rows = []
    opening_volume_history: list[float] = []

    for session_id, group in frame.groupby("session_id", sort=True):
        group = group.sort_values("timestamp_utc").copy()
        session_date = str(group["session_date"].iloc[0])
        or_bars = group[
            (group["local_minutes"] >= session_start_minute)
            & (group["local_minutes"] < or_end_minute)
        ]

        invalid_reason = None
        if len(or_bars) < max(1, opening_range_minutes // 5):
            invalid_reason = "insufficient_opening_range_bars"
        elif session_date not in daily_atr.index:
            invalid_reason = "missing_daily_atr"
        else:
            daily_row = daily_atr.loc[session_date]
            if not np.isfinite(float(daily_row["daily_atr"])):
                invalid_reason = "insufficient_daily_atr_warmup"
            elif not np.isfinite(float(daily_row["daily_close"])) or float(daily_row["daily_close"]) <= 0:
                invalid_reason = "missing_daily_close"
            elif not np.isfinite(float(daily_row["daily_atr_pct"])):
                invalid_reason = "missing_daily_atr_pct"

        if invalid_reason is None:
            or_high = float(or_bars["high"].max())
            or_low = float(or_bars["low"].min())
            or_size = or_high - or_low
            session_open = float(or_bars["open"].iloc[0])
            opening_range_volume = float(or_bars["volume"].astype(float).sum())
            daily_row = daily_atr.loc[session_date]
            daily_atr_value = float(daily_row["daily_atr"])
            daily_close = float(daily_row["daily_close"])
            daily_atr_pct = float(daily_row["daily_atr_pct"])
            or_size_vs_daily_atr = or_size / daily_atr_value if daily_atr_value > 0 else np.nan
            gap_pct = (session_open - daily_close) / daily_close if daily_close > 0 else np.nan
            volume_window = opening_volume_history[-int(opening_volume_avg_sessions) :]
            opening_volume_avg = float(np.mean(volume_window)) if volume_window else np.nan
            opening_volume_vs_avg = (
                opening_range_volume / opening_volume_avg
                if np.isfinite(opening_volume_avg) and opening_volume_avg > 0
                else np.nan
            )

            if or_size <= 0:
                invalid_reason = "invalid_opening_range_size"
            elif or_size_vs_daily_atr < min_or_size_vs_atr:
                invalid_reason = "or_size_below_min"
            elif or_size_vs_daily_atr > max_or_size_vs_atr:
                invalid_reason = "or_size_above_max"
            elif daily_atr_pct < min_daily_atr_pct:
                invalid_reason = "daily_atr_pct_below_min"
            elif gap_filter_enabled and not np.isfinite(gap_pct):
                invalid_reason = "missing_gap_pct"
            elif gap_filter_enabled and min_abs_gap_pct is not None and abs(gap_pct) < float(min_abs_gap_pct):
                invalid_reason = "gap_pct_below_min"
            elif gap_filter_enabled and max_abs_gap_pct is not None and abs(gap_pct) > float(max_abs_gap_pct):
                invalid_reason = "gap_pct_above_max"
            elif opening_volume_filter_enabled and not np.isfinite(opening_volume_vs_avg):
                invalid_reason = "insufficient_opening_volume_history"
            elif (
                opening_volume_filter_enabled
                and min_opening_volume_vs_avg is not None
                and opening_volume_vs_avg < float(min_opening_volume_vs_avg)
            ):
                invalid_reason = "opening_volume_vs_avg_below_min"

            opening_volume_history.append(opening_range_volume)

        if invalid_reason is not None:
            invalid_rows.append(
                {
                    "session_id": session_id,
                    "session": session_spec.name,
                    "session_date": session_date,
                    "invalid_reason": invalid_reason,
                    "or_bar_count": int(len(or_bars)),
                }
            )
            continue

        session_frame = group.copy()
        session_frame["or_high"] = or_high
        session_frame["or_low"] = or_low
        session_frame["or_mid"] = (or_high + or_low) / 2.0
        session_frame["or_size"] = or_size
        session_frame["daily_atr"] = daily_atr_value
        session_frame["daily_close"] = daily_close
        session_frame["daily_atr_pct"] = daily_atr_pct
        session_frame["or_size_vs_daily_atr"] = or_size_vs_daily_atr
        session_frame["or_size_vs_atr"] = or_size_vs_daily_atr
        session_frame["session_open"] = session_open
        session_frame["gap_pct"] = gap_pct
        session_frame["opening_range_volume"] = opening_range_volume
        session_frame["opening_volume_vs_avg"] = opening_volume_vs_avg
        session_frame["opening_range_end_minute"] = or_end_minute
        session_frame["breakout_above"] = (
            (session_frame["local_minutes"] >= or_end_minute)
            & (session_frame["high"].astype(float) > or_high)
        )
        session_frame["breakout_below"] = (
            (session_frame["local_minutes"] >= or_end_minute)
            & (session_frame["low"].astype(float) < or_low)
        )
        valid_rows.append(session_frame)

    valid = pd.concat(valid_rows, ignore_index=True) if valid_rows else pd.DataFrame()
    invalid = pd.DataFrame(invalid_rows)
    return valid, invalid
