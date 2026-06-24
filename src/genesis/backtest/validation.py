from __future__ import annotations

import pandas as pd

from genesis.features.sessions import hhmm_to_minutes


def coverage_ratio_5m(
    intraday: pd.DataFrame,
    start: str = "2025-01-01",
    end: str = "2026-01-01",
) -> dict:
    frame = intraday.copy()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp"], utc=True)
    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC")
    window = frame[(frame["timestamp_utc"] >= start_ts) & (frame["timestamp_utc"] < end_ts)]
    expected = int((end_ts - start_ts) / pd.Timedelta(minutes=5))
    observed = int(window["timestamp_utc"].drop_duplicates().shape[0])
    return {
        "coverage_start": start,
        "coverage_end": end,
        "expected_5m_bars": expected,
        "observed_5m_bars": observed,
        "coverage_ratio_5m": float(observed / expected) if expected else 0.0,
    }


def daily_warmup_status(
    daily: pd.DataFrame,
    first_session_date: str | None,
    warmup_days: int = 14,
    required_start: str = "2024-12-01",
    required_end: str = "2025-12-31",
) -> dict:
    if daily.empty:
        return {
            "daily_start": "",
            "daily_end": "",
            "daily_warmup_ok": False,
            "daily_warmup_bars": 0,
            "daily_required_warmup_bars": warmup_days,
        }
    frame = daily.copy()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["daily_date"] = frame["timestamp_utc"].dt.strftime("%Y-%m-%d")
    daily_start = str(frame["daily_date"].min())
    daily_end = str(frame["daily_date"].max())
    if first_session_date:
        warmup_bars = int((frame["daily_date"] < first_session_date).sum())
    else:
        warmup_bars = 0
    ok = warmup_bars >= warmup_days
    return {
        "daily_start": daily_start,
        "daily_end": daily_end,
        "daily_warmup_ok": bool(ok),
        "daily_warmup_bars": warmup_bars,
        "daily_required_warmup_bars": warmup_days,
    }


def relevant_5m_gaps(
    intraday: pd.DataFrame,
    timezone: str = "America/New_York",
    entry_start: str = "09:45",
    time_exit_at: str = "16:00",
    min_gap_minutes: int = 60,
) -> pd.DataFrame:
    frame = intraday.copy()
    if frame.empty:
        return pd.DataFrame(columns=["gap_start", "gap_end", "gap_minutes", "session_date"])
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.drop_duplicates(subset=["timestamp_utc"]).sort_values("timestamp_utc").reset_index(drop=True)
    frame["timestamp_local"] = frame["timestamp_utc"].dt.tz_convert(timezone)
    frame["session_date"] = frame["timestamp_local"].dt.strftime("%Y-%m-%d")
    frame["local_minutes"] = frame["timestamp_local"].dt.hour * 60 + frame["timestamp_local"].dt.minute
    frame["prev_timestamp_utc"] = frame["timestamp_utc"].shift(1)
    frame["gap_minutes"] = (frame["timestamp_utc"] - frame["prev_timestamp_utc"]).dt.total_seconds() / 60.0

    start_min = hhmm_to_minutes(entry_start)
    end_min = hhmm_to_minutes(time_exit_at)
    mask = (
        (frame["gap_minutes"] > min_gap_minutes)
        & (frame["local_minutes"] >= start_min)
        & (frame["local_minutes"] <= end_min)
    )
    gaps = frame.loc[mask, ["prev_timestamp_utc", "timestamp_utc", "gap_minutes", "session_date"]].copy()
    if gaps.empty:
        return pd.DataFrame(columns=["gap_start", "gap_end", "gap_minutes", "session_date"])
    gaps = gaps.rename(columns={"prev_timestamp_utc": "gap_start", "timestamp_utc": "gap_end"})
    return gaps.reset_index(drop=True)


def equity_regular_session_coverage(
    intraday: pd.DataFrame,
    daily: pd.DataFrame | None = None,
    timezone: str = "America/New_York",
    session_start: str = "09:30",
    session_end: str = "16:00",
    bars_per_full_day: int = 78,
) -> dict:
    if intraday.empty:
        return {
            "coverage_ratio_5m": 0.0,
            "regular_session_coverage_ratio": 0.0,
            "total_regular_sessions_detected": 0,
            "complete_regular_sessions": 0,
            "partial_regular_sessions": 0,
            "expected_regular_bars": 0,
            "observed_regular_bars": 0,
            "regular_session_expected_bars": 0,
            "regular_session_observed_bars": 0,
            "extended_hours_bars_count": 0,
            "coverage_denominator_source": "intraday_detected",
            "coverage_is_provisional": True,
        }
    frame = intraday.copy()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["timestamp_local"] = frame["timestamp_utc"].dt.tz_convert(timezone)
    frame["session_date"] = frame["timestamp_local"].dt.strftime("%Y-%m-%d")
    frame["local_minutes"] = frame["timestamp_local"].dt.hour * 60 + frame["timestamp_local"].dt.minute
    start_min = hhmm_to_minutes(session_start)
    end_min = hhmm_to_minutes(session_end)
    regular = frame[(frame["local_minutes"] >= start_min) & (frame["local_minutes"] < end_min)]
    extended_hours_count = int(len(frame) - len(regular))

    if daily is not None and not daily.empty:
        daily_frame = daily.copy()
        daily_frame["daily_date"] = pd.to_datetime(daily_frame["timestamp"], utc=True).dt.strftime("%Y-%m-%d")
        expected_dates = sorted(set(daily_frame["daily_date"]))
        denominator_source = "daily"
        provisional = False
    else:
        expected_dates = sorted(set(regular["session_date"]))
        denominator_source = "intraday_detected"
        provisional = True
    counts = regular.groupby("session_date")["timestamp_utc"].nunique()
    expected_bars = len(expected_dates) * bars_per_full_day
    observed_bars = int(counts.reindex(expected_dates, fill_value=0).sum())
    partial = int((counts.reindex(expected_dates, fill_value=0) < bars_per_full_day).sum())
    complete = int((counts.reindex(expected_dates, fill_value=0) >= bars_per_full_day).sum())
    ratio = float(observed_bars / expected_bars) if expected_bars else 0.0
    return {
        "coverage_ratio_5m": ratio,
        "regular_session_coverage_ratio": ratio,
        "total_regular_sessions_detected": int(counts[counts > 0].shape[0]),
        "complete_regular_sessions": complete,
        "partial_regular_sessions": partial,
        "expected_regular_bars": int(expected_bars),
        "observed_regular_bars": int(observed_bars),
        "regular_session_expected_bars": int(expected_bars),
        "regular_session_observed_bars": int(observed_bars),
        "extended_hours_bars_count": extended_hours_count,
        "coverage_denominator_source": denominator_source,
        "coverage_is_provisional": provisional,
    }
