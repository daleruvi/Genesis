from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from zoneinfo import ZoneInfo

import pandas as pd


@dataclass(frozen=True)
class SessionSpec:
    name: str = "ny"
    timezone: str = "America/New_York"
    session_start: str = "09:30"

    @property
    def start_time(self) -> time:
        return parse_hhmm(self.session_start)


def parse_hhmm(value: str) -> time:
    parts = str(value).split(":", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid HH:MM value: {value}")
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"Invalid HH:MM value: {value}")
    return time(hour=hour, minute=minute)


def minutes_since_midnight(values: pd.Series) -> pd.Series:
    return values.dt.hour * 60 + values.dt.minute


def hhmm_to_minutes(value: str) -> int:
    parsed = parse_hhmm(value)
    return parsed.hour * 60 + parsed.minute


def normalize_intraday_timestamps(df: pd.DataFrame, timestamp_col: str = "timestamp") -> pd.DataFrame:
    frame = df.copy()
    if timestamp_col not in frame.columns:
        raise ValueError(f"Missing timestamp column: {timestamp_col}")
    frame["timestamp_utc"] = pd.to_datetime(frame[timestamp_col], utc=True)
    return frame.sort_values("timestamp_utc").reset_index(drop=True)


def assign_synthetic_session(df: pd.DataFrame, spec: SessionSpec) -> pd.DataFrame:
    frame = normalize_intraday_timestamps(df)
    tz = ZoneInfo(spec.timezone)
    frame["timestamp_local"] = frame["timestamp_utc"].dt.tz_convert(tz)
    frame["session"] = spec.name
    frame["session_date"] = frame["timestamp_local"].dt.strftime("%Y-%m-%d")
    frame["session_id"] = frame["session"] + "_" + frame["session_date"]
    frame["local_minutes"] = minutes_since_midnight(frame["timestamp_local"])
    return frame
