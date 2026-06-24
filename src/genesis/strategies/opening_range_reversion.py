from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from genesis.features.opening_range import build_opening_range_dataset
from genesis.features.sessions import SessionSpec, hhmm_to_minutes


@dataclass(frozen=True)
class OpeningRangeReversionConfig:
    session: str = "ny"
    timezone: str = "America/New_York"
    session_start: str = "09:30"
    opening_range_minutes: int = 15
    entry_start: str = "09:45"
    entry_end: str = "11:30"
    time_exit_at: str = "16:00"
    atr_mode: str = "daily"
    daily_atr_period: int = 14
    atr_buffer_mult: float = 0.25
    min_or_size_vs_atr: float = 0.05
    max_or_size_vs_atr: float = 0.60
    min_daily_atr_pct: float = 0.015
    gap_filter_enabled: bool = False
    min_abs_gap_pct: float | None = None
    max_abs_gap_pct: float | None = None
    opening_volume_filter_enabled: bool = False
    min_opening_volume_vs_avg: float | None = None
    opening_volume_avg_sessions: int = 20
    exclude_earnings_days: bool = False
    trend_filter: str = "ema"
    ema_period: int = 50
    fee_rate: float = 0.0005
    slippage_rate: float = 0.0005
    initial_equity: float = 10000.0
    symbol: str = "BTCUSDT"
    data_adjusted: bool = False
    decision_profile: str = "demo_gate"
    dataset_path: str = "data/raw/btc_usdt_usdt_5m_2025.parquet"
    daily_dataset_path: str = "data/raw/btc_usdt_usdt_1d_20241201_20260101.parquet"
    output_prefix: str = "opening_range_reversion_ny"

    @classmethod
    def from_mapping(cls, values: dict[str, Any]) -> "OpeningRangeReversionConfig":
        values = dict(values)
        if "min_or_size_vs_daily_atr" in values and "min_or_size_vs_atr" not in values:
            values["min_or_size_vs_atr"] = values["min_or_size_vs_daily_atr"]
        if "max_or_size_vs_daily_atr" in values and "max_or_size_vs_atr" not in values:
            values["max_or_size_vs_atr"] = values["max_or_size_vs_daily_atr"]
        allowed = set(cls.__dataclass_fields__.keys())
        filtered = {key: value for key, value in values.items() if key in allowed}
        config = cls(**filtered)
        config.validate()
        return config

    def validate(self) -> None:
        if self.atr_mode != "daily":
            raise ValueError("Opening Range Reversion Fase 2 only supports atr_mode=daily.")
        if self.trend_filter not in {"ema", "none"}:
            raise ValueError("trend_filter must be one of: ema, none.")
        if self.exclude_earnings_days:
            raise ValueError("exclude_earnings_days=true requires an earnings calendar, not available in Fase 2.8.")
        if self.decision_profile not in {"demo_gate", "market_fit"}:
            raise ValueError("decision_profile must be one of: demo_gate, market_fit.")

    @property
    def session_spec(self) -> SessionSpec:
        return SessionSpec(name=self.session, timezone=self.timezone, session_start=self.session_start)


def load_strategy_config(path: Path | str, overrides: dict[str, Any] | None = None) -> OpeningRangeReversionConfig:
    config_path = Path(path)
    values = yaml.safe_load(config_path.read_text(encoding="utf-8")) if config_path.exists() else {}
    if values is None:
        values = {}
    if overrides:
        values.update({key: value for key, value in overrides.items() if value is not None})
    return OpeningRangeReversionConfig.from_mapping(values)


def build_strategy_frame(
    intraday_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    config: OpeningRangeReversionConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    config.validate()
    return build_opening_range_dataset(
        intraday_df=intraday_df,
        daily_df=daily_df,
        session_spec=config.session_spec,
        opening_range_minutes=config.opening_range_minutes,
        daily_atr_period=config.daily_atr_period,
        min_or_size_vs_atr=config.min_or_size_vs_atr,
        max_or_size_vs_atr=config.max_or_size_vs_atr,
        min_daily_atr_pct=config.min_daily_atr_pct,
        ema_period=config.ema_period,
        gap_filter_enabled=config.gap_filter_enabled,
        min_abs_gap_pct=config.min_abs_gap_pct,
        max_abs_gap_pct=config.max_abs_gap_pct,
        opening_volume_filter_enabled=config.opening_volume_filter_enabled,
        min_opening_volume_vs_avg=config.min_opening_volume_vs_avg,
        opening_volume_avg_sessions=config.opening_volume_avg_sessions,
    )


def _passes_trend_filter(row: pd.Series, side: str, config: OpeningRangeReversionConfig) -> bool:
    if config.trend_filter == "none":
        return True
    ema_value = row.get(f"ema_{config.ema_period}")
    if pd.isna(ema_value):
        return False
    if side == "long":
        return float(row["close"]) > float(ema_value)
    return float(row["close"]) < float(ema_value)


def _signal_payload(
    group: pd.DataFrame,
    row_idx: int,
    side: str,
    breakout_time,
    reentry_time,
    config: OpeningRangeReversionConfig,
) -> dict:
    confirmation = group.iloc[row_idx]
    if row_idx + 1 >= len(group):
        return {
            "session_id": confirmation["session_id"],
            "session": confirmation["session"],
            "session_date": confirmation["session_date"],
            "breakout_time": breakout_time,
            "breakout_side": "above" if side == "short" else "below",
            "reentry_time": reentry_time,
            "confirmation_time": confirmation["timestamp_utc"],
            "invalid_signal_reason": "no_next_entry_bar",
        }

    entry_row = group.iloc[row_idx + 1]
    stop = (
        float(confirmation["or_low"]) - float(confirmation["daily_atr"]) * config.atr_buffer_mult
        if side == "long"
        else float(confirmation["or_high"]) + float(confirmation["daily_atr"]) * config.atr_buffer_mult
    )
    return {
        "session_id": confirmation["session_id"],
        "session": confirmation["session"],
        "session_date": confirmation["session_date"],
        "side": side,
        "breakout_time": breakout_time,
        "breakout_side": "above" if side == "short" else "below",
        "reentry_time": reentry_time,
        "confirmation_time": confirmation["timestamp_utc"],
        "entry_time": entry_row["timestamp_utc"],
        "entry_price": float(entry_row["open"]),
        "entry_reason": "breakout_reentry_v1",
        "stop": stop,
        "tp_midpoint": float(confirmation["or_mid"]),
        "tp_opposite_extreme": float(confirmation["or_high"] if side == "long" else confirmation["or_low"]),
        "or_high": float(confirmation["or_high"]),
        "or_low": float(confirmation["or_low"]),
        "or_mid": float(confirmation["or_mid"]),
        "or_size": float(confirmation["or_size"]),
        "daily_atr": float(confirmation["daily_atr"]),
        "daily_atr_pct": float(confirmation["daily_atr_pct"]),
        "or_size_vs_daily_atr": float(confirmation["or_size_vs_daily_atr"]),
        "or_size_vs_atr": float(confirmation["or_size_vs_daily_atr"]),
        "gap_pct": float(confirmation.get("gap_pct", 0.0)),
        "opening_range_volume": float(confirmation.get("opening_range_volume", 0.0)),
        "opening_volume_vs_avg": float(confirmation.get("opening_volume_vs_avg", 0.0)),
        "invalid_signal_reason": "",
    }


def generate_opening_range_signals(
    strategy_frame: pd.DataFrame,
    config: OpeningRangeReversionConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if strategy_frame.empty:
        return pd.DataFrame(), pd.DataFrame()

    entry_start = hhmm_to_minutes(config.entry_start)
    entry_end = hhmm_to_minutes(config.entry_end)
    signals: list[dict] = []
    invalid_signals: list[dict] = []

    for _, group in strategy_frame.groupby("session_id", sort=True):
        group = group.sort_values("timestamp_utc").reset_index(drop=True)
        broke_above = False
        broke_below = False
        breakout_above_time = None
        breakout_below_time = None
        entered = False

        for idx, row in group.iterrows():
            minute = int(row["local_minutes"])
            if minute < entry_start:
                if bool(row.get("breakout_above")) and not broke_above:
                    broke_above = True
                    breakout_above_time = row["timestamp_utc"]
                if bool(row.get("breakout_below")) and not broke_below:
                    broke_below = True
                    breakout_below_time = row["timestamp_utc"]
                continue
            if minute > entry_end or entered:
                break

            if bool(row.get("breakout_above")) and not broke_above:
                broke_above = True
                breakout_above_time = row["timestamp_utc"]
            if bool(row.get("breakout_below")) and not broke_below:
                broke_below = True
                breakout_below_time = row["timestamp_utc"]

            short_reentry = broke_above and float(row["close"]) < float(row["or_high"])
            short_confirm = short_reentry and float(row["close"]) < float(row["open"])
            if short_confirm and _passes_trend_filter(row, "short", config):
                payload = _signal_payload(group, idx, "short", breakout_above_time, row["timestamp_utc"], config)
                if payload.get("invalid_signal_reason"):
                    invalid_signals.append(payload)
                else:
                    signals.append(payload)
                entered = True
                break

            long_reentry = broke_below and float(row["close"]) > float(row["or_low"])
            long_confirm = long_reentry and float(row["close"]) > float(row["open"])
            if long_confirm and _passes_trend_filter(row, "long", config):
                payload = _signal_payload(group, idx, "long", breakout_below_time, row["timestamp_utc"], config)
                if payload.get("invalid_signal_reason"):
                    invalid_signals.append(payload)
                else:
                    signals.append(payload)
                entered = True
                break

    return pd.DataFrame(signals), pd.DataFrame(invalid_signals)
