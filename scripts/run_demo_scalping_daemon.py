import os
import time
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.alpha.alpha_ensemble import AlphaEnsemble
from genesis.alpha.alpha_generator import AlphaGenerator
from genesis.config.settings import (
    ALPHA_STORE_DIR,
    DEMO_LOOP_DATA_SYMBOL,
    DEMO_LOOP_OHLCV_LIMIT,
    DEMO_LOOP_SIGNAL_THRESHOLD,
    DEMO_LOOP_TRADE_SYMBOL,
    ENSEMBLE_MAX_TEST_TURNOVER,
    ENSEMBLE_MIN_TEST_PROFIT_FACTOR,
    ENSEMBLE_MIN_TEST_SHARPE,
    RISK_MAX_OPEN_POSITIONS,
    RISK_MAX_POSITION_USDT,
    SCALP_DAEMON_EXECUTE,
    SCALP_DAEMON_HEARTBEAT_FILE,
    SCALP_DAEMON_INTERVAL_SECONDS,
    SCALP_DAEMON_OHLCV_LIMIT,
    SCALP_DAEMON_OUTSIDE_SESSION_MODE,
    SCALP_DAEMON_JOURNAL_FILE,
    SCALP_DAEMON_LOCK_FILE,
    SCALP_DAEMON_MAX_CYCLES,
    SCALP_DAEMON_MAX_DAILY_LOSS_USDT,
    SCALP_DAEMON_MAX_TRADES_PER_DAY,
    SCALP_DAEMON_NOTIONAL_USDT,
    SCALP_DAEMON_STOP_LOSS_PCT,
    SCALP_DAEMON_TAKE_PROFIT_PCT,
    SCALP_DAEMON_TIME_STOP_MINUTES,
    SCALP_DAEMON_SESSION_ENABLED,
    SCALP_DAEMON_SESSION_END,
    SCALP_DAEMON_SESSION_START,
    SCALP_DAEMON_SESSION_TIMEZONE,
    SCALP_DAEMON_TIMEFRAME,
    SCALP_DAEMON_WEEKDAYS_ONLY,
    TRADING_STYLE,
    SIZING_DRAWDOWN_CUTOFF,
    SIZING_DRAWDOWN_FLOOR_SCALE,
    SIZING_MAX_MULTIPLIER,
    SIZING_MIN_CONVICTION_SCALE,
    SIZING_MIN_MULTIPLIER,
    SIZING_TARGET_ANNUAL_VOL,
)
from genesis.data.market_data_loader import MarketDataLoader
from genesis.execution.execution_engine import ExecutionEngine
from genesis.features.feature_engineering import FeatureEngineer
from genesis.llm.regime_detector import RegimeDetector
from genesis.portfolio.position_sizing import PositionSizer
from genesis.portfolio.risk_manager import RiskManager
from genesis.utils.journal import JsonlJournal


def section(title):
    print(f"\n=== {title} ===")


def utc_now():
    return datetime.now(timezone.utc)


def parse_hhmm(value: str) -> tuple[int, int]:
    parts = value.split(":", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid HH:MM value: {value}")
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"Invalid HH:MM value: {value}")
    return hour, minute


def session_status(now_utc: datetime) -> dict:
    if not SCALP_DAEMON_SESSION_ENABLED:
        return {"open": True, "local_time": now_utc.isoformat(), "reason": "session_filter_disabled"}

    tz = ZoneInfo(SCALP_DAEMON_SESSION_TIMEZONE)
    local_now = now_utc.astimezone(tz)
    if SCALP_DAEMON_WEEKDAYS_ONLY and local_now.weekday() >= 5:
        return {"open": False, "local_time": local_now.isoformat(), "reason": "weekend_block"}

    start_hour, start_minute = parse_hhmm(SCALP_DAEMON_SESSION_START)
    end_hour, end_minute = parse_hhmm(SCALP_DAEMON_SESSION_END)
    start_dt = local_now.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    end_dt = local_now.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)

    # Handles normal sessions (start < end) and overnight sessions (start > end).
    if end_dt > start_dt:
        is_open = start_dt <= local_now <= end_dt
    else:
        is_open = local_now >= start_dt or local_now <= end_dt

    return {
        "open": is_open,
        "local_time": local_now.isoformat(),
        "reason": "inside_window" if is_open else "outside_window",
    }


def parse_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def load_research_inputs():
    rankings_path = ALPHA_STORE_DIR / "selected_alpha_rankings.csv"
    regime_path = ALPHA_STORE_DIR / "alpha_regime_performance.csv"
    if not rankings_path.exists() or not regime_path.exists():
        missing = [str(path) for path in (rankings_path, regime_path) if not path.exists()]
        raise FileNotFoundError(
            "Missing alpha research artifacts. Run scripts/run_alpha_research.py and "
            f"scripts/run_regime_analysis.py first. Missing: {missing}"
        )
    return pd.read_csv(rankings_path), pd.read_csv(regime_path)


def acquire_lock(lock_path: Path, stale_hours=18):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    now = utc_now()
    if lock_path.exists():
        mtime = datetime.fromtimestamp(lock_path.stat().st_mtime, tz=timezone.utc)
        if now - mtime < timedelta(hours=stale_hours):
            raise RuntimeError(f"Daemon lock already exists: {lock_path}")
        lock_path.unlink(missing_ok=True)
    payload = f"{os.getpid()}\n{now.isoformat()}\n"
    lock_path.write_text(payload, encoding="utf-8")


def release_lock(lock_path: Path):
    lock_path.unlink(missing_ok=True)


def count_today_opens(journal_path: Path):
    if not journal_path.exists():
        return 0
    today = utc_now().date()
    count = 0
    for line in journal_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        rec_time = pd.to_datetime(row.get("recorded_at"), utc=True, errors="coerce")
        if pd.isna(rec_time) or rec_time.date() != today:
            continue
        actions = row.get("execution_actions") or row.get("actions") or []
        if isinstance(actions, list) and any(action.startswith("open_") for action in actions):
            count += 1
    return count


def usdt_total(balance):
    total = (balance.get("total") or {}).get("USDT")
    return parse_float(total, default=0.0)


def position_pnl_pct(position):
    candidates = [
        position.get("percentage"),
        position.get("unrealizedPnlPct"),
        (position.get("info") or {}).get("unrealizedProfitRate"),
        (position.get("info") or {}).get("unrealizedProfitRatio"),
    ]
    for value in candidates:
        if value is None:
            continue
        pct = parse_float(value, default=None)
        if pct is None:
            continue
        if abs(pct) > 1.0:
            pct = pct / 100.0
        return pct
    return None


def should_force_close(active_positions, opened_at):
    if not active_positions:
        return None
    position = active_positions[0]
    pct = position_pnl_pct(position)
    if pct is not None:
        if pct <= -SCALP_DAEMON_STOP_LOSS_PCT:
            return f"stop_loss_triggered ({pct:.4f})"
        if pct >= SCALP_DAEMON_TAKE_PROFIT_PCT:
            return f"take_profit_triggered ({pct:.4f})"
    if opened_at is not None:
        elapsed = utc_now() - opened_at
        if elapsed.total_seconds() >= SCALP_DAEMON_TIME_STOP_MINUTES * 60:
            return f"time_stop_triggered ({int(elapsed.total_seconds() // 60)} min)"
    return None


def build_signal_and_notional(features, rankings, regime_performance):
    generated_alphas = AlphaGenerator(features).generate_default_alphas()
    alpha_frame = pd.DataFrame(generated_alphas).reindex(features.index)
    latest_alphas = alpha_frame.iloc[-1]

    regime_detector = RegimeDetector()
    detected = regime_detector.detect(features)
    current_regime = detected.iloc[-1]["regime"]

    ensemble = AlphaEnsemble(
        rankings=rankings,
        regime_performance=regime_performance,
        signal_threshold=DEMO_LOOP_SIGNAL_THRESHOLD,
        min_test_sharpe=ENSEMBLE_MIN_TEST_SHARPE,
        min_test_profit_factor=ENSEMBLE_MIN_TEST_PROFIT_FACTOR,
        max_test_turnover=ENSEMBLE_MAX_TEST_TURNOVER,
    )
    decision = ensemble.decide(latest_alphas=latest_alphas, regime=current_regime)

    recent_drawdown = (features["close"].iloc[-20:] / features["close"].iloc[-20:].cummax() - 1.0).min()
    realized_annual_vol = float(features.iloc[-1]["volatility_20"]) * (365 * 6) ** 0.5
    sizer = PositionSizer(
        target_annual_vol=SIZING_TARGET_ANNUAL_VOL,
        min_multiplier=SIZING_MIN_MULTIPLIER,
        max_multiplier=SIZING_MAX_MULTIPLIER,
        min_conviction_scale=SIZING_MIN_CONVICTION_SCALE,
        drawdown_cutoff=SIZING_DRAWDOWN_CUTOFF,
        drawdown_floor_scale=SIZING_DRAWDOWN_FLOOR_SCALE,
    )
    sizing = sizer.size_notional(
        base_notional_usdt=SCALP_DAEMON_NOTIONAL_USDT,
        conviction=decision["conviction"],
        realized_annual_vol=realized_annual_vol,
        recent_drawdown=recent_drawdown,
    )

    return decision, sizing, current_regime


def main():
    section("Scalping daemon boot")
    print(f"Trading style profile: {TRADING_STYLE}")
    print(f"Execute orders: {SCALP_DAEMON_EXECUTE}")
    print(f"Loop interval seconds: {SCALP_DAEMON_INTERVAL_SECONDS}")
    print(f"Data timeframe: {SCALP_DAEMON_TIMEFRAME}")
    print(f"OHLCV limit: {SCALP_DAEMON_OHLCV_LIMIT}")
    print(f"Session filter enabled: {SCALP_DAEMON_SESSION_ENABLED}")
    print(f"Session timezone: {SCALP_DAEMON_SESSION_TIMEZONE}")
    print(f"Session window: {SCALP_DAEMON_SESSION_START} -> {SCALP_DAEMON_SESSION_END}")
    print(f"Weekdays only: {SCALP_DAEMON_WEEKDAYS_ONLY}")
    print(f"Outside session mode: {SCALP_DAEMON_OUTSIDE_SESSION_MODE}")
    print(f"Max cycles: {SCALP_DAEMON_MAX_CYCLES} (0 means infinite)")
    print(f"Ensemble thresholds: sharpe>{ENSEMBLE_MIN_TEST_SHARPE}, pf>{ENSEMBLE_MIN_TEST_PROFIT_FACTOR}, turnover<={ENSEMBLE_MAX_TEST_TURNOVER}")
    print(f"Max daily loss USDT: {SCALP_DAEMON_MAX_DAILY_LOSS_USDT}")
    print(f"Max trades/day: {SCALP_DAEMON_MAX_TRADES_PER_DAY}")
    print(f"Stop loss pct: {SCALP_DAEMON_STOP_LOSS_PCT}")
    print(f"Take profit pct: {SCALP_DAEMON_TAKE_PROFIT_PCT}")
    print(f"Time stop minutes: {SCALP_DAEMON_TIME_STOP_MINUTES}")
    print(f"Lock file: {SCALP_DAEMON_LOCK_FILE}")
    print(f"Heartbeat file: {SCALP_DAEMON_HEARTBEAT_FILE}")
    print(f"Journal file: {SCALP_DAEMON_JOURNAL_FILE}")

    lock_path = Path(SCALP_DAEMON_LOCK_FILE)
    heartbeat = JsonlJournal(SCALP_DAEMON_HEARTBEAT_FILE)
    run_journal = JsonlJournal(SCALP_DAEMON_JOURNAL_FILE)

    acquire_lock(lock_path)
    print("Daemon lock acquired.")

    engine = ExecutionEngine(demo_trading=True)
    risk_manager = RiskManager(
        max_position_usdt=RISK_MAX_POSITION_USDT,
        max_open_positions=RISK_MAX_OPEN_POSITIONS,
    )
    loader = MarketDataLoader()
    rankings, regime_performance = load_research_inputs()

    start_balance = usdt_total(engine.client.fetch_balance())
    trade_count_today = count_today_opens(Path(SCALP_DAEMON_JOURNAL_FILE))
    print(f"Session start balance USDT: {start_balance}")
    print(f"Trades already opened today (from journal): {trade_count_today}")

    position_opened_at = None
    cycle = 0

    try:
        while True:
            cycle += 1
            cycle_time = utc_now()
            section(f"Cycle {cycle} - heartbeat")

            active_positions = engine.get_active_positions(DEMO_LOOP_TRADE_SYMBOL)
            if active_positions and position_opened_at is None:
                position_opened_at = cycle_time

            balance_now = usdt_total(engine.client.fetch_balance())
            session_pnl = balance_now - start_balance
            daily_loss = max(0.0, -session_pnl)

            force_reason = should_force_close(active_positions, position_opened_at)
            kill_switch = daily_loss >= SCALP_DAEMON_MAX_DAILY_LOSS_USDT

            if kill_switch:
                print(f"Kill switch triggered: daily loss {daily_loss:.2f} USDT")
                close_responses = engine.close_positions(DEMO_LOOP_TRADE_SYMBOL) if SCALP_DAEMON_EXECUTE else []
                heartbeat.append(
                    {
                        "event": "kill_switch",
                        "cycle": cycle,
                        "daily_loss_usdt": daily_loss,
                        "session_pnl_usdt": session_pnl,
                        "close_responses": close_responses,
                    }
                )
                break

            if force_reason:
                print(f"Position guard triggered: {force_reason}")
                close_responses = engine.close_positions(DEMO_LOOP_TRADE_SYMBOL) if SCALP_DAEMON_EXECUTE else []
                run_journal.append(
                    {
                        "event": "forced_close",
                        "cycle": cycle,
                        "reason": force_reason,
                        "execute_orders": SCALP_DAEMON_EXECUTE,
                        "close_responses": close_responses,
                    }
                )
                position_opened_at = None
                active_positions = []

            session = session_status(cycle_time)
            if not session["open"]:
                print(
                    {
                        "cycle": cycle,
                        "event": "session_closed",
                        "local_time": session["local_time"],
                        "reason": session["reason"],
                        "mode": SCALP_DAEMON_OUTSIDE_SESSION_MODE,
                    }
                )
                close_responses = []
                if SCALP_DAEMON_OUTSIDE_SESSION_MODE.lower() == "flat" and active_positions:
                    close_responses = engine.close_positions(DEMO_LOOP_TRADE_SYMBOL) if SCALP_DAEMON_EXECUTE else []
                    position_opened_at = None
                heartbeat.append(
                    {
                        "event": "session_closed",
                        "cycle": cycle,
                        "local_time": session["local_time"],
                        "reason": session["reason"],
                        "outside_session_mode": SCALP_DAEMON_OUTSIDE_SESSION_MODE,
                        "close_responses": close_responses,
                    }
                )
                if SCALP_DAEMON_MAX_CYCLES > 0 and cycle >= SCALP_DAEMON_MAX_CYCLES:
                    print("Reached max cycles, stopping daemon.")
                    break
                time.sleep(SCALP_DAEMON_INTERVAL_SECONDS)
                continue

            raw_df = loader.fetch_ohlcv(
                symbol=DEMO_LOOP_DATA_SYMBOL,
                timeframe=SCALP_DAEMON_TIMEFRAME,
                limit=SCALP_DAEMON_OHLCV_LIMIT,
            )
            features = (
                FeatureEngineer(raw_df)
                .returns()
                .volatility()
                .momentum()
                .volume_features()
                .build()
            )
            decision, sizing, regime = build_signal_and_notional(features, rankings, regime_performance)
            signal = decision["signal"]

            if trade_count_today >= SCALP_DAEMON_MAX_TRADES_PER_DAY and signal in ("long", "short"):
                print("Trade limit reached for today, forcing flat signal.")
                signal = "flat"

            # Anti-duplicate guard: after an open, wait one loop window before allowing another open
            # if the exchange state has not propagated yet.
            if (
                signal in ("long", "short")
                and not active_positions
                and position_opened_at is not None
                and (cycle_time - position_opened_at).total_seconds() < (SCALP_DAEMON_INTERVAL_SECONDS * 1.2)
            ):
                print("Open cooldown active, forcing flat signal to avoid duplicate entries.")
                signal = "flat"

            risk_check = risk_manager.can_open_new_position(active_positions, sizing["final_notional_usdt"])
            target_usdt = risk_check["target_usdt"]
            if signal in ("long", "short") and not risk_check["allowed"] and not active_positions:
                signal = "flat"

            plan = engine.reconcile_signal(
                symbol=DEMO_LOOP_TRADE_SYMBOL,
                signal=signal,
                usdt_notional=target_usdt,
                execute=SCALP_DAEMON_EXECUTE,
            )

            actions = plan.get("actions", [])
            if any(action.startswith("open_") for action in actions):
                trade_count_today += 1
                if SCALP_DAEMON_EXECUTE:
                    position_opened_at = cycle_time
            if "close_positions" in actions and not any(action.startswith("open_") for action in actions):
                position_opened_at = None

            print(
                {
                    "cycle": cycle,
                    "signal": signal,
                    "regime": regime,
                    "conviction": round(decision["conviction"], 4),
                    "votes_used": decision["votes_used"],
                    "eligible_alphas": decision["eligible_alphas"],
                    "notional_usdt": round(target_usdt, 2),
                    "session_pnl_usdt": round(session_pnl, 4),
                    "daily_loss_usdt": round(daily_loss, 4),
                    "trade_count_today": trade_count_today,
                    "actions": actions,
                }
            )

            run_journal.append(
                {
                    "event": "cycle",
                    "cycle": cycle,
                    "execute_orders": SCALP_DAEMON_EXECUTE,
                    "signal": signal,
                    "regime": regime,
                    "conviction": decision["conviction"],
                    "votes_used": decision["votes_used"],
                    "eligible_alphas": decision["eligible_alphas"],
                    "sizing": sizing,
                    "risk_check": risk_check,
                    "execution_actions": actions,
                    "plan": plan,
                    "session_pnl_usdt": session_pnl,
                    "daily_loss_usdt": daily_loss,
                    "trade_count_today": trade_count_today,
                }
            )
            heartbeat.append(
                {
                    "event": "heartbeat",
                    "cycle": cycle,
                    "timestamp": cycle_time.isoformat(),
                    "active_positions": len(active_positions),
                    "session_pnl_usdt": session_pnl,
                    "daily_loss_usdt": daily_loss,
                    "trade_count_today": trade_count_today,
                    "signal": signal,
                    "regime": regime,
                }
            )

            if SCALP_DAEMON_MAX_CYCLES > 0 and cycle >= SCALP_DAEMON_MAX_CYCLES:
                print("Reached max cycles, stopping daemon.")
                break

            time.sleep(SCALP_DAEMON_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("Daemon interrupted by user.")
    finally:
        release_lock(lock_path)
        print("Daemon lock released.")


if __name__ == "__main__":
    main()
