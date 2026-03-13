from _bootstrap import bootstrap

bootstrap()

import pandas as pd

from genesis.alpha.alpha_ensemble import AlphaEnsemble
from genesis.alpha.alpha_generator import AlphaGenerator
from genesis.config.settings import (
    DEMO_LOOP_DATA_SYMBOL,
    DEMO_LOOP_EXECUTE,
    DEMO_LOOP_JOURNAL_FILE,
    DEMO_LOOP_NOTIONAL_USDT,
    DEMO_LOOP_OHLCV_LIMIT,
    DEMO_LOOP_SIGNAL_THRESHOLD,
    DEMO_LOOP_TIMEFRAME,
    DEMO_LOOP_TRADE_SYMBOL,
    ENSEMBLE_MAX_TEST_TURNOVER,
    ENSEMBLE_MIN_TEST_PROFIT_FACTOR,
    ENSEMBLE_MIN_TEST_SHARPE,
    SIZING_DRAWDOWN_CUTOFF,
    SIZING_DRAWDOWN_FLOOR_SCALE,
    SIZING_MAX_MULTIPLIER,
    SIZING_MIN_CONVICTION_SCALE,
    SIZING_MIN_MULTIPLIER,
    SIZING_TARGET_ANNUAL_VOL,
    ALPHA_STORE_DIR,
    RISK_MAX_OPEN_POSITIONS,
    RISK_MAX_POSITION_USDT,
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


def main():
    section("Step 1/7 - Demo loop configuration")
    print("Running demo trading loop")
    print(f"Data symbol: {DEMO_LOOP_DATA_SYMBOL}")
    print(f"Trade symbol: {DEMO_LOOP_TRADE_SYMBOL}")
    print(f"Execute orders: {DEMO_LOOP_EXECUTE}")
    print(f"Requested notional USDT: {DEMO_LOOP_NOTIONAL_USDT}")
    print(f"Data timeframe: {DEMO_LOOP_TIMEFRAME}")
    print(f"Signal threshold: {DEMO_LOOP_SIGNAL_THRESHOLD}")
    print(f"Ensemble thresholds: sharpe>{ENSEMBLE_MIN_TEST_SHARPE}, pf>{ENSEMBLE_MIN_TEST_PROFIT_FACTOR}, turnover<={ENSEMBLE_MAX_TEST_TURNOVER}")
    print(f"Journal file: {DEMO_LOOP_JOURNAL_FILE}")
    print(f"Sizing target annual vol: {SIZING_TARGET_ANNUAL_VOL}")

    section("Step 2/7 - Load market data and build features")
    loader = MarketDataLoader()
    raw_df = loader.fetch_ohlcv(
        symbol=DEMO_LOOP_DATA_SYMBOL,
        timeframe=DEMO_LOOP_TIMEFRAME,
        limit=DEMO_LOOP_OHLCV_LIMIT,
    )

    features = (
        FeatureEngineer(raw_df)
        .returns()
        .volatility()
        .momentum()
        .volume_features()
        .build()
    )

    section("Step 3/7 - Load research artifacts and build alpha snapshot")
    rankings, regime_performance = load_research_inputs()
    generated_alphas = AlphaGenerator(features).generate_default_alphas()
    alpha_frame = pd.DataFrame(generated_alphas).reindex(features.index)
    latest_alphas = alpha_frame.iloc[-1]
    section("Step 4/7 - Detect current regime and compute ensemble signal")
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
    signal = decision["signal"]
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
        base_notional_usdt=DEMO_LOOP_NOTIONAL_USDT,
        conviction=decision["conviction"],
        realized_annual_vol=realized_annual_vol,
        recent_drawdown=recent_drawdown,
    )

    print(f"Current regime: {current_regime}")
    print(f"Generated signal: {signal}")
    print(
        {
            "conviction": round(decision["conviction"], 4),
            "votes_used": decision["votes_used"],
            "used_fallback": decision["used_fallback"],
            "eligible_alphas": decision["eligible_alphas"],
            "alphas_used": [vote["alpha"] for vote in decision["votes"]],
        }
    )
    print(
        {
            "realized_annual_vol": round(realized_annual_vol, 4),
            "recent_drawdown": round(float(recent_drawdown), 4),
            "final_notional_usdt": round(sizing["final_notional_usdt"], 2),
            "volatility_multiplier": round(sizing["volatility_multiplier"], 3),
            "conviction_multiplier": round(sizing["conviction_multiplier"], 3),
            "drawdown_multiplier": round(sizing["drawdown_multiplier"], 3),
        }
    )
    print(features.tail(3))

    section("Step 5/7 - Apply risk checks and position sizing")
    engine = ExecutionEngine(demo_trading=True)
    risk_manager = RiskManager(
        max_position_usdt=RISK_MAX_POSITION_USDT,
        max_open_positions=RISK_MAX_OPEN_POSITIONS,
    )

    active_positions = engine.get_active_positions(DEMO_LOOP_TRADE_SYMBOL)
    risk_check = risk_manager.can_open_new_position(active_positions, sizing["final_notional_usdt"])
    target_usdt = risk_check["target_usdt"]

    print(f"Active positions: {risk_check['active_positions']}")
    print(f"Risk target USDT: {target_usdt}")

    if signal in ("long", "short") and not risk_check["allowed"] and not active_positions:
        print("Risk manager blocked opening a new position.")
        return

    section("Step 6/7 - Build execution plan (and execute if enabled)")
    plan = engine.reconcile_signal(
        symbol=DEMO_LOOP_TRADE_SYMBOL,
        signal=signal,
        usdt_notional=target_usdt,
        execute=DEMO_LOOP_EXECUTE,
    )

    print("Execution plan:")
    print(plan)

    section("Step 7/7 - Persist run journal")
    journal = JsonlJournal(DEMO_LOOP_JOURNAL_FILE)
    journal_path = journal.append(
        {
            "loop_type": "demo_trading_loop",
            "data_symbol": DEMO_LOOP_DATA_SYMBOL,
            "trade_symbol": DEMO_LOOP_TRADE_SYMBOL,
            "execute_orders": DEMO_LOOP_EXECUTE,
            "notional_usdt": DEMO_LOOP_NOTIONAL_USDT,
            "signal_threshold": DEMO_LOOP_SIGNAL_THRESHOLD,
            "current_regime": current_regime,
            "signal": signal,
            "conviction": round(decision["conviction"], 6),
            "votes_used": decision["votes_used"],
            "used_fallback": decision["used_fallback"],
            "votes": decision["votes"],
            "latest_close": float(features.iloc[-1]["close"]),
            "latest_timestamp": str(features.iloc[-1]["timestamp"]),
            "sizing": sizing,
            "risk_check": risk_check,
            "execution_plan": plan,
        }
    )
    print(f"Journal updated: {journal_path}")


if __name__ == "__main__":
    main()
