import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.alpha.alpha_research import AlphaResearch
from genesis.backtest.engine import BacktestEngine
from genesis.config.settings import ALPHA_STORE_DIR, FEATURE_STORE_DIR, ensure_data_dirs
from genesis.llm.regime_detector import RegimeDetector


def section(title):
    print(f"\n=== {title} ===")


def main():
    ensure_data_dirs()
    section("Step 1/4 - Load selected alphas and features")
    features = pd.read_parquet(FEATURE_STORE_DIR / "btc_features_4h.parquet")
    rankings = pd.read_csv(ALPHA_STORE_DIR / "selected_alpha_rankings.csv")
    alphas = pd.read_parquet(ALPHA_STORE_DIR / "selected_alphas.parquet")

    alpha_frame = alphas.dropna().copy()
    features = features.loc[alpha_frame.index].copy()
    close = features["close"]

    section("Step 2/4 - Detect market regimes")
    detector = RegimeDetector()
    detected = detector.detect(features)
    distribution = detector.regime_distribution(features)

    section("Step 3/4 - Evaluate alpha performance by regime")
    research = AlphaResearch(close=close)
    backtest_engine = BacktestEngine()

    regime_rows: list[dict] = []
    for alpha_name in rankings["alpha"].tolist():
        if alpha_name not in alpha_frame.columns:
            continue

        positions = research._positions_from_alpha(
            alpha=alpha_frame[alpha_name],
            signal_mode="quantile",
            lower_q=0.2,
            upper_q=0.8,
        )
        backtest = backtest_engine.run(close, positions)
        regime_summary = detector.performance_by_regime(
            returns=backtest["strategy_returns"],
            regimes=detected["regime"],
            turnover=backtest["turnover"],
        )
        regime_summary.insert(0, "alpha", alpha_name)
        regime_rows.append(regime_summary)

    regime_performance = (
        pd.concat(regime_rows, ignore_index=True)
        if regime_rows
        else pd.DataFrame(columns=["alpha", "regime"])
    )

    distribution_path = ALPHA_STORE_DIR / "regime_distribution.csv"
    performance_path = ALPHA_STORE_DIR / "alpha_regime_performance.csv"
    detected_path = ALPHA_STORE_DIR / "detected_regimes.parquet"

    section("Step 4/4 - Save regime artifacts and print summary")
    distribution.to_csv(distribution_path, index=False)
    regime_performance.to_csv(performance_path, index=False)
    detected.to_parquet(detected_path, engine="pyarrow")

    print("Regime distribution:")
    print(distribution)
    print("\nAlpha performance by regime:")
    print(regime_performance.head(20))
    print(f"\nregime distribution saved -> {distribution_path}")
    print(f"regime performance saved -> {performance_path}")
    print(f"detected regimes saved -> {detected_path}")


if __name__ == "__main__":
    main()
