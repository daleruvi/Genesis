import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.alpha.alpha_research import AlphaResearch
from genesis.alpha.alpha_selection import AlphaSelector
from genesis.config.settings import ALPHA_STORE_DIR, FEATURE_STORE_DIR, ensure_data_dirs


def section(title):
    print(f"\n=== {title} ===")


def main():
    ensure_data_dirs()
    section("Step 1/5 - Load features and generated alphas")
    features = pd.read_parquet(FEATURE_STORE_DIR / "btc_features_4h.parquet")
    alphas = pd.read_parquet(ALPHA_STORE_DIR / "generated_alphas.parquet")

    alpha_frame = alphas.dropna().copy()
    close = features.loc[alpha_frame.index, "close"]

    section("Step 2/5 - Rank alphas with train/test + temporal stability")
    research = AlphaResearch(close=close)
    rankings = research.evaluate_alphas(alpha_frame, signal_mode="quantile", lower_q=0.2, upper_q=0.8)

    section("Step 3/5 - Select diversified alphas and correlations")
    selector = AlphaSelector(max_correlation=0.8, top_n=5)
    selected = selector.select(rankings, alpha_frame)
    correlation = selector.correlation_matrix(alpha_frame)
    selected_correlation = selector.selected_correlation_matrix(selected, alpha_frame)
    stability_rows = []
    section("Step 4/5 - Build temporal stability windows for top alphas")
    for alpha_name in rankings["alpha"].head(10):
        windows = research.temporal_stability(
            alpha=alpha_frame[alpha_name],
            signal_mode="quantile",
            lower_q=0.2,
            upper_q=0.8,
        )
        if windows.empty:
            continue
        windows.insert(0, "alpha", alpha_name)
        stability_rows.append(windows)
    stability_report = pd.concat(stability_rows, ignore_index=True) if stability_rows else pd.DataFrame()

    rankings_path = ALPHA_STORE_DIR / "alpha_rankings.csv"
    correlation_path = ALPHA_STORE_DIR / "alpha_correlation.csv"
    selected_path = ALPHA_STORE_DIR / "selected_alpha_rankings.csv"
    selected_correlation_path = ALPHA_STORE_DIR / "selected_alpha_correlation.csv"
    selected_alphas_path = ALPHA_STORE_DIR / "selected_alphas.parquet"
    stability_path = ALPHA_STORE_DIR / "alpha_temporal_stability.csv"

    section("Step 5/5 - Save research artifacts and print summary")
    rankings.to_csv(rankings_path, index=False)
    correlation.to_csv(correlation_path, index=True)
    selected.to_csv(selected_path, index=False)
    selected_correlation.to_csv(selected_correlation_path, index=True)
    if not stability_report.empty:
        stability_report.to_csv(stability_path, index=False)

    if not selected.empty:
        selected_names = selected["alpha"].tolist()
        alpha_frame[selected_names].to_parquet(selected_alphas_path, engine="pyarrow")

    print("Top ranked alphas:")
    print(rankings.loc[:, ["alpha", "score", "test_sharpe", "test_ic_abs", "test_total_return"]].head(10))
    print("\nSelected diversified alphas:")
    print(selected.loc[:, ["selection_rank", "alpha", "score", "test_sharpe", "test_ic_abs"]])
    print(f"\nrankings saved -> {rankings_path}")
    print(f"correlation saved -> {correlation_path}")
    print(f"selected rankings saved -> {selected_path}")
    print(f"selected correlation saved -> {selected_correlation_path}")
    if not stability_report.empty:
        print(f"temporal stability saved -> {stability_path}")
    if not selected.empty:
        print(f"selected alphas saved -> {selected_alphas_path}")


if __name__ == "__main__":
    main()
