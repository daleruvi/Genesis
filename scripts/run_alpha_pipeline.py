import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.alpha.alpha_evaluator import AlphaEvaluator
from genesis.alpha.alpha_generator import AlphaGenerator
from genesis.config.settings import FEATURE_STORE_DIR, ensure_data_dirs


def section(title):
    print(f"\n=== {title} ===")


def main():
    ensure_data_dirs()
    section("Step 1/4 - Load feature dataset")
    df = pd.read_parquet(FEATURE_STORE_DIR / "btc_features_4h.parquet")

    section("Step 2/4 - Generate and save alphas")
    generator = AlphaGenerator(df)
    generator.generate_default_alphas()

    alphas = generator.save().dropna()
    print(f"\nTotal alphas generated: {len(alphas.columns)}")

    section("Step 3/4 - Compute forward returns")
    forward_returns = df["close"].pct_change().shift(-1)
    evaluator = AlphaEvaluator(df, forward_returns)

    section("Step 4/4 - Evaluate alphas")
    results = []
    for alpha_name in alphas.columns:
        alpha = alphas[alpha_name]
        metrics = evaluator.evaluate_alpha(alpha)
        metrics["alpha"] = alpha_name
        results.append(metrics)

    results_df = pd.DataFrame(results)
    print("\nAlpha evaluation:")
    print(results_df.head())


if __name__ == "__main__":
    main()
