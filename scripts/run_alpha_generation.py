import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.alpha.alpha_generator import AlphaGenerator
from genesis.config.settings import FEATURE_STORE_DIR, ensure_data_dirs


def section(title):
    print(f"\n=== {title} ===")


def main():
    ensure_data_dirs()
    section("Step 1/3 - Load feature dataset")
    df = pd.read_parquet(FEATURE_STORE_DIR / "btc_features_4h.parquet")

    section("Step 2/3 - Generate default alpha set")
    generator = AlphaGenerator(df)
    generator.generate_default_alphas()

    section("Step 3/3 - Save and preview alpha dataset")
    alphas = generator.save()
    print(alphas.head())
    print(f"\nTotal alphas generated: {len(alphas.columns)}")


if __name__ == "__main__":
    main()
