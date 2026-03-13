import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.config.settings import RAW_DATA_DIR, ensure_data_dirs
from genesis.features.feature_engineering import FeatureEngineer
from genesis.features.feature_store import FeatureStore


def section(title):
    print(f"\n=== {title} ===")


def main():
    ensure_data_dirs()
    section("Step 1/3 - Load raw market dataset")
    df = pd.read_parquet(RAW_DATA_DIR / "btc_4h.parquet")

    section("Step 2/3 - Build engineered features")
    features = (
        FeatureEngineer(df)
        .returns()
        .volatility()
        .momentum()
        .volume_features()
        .build()
    )

    section("Step 3/3 - Save and preview feature dataset")
    store = FeatureStore()
    store.save(features, "btc_features_4h")
    print(features.head())


if __name__ == "__main__":
    main()
