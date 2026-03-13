from pathlib import Path

import pandas as pd

from genesis.config.settings import FEATURE_STORE_DIR, ensure_data_dirs


class FeatureStore:
    def __init__(self, base_path: Path | str | None = None):
        ensure_data_dirs()
        self.base_path = Path(base_path) if base_path is not None else FEATURE_STORE_DIR
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save(self, df: pd.DataFrame, name: str) -> Path:
        file_path = self.base_path / f"{name}.parquet"
        df.to_parquet(file_path)
        return file_path

    def load(self, name: str) -> pd.DataFrame:
        return pd.read_parquet(self.base_path / f"{name}.parquet")
