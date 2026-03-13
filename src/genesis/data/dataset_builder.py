from pathlib import Path

from genesis.config.settings import RAW_DATA_DIR, ensure_data_dirs


class DatasetBuilder:
    def __init__(self, path: Path | str | None = None):
        ensure_data_dirs()
        self.path = Path(path) if path is not None else RAW_DATA_DIR
        self.path.mkdir(parents=True, exist_ok=True)

    def save_dataset(self, df, name: str) -> Path:
        file_path = self.path / f"{name}.parquet"
        df.to_parquet(file_path)
        print(f"dataset saved -> {file_path}")
        return file_path
