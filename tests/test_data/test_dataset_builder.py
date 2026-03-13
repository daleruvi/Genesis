import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from src.genesis.data.dataset_builder import DatasetBuilder


class DatasetBuilderTest(unittest.TestCase):
    def test_save_dataset_creates_parquet_file(self):
        df = pd.DataFrame({"close": [1, 2, 3]})
        tmp_dir = PROJECT_ROOT / "tests" / "_tmp_data"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        builder = DatasetBuilder(tmp_dir)
        file_path = builder.save_dataset(df, "sample")
        self.assertTrue(Path(file_path).exists())
        Path(file_path).unlink()


if __name__ == "__main__":
    unittest.main()
