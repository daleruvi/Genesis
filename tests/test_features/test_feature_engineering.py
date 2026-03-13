import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from src.genesis.features.feature_engineering import FeatureEngineer


class FeatureEngineeringTest(unittest.TestCase):
    def test_build_generates_expected_columns(self):
        df = pd.DataFrame(
            {
                "close": [
                    100, 101, 102, 103, 104, 105, 106,
                    107, 108, 109, 110, 111, 112, 113,
                    114, 115, 116, 117, 118, 119, 120,
                ],
                "volume": [10] * 21,
            }
        )

        features = (
            FeatureEngineer(df)
            .returns()
            .volatility()
            .momentum()
            .volume_features()
            .build()
        )

        self.assertIn("returns_1", features.columns)
        self.assertIn("volatility_10", features.columns)
        self.assertIn("momentum_10", features.columns)
        self.assertIn("volume_ratio", features.columns)


if __name__ == "__main__":
    unittest.main()
