import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from src.genesis.alpha.alpha_generator import AlphaGenerator


class AlphaGeneratorTest(unittest.TestCase):
    def test_generator_creates_alpha_columns(self):
        df = pd.DataFrame(
            {
                "returns_1": [0.1, 0.2, 0.3, 0.4, 0.5],
                "returns_5": [0.2, 0.1, 0.4, 0.3, 0.6],
                "momentum_10": [1, 2, 3, 4, 5],
                "momentum_20": [2, 3, 4, 5, 6],
                "volatility_10": [0.01, 0.02, 0.03, 0.04, 0.05],
                "volume_ratio": [1.1, 1.0, 1.3, 1.2, 1.4],
                "close": [100, 101, 102, 103, 104],
            }
        )

        generator = AlphaGenerator(df)
        generator.generate_pairwise_alphas(["returns_1", "returns_5"])
        generator.generate_momentum_alphas("close")
        alphas = generator.build()

        self.assertGreater(len(alphas.columns), 0)


if __name__ == "__main__":
    unittest.main()
