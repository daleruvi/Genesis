import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from genesis.alpha.alpha_selection import AlphaSelector


class AlphaSelectionTest(unittest.TestCase):
    def test_selector_filters_highly_correlated_alphas(self):
        rankings = pd.DataFrame(
            [
                {"alpha": "alpha_1", "score": 0.95},
                {"alpha": "alpha_2", "score": 0.90},
                {"alpha": "alpha_3", "score": 0.80},
            ]
        )
        alphas = pd.DataFrame(
            {
                "alpha_1": [1.0, 2.0, 3.0, 4.0, 5.0],
                "alpha_2": [1.01, 2.02, 3.03, 4.04, 5.05],
                "alpha_3": [-1.0, 1.0, -1.0, 1.0, -1.0],
            }
        )

        selector = AlphaSelector(max_correlation=0.8, top_n=3)
        selected = selector.select(rankings, alphas)

        self.assertEqual(selected["alpha"].tolist(), ["alpha_1", "alpha_3"])


if __name__ == "__main__":
    unittest.main()
