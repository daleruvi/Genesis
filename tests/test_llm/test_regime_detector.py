import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from genesis.llm.regime_detector import RegimeDetector


class RegimeDetectorTest(unittest.TestCase):
    def test_detect_adds_regime_column(self):
        close = pd.Series(
            [100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
             110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
             120, 121, 122, 123, 124],
            dtype=float,
        )
        df = pd.DataFrame({"close": close})

        detector = RegimeDetector()
        detected = detector.detect(df)

        self.assertIn("regime", detected.columns)
        self.assertIn("trend_strength", detected.columns)
        self.assertTrue(detected["regime"].notna().any())

    def test_performance_by_regime_groups_returns(self):
        detector = RegimeDetector(periods_per_year=10)
        returns = pd.Series([0.01, 0.02, -0.01, 0.0, 0.03, -0.02], dtype=float)
        regimes = pd.Series(
            ["trend_up_low_vol", "trend_up_low_vol", "chop_low_vol", "chop_low_vol", "trend_up_low_vol", "chop_low_vol"]
        )

        summary = detector.performance_by_regime(returns=returns, regimes=regimes)

        self.assertEqual(set(summary["regime"]), {"trend_up_low_vol", "chop_low_vol"})
        self.assertIn("sharpe", summary.columns)
        self.assertIn("total_return", summary.columns)


if __name__ == "__main__":
    unittest.main()
