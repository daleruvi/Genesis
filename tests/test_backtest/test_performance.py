import math
import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from genesis.backtest.performance import PerformanceAnalyzer


class PerformanceAnalyzerTest(unittest.TestCase):
    def test_summary_contains_core_metrics(self):
        analyzer = PerformanceAnalyzer(periods_per_year=4)
        returns = pd.Series([0.01, -0.01, 0.02, 0.0])
        summary = analyzer.summarize(returns)
        self.assertIn("total_return", summary)
        self.assertIn("sharpe", summary)
        self.assertIn("max_drawdown", summary)

    def test_profit_factor_handles_no_losses(self):
        analyzer = PerformanceAnalyzer()
        returns = pd.Series([0.01, 0.02, 0.0])
        pf = analyzer.profit_factor(returns)
        self.assertTrue(math.isinf(pf))


if __name__ == "__main__":
    unittest.main()
