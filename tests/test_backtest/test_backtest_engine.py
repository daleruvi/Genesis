import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from genesis.backtest.engine import BacktestEngine


class BacktestEngineTest(unittest.TestCase):
    def test_run_returns_expected_columns(self):
        engine = BacktestEngine(fee_rate=0.0, slippage_rate=0.0)
        close = pd.Series([100, 101, 102, 101])
        positions = pd.Series([1, 1, -1, -1])
        results = engine.run(close, positions)
        self.assertIn("strategy_returns", results.columns)
        self.assertIn("benchmark_returns", results.columns)
        self.assertIn("turnover", results.columns)

    def test_quantile_signal_creates_flat_zone(self):
        engine = BacktestEngine()
        signal = pd.Series([1, 2, 3, 4, 5])
        positions = engine.signal_to_positions(signal, mode="quantile", lower_q=0.2, upper_q=0.8)
        self.assertIn(0.0, positions.tolist())


if __name__ == "__main__":
    unittest.main()
