import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from genesis.alpha.alpha_research import AlphaResearch


class AlphaResearchTest(unittest.TestCase):
    def test_evaluate_alphas_returns_ranked_frame(self):
        close = pd.Series([100, 101, 102, 103, 104, 105, 106, 107, 108, 109], dtype=float)
        alphas = pd.DataFrame(
            {
                "alpha_good": [1.0] * 10,
                "alpha_alt": [0.2, 0.3, 0.1, 0.4, 0.3, 0.2, 0.4, 0.5, 0.3, 0.2],
                "alpha_bad": [-1.0] * 10,
            }
        )

        research = AlphaResearch(close=close, fee_rate=0.0, slippage_rate=0.0, train_ratio=0.6, periods_per_year=10)
        rankings = research.evaluate_alphas(alphas, signal_mode="sign")

        self.assertIn("score", rankings.columns)
        self.assertIn("test_sharpe", rankings.columns)
        self.assertIn("test_ic_abs", rankings.columns)
        self.assertEqual(len(rankings), 3)
        self.assertGreater(
            rankings.set_index("alpha").loc["alpha_good", "score"],
            rankings.set_index("alpha").loc["alpha_bad", "score"],
        )

    def test_temporal_stability_returns_windows(self):
        close = pd.Series([100 + i for i in range(220)], dtype=float)
        alpha = pd.Series([1.0 if i % 2 == 0 else -1.0 for i in range(220)], dtype=float)
        research = AlphaResearch(close=close, fee_rate=0.0, slippage_rate=0.0, train_ratio=0.6, periods_per_year=10)

        windows = research.temporal_stability(
            alpha=alpha,
            signal_mode="sign",
            window_size=60,
            step_size=30,
        )
        summary = research.summarize_temporal_stability(windows)

        self.assertGreater(len(windows), 0)
        self.assertIn("sharpe", windows.columns)
        self.assertIn("temporal_window_count", summary)
        self.assertEqual(summary["temporal_window_count"], len(windows))


if __name__ == "__main__":
    unittest.main()
