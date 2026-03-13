import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from genesis.alpha.alpha_ensemble import AlphaEnsemble


class AlphaEnsembleTest(unittest.TestCase):
    def test_decide_uses_positive_regime_edge(self):
        rankings = pd.DataFrame(
            [
                {"alpha": "alpha_1", "selection_rank": 1, "score": 0.9, "test_sharpe": 2.0, "temporal_consistency": 0.5},
                {"alpha": "alpha_2", "selection_rank": 2, "score": 0.8, "test_sharpe": 1.5, "temporal_consistency": 0.4},
            ]
        )
        rankings["test_profit_factor"] = [1.2, 1.1]
        rankings["test_total_turnover"] = [120.0, 140.0]
        regime_performance = pd.DataFrame(
            [
                {"alpha": "alpha_1", "regime": "trend_up_low_vol", "sharpe": 3.0, "total_return": 0.2, "profit_factor": 1.8},
                {"alpha": "alpha_2", "regime": "trend_up_low_vol", "sharpe": -1.0, "total_return": -0.1, "profit_factor": 0.8},
            ]
        )
        latest = pd.Series({"alpha_1": 1.0, "alpha_2": -1.0})

        ensemble = AlphaEnsemble(rankings=rankings, regime_performance=regime_performance, signal_threshold=0.1)
        decision = ensemble.decide(latest_alphas=latest, regime="trend_up_low_vol")

        self.assertEqual(decision["signal"], "long")
        self.assertGreaterEqual(decision["votes_used"], 1)
        self.assertGreater(decision["conviction"], 0)

    def test_decide_uses_fallback_without_regime_support(self):
        rankings = pd.DataFrame(
            [{"alpha": "alpha_1", "selection_rank": 1, "score": 0.9, "test_sharpe": 2.0, "temporal_consistency": 0.5}]
        )
        rankings["test_profit_factor"] = [1.2]
        rankings["test_total_turnover"] = [100.0]
        regime_performance = pd.DataFrame(
            [{"alpha": "alpha_1", "regime": "trend_down_low_vol", "sharpe": -2.0, "total_return": -0.2, "profit_factor": 0.7}]
        )
        latest = pd.Series({"alpha_1": 1.0})

        ensemble = AlphaEnsemble(
            rankings=rankings,
            regime_performance=regime_performance,
            signal_threshold=0.1,
            fallback_multiplier=0.2,
        )
        decision = ensemble.decide(latest_alphas=latest, regime="trend_down_low_vol")

        self.assertEqual(decision["signal"], "long")
        self.assertTrue(decision["used_fallback"])


if __name__ == "__main__":
    unittest.main()
