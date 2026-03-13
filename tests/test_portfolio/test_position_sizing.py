import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from genesis.portfolio.position_sizing import PositionSizer


class PositionSizingTest(unittest.TestCase):
    def test_size_notional_scales_with_vol_and_conviction(self):
        sizer = PositionSizer(target_annual_vol=0.5, min_multiplier=0.25, max_multiplier=2.0, min_conviction_scale=0.2)
        result = sizer.size_notional(
            base_notional_usdt=100.0,
            conviction=0.5,
            realized_annual_vol=1.0,
            recent_drawdown=0.0,
        )
        self.assertAlmostEqual(result["volatility_multiplier"], 0.5, places=6)
        self.assertAlmostEqual(result["conviction_multiplier"], 0.5, places=6)
        self.assertGreater(result["final_notional_usdt"], 0.0)

    def test_drawdown_penalty_reduces_size(self):
        sizer = PositionSizer(drawdown_cutoff=0.1, drawdown_floor_scale=0.3)
        low_dd = sizer.size_notional(100.0, conviction=1.0, realized_annual_vol=0.6, recent_drawdown=0.05)
        high_dd = sizer.size_notional(100.0, conviction=1.0, realized_annual_vol=0.6, recent_drawdown=0.25)
        self.assertLess(high_dd["drawdown_multiplier"], low_dd["drawdown_multiplier"])
        self.assertLess(high_dd["final_notional_usdt"], low_dd["final_notional_usdt"])


if __name__ == "__main__":
    unittest.main()
