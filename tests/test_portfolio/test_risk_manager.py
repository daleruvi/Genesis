import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from genesis.portfolio.risk_manager import RiskManager


class RiskManagerTest(unittest.TestCase):
    def test_caps_target_usdt(self):
        manager = RiskManager(max_position_usdt=100, max_open_positions=1)
        result = manager.can_open_new_position([], 250)
        self.assertTrue(result["allowed"])
        self.assertEqual(result["target_usdt"], 100)

    def test_blocks_when_max_positions_reached(self):
        manager = RiskManager(max_position_usdt=100, max_open_positions=1)
        positions = [{"contracts": 0.01}]
        result = manager.can_open_new_position(positions, 50)
        self.assertFalse(result["allowed"])


if __name__ == "__main__":
    unittest.main()
