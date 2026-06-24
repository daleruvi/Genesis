import unittest

import pandas as pd

from genesis.backtest.trade_engine import apply_slippage, backtest_signals
from genesis.strategies.opening_range_reversion import OpeningRangeReversionConfig


def make_signal(side="long"):
    return pd.DataFrame(
        [
            {
                "session_id": "ny_2024-12-20",
                "session": "ny",
                "session_date": "2024-12-20",
                "side": side,
                "breakout_time": pd.Timestamp("2024-12-20 14:45:00", tz="UTC"),
                "breakout_side": "below" if side == "long" else "above",
                "reentry_time": pd.Timestamp("2024-12-20 14:50:00", tz="UTC"),
                "confirmation_time": pd.Timestamp("2024-12-20 14:50:00", tz="UTC"),
                "entry_time": pd.Timestamp("2024-12-20 14:55:00", tz="UTC"),
                "entry_price": 100.0,
                "entry_reason": "breakout_reentry_v1",
                "stop": 95.0 if side == "long" else 105.0,
                "tp_midpoint": 102.0 if side == "long" else 98.0,
                "tp_opposite_extreme": 104.0 if side == "long" else 96.0,
                "or_high": 104.0,
                "or_low": 96.0,
                "or_mid": 100.0,
                "or_size": 8.0,
                "daily_atr": 10.0,
                "daily_atr_pct": 0.1,
                "or_size_vs_daily_atr": 0.8,
            }
        ]
    )


def make_bars(high=103.0, low=94.0, close=101.0):
    return pd.DataFrame(
        {
            "session_id": ["ny_2024-12-20"] * 2,
            "timestamp_utc": pd.date_range("2024-12-20 14:55:00", periods=2, freq="5min", tz="UTC"),
            "local_minutes": [895, 960],
            "open": [100.0, 101.0],
            "high": [high, 101.0],
            "low": [low, 99.0],
            "close": [close, 100.0],
        }
    )


class TradeEngineTest(unittest.TestCase):
    def test_slippage_is_against_trade_direction(self):
        self.assertEqual(apply_slippage(100, "long", "entry", 0.01), 101)
        self.assertEqual(apply_slippage(100, "long", "exit", 0.01), 99)
        self.assertEqual(apply_slippage(100, "short", "entry", 0.01), 99)
        self.assertEqual(apply_slippage(100, "short", "exit", 0.01), 101)

    def test_stop_wins_when_stop_and_tp_hit_same_bar(self):
        config = OpeningRangeReversionConfig(trend_filter="none", slippage_rate=0.0, fee_rate=0.0)

        trades, _ = backtest_signals(make_signal("long"), make_bars(high=103, low=94), config)

        midpoint = trades[trades["tp_variant"] == "midpoint"].iloc[0]
        self.assertEqual(midpoint["exit_reason"], "stop")
        self.assertLess(midpoint["return"], 0)

    def test_time_exit_closes_trade_without_stop_or_tp(self):
        config = OpeningRangeReversionConfig(trend_filter="none", slippage_rate=0.0, fee_rate=0.0)

        trades, _ = backtest_signals(make_signal("long"), make_bars(high=101, low=99, close=100.5), config)

        midpoint = trades[trades["tp_variant"] == "midpoint"].iloc[0]
        self.assertEqual(midpoint["exit_reason"], "time_exit")
        self.assertEqual(midpoint["bars_in_trade"], 2)


if __name__ == "__main__":
    unittest.main()
