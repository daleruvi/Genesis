import unittest

import pandas as pd

from genesis.strategies.opening_range_reversion import (
    OpeningRangeReversionConfig,
    generate_opening_range_signals,
)


def make_strategy_frame():
    timestamps = pd.date_range("2024-12-20 14:30:00", periods=7, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp_utc": timestamps,
            "local_minutes": [570, 575, 580, 585, 590, 595, 600],
            "session": ["ny"] * 7,
            "session_id": ["ny_2024-12-20"] * 7,
            "session_date": ["2024-12-20"] * 7,
            "open": [100, 101, 102, 106, 104, 103, 102],
            "high": [102, 103, 104, 107, 105, 104, 103],
            "low": [99, 100, 101, 103, 101, 100, 99],
            "close": [101, 102, 103, 106, 102, 101, 100],
            "or_high": [104] * 7,
            "or_low": [99] * 7,
            "or_mid": [101.5] * 7,
            "or_size": [5] * 7,
            "daily_atr": [10] * 7,
            "daily_atr_pct": [0.10] * 7,
            "or_size_vs_daily_atr": [0.5] * 7,
            "or_size_vs_atr": [0.5] * 7,
            "breakout_above": [False, False, False, True, False, False, False],
            "breakout_below": [False] * 7,
            "ema_50": [90] * 7,
        }
    )


class OpeningRangeReversionSignalTest(unittest.TestCase):
    def test_trend_filter_none_allows_signal_that_fails_ema(self):
        config = OpeningRangeReversionConfig(trend_filter="none")

        signals, invalid = generate_opening_range_signals(make_strategy_frame(), config)

        self.assertTrue(invalid.empty)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals.iloc[0]["side"], "short")
        self.assertEqual(signals.iloc[0]["entry_price"], 103)
        self.assertEqual(pd.Timestamp(signals.iloc[0]["entry_time"]), pd.Timestamp("2024-12-20 14:55:00", tz="UTC"))

    def test_trend_filter_ema_blocks_short_above_ema(self):
        config = OpeningRangeReversionConfig(trend_filter="ema")

        signals, _ = generate_opening_range_signals(make_strategy_frame(), config)

        self.assertTrue(signals.empty)

    def test_signal_without_next_bar_records_invalid_reason(self):
        config = OpeningRangeReversionConfig(trend_filter="none")
        frame = make_strategy_frame().iloc[:5].copy()

        signals, invalid = generate_opening_range_signals(frame, config)

        self.assertTrue(signals.empty)
        self.assertEqual(invalid.iloc[0]["invalid_signal_reason"], "no_next_entry_bar")

    def test_invalid_atr_mode_fails(self):
        with self.assertRaisesRegex(ValueError, "atr_mode=daily"):
            OpeningRangeReversionConfig(atr_mode="intraday").validate()

    def test_daily_atr_aliases_map_to_existing_or_filter_fields(self):
        config = OpeningRangeReversionConfig.from_mapping(
            {
                "min_or_size_vs_daily_atr": 0.12,
                "max_or_size_vs_daily_atr": 0.34,
            }
        )

        self.assertEqual(config.min_or_size_vs_atr, 0.12)
        self.assertEqual(config.max_or_size_vs_atr, 0.34)

    def test_exclude_earnings_days_requires_calendar(self):
        with self.assertRaisesRegex(ValueError, "earnings calendar"):
            OpeningRangeReversionConfig(exclude_earnings_days=True).validate()


if __name__ == "__main__":
    unittest.main()
