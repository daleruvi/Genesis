import unittest

import pandas as pd

from genesis.features.opening_range import build_opening_range_dataset
from genesis.features.sessions import SessionSpec, assign_synthetic_session


def make_daily(start="2024-12-01", periods=25):
    dates = pd.date_range(start, periods=periods, freq="D")
    return pd.DataFrame(
        {
            "timestamp": dates,
            "open": [100.0] * periods,
            "high": [110.0] * periods,
            "low": [100.0] * periods,
            "close": [105.0] * periods,
            "volume": [1.0] * periods,
        }
    )


class SessionOpeningRangeTest(unittest.TestCase):
    def test_assigns_ny_session_id_from_utc_timestamp(self):
        df = pd.DataFrame(
            {
                "timestamp": ["2024-12-20 14:30:00"],
                "open": [100],
                "high": [101],
                "low": [99],
                "close": [100],
                "volume": [1],
            }
        )

        result = assign_synthetic_session(df, SessionSpec())

        self.assertEqual(result.iloc[0]["session_id"], "ny_2024-12-20")
        self.assertEqual(result.iloc[0]["local_minutes"], 9 * 60 + 30)

    def test_opening_range_uses_shifted_daily_atr(self):
        intraday = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-12-20 14:30:00", periods=6, freq="5min"),
                "open": [100, 101, 102, 104, 103, 102],
                "high": [102, 103, 104, 105, 104, 103],
                "low": [99, 100, 101, 102, 101, 100],
                "close": [101, 102, 103, 103, 102, 101],
                "volume": [1] * 6,
            }
        )

        frame, invalid = build_opening_range_dataset(
            intraday,
            make_daily(),
            SessionSpec(),
            daily_atr_period=14,
            min_or_size_vs_atr=0.01,
            max_or_size_vs_atr=2.0,
            min_daily_atr_pct=0.01,
        )

        self.assertTrue(invalid.empty)
        self.assertEqual(frame["or_high"].iloc[0], 104)
        self.assertEqual(frame["or_low"].iloc[0], 99)
        self.assertEqual(frame["daily_atr"].iloc[0], 10.0)
        self.assertAlmostEqual(frame["daily_atr_pct"].iloc[0], 10.0 / 105.0)
        self.assertAlmostEqual(frame["or_size_vs_daily_atr"].iloc[0], 0.5)

    def test_session_without_or_bars_is_invalid(self):
        intraday = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-12-20 15:00:00", periods=3, freq="5min"),
                "open": [100, 101, 102],
                "high": [101, 102, 103],
                "low": [99, 100, 101],
                "close": [100, 101, 102],
                "volume": [1] * 3,
            }
        )

        frame, invalid = build_opening_range_dataset(intraday, make_daily(), SessionSpec())

        self.assertTrue(frame.empty)
        self.assertEqual(invalid.iloc[0]["invalid_reason"], "insufficient_opening_range_bars")

    def test_ny_equity_session_and_opening_range_window(self):
        intraday = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-12-20 14:30:00", periods=6, freq="5min"),
                "open": [100, 101, 102, 104, 103, 102],
                "high": [102, 103, 104, 105, 104, 103],
                "low": [99, 100, 101, 102, 101, 100],
                "close": [101, 102, 103, 103, 102, 101],
                "volume": [100, 110, 120, 130, 140, 150],
            }
        )

        frame, invalid = build_opening_range_dataset(
            intraday,
            make_daily(),
            SessionSpec(name="ny_equity"),
            daily_atr_period=14,
            min_or_size_vs_atr=0.01,
            max_or_size_vs_atr=2.0,
            min_daily_atr_pct=0.01,
        )

        self.assertTrue(invalid.empty)
        self.assertEqual(frame.iloc[0]["session_id"], "ny_equity_2024-12-20")
        self.assertEqual(frame["or_high"].iloc[0], 104)
        self.assertEqual(frame["or_low"].iloc[0], 99)
        self.assertEqual(frame["opening_range_volume"].iloc[0], 330)

    def test_gap_filter_invalidates_session(self):
        intraday = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-12-20 14:30:00", periods=6, freq="5min"),
                "open": [120, 121, 122, 124, 123, 122],
                "high": [122, 123, 124, 125, 124, 123],
                "low": [119, 120, 121, 122, 121, 120],
                "close": [121, 122, 123, 123, 122, 121],
                "volume": [1] * 6,
            }
        )

        frame, invalid = build_opening_range_dataset(
            intraday,
            make_daily(),
            SessionSpec(),
            daily_atr_period=14,
            min_or_size_vs_atr=0.01,
            max_or_size_vs_atr=2.0,
            min_daily_atr_pct=0.01,
            gap_filter_enabled=True,
            max_abs_gap_pct=0.05,
        )

        self.assertTrue(frame.empty)
        self.assertEqual(invalid.iloc[0]["invalid_reason"], "gap_pct_above_max")

    def test_opening_volume_filter_uses_history(self):
        day1 = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-12-20 14:30:00", periods=6, freq="5min"),
                "open": [100, 101, 102, 104, 103, 102],
                "high": [102, 103, 104, 105, 104, 103],
                "low": [99, 100, 101, 102, 101, 100],
                "close": [101, 102, 103, 103, 102, 101],
                "volume": [100, 100, 100, 1, 1, 1],
            }
        )
        day2 = day1.copy()
        day2["timestamp"] = pd.date_range("2024-12-21 14:30:00", periods=6, freq="5min")
        day2["volume"] = [100, 100, 100, 1, 1, 1]
        day3 = day1.copy()
        day3["timestamp"] = pd.date_range("2024-12-22 14:30:00", periods=6, freq="5min")
        day3["volume"] = [10, 10, 10, 1, 1, 1]
        intraday = pd.concat([day1, day2, day3], ignore_index=True)

        frame, invalid = build_opening_range_dataset(
            intraday,
            make_daily(),
            SessionSpec(),
            daily_atr_period=14,
            min_or_size_vs_atr=0.01,
            max_or_size_vs_atr=2.0,
            min_daily_atr_pct=0.01,
            opening_volume_filter_enabled=True,
            min_opening_volume_vs_avg=0.5,
            opening_volume_avg_sessions=1,
        )

        self.assertEqual(len(frame["session_id"].unique()), 1)
        self.assertEqual(invalid.iloc[-1]["invalid_reason"], "opening_volume_vs_avg_below_min")


if __name__ == "__main__":
    unittest.main()
