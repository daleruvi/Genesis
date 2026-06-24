import unittest

import pandas as pd

from genesis.backtest.validation import (
    coverage_ratio_5m,
    daily_warmup_status,
    equity_regular_session_coverage,
    relevant_5m_gaps,
)


class EmpiricalValidationTest(unittest.TestCase):
    def test_coverage_ratio_5m_counts_expected_window(self):
        df = pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=12, freq="5min")})

        result = coverage_ratio_5m(df, start="2025-01-01", end="2025-01-01 01:00:00")

        self.assertEqual(result["expected_5m_bars"], 12)
        self.assertEqual(result["observed_5m_bars"], 12)
        self.assertEqual(result["coverage_ratio_5m"], 1.0)

    def test_daily_warmup_requires_prior_bars(self):
        daily = pd.DataFrame({"timestamp": pd.date_range("2024-12-01", periods=20, freq="D")})

        result = daily_warmup_status(
            daily,
            first_session_date="2024-12-20",
            warmup_days=14,
            required_end="2024-12-20",
        )

        self.assertTrue(result["daily_warmup_ok"])
        self.assertGreaterEqual(result["daily_warmup_bars"], 14)

    def test_daily_warmup_uses_available_trading_bars_not_calendar_start(self):
        daily = pd.DataFrame({"timestamp": pd.bdate_range("2024-12-02", periods=22)})

        result = daily_warmup_status(
            daily,
            first_session_date="2025-01-01",
            warmup_days=14,
            required_start="2024-12-01",
        )

        self.assertEqual(result["daily_start"], "2024-12-02")
        self.assertEqual(result["daily_warmup_bars"], 22)
        self.assertTrue(result["daily_warmup_ok"])

    def test_daily_warmup_first_trading_day_after_weekend_is_valid(self):
        daily = pd.DataFrame({"timestamp": pd.bdate_range("2024-12-02", periods=21)})

        result = daily_warmup_status(
            daily,
            first_session_date="2025-01-01",
            warmup_days=14,
            required_start="2024-12-01",
        )

        self.assertGreaterEqual(result["daily_warmup_bars"], 14)
        self.assertTrue(result["daily_warmup_ok"])

    def test_reports_relevant_ny_gap_over_60_minutes(self):
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2025-01-02 14:45:00",
                    "2025-01-02 16:00:00",
                ]
            }
        )

        gaps = relevant_5m_gaps(df)

        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps.iloc[0]["gap_minutes"], 75.0)

    def test_equity_regular_session_coverage_counts_full_and_partial_days(self):
        premarket = pd.date_range("2025-01-02 12:00:00", periods=2, freq="5min")
        full_day = pd.date_range("2025-01-02 14:30:00", periods=78, freq="5min")
        partial_day = pd.date_range("2025-01-03 14:30:00", periods=70, freq="5min")
        after_hours = pd.date_range("2025-01-03 21:00:00", periods=3, freq="5min")
        intraday = pd.DataFrame({"timestamp": list(premarket) + list(full_day) + list(partial_day) + list(after_hours)})
        daily = pd.DataFrame({"timestamp": pd.to_datetime(["2025-01-02", "2025-01-03"])})

        result = equity_regular_session_coverage(intraday, daily)

        self.assertEqual(result["expected_regular_bars"], 156)
        self.assertEqual(result["observed_regular_bars"], 148)
        self.assertEqual(result["complete_regular_sessions"], 1)
        self.assertEqual(result["partial_regular_sessions"], 1)
        self.assertEqual(result["extended_hours_bars_count"], 5)
        self.assertEqual(result["coverage_denominator_source"], "daily")
        self.assertFalse(result["coverage_is_provisional"])
        self.assertAlmostEqual(result["coverage_ratio_5m"], 148 / 156)

    def test_equity_regular_session_coverage_without_daily_is_provisional(self):
        full_day = pd.date_range("2025-01-02 14:30:00", periods=78, freq="5min")
        intraday = pd.DataFrame({"timestamp": list(full_day)})

        result = equity_regular_session_coverage(intraday, daily=None)

        self.assertEqual(result["coverage_denominator_source"], "intraday_detected")
        self.assertTrue(result["coverage_is_provisional"])
        self.assertEqual(result["total_regular_sessions_detected"], 1)


if __name__ == "__main__":
    unittest.main()
