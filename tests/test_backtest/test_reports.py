import unittest

import pandas as pd

from genesis.backtest.reports import build_summary, decision_for_variant, render_validation_markdown


class OpeningRangeReportsTest(unittest.TestCase):
    def test_summary_contains_required_counts_and_duration(self):
        trades = pd.DataFrame(
            {
                "tp_variant": ["midpoint", "midpoint"],
                "return": [0.01, -0.005],
                "entry_time": ["2025-01-02 15:00:00", "2025-02-03 15:00:00"],
                "rr": [1.0, -0.5],
                "bars_in_trade": [3, 5],
            }
        )
        signals = pd.DataFrame({"session_id": ["ny_2024-12-20"]})
        invalid = pd.DataFrame({"session_id": ["ny_2024-12-21"], "invalid_reason": ["missing_daily_atr"]})

        summary = build_summary(
            trades,
            signals,
            invalid,
            total_sessions=2,
            valid_sessions=1,
            data_quality={"coverage_ratio_5m": 1.0},
        )

        row = summary.iloc[0]
        self.assertIn("avg_bars_in_trade", summary.columns)
        self.assertIn("median_bars_in_trade", summary.columns)
        self.assertEqual(row["total_sessions"], 2)
        self.assertEqual(row["valid_sessions"], 1)
        self.assertEqual(row["invalid_sessions"], 1)
        self.assertEqual(row["signals_count"], 1)
        self.assertEqual(row["trades_count"], 2)
        self.assertEqual(row["avg_bars_in_trade"], 4)
        self.assertEqual(row["median_bars_in_trade"], 4)
        self.assertIn("decision", summary.columns)
        self.assertIn("return_to_drawdown_ratio", summary.columns)
        self.assertIn("top_5_trades_pnl_share", summary.columns)
        self.assertIn("top_month_return_share", summary.columns)

    def test_markdown_mentions_explicit_daily_atr_name(self):
        summary = pd.DataFrame(
            {
                "tp_variant": ["midpoint"],
                "total_return": [0.01],
                "win_rate": [1.0],
                "profit_factor": [2.0],
                "max_drawdown": [0.0],
                "expectancy": [0.01],
                "avg_bars_in_trade": [3.0],
                "median_bars_in_trade": [3.0],
                "signals_count": [1],
                "trades_count": [1],
                "signal_to_trade_rate": [1.0],
                "coverage_ratio_5m": [1.0],
                "invalid_sessions_ratio": [0.0],
                "decision": ["continue"],
                "return_to_drawdown_ratio": [1.0],
                "top_5_trades_pnl_share": [0.5],
                "top_month_return_share": [0.5],
                "symbol": ["QQQ"],
                "adjusted_data": [True],
            }
        )

        markdown = render_validation_markdown(summary, pd.DataFrame(), "test")

        self.assertIn("or_size_vs_daily_atr", markdown)
        self.assertIn("Decision Inputs", markdown)
        self.assertIn("Recommendation", markdown)

    def test_decision_rules_are_per_variant_and_strict(self):
        row = {
            "trades_count": 50,
            "total_return": 0.1,
            "expectancy": 0.01,
            "profit_factor": 1.2,
            "return_to_drawdown_ratio": 0.6,
            "top_5_trades_pnl_share": 0.5,
            "coverage_ratio_5m": 1.0,
            "invalid_sessions_ratio": 0.0,
        }

        self.assertEqual(decision_for_variant(row), "demo")
        row["coverage_ratio_5m"] = 0.8
        self.assertEqual(decision_for_variant(row), "continue")
        row["coverage_ratio_5m"] = 0.4
        self.assertEqual(decision_for_variant(row), "no-go")

    def test_market_fit_decision_profile_uses_equity_labels(self):
        row = {
            "decision_profile": "market_fit",
            "trades_count": 50,
            "total_return": 0.1,
            "expectancy": 0.01,
            "profit_factor": 1.2,
            "return_to_drawdown_ratio": 0.6,
            "top_5_trades_pnl_share": 0.5,
            "top_month_return_share": 0.5,
            "coverage_ratio_5m": 1.0,
            "daily_warmup_ok": True,
        }

        self.assertEqual(decision_for_variant(row), "candidate_for_validation")
        row["top_month_return_share"] = 0.8
        self.assertEqual(decision_for_variant(row), "continue_research")
        row["coverage_ratio_5m"] = 0.8
        self.assertEqual(decision_for_variant(row), "no_data")

    def test_market_fit_decision_is_no_go_for_valid_negative_performance(self):
        row = {
            "decision_profile": "market_fit",
            "trades_count": 42,
            "total_return": -0.12,
            "expectancy": -0.003,
            "profit_factor": 0.8,
            "return_to_drawdown_ratio": -0.4,
            "top_5_trades_pnl_share": 0.5,
            "top_month_return_share": 0.5,
            "coverage_ratio_5m": 0.9946,
            "daily_warmup_ok": True,
        }

        self.assertEqual(decision_for_variant(row), "no_go")

    def test_market_fit_decision_no_data_only_when_quality_blocks_validation(self):
        row = {
            "decision_profile": "market_fit",
            "trades_count": 42,
            "total_return": -0.12,
            "expectancy": -0.003,
            "profit_factor": 0.8,
            "coverage_ratio_5m": 0.5,
            "daily_warmup_ok": True,
        }

        self.assertEqual(decision_for_variant(row), "no_data")
        row["coverage_ratio_5m"] = 0.9946
        row["daily_warmup_ok"] = False
        self.assertEqual(decision_for_variant(row), "no_data")

    def test_market_fit_partial_sessions_do_not_force_no_data_when_coverage_is_sufficient(self):
        row = {
            "decision_profile": "market_fit",
            "trades_count": 42,
            "total_return": -0.12,
            "expectancy": -0.003,
            "profit_factor": 0.8,
            "coverage_ratio_5m": 0.9946,
            "daily_warmup_ok": True,
            "partial_regular_sessions": 3,
            "ny_gap_count_gt_60m": 3,
        }

        self.assertEqual(decision_for_variant(row), "no_go")


if __name__ == "__main__":
    unittest.main()
