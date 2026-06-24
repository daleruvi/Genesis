import unittest

import pandas as pd

from genesis.backtest.postmortem import (
    ORRArtifactSpec,
    build_comparison_summary,
    build_side_breakdown,
    build_trade_diagnostics,
    decide_postmortem,
    render_postmortem_markdown,
)


class ORRPostmortemTest(unittest.TestCase):
    def test_run_postmortem_normalizes_multi_symbol_outputs(self):
        loaded = [
            {
                "spec": ORRArtifactSpec.from_symbol("BTCUSDT"),
                "summary": self._summary_fixture(),
            },
            {
                "spec": ORRArtifactSpec.from_symbol("QQQ"),
                "summary": self._summary_fixture(),
            },
        ]

        summary = build_comparison_summary(loaded)

        self.assertEqual(set(summary["symbol"]), {"BTCUSDT", "QQQ"})
        self.assertEqual(set(summary["market_type"]), {"crypto_futures", "equity"})

    def test_trade_diagnostics_reports_max_consecutive_losses(self):
        trades = pd.DataFrame(
            {
                "symbol": ["QQQ"] * 4,
                "market_type": ["equity"] * 4,
                "tp_variant": ["midpoint"] * 4,
                "side": ["long", "long", "short", "short"],
                "return": [-0.01, -0.02, 0.03, -0.01],
                "entry_time": pd.date_range("2025-01-02 14:30:00", periods=4, freq="h"),
                "bars_in_trade": [1, 2, 3, 4],
            }
        )
        signals = pd.DataFrame({"symbol": ["QQQ"] * 4, "market_type": ["equity"] * 4, "breakout_side": ["above", "below", "above", "below"]})
        summary = pd.DataFrame({"symbol": ["QQQ"], "market_type": ["equity"], "tp_variant": ["midpoint"], "top_5_trades_pnl_share": [0.5]})

        diagnostics = build_trade_diagnostics(trades, signals, summary)

        self.assertEqual(diagnostics.iloc[0]["max_consecutive_losses"], 2)
        self.assertEqual(diagnostics.iloc[0]["long_trades"], 2)
        self.assertEqual(diagnostics.iloc[0]["short_trades"], 2)

    def test_side_breakdown_groups_per_side(self):
        trades = pd.DataFrame(
            {
                "symbol": ["SPY", "SPY", "SPY"],
                "market_type": ["equity", "equity", "equity"],
                "tp_variant": ["midpoint", "midpoint", "midpoint"],
                "side": ["long", "long", "short"],
                "return": [0.01, -0.02, -0.01],
            }
        )

        breakdown = build_side_breakdown(trades)

        self.assertEqual(set(breakdown["side"]), {"long", "short"})
        self.assertEqual(int(breakdown.loc[breakdown["side"] == "long", "trades_count"].iloc[0]), 2)

    def test_decision_abandon_when_all_variants_fail(self):
        summary = pd.DataFrame(
            {
                "symbol": ["BTCUSDT", "QQQ"],
                "tp_variant": ["midpoint", "opposite_extreme"],
                "profit_factor": [0.5, 0.9],
                "expectancy": [-0.01, 0.0],
                "total_return": [-0.2, -0.1],
            }
        )

        self.assertEqual(decide_postmortem(summary), "abandon")

    def test_markdown_generation_contains_required_sections(self):
        summary = pd.DataFrame(
            {
                "symbol": ["QQQ"],
                "market_type": ["equity"],
                "tp_variant": ["midpoint"],
                "total_return": [-0.1],
                "win_rate": [0.3],
                "profit_factor": [0.2],
                "max_drawdown": [-0.1],
                "expectancy": [-0.01],
                "trades_count": [10],
                "return_to_drawdown_ratio": [-1.0],
                "top_5_trades_pnl_share": [0.5],
                "decision": ["no_go"],
            }
        )

        markdown = render_postmortem_markdown(summary, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "abandon")

        self.assertIn("Executive Summary", markdown)
        self.assertIn("Failure Hypotheses", markdown)
        self.assertIn("Final Recommendation", markdown)
        self.assertIn("abandon", markdown)

    def _summary_fixture(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "tp_variant": ["midpoint", "opposite_extreme"],
                "total_return": [-0.1, -0.05],
                "win_rate": [0.3, 0.5],
                "profit_factor": [0.2, 0.8],
                "max_drawdown": [-0.1, -0.08],
                "expectancy": [-0.01, -0.005],
                "trades_count": [2, 2],
                "return_to_drawdown_ratio": [-1.0, -0.6],
                "top_5_trades_pnl_share": [0.5, 0.4],
                "decision": ["no_go", "no_go"],
            }
        )


if __name__ == "__main__":
    unittest.main()
