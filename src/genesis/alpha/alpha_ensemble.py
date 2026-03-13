from __future__ import annotations

import math

import pandas as pd


class AlphaEnsemble:
    def __init__(
        self,
        rankings: pd.DataFrame,
        regime_performance: pd.DataFrame,
        signal_threshold: float = 0.15,
        fallback_multiplier: float = 0.15,
        min_test_sharpe: float = 0.0,
        min_test_profit_factor: float = 1.0,
        max_test_turnover: float = 250.0,
    ):
        self.rankings = rankings.copy()
        self.regime_performance = regime_performance.copy()
        self.signal_threshold = float(signal_threshold)
        self.fallback_multiplier = float(fallback_multiplier)
        self.min_test_sharpe = float(min_test_sharpe)
        self.min_test_profit_factor = float(min_test_profit_factor)
        self.max_test_turnover = float(max_test_turnover)
        self.filtered_rankings = self._filter_rankings(self.rankings)

    def _filter_rankings(self, rankings: pd.DataFrame) -> pd.DataFrame:
        if rankings.empty:
            return rankings.copy()

        frame = rankings.copy()
        for col in ("test_sharpe", "test_profit_factor", "test_total_turnover"):
            if col not in frame.columns:
                frame[col] = 0.0

        mask = (
            (frame["test_sharpe"] > self.min_test_sharpe)
            & (frame["test_profit_factor"] > self.min_test_profit_factor)
            & (frame["test_total_turnover"] <= self.max_test_turnover)
        )
        filtered = frame.loc[mask].copy()
        if filtered.empty:
            return filtered
        return filtered.sort_values(["selection_rank", "score"], ascending=[True, False]).reset_index(drop=True)

    def _base_weight(self, row: pd.Series) -> float:
        score = float(row.get("score", 0.0) or 0.0)
        test_sharpe = float(row.get("test_sharpe", 0.0) or 0.0)
        temporal = float(row.get("temporal_consistency", 0.0) or 0.0)
        return max(0.0, score) + max(0.0, test_sharpe) * 0.05 + max(0.0, temporal) * 0.1

    def _regime_multiplier(self, alpha_name: str, regime: str) -> float:
        match = self.regime_performance[
            (self.regime_performance["alpha"] == alpha_name)
            & (self.regime_performance["regime"] == regime)
        ]
        if match.empty:
            return 0.0

        row = match.iloc[0]
        sharpe = float(row.get("sharpe", 0.0) or 0.0)
        total_return = float(row.get("total_return", 0.0) or 0.0)
        profit_factor = float(row.get("profit_factor", 0.0) or 0.0)
        if sharpe <= 0 or total_return <= 0:
            return 0.0

        pf_bonus = min(max(profit_factor, 0.0), 3.0) / 3.0
        return max(0.0, sharpe) * (1.0 + max(0.0, total_return)) * (1.0 + pf_bonus)

    def decide(self, latest_alphas: pd.Series, regime: str) -> dict:
        votes = []
        weighted_sum = 0.0
        total_weight = 0.0
        used_fallback = False

        for _, row in self.filtered_rankings.iterrows():
            alpha_name = row["alpha"]
            if alpha_name not in latest_alphas.index:
                continue

            raw_value = latest_alphas[alpha_name]
            if pd.isna(raw_value):
                continue

            direction = 1 if raw_value > 0 else (-1 if raw_value < 0 else 0)
            if direction == 0:
                continue

            base_weight = self._base_weight(row)
            regime_multiplier = self._regime_multiplier(alpha_name, regime)
            if regime_multiplier <= 0:
                regime_multiplier = self.fallback_multiplier
                used_fallback = True
            weight = base_weight * regime_multiplier
            if weight <= 0 or math.isnan(weight):
                continue

            vote = {
                "alpha": alpha_name,
                "raw_value": float(raw_value),
                "direction": "long" if direction > 0 else "short",
                "base_weight": base_weight,
                "regime_multiplier": regime_multiplier,
                "weight": weight,
            }
            votes.append(vote)
            weighted_sum += direction * weight
            total_weight += weight

        conviction = (weighted_sum / total_weight) if total_weight > 0 else 0.0
        if conviction >= self.signal_threshold:
            signal = "long"
        elif conviction <= -self.signal_threshold:
            signal = "short"
        else:
            signal = "flat"

        return {
            "signal": signal,
            "regime": regime,
            "conviction": conviction,
            "votes_used": len(votes),
            "total_weight": total_weight,
            "used_fallback": used_fallback,
            "eligible_alphas": self.filtered_rankings["alpha"].tolist() if "alpha" in self.filtered_rankings else [],
            "votes": votes,
        }
