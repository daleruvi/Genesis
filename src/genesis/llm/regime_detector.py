from __future__ import annotations

import pandas as pd

from genesis.backtest.performance import PerformanceAnalyzer


class RegimeDetector:
    def __init__(
        self,
        trend_threshold: float = 0.02,
        high_vol_quantile: float = 0.7,
        periods_per_year: int = 365 * 6,
    ):
        self.trend_threshold = trend_threshold
        self.high_vol_quantile = high_vol_quantile
        self.performance = PerformanceAnalyzer(periods_per_year=periods_per_year)

    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        frame = df.copy()
        if "returns_1" not in frame.columns and "close" in frame.columns:
            frame["returns_1"] = frame["close"].pct_change()
        if "volatility_20" not in frame.columns and "returns_1" in frame.columns:
            frame["volatility_20"] = frame["returns_1"].rolling(20).std()
        if "momentum_20" not in frame.columns and "close" in frame.columns:
            frame["momentum_20"] = frame["close"] / frame["close"].shift(20)
        return frame

    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        frame = self.prepare_features(df)
        trend_strength = frame["momentum_20"] - 1.0
        vol_cutoff = frame["volatility_20"].dropna().quantile(self.high_vol_quantile)
        is_high_vol = frame["volatility_20"] >= vol_cutoff

        regime = pd.Series(index=frame.index, dtype="object")
        regime.loc[(trend_strength >= self.trend_threshold) & is_high_vol] = "trend_up_high_vol"
        regime.loc[(trend_strength >= self.trend_threshold) & (~is_high_vol)] = "trend_up_low_vol"
        regime.loc[(trend_strength <= -self.trend_threshold) & is_high_vol] = "trend_down_high_vol"
        regime.loc[(trend_strength <= -self.trend_threshold) & (~is_high_vol)] = "trend_down_low_vol"
        neutral_mask = trend_strength.abs() < self.trend_threshold
        regime.loc[neutral_mask & is_high_vol] = "chop_high_vol"
        regime.loc[neutral_mask & (~is_high_vol)] = "chop_low_vol"
        regime = regime.fillna("unknown")

        frame["trend_strength"] = trend_strength
        frame["regime"] = regime
        return frame

    def regime_distribution(self, df: pd.DataFrame) -> pd.DataFrame:
        detected = self.detect(df)
        counts = detected["regime"].value_counts(dropna=False).rename_axis("regime").reset_index(name="count")
        counts["ratio"] = counts["count"] / counts["count"].sum()
        return counts

    def performance_by_regime(
        self,
        returns: pd.Series,
        regimes: pd.Series,
        turnover: pd.Series | None = None,
    ) -> pd.DataFrame:
        frame = pd.DataFrame({"returns": returns, "regime": regimes}).dropna(subset=["regime"])
        if turnover is not None:
            frame["turnover"] = turnover.reindex(frame.index).fillna(0.0)

        rows: list[dict] = []
        for regime_name, group in frame.groupby("regime"):
            summary = self.performance.summarize(
                group["returns"],
                turnover=group.get("turnover"),
            )
            rows.append(
                {
                    "regime": regime_name,
                    "observations": int(len(group)),
                    **summary,
                }
            )

        return pd.DataFrame(rows).sort_values("regime").reset_index(drop=True)
