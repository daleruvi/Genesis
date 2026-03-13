import math

import numpy as np
import pandas as pd


class PerformanceAnalyzer:
    def __init__(self, periods_per_year=365 * 6):
        self.periods_per_year = periods_per_year

    def cumulative_curve(self, returns: pd.Series) -> pd.Series:
        returns = returns.fillna(0.0)
        return (1.0 + returns).cumprod()

    def total_return(self, returns: pd.Series) -> float:
        curve = self.cumulative_curve(returns)
        return float(curve.iloc[-1] - 1.0) if len(curve) else 0.0

    def annualized_return(self, returns: pd.Series) -> float:
        returns = returns.fillna(0.0)
        if len(returns) == 0:
            return 0.0
        total = self.total_return(returns)
        years = len(returns) / float(self.periods_per_year)
        if years <= 0:
            return 0.0
        if total <= -1.0:
            return -1.0
        return float((1.0 + total) ** (1.0 / years) - 1.0)

    def sharpe_ratio(self, returns: pd.Series) -> float:
        returns = returns.fillna(0.0)
        std = returns.std()
        if std == 0 or math.isnan(std):
            return 0.0
        return float((returns.mean() / std) * np.sqrt(self.periods_per_year))

    def max_drawdown(self, returns: pd.Series) -> float:
        curve = self.cumulative_curve(returns)
        if len(curve) == 0:
            return 0.0
        peak = curve.cummax()
        drawdown = (curve / peak) - 1.0
        return float(drawdown.min())

    def win_rate(self, returns: pd.Series) -> float:
        returns = returns.dropna()
        non_zero = returns[returns != 0]
        if len(non_zero) == 0:
            return 0.0
        return float((non_zero > 0).mean())

    def profit_factor(self, returns: pd.Series) -> float:
        returns = returns.fillna(0.0)
        gains = returns[returns > 0].sum()
        losses = -returns[returns < 0].sum()
        if losses == 0:
            return float("inf") if gains > 0 else 0.0
        return float(gains / losses)

    def summarize(self, returns: pd.Series, turnover: pd.Series | None = None) -> dict:
        summary = {
            "total_return": self.total_return(returns),
            "annualized_return": self.annualized_return(returns),
            "sharpe": self.sharpe_ratio(returns),
            "max_drawdown": self.max_drawdown(returns),
            "win_rate": self.win_rate(returns),
            "profit_factor": self.profit_factor(returns),
        }
        if turnover is not None:
            summary["avg_turnover"] = float(turnover.fillna(0.0).mean())
            summary["total_turnover"] = float(turnover.fillna(0.0).sum())
        return summary
