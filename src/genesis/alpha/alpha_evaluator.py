import pandas as pd
import numpy as np


class AlphaEvaluator:

    def __init__(self, df, forward_returns):

        self.df = df
        self.forward_returns = forward_returns

    def information_coefficient(self, alpha):

        aligned = pd.concat(
            [alpha, self.forward_returns],
            axis=1
        ).dropna()

        ic = aligned.corr().iloc[0, 1]

        return ic

    def sharpe_ratio(self, strategy_returns):

        mean = strategy_returns.mean()
        std = strategy_returns.std()

        if std == 0:
            return 0

        sharpe = mean / std

        return sharpe

    def max_drawdown(self, strategy_returns):

        cumulative = (1 + strategy_returns).cumprod()

        peak = cumulative.cummax()

        drawdown = (cumulative - peak) / peak

        return drawdown.min()

    def evaluate_alpha(self, alpha):

        ic = self.information_coefficient(alpha)

        strategy_returns = alpha.shift(1) * self.forward_returns

        sharpe = self.sharpe_ratio(strategy_returns)

        mdd = self.max_drawdown(strategy_returns)

        return {
            "IC": ic,
            "Sharpe": sharpe,
            "MaxDrawdown": mdd
        }