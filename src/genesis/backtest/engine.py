import numpy as np
import pandas as pd


class BacktestEngine:
    def __init__(self, fee_rate=0.0005, slippage_rate=0.0005):
        self.fee_rate = fee_rate
        self.slippage_rate = slippage_rate

    def signal_to_positions(self, signal: pd.Series, mode="sign", lower_q=0.3, upper_q=0.7) -> pd.Series:
        signal = signal.astype(float)
        if mode == "sign":
            return signal.apply(lambda x: 1.0 if x > 0 else (-1.0 if x < 0 else 0.0))

        if mode == "quantile":
            lower = signal.quantile(lower_q)
            upper = signal.quantile(upper_q)
            return signal.apply(
                lambda x: 1.0 if x >= upper else (-1.0 if x <= lower else 0.0)
            )

        raise ValueError(f"Unsupported signal conversion mode: {mode}")

    def run(self, close: pd.Series, positions: pd.Series) -> pd.DataFrame:
        close = close.astype(float)
        positions = positions.astype(float).reindex(close.index).fillna(0.0)
        asset_returns = close.pct_change().fillna(0.0)
        shifted_positions = positions.shift(1).fillna(0.0)
        turnover = positions.diff().abs().fillna(positions.abs())
        costs = turnover * (self.fee_rate + self.slippage_rate)
        strategy_returns = (shifted_positions * asset_returns) - costs
        benchmark_returns = asset_returns

        result = pd.DataFrame(
            {
                "close": close,
                "asset_returns": asset_returns,
                "positions": positions,
                "shifted_positions": shifted_positions,
                "turnover": turnover,
                "costs": costs,
                "strategy_returns": strategy_returns,
                "benchmark_returns": benchmark_returns,
            }
        )
        return result

    def train_test_split(self, df: pd.DataFrame, train_ratio=0.7) -> tuple[pd.DataFrame, pd.DataFrame]:
        split_idx = max(1, int(len(df) * train_ratio))
        train = df.iloc[:split_idx].copy()
        test = df.iloc[split_idx:].copy()
        return train, test
