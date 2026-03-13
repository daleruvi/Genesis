import pandas as pd
import numpy as np


class FeatureEngineer:

    def __init__(self, df):
        self.df = df.copy()

    def returns(self):

        self.df["returns_1"] = self.df["close"].pct_change(1)
        self.df["returns_5"] = self.df["close"].pct_change(5)
        self.df["returns_10"] = self.df["close"].pct_change(10)

        return self

    def volatility(self):

        self.df["volatility_10"] = self.df["returns_1"].rolling(10).std()
        self.df["volatility_20"] = self.df["returns_1"].rolling(20).std()

        return self

    def momentum(self):

        self.df["momentum_10"] = self.df["close"] / self.df["close"].shift(10)
        self.df["momentum_20"] = self.df["close"] / self.df["close"].shift(20)

        return self

    def volume_features(self):

        self.df["volume_ma20"] = self.df["volume"].rolling(20).mean()
        self.df["volume_ratio"] = self.df["volume"] / self.df["volume_ma20"]

        return self

    def build(self):

        self.df = self.df.dropna()

        return self.df