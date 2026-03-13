import numpy as np
import pandas as pd


class Operators:

    @staticmethod
    def rank(series):
        return series.rank(pct=True)

    @staticmethod
    def zscore(series, window=20):
        mean = series.rolling(window).mean()
        std = series.rolling(window).std()
        return (series - mean) / std

    @staticmethod
    def delta(series, period=1):
        return series.diff(period)

    @staticmethod
    def ts_mean(series, window=10):
        return series.rolling(window).mean()

    @staticmethod
    def ts_std(series, window=10):
        return series.rolling(window).std()

    @staticmethod
    def ts_rank(series, window=10):
        return series.rolling(window).apply(lambda x: pd.Series(x).rank().iloc[-1])

    @staticmethod
    def normalize(series):
        return (series - series.min()) / (series.max() - series.min())