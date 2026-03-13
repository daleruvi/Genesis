import pandas as pd
import numpy as np


class Indicators:

    @staticmethod
    def sma(series, period=20):
        return series.rolling(period).mean()

    @staticmethod
    def ema(series, period=20):
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def rsi(close, period=14):

        delta = close.diff()

        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()

        rs = avg_gain / avg_loss

        rsi = 100 - (100 / (1 + rs))

        return rsi

    @staticmethod
    def macd(close, fast=12, slow=26, signal=9):

        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()

        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()

        histogram = macd_line - signal_line

        return macd_line, signal_line, histogram

    @staticmethod
    def atr(df, period=14):

        high = df["high"]
        low = df["low"]
        close = df["close"]

        prev_close = close.shift(1)

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = tr.rolling(period).mean()

        return atr

    @staticmethod
    def bollinger_bands(close, period=20, std=2):

        sma = close.rolling(period).mean()
        deviation = close.rolling(period).std()

        upper = sma + std * deviation
        lower = sma - std * deviation

        return sma, upper, lower

    @staticmethod
    def momentum(close, period=10):

        return close / close.shift(period)

    @staticmethod
    def volatility(close, period=20):

        returns = close.pct_change()

        return returns.rolling(period).std()