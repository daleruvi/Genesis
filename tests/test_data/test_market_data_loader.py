import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from genesis.data.market_data_loader import MarketDataLoader


class FakeExchange:
    def __init__(self, rows):
        self.rows = rows

    def fetch_ohlcv(self, symbol, timeframe, since=None, limit=500, params=None):
        if since is None:
            base = self.rows
        else:
            base = [row for row in self.rows if int(row[0]) >= int(since)]
        until = None if params is None else params.get("until")
        if until is not None:
            base = [row for row in base if int(row[0]) <= int(until)]
        return base[:limit]


class MarketDataLoaderTest(unittest.TestCase):
    def test_timeframe_to_millis(self):
        self.assertEqual(MarketDataLoader._timeframe_to_millis("5m"), 5 * 60 * 1000)
        self.assertEqual(MarketDataLoader._timeframe_to_millis("1h"), 60 * 60 * 1000)
        self.assertEqual(MarketDataLoader._timeframe_to_millis("1d"), 24 * 60 * 60 * 1000)

    def test_fetch_ohlcv_range_paginates_and_filters_end(self):
        start = pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
        rows = []
        for i in range(12):
            ts = int((start + pd.Timedelta(hours=i)).timestamp() * 1000)
            rows.append([ts, 1 + i, 2 + i, 0 + i, 1.5 + i, 100 + i])

        loader = MarketDataLoader(exchange=FakeExchange(rows))
        df = loader.fetch_ohlcv_range(
            symbol="BTC/USDT",
            timeframe="1h",
            start="2025-01-01 03:00:00",
            end="2025-01-01 09:00:00",
            limit_per_call=2,
        )

        self.assertFalse(df.empty)
        self.assertEqual(len(df), 6)  # hours 03,04,05,06,07,08
        self.assertEqual(df["timestamp"].min(), pd.Timestamp("2025-01-01 03:00:00"))
        self.assertEqual(df["timestamp"].max(), pd.Timestamp("2025-01-01 08:00:00"))


if __name__ == "__main__":
    unittest.main()
