import io
import sys
import unittest
import zipfile
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from genesis.data.providers.binance_vision import (  # noqa: E402
    BinanceVisionProvider,
    normalize_binance_symbol,
    parse_binance_kline_csv,
)


def zipped_csv(text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("sample.csv", text)
    return buffer.getvalue()


class FakeResponse:
    def __init__(self, status_code: int, content: bytes = b""):
        self.status_code = status_code
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.urls = []

    def get(self, url, timeout=60):
        self.urls.append(url)
        if not self.responses:
            return FakeResponse(404)
        return self.responses.pop(0)


class BinanceVisionProviderTest(unittest.TestCase):
    def test_normalize_symbol_for_futures(self):
        self.assertEqual(normalize_binance_symbol("BTC/USDT:USDT"), "BTCUSDT")
        self.assertEqual(normalize_binance_symbol("BTCUSDT"), "BTCUSDT")

    def test_parse_kline_csv_to_standard_ohlcv(self):
        csv = "\n".join(
            [
                "1735689900000,2,3,1,2.5,20,1735690199999,0,0,0,0,0",
                "1735689600000,1,2,0.5,1.5,10,1735689899999,0,0,0,0,0",
                "1735689600000,1,2,0.5,1.5,10,1735689899999,0,0,0,0,0",
            ]
        )

        df = parse_binance_kline_csv(csv)

        self.assertEqual(df.columns.tolist(), ["timestamp", "open", "high", "low", "close", "volume"])
        self.assertEqual(len(df), 2)
        self.assertEqual(df["timestamp"].tolist(), sorted(df["timestamp"].tolist()))
        self.assertEqual(df.iloc[0]["timestamp"], pd.Timestamp("2025-01-01 00:00:00"))
        self.assertTrue(pd.api.types.is_numeric_dtype(df["close"]))

    def test_fetch_range_uses_monthly_zip(self):
        csv = "1735689600000,1,2,0.5,1.5,10,1735689899999,0,0,0,0,0"
        session = FakeSession([FakeResponse(200, zipped_csv(csv))])
        provider = BinanceVisionProvider(session=session)

        df = provider.fetch_ohlcv_range(
            symbol="BTC/USDT:USDT",
            timeframe="5m",
            start="2025-01-01",
            end="2025-02-01",
        )

        self.assertEqual(len(df), 1)
        self.assertIn("/data/futures/um/monthly/klines/BTCUSDT/5m/BTCUSDT-5m-2025-01.zip", session.urls[0])


if __name__ == "__main__":
    unittest.main()
