import unittest
from unittest.mock import patch

import pandas as pd
import requests

from genesis.data.providers.polygon import (
    PolygonConfigurationError,
    PolygonRateLimitConfig,
    PolygonRateLimitError,
    PolygonStocksProvider,
    parse_polygon_aggregates,
    polygon_timeframe,
    sanitize_polygon_url,
)


class FakeResponse:
    def __init__(self, payload, status_code=200, headers=None, url="https://api.polygon.io/test?apiKey=secret"):
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.url = url

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error for url: {self.url}")


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=60):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        response = self.responses.pop(0)
        return response if isinstance(response, FakeResponse) else FakeResponse(response)


class PolygonProviderTest(unittest.TestCase):
    def test_parse_aggregates_to_standard_ohlcv_sorted_and_deduped(self):
        payload = {
            "results": [
                {"t": 1735689900000, "o": "2", "h": "3", "l": "1", "c": "2.5", "v": "20"},
                {"t": 1735689600000, "o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10"},
                {"t": 1735689600000, "o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10"},
            ]
        }

        df = parse_polygon_aggregates(payload)

        self.assertEqual(df.columns.tolist(), ["timestamp", "open", "high", "low", "close", "volume"])
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]["timestamp"], pd.Timestamp("2025-01-01 00:00:00"))
        self.assertTrue(pd.api.types.is_numeric_dtype(df["close"]))

    def test_timeframe_mapping(self):
        self.assertEqual(polygon_timeframe("5m"), (5, "minute"))
        self.assertEqual(polygon_timeframe("1d"), (1, "day"))
        with self.assertRaisesRegex(ValueError, "only supports"):
            polygon_timeframe("1h")

    def test_provider_requires_api_key_and_stocks_market_type(self):
        with self.assertRaisesRegex(ValueError, "market_type=stocks"):
            PolygonStocksProvider(market_type="indices", api_key="x")
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(PolygonConfigurationError):
                PolygonStocksProvider(api_key="")

    def test_basic_rate_limit_config_uses_safety_margin(self):
        with patch.dict(
            "os.environ",
            {
                "POLYGON_PLAN": "basic",
                "POLYGON_RATE_LIMIT_SAFETY_MARGIN": "1",
                "POLYGON_MAX_RETRIES": "5",
                "POLYGON_BACKOFF_BASE_SECONDS": "15",
            },
            clear=True,
        ):
            config = PolygonRateLimitConfig.from_env()

        self.assertEqual(config.calls_per_minute, 5)
        self.assertEqual(config.effective_calls_per_minute, 4)
        self.assertGreaterEqual(config.sleep_seconds, 15)

    def test_rate_limit_override_and_non_basic_plan(self):
        with patch.dict("os.environ", {"POLYGON_PLAN": "starter"}, clear=True):
            starter = PolygonRateLimitConfig.from_env()
        with patch.dict(
            "os.environ",
            {"POLYGON_PLAN": "starter", "POLYGON_RATE_LIMIT_CALLS_PER_MINUTE": "10", "POLYGON_RATE_LIMIT_SAFETY_MARGIN": "2"},
            clear=True,
        ):
            override = PolygonRateLimitConfig.from_env()

        self.assertEqual(starter.sleep_seconds, 0)
        self.assertEqual(override.effective_calls_per_minute, 8)
        self.assertEqual(override.sleep_seconds, 7.5)

    def test_fetch_range_uses_adjusted_true_and_preserves_next_url_auth(self):
        first = {
            "results": [{"t": 1735689600000, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 10}],
            "next_url": "https://api.polygon.io/v2/aggs/ticker/ABC/range/5/minute/2025-01-01/2025-01-01?cursor=abc",
        }
        second = {
            "results": [{"t": 1735689900000, "o": 2, "h": 3, "l": 1.5, "c": 2.5, "v": 20}],
        }
        session = FakeSession([first, second])
        rate_limit = PolygonRateLimitConfig(sleep_seconds=0, max_retries=0)
        provider = PolygonStocksProvider(api_key="secret", session=session, rate_limit=rate_limit)

        df = provider.fetch_ohlcv_range("ABC", "5m", "2025-01-01", "2025-01-02")

        self.assertEqual(len(df), 2)
        self.assertEqual(session.calls[0]["params"]["adjusted"], "true")
        self.assertEqual(session.calls[0]["params"]["apiKey"], "secret")
        self.assertIsNone(session.calls[1]["params"])
        self.assertIn("cursor=abc", session.calls[1]["url"])
        self.assertIn("apiKey=secret", session.calls[1]["url"])

    def test_429_retry_uses_retry_after(self):
        sleeps = []
        session = FakeSession(
            [
                FakeResponse({}, status_code=429, headers={"Retry-After": "2"}),
                FakeResponse({"results": [{"t": 1735689600000, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 10}]}),
            ]
        )
        rate_limit = PolygonRateLimitConfig(sleep_seconds=0, max_retries=1, backoff_base_seconds=15)
        provider = PolygonStocksProvider(api_key="secret", session=session, rate_limit=rate_limit, sleeper=sleeps.append)

        df = provider.fetch_ohlcv_range("QQQ", "5m", "2025-01-01", "2025-01-02")

        self.assertEqual(len(df), 1)
        self.assertIn(2.0, sleeps)

    def test_429_retry_uses_exponential_backoff_without_retry_after(self):
        sleeps = []
        session = FakeSession(
            [
                FakeResponse({}, status_code=429),
                FakeResponse({}, status_code=429),
                FakeResponse({"results": [{"t": 1735689600000, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 10}]}),
            ]
        )
        rate_limit = PolygonRateLimitConfig(sleep_seconds=0, max_retries=2, backoff_base_seconds=3)
        provider = PolygonStocksProvider(api_key="secret", session=session, rate_limit=rate_limit, sleeper=sleeps.append)

        provider.fetch_ohlcv_range("QQQ", "5m", "2025-01-01", "2025-01-02")

        self.assertEqual(sleeps, [3.0, 6.0])

    def test_rate_limit_error_is_sanitized_after_retries(self):
        session = FakeSession([FakeResponse({}, status_code=429, url="https://api.polygon.io/test?apiKey=secret")])
        rate_limit = PolygonRateLimitConfig(sleep_seconds=0, max_retries=0, backoff_base_seconds=1)
        provider = PolygonStocksProvider(api_key="secret", session=session, rate_limit=rate_limit, sleeper=lambda _: None)

        with self.assertRaises(PolygonRateLimitError) as ctx:
            provider.fetch_ohlcv_range("QQQ", "5m", "2025-01-01", "2025-01-02")

        self.assertIn("rate limit", str(ctx.exception))
        self.assertNotIn("secret", str(ctx.exception))

    def test_sanitize_polygon_url_redacts_api_key(self):
        sanitized = sanitize_polygon_url("https://api.polygon.io/path?cursor=x&apiKey=secret")

        self.assertEqual(sanitized, "https://api.polygon.io/path?cursor=x&apiKey=***REDACTED***")


if __name__ == "__main__":
    unittest.main()
