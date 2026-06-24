from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pandas as pd
import requests


POLYGON_BASE_URL = "https://api.polygon.io"


class PolygonConfigurationError(ValueError):
    pass


class PolygonRateLimitError(RuntimeError):
    pass


class PolygonHTTPError(RuntimeError):
    pass


def sanitize_polygon_url(value: str) -> str:
    return re.sub(r"(apiKey=)[^&\s)]+", r"\1***REDACTED***", str(value))


def _extract_api_key(value: str) -> str | None:
    parts = urlsplit(str(value))
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    return query.get("apiKey")


def polygon_timeframe(timeframe: str) -> tuple[int, str]:
    tf = str(timeframe).strip().lower()
    if tf == "5m":
        return 5, "minute"
    if tf == "1d":
        return 1, "day"
    raise ValueError(f"Polygon provider only supports 5m and 1d for this phase, got: {timeframe}")


def parse_polygon_aggregates(payload: dict) -> pd.DataFrame:
    rows = payload.get("results") or []
    if not rows:
        return _empty_ohlcv()
    frame = pd.DataFrame(rows)
    required = {"t", "o", "h", "l", "c", "v"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Polygon aggregate response missing fields: {sorted(missing)}")
    ohlcv = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(pd.to_numeric(frame["t"], errors="coerce"), unit="ms", utc=True).dt.tz_localize(None),
            "open": pd.to_numeric(frame["o"], errors="coerce"),
            "high": pd.to_numeric(frame["h"], errors="coerce"),
            "low": pd.to_numeric(frame["l"], errors="coerce"),
            "close": pd.to_numeric(frame["c"], errors="coerce"),
            "volume": pd.to_numeric(frame["v"], errors="coerce"),
        }
    )
    return normalize_ohlcv(ohlcv)


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return _empty_ohlcv()
    frame = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True).dt.tz_localize(None)
    for column in ["open", "high", "low", "close", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["timestamp", "open", "high", "low", "close", "volume"])
    return frame.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)


def _empty_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])


@dataclass
class PolygonRateLimitConfig:
    plan: str = "basic"
    calls_per_minute: int = 5
    safety_margin: int = 1
    effective_calls_per_minute: int = 4
    sleep_seconds: float = 15.0
    max_retries: int = 5
    backoff_base_seconds: float = 15.0

    @classmethod
    def from_env(cls) -> "PolygonRateLimitConfig":
        plan = os.getenv("POLYGON_PLAN", "basic").strip().lower()
        safety_margin = _env_int("POLYGON_RATE_LIMIT_SAFETY_MARGIN", 1)
        max_retries = _env_int("POLYGON_MAX_RETRIES", 5)
        backoff_base = _env_float("POLYGON_BACKOFF_BASE_SECONDS", 15.0)
        override_calls = os.getenv("POLYGON_RATE_LIMIT_CALLS_PER_MINUTE", "").strip()
        if override_calls:
            calls_per_minute = int(override_calls)
        elif plan == "basic":
            calls_per_minute = 5
        else:
            calls_per_minute = 0
        effective = max(0, calls_per_minute - safety_margin) if calls_per_minute > 0 else 0
        sleep = 60.0 / effective if effective > 0 else 0.0
        return cls(
            plan=plan,
            calls_per_minute=calls_per_minute,
            safety_margin=safety_margin,
            effective_calls_per_minute=effective,
            sleep_seconds=sleep,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base,
        )


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    return default if value == "" else int(value)


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name, "").strip()
    return default if value == "" else float(value)


class PolygonRateLimiter:
    def __init__(self, sleep_seconds: float, sleeper=time.sleep):
        self.sleep_seconds = float(sleep_seconds)
        self.sleeper = sleeper
        self._has_seen_request = False

    def wait(self) -> None:
        if self.sleep_seconds <= 0:
            return
        if self._has_seen_request:
            self.sleeper(self.sleep_seconds)
        self._has_seen_request = True


@dataclass
class PolygonStocksProvider:
    market_type: str = "stocks"
    api_key: str | None = None
    session: requests.Session | None = None
    base_url: str = POLYGON_BASE_URL
    rate_limit: PolygonRateLimitConfig | None = None
    sleeper: object = time.sleep

    def __post_init__(self):
        if self.market_type != "stocks":
            raise ValueError("Polygon provider in Fase 2.8 supports only market_type=stocks.")
        self.api_key = self.api_key or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise PolygonConfigurationError("POLYGON_API_KEY is required for provider=polygon; phase status is no_data.")
        self.session = self.session or requests.Session()
        self.rate_limit = self.rate_limit or PolygonRateLimitConfig.from_env()
        self.rate_limiter = PolygonRateLimiter(self.rate_limit.sleep_seconds, sleeper=self.sleeper)

    def fetch_ohlcv_range(
        self,
        symbol: str,
        timeframe: str,
        start: str,
        end: str | None = None,
        limit_per_call: int | None = None,
    ) -> pd.DataFrame:
        del limit_per_call
        if end is None:
            raise ValueError("Polygon range download requires an exclusive end date.")
        multiplier, timespan = polygon_timeframe(timeframe)
        from_date = pd.to_datetime(start, utc=True).strftime("%Y-%m-%d")
        # Polygon date ranges are inclusive by date; subtract one day from exclusive end.
        to_date = (pd.to_datetime(end, utc=True) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        url = f"{self.base_url}/v2/aggs/ticker/{str(symbol).upper()}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": "50000",
            "apiKey": self.api_key,
        }
        chunks = []
        next_url = url
        next_params = params
        while next_url:
            payload = self._request_json(next_url, next_params)
            chunks.append(parse_polygon_aggregates(payload))
            next_url = payload.get("next_url")
            next_params = None
            if next_url:
                next_url = self._with_api_key(next_url)
        if not chunks:
            return _empty_ohlcv()
        frame = normalize_ohlcv(pd.concat(chunks, ignore_index=True))
        start_ts = pd.to_datetime(start, utc=True).tz_localize(None)
        end_ts = pd.to_datetime(end, utc=True).tz_localize(None)
        frame = frame[(frame["timestamp"] >= start_ts) & (frame["timestamp"] < end_ts)]
        return normalize_ohlcv(frame)

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 500) -> pd.DataFrame:
        end = pd.Timestamp.utcnow().ceil("D")
        start = end - pd.Timedelta(days=max(2, int(limit)))
        df = self.fetch_ohlcv_range(symbol=symbol, timeframe=timeframe, start=str(start), end=str(end))
        return df.tail(limit).reset_index(drop=True)

    def _with_api_key(self, url: str) -> str:
        parts = urlsplit(url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        query["apiKey"] = str(self.api_key)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))

    def _request_json(self, url: str, params: dict | None) -> dict:
        last_status = None
        for attempt in range(0, int(self.rate_limit.max_retries) + 1):
            self.rate_limiter.wait()
            response = self.session.get(url, params=params, timeout=60)
            last_status = getattr(response, "status_code", None)
            if last_status == 429:
                if attempt >= int(self.rate_limit.max_retries):
                    raise PolygonRateLimitError(
                        f"Polygon rate limit persisted after {self.rate_limit.max_retries} retries for "
                        f"{sanitize_polygon_url(getattr(response, 'url', url))}"
                    )
                retry_after = getattr(response, "headers", {}).get("Retry-After")
                wait_seconds = float(retry_after) if retry_after else float(self.rate_limit.backoff_base_seconds) * (2 ** attempt)
                self.sleeper(wait_seconds)
                continue
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                raise PolygonHTTPError(sanitize_polygon_url(str(exc))) from exc
            except Exception as exc:
                raise PolygonHTTPError(sanitize_polygon_url(str(exc))) from exc
            return response.json()
        raise PolygonRateLimitError(f"Polygon request failed with status={last_status}")
