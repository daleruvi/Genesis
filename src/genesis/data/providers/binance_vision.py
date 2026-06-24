from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests


BINANCE_VISION_BASE_URL = "https://data.binance.vision"
KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]


def normalize_binance_symbol(symbol: str) -> str:
    raw = str(symbol).strip().upper()
    if "/" not in raw:
        return raw.replace("-", "").replace("_", "")
    base = raw.split("/", 1)[0]
    quote = raw.split("/", 1)[1].split(":", 1)[0]
    return f"{base}{quote}".replace("-", "").replace("_", "")


def parse_binance_kline_csv(content: bytes | str) -> pd.DataFrame:
    source = io.BytesIO(content) if isinstance(content, bytes) else io.StringIO(content)
    df = pd.read_csv(source, header=None)
    if df.empty:
        return _empty_ohlcv()
    df = df.iloc[:, : len(KLINE_COLUMNS)].copy()
    df.columns = KLINE_COLUMNS[: len(df.columns)]
    ohlcv = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(pd.to_numeric(df["open_time"], errors="coerce"), unit="ms", utc=True).dt.tz_localize(None),
            "open": pd.to_numeric(df["open"], errors="coerce"),
            "high": pd.to_numeric(df["high"], errors="coerce"),
            "low": pd.to_numeric(df["low"], errors="coerce"),
            "close": pd.to_numeric(df["close"], errors="coerce"),
            "volume": pd.to_numeric(df["volume"], errors="coerce"),
        }
    )
    return normalize_ohlcv(ohlcv)


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return _empty_ohlcv()
    frame = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True).dt.tz_localize(None)
    for col in ["open", "high", "low", "close", "volume"]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(subset=["timestamp", "open", "high", "low", "close", "volume"])
    frame = frame.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return frame


def _empty_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])


@dataclass
class BinanceVisionProvider:
    market_type: str = "futures_um"
    base_url: str = BINANCE_VISION_BASE_URL
    session: requests.Session | None = None

    def __post_init__(self):
        if self.market_type not in {"futures_um", "spot"}:
            raise ValueError(f"Unsupported Binance Vision market_type: {self.market_type}")
        self.session = self.session or requests.Session()

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
            raise ValueError("Binance Vision range download requires an exclusive end date.")
        symbol = normalize_binance_symbol(symbol)
        start_ts = pd.to_datetime(start, utc=True)
        end_ts = pd.to_datetime(end, utc=True)
        if start_ts >= end_ts:
            raise ValueError(f"Invalid range: start ({start_ts}) must be before end ({end_ts}).")

        chunks: list[pd.DataFrame] = []
        for month_start, month_end in _month_windows(start_ts, end_ts):
            monthly = self._download_monthly(symbol, timeframe, month_start)
            if monthly is not None:
                chunks.append(monthly)
                continue
            chunks.extend(self._download_daily_range(symbol, timeframe, month_start, month_end))

        if not chunks:
            return _empty_ohlcv()
        df = normalize_ohlcv(pd.concat(chunks, ignore_index=True))
        df = df[(df["timestamp"] >= start_ts.tz_localize(None)) & (df["timestamp"] < end_ts.tz_localize(None))]
        return normalize_ohlcv(df)

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 500) -> pd.DataFrame:
        end = pd.Timestamp.utcnow().ceil("D")
        start = end - pd.Timedelta(days=max(2, int(limit)))
        df = self.fetch_ohlcv_range(symbol=symbol, timeframe=timeframe, start=str(start), end=str(end))
        return df.tail(limit).reset_index(drop=True)

    def _download_monthly(self, symbol: str, timeframe: str, month_start: pd.Timestamp) -> pd.DataFrame | None:
        name = f"{symbol}-{timeframe}-{month_start:%Y-%m}.zip"
        url = self._url("monthly", symbol, timeframe, name)
        content = self._get_zip(url, missing_ok=True)
        if content is None:
            return None
        return parse_binance_kline_csv(content)

    def _download_daily_range(self, symbol: str, timeframe: str, start: pd.Timestamp, end: pd.Timestamp) -> list[pd.DataFrame]:
        chunks: list[pd.DataFrame] = []
        cursor = start.normalize()
        while cursor < end:
            name = f"{symbol}-{timeframe}-{cursor:%Y-%m-%d}.zip"
            url = self._url("daily", symbol, timeframe, name)
            content = self._get_zip(url, missing_ok=True)
            if content is not None:
                chunks.append(parse_binance_kline_csv(content))
            cursor += pd.Timedelta(days=1)
        return chunks

    def _url(self, cadence: str, symbol: str, timeframe: str, file_name: str) -> str:
        if self.market_type == "spot":
            market_path = "spot"
        else:
            market_path = "futures/um"
        return f"{self.base_url}/data/{market_path}/{cadence}/klines/{symbol}/{timeframe}/{file_name}"

    def _get_zip(self, url: str, missing_ok: bool) -> bytes | None:
        response = self.session.get(url, timeout=60)
        if response.status_code == 404 and missing_ok:
            return None
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            csv_names = [name for name in zf.namelist() if Path(name).suffix.lower() == ".csv"]
            if not csv_names:
                return b""
            return zf.read(csv_names[0])


def _month_windows(start: pd.Timestamp, end: pd.Timestamp) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    windows = []
    cursor = start.normalize().replace(day=1)
    while cursor < end:
        next_month = (cursor + pd.offsets.MonthBegin(1)).normalize()
        window_start = max(cursor, start)
        window_end = min(next_month, end)
        if window_start < window_end:
            windows.append((window_start, window_end))
        cursor = next_month
    return windows
