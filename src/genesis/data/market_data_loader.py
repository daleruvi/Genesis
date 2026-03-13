import pandas as pd

from genesis.config.settings import BINGX_DEFAULT_SYMBOL, BINGX_DEFAULT_TIMEFRAME
from genesis.data.providers.bingx_client import create_bingx_sync_client


class MarketDataLoader:
    def __init__(self, exchange=None):
        self.exchange = exchange or create_bingx_sync_client(public_only=True)

    def fetch_ohlcv(self, symbol=None, timeframe=None, limit=500):
        symbol = symbol or BINGX_DEFAULT_SYMBOL
        timeframe = timeframe or BINGX_DEFAULT_TIMEFRAME

        data = self.exchange.fetch_ohlcv(
            symbol,
            timeframe,
            limit=limit,
        )

        df = pd.DataFrame(
            data,
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ],
        )

        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df

    def fetch_ohlcv_range(
        self,
        symbol=None,
        timeframe=None,
        start=None,
        end=None,
        limit_per_call=1000,
        max_calls=5000,
    ):
        symbol = symbol or BINGX_DEFAULT_SYMBOL
        timeframe = timeframe or BINGX_DEFAULT_TIMEFRAME
        if start is None:
            raise ValueError("`start` is required for range download.")

        since_ms = self._to_millis(start)
        end_ms = self._to_millis(end) if end is not None else None
        tf_ms = self._timeframe_to_millis(timeframe)
        current_limit = max(1, int(limit_per_call))
        wide_skip_ms = 30 * 24 * 60 * 60 * 1000

        rows = []
        calls = 0
        last_seen = None

        while True:
            if max_calls and calls >= max_calls:
                break

            until_ms = since_ms + (current_limit * tf_ms) - 1
            if end_ms is not None:
                until_ms = min(until_ms, end_ms - 1)

            try:
                batch = self.exchange.fetch_ohlcv(
                    symbol,
                    timeframe,
                    since=since_ms,
                    limit=current_limit,
                    params={"until": until_ms},
                )
            except Exception as exc:
                msg = str(exc).lower()
                if "kline not found" in msg:
                    # Some BingX routes return "kline not found" for narrow windows with endTime.
                    # Retry without `until` first; if still empty/unavailable, move to next window.
                    try:
                        batch = self.exchange.fetch_ohlcv(
                            symbol,
                            timeframe,
                            since=since_ms,
                            limit=current_limit,
                        )
                    except Exception as retry_exc:
                        retry_msg = str(retry_exc).lower()
                        if "100204" in retry_msg or "date of query is too wide" in retry_msg:
                            if current_limit > 1:
                                current_limit = max(1, current_limit // 2)
                            else:
                                since_ms = since_ms + max(tf_ms, wide_skip_ms)
                                if end_ms is not None and since_ms >= end_ms:
                                    break
                            continue
                        if "kline not found" in retry_msg:
                            since_ms = since_ms + (current_limit * tf_ms)
                            if end_ms is not None and since_ms >= end_ms:
                                break
                            continue
                        raise
                    # Fallback succeeded, continue processing this batch.
                elif "100204" in msg or "date of query is too wide" in msg:
                    # BingX may still reject old windows even with small limits.
                    # First shrink request size; once at 1, jump forward in time.
                    if current_limit > 1:
                        current_limit = max(1, current_limit // 2)
                    else:
                        since_ms = since_ms + max(tf_ms, wide_skip_ms)
                        if end_ms is not None and since_ms >= end_ms:
                            break
                    continue
                else:
                    raise
            calls += 1
            if not batch:
                break

            raw_last_ts = int(batch[-1][0])
            filtered = batch
            if end_ms is not None:
                filtered = [row for row in filtered if int(row[0]) < end_ms]
            if filtered:
                rows.extend(filtered)

            next_since = raw_last_ts + tf_ms
            if end_ms is not None and next_since >= end_ms:
                break
            if last_seen is not None and raw_last_ts <= last_seen:
                break
            last_seen = raw_last_ts
            since_ms = next_since

        df = pd.DataFrame(
            rows,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        if df.empty:
            return df

        df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        if end is not None:
            end_ts = pd.to_datetime(end, utc=True).tz_localize(None) if pd.Timestamp(end).tz is not None else pd.to_datetime(end)
            df = df[df["timestamp"] < end_ts]
        return df.reset_index(drop=True)

    @staticmethod
    def _to_millis(value):
        ts = pd.to_datetime(value, utc=True)
        return int(ts.timestamp() * 1000)

    @staticmethod
    def _timeframe_to_millis(timeframe: str) -> int:
        tf = str(timeframe).strip().lower()
        if len(tf) < 2:
            raise ValueError(f"Invalid timeframe: {timeframe}")
        unit = tf[-1]
        amount = int(tf[:-1])
        if unit == "m":
            return amount * 60 * 1000
        if unit == "h":
            return amount * 60 * 60 * 1000
        if unit == "d":
            return amount * 24 * 60 * 60 * 1000
        if unit == "w":
            return amount * 7 * 24 * 60 * 60 * 1000
        raise ValueError(f"Unsupported timeframe unit: {timeframe}")
