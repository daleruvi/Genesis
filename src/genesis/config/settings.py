import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
FEATURE_STORE_DIR = DATA_DIR / "feature_store"
ALPHA_STORE_DIR = DATA_DIR / "alpha_store"
JOURNAL_DIR = DATA_DIR / "journals"
ENV_FILE = PROJECT_ROOT / ".env"


def _load_dotenv(env_file: Path) -> None:
    if not env_file.exists():
        return

    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    return float(value)


def env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


_load_dotenv(ENV_FILE)


TRADING_STYLE = env_str("TRADING_STYLE", "swing").strip().lower()
if TRADING_STYLE not in {"swing", "day"}:
    TRADING_STYLE = "swing"

if TRADING_STYLE == "day":
    _default_trading_timeframe = "15m"
    _default_trading_ohlcv_limit = 500
    _default_trading_interval_seconds = 300
    _default_trading_session_enabled = True
    _default_trading_session_start = "09:30"
    _default_trading_session_end = "16:00"
    _default_trading_weekdays_only = True
    _default_trading_max_trades_per_day = 20
    _default_trading_max_daily_loss_usdt = 50.0
    _default_trading_stop_loss_pct = 0.008
    _default_trading_take_profit_pct = 0.015
else:
    _default_trading_timeframe = "4h"
    _default_trading_ohlcv_limit = 300
    _default_trading_interval_seconds = 1800
    _default_trading_session_enabled = False
    _default_trading_session_start = "00:00"
    _default_trading_session_end = "23:59"
    _default_trading_weekdays_only = False
    _default_trading_max_trades_per_day = 6
    _default_trading_max_daily_loss_usdt = 80.0
    _default_trading_stop_loss_pct = 0.02
    _default_trading_take_profit_pct = 0.04

TRADING_DATA_TIMEFRAME = env_str("TRADING_DATA_TIMEFRAME", _default_trading_timeframe)
TRADING_OHLCV_LIMIT = env_int("TRADING_OHLCV_LIMIT", _default_trading_ohlcv_limit)
TRADING_LOOP_INTERVAL_SECONDS = env_int("TRADING_LOOP_INTERVAL_SECONDS", _default_trading_interval_seconds)
TRADING_SESSION_ENABLED = env_bool("TRADING_SESSION_ENABLED", _default_trading_session_enabled)
TRADING_SESSION_TIMEZONE = env_str("TRADING_SESSION_TIMEZONE", "America/New_York")
TRADING_SESSION_START = env_str("TRADING_SESSION_START", _default_trading_session_start)
TRADING_SESSION_END = env_str("TRADING_SESSION_END", _default_trading_session_end)
TRADING_WEEKDAYS_ONLY = env_bool("TRADING_WEEKDAYS_ONLY", _default_trading_weekdays_only)
TRADING_MAX_TRADES_PER_DAY = env_int("TRADING_MAX_TRADES_PER_DAY", _default_trading_max_trades_per_day)
TRADING_MAX_DAILY_LOSS_USDT = env_float("TRADING_MAX_DAILY_LOSS_USDT", _default_trading_max_daily_loss_usdt)
TRADING_STOP_LOSS_PCT = env_float("TRADING_STOP_LOSS_PCT", _default_trading_stop_loss_pct)
TRADING_TAKE_PROFIT_PCT = env_float("TRADING_TAKE_PROFIT_PCT", _default_trading_take_profit_pct)


BINGX_API_KEY = os.getenv("BINGX_API_KEY")
BINGX_SECRET = os.getenv("BINGX_SECRET")
BINGX_DEFAULT_SYMBOL = env_str("BINGX_DEFAULT_SYMBOL", "BTC/USDT")
BINGX_DEFAULT_TIMEFRAME = env_str("BINGX_DEFAULT_TIMEFRAME", TRADING_DATA_TIMEFRAME)
BINGX_ENABLE_RATE_LIMIT = env_bool("BINGX_ENABLE_RATE_LIMIT", True)
BINGX_TIMEOUT_MS = env_int("BINGX_TIMEOUT_MS", 10000)
BINGX_SANDBOX = env_bool("BINGX_SANDBOX", False)
BINGX_DEMO_TRADING = env_bool("BINGX_DEMO_TRADING", False)
DAY_MTF_SYMBOL = env_str("DAY_MTF_SYMBOL", BINGX_DEFAULT_SYMBOL)
DAY_MTF_D1_LIMIT = env_int("DAY_MTF_D1_LIMIT", 700)
DAY_MTF_H1_LIMIT = env_int("DAY_MTF_H1_LIMIT", 3000)
DAY_MTF_M5_LIMIT = env_int("DAY_MTF_M5_LIMIT", 12000)
DAY_MTF_INITIAL_EQUITY_USDT = env_float("DAY_MTF_INITIAL_EQUITY_USDT", 10000.0)
DAY_MTF_RISK_PER_TRADE = env_float("DAY_MTF_RISK_PER_TRADE", 0.01)
DAY_MTF_RR = env_float("DAY_MTF_RR", 1.2)
DAY_MTF_FEE_RATE = env_float("DAY_MTF_FEE_RATE", 0.0005)
DAY_MTF_SLIPPAGE_RATE = env_float("DAY_MTF_SLIPPAGE_RATE", 0.0005)
DAY_MTF_MAX_HOLD_BARS = env_int("DAY_MTF_MAX_HOLD_BARS", 36)
DAY_MTF_LOOKBACK_EXTREME = env_int("DAY_MTF_LOOKBACK_EXTREME", 20)
DAY_MTF_LOOKBACK_STRUCTURE = env_int("DAY_MTF_LOOKBACK_STRUCTURE", 12)
DAY_MTF_ZONE_TOLERANCE_PCT = env_float("DAY_MTF_ZONE_TOLERANCE_PCT", 0.002)
DAY_MTF_DAILY_BIAS_MODE = env_str("DAY_MTF_DAILY_BIAS_MODE", "strict")
DAY_MTF_BALANCED_RET_COL = env_str("DAY_MTF_BALANCED_RET_COL", "ret_1")
DAY_MTF_BALANCED_THRESHOLD = env_float("DAY_MTF_BALANCED_THRESHOLD", 0.0)
DAY_MTF_ENTRY_WINDOW_BARS = env_int("DAY_MTF_ENTRY_WINDOW_BARS", 3)
DAY_MTF_M5_TRIGGER_MODE = env_str("DAY_MTF_M5_TRIGGER_MODE", "ema_cross")
DAY_MTF_M5_BREAKOUT_LOOKBACK = env_int("DAY_MTF_M5_BREAKOUT_LOOKBACK", 3)
DAY_MTF_MIN_RISK_DISTANCE_PCT = env_float("DAY_MTF_MIN_RISK_DISTANCE_PCT", 0.0015)
DAY_MTF_MIN_IMPULSE_PCT = env_float("DAY_MTF_MIN_IMPULSE_PCT", 0.003)
DAY_MTF_START_DATE = env_str("DAY_MTF_START_DATE", "")
DAY_MTF_END_DATE = env_str("DAY_MTF_END_DATE", "")
DAY_MTF_CHUNK_LIMIT = env_int("DAY_MTF_CHUNK_LIMIT", 1000)
DAY_MTF_COVERAGE_MODE = env_str("DAY_MTF_COVERAGE_MODE", "warn")
DAY_MTF_MIN_M5_COVERAGE_RATIO = env_float("DAY_MTF_MIN_M5_COVERAGE_RATIO", 0.0)
DAY_MTF_OUTPUT_PREFIX = env_str("DAY_MTF_OUTPUT_PREFIX", "day_mtf_reversal_btc")
DATA_PIPELINE_SYMBOL = env_str("DATA_PIPELINE_SYMBOL", BINGX_DEFAULT_SYMBOL)
DATA_PIPELINE_TIMEFRAMES = env_str("DATA_PIPELINE_TIMEFRAMES", BINGX_DEFAULT_TIMEFRAME)
DATA_PIPELINE_LIMIT = env_int("DATA_PIPELINE_LIMIT", 1000)
DATA_PIPELINE_START_DATE = env_str("DATA_PIPELINE_START_DATE", "")
DATA_PIPELINE_END_DATE = env_str("DATA_PIPELINE_END_DATE", "")
DATA_PIPELINE_CHUNK_LIMIT = env_int("DATA_PIPELINE_CHUNK_LIMIT", 1000)
DATA_PIPELINE_PARTITION = env_str("DATA_PIPELINE_PARTITION", "none")
DATA_PIPELINE_OUTPUT_NAME = env_str("DATA_PIPELINE_OUTPUT_NAME", "btc_4h")
BINGX_DEMO_SYMBOL = env_str("BINGX_DEMO_SYMBOL", "BTC/USDT:USDT")
BINGX_DEMO_TEST_SIDE = env_str("BINGX_DEMO_TEST_SIDE", "buy")
BINGX_DEMO_TEST_USDT = env_float("BINGX_DEMO_TEST_USDT", 100.0)
BINGX_DEMO_OPEN_SIDE = env_str("BINGX_DEMO_OPEN_SIDE", "buy")
BINGX_DEMO_OPEN_USDT = env_float("BINGX_DEMO_OPEN_USDT", 100.0)
BINGX_DEMO_CYCLE_SIDE = env_str("BINGX_DEMO_CYCLE_SIDE", "buy")
BINGX_DEMO_CYCLE_USDT = env_float("BINGX_DEMO_CYCLE_USDT", 100.0)
BINGX_DEMO_CYCLE_PRICE_OFFSET_PCT = env_float("BINGX_DEMO_CYCLE_PRICE_OFFSET_PCT", 0.2)
DEMO_LOOP_EXECUTE = env_bool("DEMO_LOOP_EXECUTE", False)
DEMO_LOOP_DATA_SYMBOL = env_str("DEMO_LOOP_DATA_SYMBOL", BINGX_DEFAULT_SYMBOL)
DEMO_LOOP_TRADE_SYMBOL = env_str("DEMO_LOOP_TRADE_SYMBOL", BINGX_DEMO_SYMBOL)
DEMO_LOOP_TIMEFRAME = env_str("DEMO_LOOP_TIMEFRAME", TRADING_DATA_TIMEFRAME)
DEMO_LOOP_NOTIONAL_USDT = env_float("DEMO_LOOP_NOTIONAL_USDT", 100.0)
DEMO_LOOP_OHLCV_LIMIT = env_int("DEMO_LOOP_OHLCV_LIMIT", TRADING_OHLCV_LIMIT)
DEMO_LOOP_SIGNAL_THRESHOLD = env_float("DEMO_LOOP_SIGNAL_THRESHOLD", 0.15)
DEMO_LOOP_JOURNAL_FILE = env_str("DEMO_LOOP_JOURNAL_FILE", str(JOURNAL_DIR / "demo_loop_runs.jsonl"))
SCALP_DAEMON_EXECUTE = env_bool("SCALP_DAEMON_EXECUTE", False)
SCALP_DAEMON_INTERVAL_SECONDS = env_int("SCALP_DAEMON_INTERVAL_SECONDS", TRADING_LOOP_INTERVAL_SECONDS)
SCALP_DAEMON_MAX_CYCLES = env_int("SCALP_DAEMON_MAX_CYCLES", 0)
SCALP_DAEMON_TIMEFRAME = env_str("SCALP_DAEMON_TIMEFRAME", TRADING_DATA_TIMEFRAME)
SCALP_DAEMON_OHLCV_LIMIT = env_int("SCALP_DAEMON_OHLCV_LIMIT", TRADING_OHLCV_LIMIT)
SCALP_DAEMON_SESSION_ENABLED = env_bool("SCALP_DAEMON_SESSION_ENABLED", TRADING_SESSION_ENABLED)
SCALP_DAEMON_SESSION_TIMEZONE = env_str("SCALP_DAEMON_SESSION_TIMEZONE", TRADING_SESSION_TIMEZONE)
SCALP_DAEMON_SESSION_START = env_str("SCALP_DAEMON_SESSION_START", TRADING_SESSION_START)
SCALP_DAEMON_SESSION_END = env_str("SCALP_DAEMON_SESSION_END", TRADING_SESSION_END)
SCALP_DAEMON_WEEKDAYS_ONLY = env_bool("SCALP_DAEMON_WEEKDAYS_ONLY", TRADING_WEEKDAYS_ONLY)
SCALP_DAEMON_OUTSIDE_SESSION_MODE = env_str("SCALP_DAEMON_OUTSIDE_SESSION_MODE", "pause")
SCALP_DAEMON_MAX_TRADES_PER_DAY = env_int("SCALP_DAEMON_MAX_TRADES_PER_DAY", TRADING_MAX_TRADES_PER_DAY)
SCALP_DAEMON_MAX_DAILY_LOSS_USDT = env_float("SCALP_DAEMON_MAX_DAILY_LOSS_USDT", TRADING_MAX_DAILY_LOSS_USDT)
SCALP_DAEMON_STOP_LOSS_PCT = env_float("SCALP_DAEMON_STOP_LOSS_PCT", TRADING_STOP_LOSS_PCT)
SCALP_DAEMON_TAKE_PROFIT_PCT = env_float("SCALP_DAEMON_TAKE_PROFIT_PCT", TRADING_TAKE_PROFIT_PCT)
SCALP_DAEMON_TIME_STOP_MINUTES = env_int("SCALP_DAEMON_TIME_STOP_MINUTES", 45)
SCALP_DAEMON_NOTIONAL_USDT = env_float("SCALP_DAEMON_NOTIONAL_USDT", DEMO_LOOP_NOTIONAL_USDT)
SCALP_DAEMON_LOCK_FILE = env_str("SCALP_DAEMON_LOCK_FILE", str(JOURNAL_DIR / "demo_scalping_daemon.lock"))
SCALP_DAEMON_HEARTBEAT_FILE = env_str("SCALP_DAEMON_HEARTBEAT_FILE", str(JOURNAL_DIR / "demo_scalping_heartbeat.jsonl"))
SCALP_DAEMON_JOURNAL_FILE = env_str("SCALP_DAEMON_JOURNAL_FILE", str(JOURNAL_DIR / "demo_scalping_runs.jsonl"))
SIZING_TARGET_ANNUAL_VOL = env_float("SIZING_TARGET_ANNUAL_VOL", 0.6)
SIZING_MIN_MULTIPLIER = env_float("SIZING_MIN_MULTIPLIER", 0.25)
SIZING_MAX_MULTIPLIER = env_float("SIZING_MAX_MULTIPLIER", 1.5)
SIZING_MIN_CONVICTION_SCALE = env_float("SIZING_MIN_CONVICTION_SCALE", 0.25)
SIZING_DRAWDOWN_CUTOFF = env_float("SIZING_DRAWDOWN_CUTOFF", 0.10)
SIZING_DRAWDOWN_FLOOR_SCALE = env_float("SIZING_DRAWDOWN_FLOOR_SCALE", 0.35)
ENSEMBLE_MIN_TEST_SHARPE = env_float("ENSEMBLE_MIN_TEST_SHARPE", 0.0)
ENSEMBLE_MIN_TEST_PROFIT_FACTOR = env_float("ENSEMBLE_MIN_TEST_PROFIT_FACTOR", 1.0)
ENSEMBLE_MAX_TEST_TURNOVER = env_float("ENSEMBLE_MAX_TEST_TURNOVER", 250.0)
RISK_MAX_POSITION_USDT = env_float("RISK_MAX_POSITION_USDT", 100.0)
RISK_MAX_OPEN_POSITIONS = env_int("RISK_MAX_OPEN_POSITIONS", 1)


def ensure_data_dirs() -> None:
    for path in (
        DATA_DIR,
        RAW_DATA_DIR,
        PROCESSED_DATA_DIR,
        FEATURE_STORE_DIR,
        ALPHA_STORE_DIR,
        JOURNAL_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)
