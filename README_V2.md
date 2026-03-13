# GENESIS V2

## Estado actual

GENESIS hoy es una base funcional de research cuantitativo + ejecucion demo.
No es todavia un sistema validado end-to-end para produccion real.

Lo que si esta operativo:

- pipeline de datos (`run_data_pipeline.py`)
- pipeline de features (`run_feature_pipeline.py`)
- generacion y evaluacion de alphas (`run_alpha_generation.py`, `run_alpha_research.py`)
- analisis de regimen (`run_regime_analysis.py`)
- backtesting base con costos (`run_backtest_alpha.py`)
- loop demo con decision por ensemble + regime + sizing (`run_demo_trading_loop.py`)
- daemon nocturno demo con guardrails (`run_demo_scalping_daemon.py`)
- journals y reportes de operacion (`run_demo_loop_journal_report.py`)

## Objetivo de V2

GENESIS V2 prioriza:

1. medir edge
2. proteger edge
3. ejecutar edge
4. escalar edge

La arquitectura ayuda, pero no reemplaza validacion cuantitativa.

## Arquitectura tecnica vigente

```text
src/genesis/
  config/settings.py
  data/
  features/
  alpha/
    alpha_generator.py
    alpha_evaluator.py
    alpha_research.py
    alpha_selection.py
    alpha_ensemble.py
  backtest/
  llm/regime_detector.py
  portfolio/
    risk_manager.py
    position_sizing.py
  execution/execution_engine.py
  utils/journal.py

scripts/
  run_data_pipeline.py
  run_feature_pipeline.py
  run_alpha_generation.py
  run_alpha_pipeline.py
  run_alpha_research.py
  run_regime_analysis.py
  run_backtest_alpha.py
  run_demo_trading_loop.py
  run_demo_scalping_daemon.py
  run_demo_loop_journal_report.py
  check_bingx_*.py
  open_bingx_demo_position.py
  close_bingx_demo_positions.py

tests/
  test_data/
  test_features/
  test_alpha/
  test_backtest/
  test_llm/
  test_portfolio/
  test_utils/
```

## Que hace GENESIS hoy (resumen operativo)

### Data y features

- descarga OHLCV desde BingX
- persiste parquet
- calcula retornos, volatilidad, momentum y volumen

### Alpha research

- genera alphas base desde features
- rankea por metricas de train/test + estabilidad temporal
- mide correlacion y selecciona conjunto menos redundante
- analiza performance por regimen

### Decision y riesgo

- usa `AlphaEnsemble` con filtros de calidad:
  - `test_sharpe > ENSEMBLE_MIN_TEST_SHARPE`
  - `test_profit_factor > ENSEMBLE_MIN_TEST_PROFIT_FACTOR`
  - `test_total_turnover <= ENSEMBLE_MAX_TEST_TURNOVER`
- detecta regimen de mercado
- calcula sizing por volatilidad, conviccion y drawdown
- aplica limites de riesgo por notional y max posiciones

### Ejecucion demo

- reconciliacion de senal vs posicion actual
- open/close/hold en demo
- guardrails de daemon:
  - kill switch por perdida diaria
  - max trades por dia
  - stop loss / take profit / time stop
  - lock anti-duplicados
  - heartbeat y journal

## Orden de ejecucion desde cero

Despues de limpiar artefactos (`data/*` y journals), correr en este orden:

1. `python scripts/run_data_pipeline.py`
2. `python scripts/run_feature_pipeline.py`
3. `python scripts/run_alpha_generation.py`
4. `python scripts/run_alpha_research.py`
5. `python scripts/run_regime_analysis.py`
6. `python scripts/run_backtest_alpha.py --alpha alpha_10 --mode quantile`
7. `python scripts/run_demo_trading_loop.py` o `python scripts/run_demo_scalping_daemon.py`

Dependencias:

- `run_alpha_research.py` necesita `generated_alphas.parquet`
- `run_regime_analysis.py` necesita `selected_alpha_rankings.csv` + `selected_alphas.parquet`
- `run_demo_trading_loop.py` y `run_demo_scalping_daemon.py` necesitan `selected_alpha_rankings.csv` + `alpha_regime_performance.csv`

## Comandos disponibles

### Pipelines base

- `python scripts/run_data_pipeline.py`
- `python scripts/run_feature_pipeline.py`
- `python scripts/run_alpha_generation.py`
- `python scripts/run_alpha_pipeline.py`

Descarga historica paginada (todo 2025, multitemporal):

- `python scripts/run_data_pipeline.py --symbol "BTC/USDT:USDT" --timeframes "1d,1h,5m" --start-date 2025-01-01 --end-date 2026-01-01 --chunk-limit 1000`
- `python scripts/run_data_pipeline.py --symbol "BTC/USDT:USDT" --timeframes "5m" --start-date 2025-12-01 --end-date 2026-01-01 --chunk-limit 1000 --output-name btc_usdt_usdt_5m_dec2025`

Descarga historica particionada por mes (recomendado para 5m):

- `python scripts/run_data_pipeline.py --symbol "BTC/USDT:USDT" --timeframes "5m" --start-date 2025-01-01 --end-date 2026-01-01 --chunk-limit 1000 --partition monthly --combine-partitions --output-name btc_usdt_usdt_5m_2025`

### Research y backtest

- `python scripts/run_alpha_research.py`
- `python scripts/run_regime_analysis.py`
- `python scripts/run_backtest_alpha.py --alpha alpha_10 --mode quantile --train-ratio 0.7 --fee-rate 0.0005 --slippage-rate 0.0005`
- `python scripts/run_backtest_day_mtf_reversal.py --d1-path data/raw/btc_usdt_usdt_1d_20250101_20260101.parquet --h1-path data/raw/btc_usdt_usdt_1h_20250101_20260101.parquet --m5-path data/raw/btc_usdt_usdt_5m_dec2025.parquet --start-date 2025-01-01 --end-date 2026-01-01 --coverage-mode clip --output-prefix day_mtf_btc_clip_2025`

### Demo operativo

- `python scripts/check_bingx_connection.py`
- `python scripts/check_bingx_demo.py`
- `python scripts/check_bingx_demo_order.py`
- `python scripts/check_bingx_demo_cycle.py`
- `python scripts/open_bingx_demo_position.py`
- `python scripts/close_bingx_demo_positions.py`
- `python scripts/run_demo_trading_loop.py`
- `python scripts/run_demo_scalping_daemon.py`
- `python scripts/run_demo_loop_journal_report.py`

## Flujo recomendado de consola

Para troubleshooting y operacion estable, mantener este orden de salida:

1. Config
2. Data
3. Signals
4. Risk/Sizing
5. Execution
6. Journal/Heartbeat

Regla practica:

- si falla un paso, no continuar con el siguiente
- regenerar artefactos en orden de pipeline
- en daemon revisar primero lock, luego heartbeat, luego journal

## Variables de entorno clave

### Perfil de trading (swing -> day)

GENESIS ahora soporta un perfil global para evolucionar el estilo sin tocar codigo:

- `TRADING_STYLE` (`swing` o `day`)
- `TRADING_DATA_TIMEFRAME`
- `TRADING_OHLCV_LIMIT`
- `TRADING_LOOP_INTERVAL_SECONDS`
- `TRADING_SESSION_ENABLED`
- `TRADING_SESSION_TIMEZONE`
- `TRADING_SESSION_START`
- `TRADING_SESSION_END`
- `TRADING_WEEKDAYS_ONLY`
- `TRADING_MAX_TRADES_PER_DAY`
- `TRADING_MAX_DAILY_LOSS_USDT`
- `TRADING_STOP_LOSS_PCT`
- `TRADING_TAKE_PROFIT_PCT`

Estas variables alimentan por defecto:

- `DEMO_LOOP_*` (loop de decision)
- `SCALP_DAEMON_*` (daemon operativo)
- `run_backtest_genesis_swing.py` (defaults de stop/take)

Con esto puedes arrancar en `swing` y migrar a `day` editando solo `.env`.

### Core

- `BINGX_API_KEY`
- `BINGX_SECRET`
- `BINGX_DEFAULT_SYMBOL`
- `BINGX_DEFAULT_TIMEFRAME`

### Demo loop

- `DEMO_LOOP_EXECUTE`
- `DEMO_LOOP_DATA_SYMBOL`
- `DEMO_LOOP_TRADE_SYMBOL`
- `DEMO_LOOP_TIMEFRAME`
- `DEMO_LOOP_NOTIONAL_USDT`
- `DEMO_LOOP_OHLCV_LIMIT`
- `DEMO_LOOP_SIGNAL_THRESHOLD`
- `DEMO_LOOP_JOURNAL_FILE`

### Day MTF backtest

- `DAY_MTF_SYMBOL`
- `DAY_MTF_START_DATE`
- `DAY_MTF_END_DATE`
- `DAY_MTF_CHUNK_LIMIT`
- `DAY_MTF_DAILY_BIAS_MODE`
- `DAY_MTF_ENTRY_WINDOW_BARS`
- `DAY_MTF_M5_TRIGGER_MODE`
- `DAY_MTF_MIN_RISK_DISTANCE_PCT`
- `DAY_MTF_MIN_IMPULSE_PCT`
- `DAY_MTF_COVERAGE_MODE` (`warn` | `error` | `clip`)
- `DAY_MTF_MIN_M5_COVERAGE_RATIO`

### Data pipeline

- `DATA_PIPELINE_SYMBOL`
- `DATA_PIPELINE_TIMEFRAMES`
- `DATA_PIPELINE_START_DATE`
- `DATA_PIPELINE_END_DATE`
- `DATA_PIPELINE_CHUNK_LIMIT`
- `DATA_PIPELINE_PARTITION` (`none` | `monthly`)
- `DATA_PIPELINE_OUTPUT_NAME`

### Scalping daemon

- `SCALP_DAEMON_EXECUTE`
- `SCALP_DAEMON_INTERVAL_SECONDS`
- `SCALP_DAEMON_MAX_CYCLES`
- `SCALP_DAEMON_TIMEFRAME`
- `SCALP_DAEMON_OHLCV_LIMIT`
- `SCALP_DAEMON_SESSION_ENABLED`
- `SCALP_DAEMON_SESSION_TIMEZONE`
- `SCALP_DAEMON_SESSION_START`
- `SCALP_DAEMON_SESSION_END`
- `SCALP_DAEMON_WEEKDAYS_ONLY`
- `SCALP_DAEMON_OUTSIDE_SESSION_MODE`
- `SCALP_DAEMON_MAX_TRADES_PER_DAY`
- `SCALP_DAEMON_MAX_DAILY_LOSS_USDT`
- `SCALP_DAEMON_STOP_LOSS_PCT`
- `SCALP_DAEMON_TAKE_PROFIT_PCT`
- `SCALP_DAEMON_TIME_STOP_MINUTES`
- `SCALP_DAEMON_NOTIONAL_USDT`
- `SCALP_DAEMON_LOCK_FILE`
- `SCALP_DAEMON_HEARTBEAT_FILE`
- `SCALP_DAEMON_JOURNAL_FILE`

### Ensemble quality filters

- `ENSEMBLE_MIN_TEST_SHARPE`
- `ENSEMBLE_MIN_TEST_PROFIT_FACTOR`
- `ENSEMBLE_MAX_TEST_TURNOVER`

### Sizing y riesgo

- `SIZING_TARGET_ANNUAL_VOL`
- `SIZING_MIN_MULTIPLIER`
- `SIZING_MAX_MULTIPLIER`
- `SIZING_MIN_CONVICTION_SCALE`
- `SIZING_DRAWDOWN_CUTOFF`
- `SIZING_DRAWDOWN_FLOOR_SCALE`
- `RISK_MAX_POSITION_USDT`
- `RISK_MAX_OPEN_POSITIONS`

## Modo noche seguro (demo)

Config recomendada minima:

```env
SCALP_DAEMON_EXECUTE=true
SCALP_DAEMON_INTERVAL_SECONDS=300
SCALP_DAEMON_MAX_CYCLES=0
SCALP_DAEMON_TIMEFRAME=1m
SCALP_DAEMON_OHLCV_LIMIT=240
SCALP_DAEMON_SESSION_ENABLED=true
SCALP_DAEMON_SESSION_TIMEZONE=America/New_York
SCALP_DAEMON_SESSION_START=09:30
SCALP_DAEMON_SESSION_END=16:00
SCALP_DAEMON_WEEKDAYS_ONLY=true
SCALP_DAEMON_OUTSIDE_SESSION_MODE=pause
SCALP_DAEMON_MAX_TRADES_PER_DAY=20
SCALP_DAEMON_MAX_DAILY_LOSS_USDT=30
SCALP_DAEMON_STOP_LOSS_PCT=0.004
SCALP_DAEMON_TAKE_PROFIT_PCT=0.006
SCALP_DAEMON_TIME_STOP_MINUTES=45
SCALP_DAEMON_NOTIONAL_USDT=50
RISK_MAX_POSITION_USDT=75
RISK_MAX_OPEN_POSITIONS=1
ENSEMBLE_MIN_TEST_SHARPE=0.5
ENSEMBLE_MIN_TEST_PROFIT_FACTOR=1.05
ENSEMBLE_MAX_TEST_TURNOVER=200
```

Secuencia recomendada:

1. regenerar artefactos (data -> features -> alpha -> research -> regime)
2. dry-run 1 ciclo (`SCALP_DAEMON_EXECUTE=false`, `SCALP_DAEMON_MAX_CYCLES=1`)
3. ejecutar nocturno (`SCALP_DAEMON_EXECUTE=true`, `SCALP_DAEMON_MAX_CYCLES=0`)

Monitoreo:

- `data/journals/demo_scalping_heartbeat.jsonl`
- `data/journals/demo_scalping_runs.jsonl`
- `data/journals/demo_scalping_daemon.lock`

## Estado de desarrollo

Implementado:

- Fase 1: base de backtesting
- Fase 2: alpha research (ranking, correlacion, seleccion, estabilidad temporal)
- Fase 3: regime detection base
- Fase 4 parcial: ensemble de decision con filtros de calidad
- Fase 5 parcial: sizing cuantitativo inicial + riesgo base
- Fase 7 parcial: journals y reportes operativos

Pendiente principal:

- validacion OOS mas estricta (walk-forward/purged CV/decay)
- costos y ejecucion mas realistas para intradia/scalping
- filtros de activacion de senal por regimen mas robustos
- capa multiagente formal (cuando haya edge suficientemente estable)

## Cierre

GENESIS ya no es solo un conjunto de scripts.
Hoy es una plataforma funcional para investigar edge, ejecutar en demo con guardrails y operar con trazabilidad.
El siguiente salto de calidad depende mas de validacion estadistica y realismo de ejecucion que de agregar mas arquitectura.
