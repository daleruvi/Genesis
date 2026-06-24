# GENESIS

GENESIS es una plataforma Python de investigacion cuantitativa y trading demo para crypto. El estado actual es una base funcional para research, backtesting, seleccion de alphas y ejecucion demo con guardrails; no es todavia un sistema validado para operar capital real.

La direccion de refactor es llevar el proyecto hacia un motor modular de algotrading validable:

```text
estrategia concreta -> backtest -> validacion -> demo -> journal -> Alpha Factory
```

## Estado actual

Operativo hoy:

- ingesta OHLCV desde BingX y persistencia parquet
- feature engineering basico
- generacion, evaluacion, ranking y seleccion de alphas
- backtesting vectorizado con costos y slippage
- deteccion cuantitativa de regimen
- decision demo por ensemble + regimen + sizing
- daemon demo con lock, heartbeat, limites diarios, stop loss, take profit y time stop
- journals JSONL y reportes operativos

Limites conocidos:

- no hay validacion OOS estricta suficiente para produccion
- los costos de ejecucion intradia todavia son simplificados
- los artefactos de research deben regenerarse en orden antes de demo
- `ml/` sigue como scaffold
- `llm/` no usa LLM real; el regimen actual es heuristico

## Instalacion

Requiere Python 3.12 o superior.

Instalacion recomendada para desarrollo:

```bash
python -m pip install -e ".[dev]"
```

Comando canonico de tests:

```bash
python -m pytest -q
```

`requirements.txt` se mantiene como compatibilidad temporal para entornos existentes, pero la fuente principal de dependencias es `pyproject.toml`.

## Perfiles

GENESIS separa perfil operativo de estilo de trading.

Perfil operativo:

```env
GENESIS_PROFILE=research
GENESIS_ALLOW_LIVE=false
```

Valores soportados:

- `research`: default seguro; datos, features, research y backtests sin ejecucion de ordenes.
- `demo`: permite ejecucion demo solo cuando las banderas existentes lo pidan, por ejemplo `DEMO_LOOP_EXECUTE=true`.
- `live`: existe como perfil bloqueado; requiere `GENESIS_ALLOW_LIVE=true` y credenciales antes de cualquier flujo live futuro.

Estilo de trading:

```env
TRADING_STYLE=swing
```

Valores soportados hoy: `swing` y `day`. Este controla defaults como timeframe, limites diarios, stops y ventana de sesion; no autoriza por si solo ninguna ejecucion.

## Health check

Antes de correr pipelines o demo:

```bash
python scripts/run_health_check.py
```

El health check valida:

- version de Python
- imports runtime principales
- estructura de carpetas
- escritura en `data/`
- configuracion de perfil
- presencia de `.env`
- datos raw minimos
- artefactos alpha necesarios para demo

Estados:

- `OK`: listo
- `WARN`: falta algo no critico para el perfil actual
- `FAIL`: problema critico; el comando termina con exit code `1`

Los artefactos alpha faltantes son `WARN` en `research` y `FAIL` en `demo` o `live`.

## Flujo de pipeline

Desde cero, el orden recomendado es:

```bash
python scripts/run_data_pipeline.py
python scripts/run_feature_pipeline.py
python scripts/run_alpha_generation.py
python scripts/run_alpha_research.py
python scripts/run_regime_analysis.py
python scripts/run_backtest_alpha.py --alpha alpha_10 --mode quantile
```

Dependencias entre artefactos:

- `run_alpha_research.py` necesita `data/alpha_store/generated_alphas.parquet`
- `run_regime_analysis.py` necesita `selected_alpha_rankings.csv` y `selected_alphas.parquet`
- `run_demo_trading_loop.py` y `run_demo_scalping_daemon.py` necesitan `selected_alpha_rankings.csv` y `alpha_regime_performance.csv`

## Opening Range Reversion

La primera estrategia concreta validable vive separada de `alpha/` y se ejecuta con:

```bash
python scripts/run_backtest_opening_range_reversion.py --config config/strategies/opening_range_reversion.yaml
```

El default usa sesion NY, barras 5m, opening range de 15 minutos, ATR diario desplazado un dia para evitar lookahead, filtro EMA50 intraday, stop con buffer ATR y dos variantes de take profit: midpoint y extremo opuesto.

Artefactos generados:

- `*_summary.csv`
- `*_signals.csv`
- `*_trades.csv`
- `*_equity.csv`
- `*_invalid_sessions.csv`
- `*_daily_breakdown.csv`
- `*_weekly_breakdown.csv`
- `*_monthly_breakdown.csv`
- `*_gaps.csv`
- `*_validation.md`

Nota: el config default espera `data/raw/btc_usdt_usdt_1d_20241201_20260101.parquet` para tener warmup diario desde 2024-12-01. Si solo tienes el dataset diario 2025, usa `--daily-dataset-path data/raw/btc_usdt_usdt_1d_20250101_20260101.parquet` para pruebas acotadas.

Datasets recomendados para validacion empirica 2025. Para ORR crypto 5m, la fuente principal reproducible es Binance public historical data (`binance_vision`) sobre futures USDT-M; BingX queda compatible, pero puede no exponer suficiente profundidad historica 5m.

```bash
python scripts/run_data_pipeline.py --provider binance_vision --market-type futures_um --symbol BTCUSDT --timeframes "5m" --start-date 2025-01-01 --end-date 2026-01-01 --partition monthly --combine-partitions --output-name btcusdt_futures_um_5m_2025 --fail-fast
python scripts/run_data_pipeline.py --provider binance_vision --market-type futures_um --symbol BTCUSDT --timeframes "1d" --start-date 2024-12-01 --end-date 2026-01-01 --output-name btcusdt_futures_um_1d_20241201_20260101 --fail-fast
```

En PowerShell:

```powershell
.\.venv\Scripts\python.exe scripts\run_data_pipeline.py --provider binance_vision --market-type futures_um --symbol BTCUSDT --timeframes "5m" --start-date 2025-01-01 --end-date 2026-01-01 --partition monthly --combine-partitions --output-name btcusdt_futures_um_5m_2025 --fail-fast
.\.venv\Scripts\python.exe scripts\run_data_pipeline.py --provider binance_vision --market-type futures_um --symbol BTCUSDT --timeframes "1d" --start-date 2024-12-01 --end-date 2026-01-01 --output-name btcusdt_futures_um_1d_20241201_20260101 --fail-fast
```

Backtest ORR usando esos datasets:

```powershell
.\.venv\Scripts\python.exe scripts\run_backtest_opening_range_reversion.py --config config\strategies\opening_range_reversion.yaml --dataset-path data\raw\btcusdt_futures_um_5m_2025.parquet --daily-dataset-path data\raw\btcusdt_futures_um_1d_20241201_20260101.parquet
```

Validacion empirica antes de demo:

- `coverage_ratio_5m = observed_5m_bars / 105120` para 2025 completo.
- Si `coverage_ratio_5m < 0.90`, la decision maxima es `continue`; si es `< 0.50`, es `no-go`.
- El daily 1d se interpreta como UTC y debe cubrir desde `2024-12-01` hasta `2025-12-31`, con al menos 14 velas previas al primer `session_date`.
- `invalid_sessions_ratio = invalid_sessions / total_sessions`; si es `> 0.30`, la decision maxima es `continue`; si es `> 0.50`, es `no-go`.
- La decision se calcula por `tp_variant`, sin mezclar `midpoint` y `opposite_extreme`.
- Para recomendar `demo`, el mejor `tp_variant` debe tener al menos 50 trades, retorno total positivo, expectancy positiva, profit factor mayor a 1.15, `return_to_drawdown_ratio >= 0.5` y `top_5_trades_pnl_share <= 0.60`.

### Fase 2.8: ORR Market Fit en acciones/ETFs

ORR crypto con sesion NY sintetica queda documentado como `no_go` en `data/alpha_store/opening_range_reversion_crypto_no_go.md`: BTCUSDT futures UM 2025 tuvo cobertura completa, daily warmup correcto y sin gaps NY relevantes, pero fallo por performance.

Para validar activos con apertura oficial se usa Polygon Stocks con `adjusted=true` tanto en 5m como en 1d. Requiere `POLYGON_API_KEY`. Esta fase no implementa Polygon indices, futuros NQ/ES/YM, demo/live ni optimizacion masiva.

Polygon respeta rate limit desde `.env`. En plan Basic, si no defines override, GENESIS usa 5 calls/min con margen 1: ritmo efectivo 4 calls/min y sleep minimo de 15 segundos entre requests. Variables disponibles:

```text
POLYGON_PLAN=basic
POLYGON_RATE_LIMIT_CALLS_PER_MINUTE=
POLYGON_RATE_LIMIT_SAFETY_MARGIN=1
POLYGON_MAX_RETRIES=5
POLYGON_BACKOFF_BASE_SECONDS=15
```

El pipeline reintenta `429 Too Many Requests` con `Retry-After` o backoff exponencial, y nunca imprime `POLYGON_API_KEY`. Para stocks/ETFs, la cobertura 5m se reporta sobre regular session 09:30-16:00 NY: 78 barras por sesion. Las barras extended-hours se conservan en raw pero no inflan la cobertura regular-session.

Ejemplo QQQ:

```powershell
.\.venv\Scripts\python.exe scripts\run_data_pipeline.py --provider polygon --market-type stocks --symbol QQQ --timeframes "5m" --start-date 2025-01-01 --end-date 2026-01-01 --output-name qqq_5m_2025 --fail-fast
.\.venv\Scripts\python.exe scripts\run_data_pipeline.py --provider polygon --market-type stocks --symbol QQQ --timeframes "1d" --start-date 2024-12-01 --end-date 2026-01-01 --output-name qqq_1d_20241201_20260101 --fail-fast
.\.venv\Scripts\python.exe scripts\run_backtest_opening_range_reversion.py --config config\strategies\opening_range_reversion_ny_equity.yaml --dataset-path data\raw\qqq_5m_2025.parquet --daily-dataset-path data\raw\qqq_1d_20241201_20260101.parquet --output-prefix opening_range_reversion_ny_equity_qqq --symbol QQQ
```

Resultado QQQ Fase 2.8: datos validos, warmup daily valido y cobertura regular-session suficiente, pero ambas variantes ORR quedan `no_go` por performance negativa. No pasar a demo. Registro: `data/alpha_store/opening_range_reversion_ny_equity_market_fit_log.md`.

Siguiente validacion SPY:

```powershell
.\.venv\Scripts\python.exe scripts\run_data_pipeline.py --provider polygon --market-type stocks --symbol SPY --timeframes "5m" --start-date 2025-01-01 --end-date 2026-01-01 --partition monthly --combine-partitions --output-name spy_5m_2025 --fail-fast
.\.venv\Scripts\python.exe scripts\run_data_pipeline.py --provider polygon --market-type stocks --symbol SPY --timeframes "1d" --start-date 2024-12-01 --end-date 2026-01-01 --output-name spy_1d_20241201_20260101 --fail-fast
.\.venv\Scripts\python.exe scripts\run_backtest_opening_range_reversion.py --config config\strategies\opening_range_reversion_ny_equity.yaml --dataset-path data\raw\spy_5m_2025.parquet --daily-dataset-path data\raw\spy_1d_20241201_20260101.parquet --output-prefix opening_range_reversion_ny_equity_spy --symbol SPY
```

Repite el mismo patron para cualquier ticker valido de Polygon. Set inicial recomendado: `SPY`, `NVDA`, `TSLA`, `AAPL`. Usa nombres por ticker (`spy_5m_2025`, `opening_range_reversion_ny_equity_spy`) para no sobrescribir artefactos.

La decision market-fit se calcula por `symbol` y `tp_variant`: `no_data`, `no_go`, `continue_research` o `candidate_for_validation`. `candidate_for_validation` no autoriza demo; solo marca que vale una validacion mas profunda.

### Post-mortem ORR V1

Cuando existan los artefactos de BTCUSDT, QQQ y SPY, genera el post-mortem reproducible:

```powershell
.\.venv\Scripts\python.exe scripts\run_orr_postmortem.py --alpha-store data\alpha_store --output-prefix opening_range_reversion_v1_postmortem --symbols BTCUSDT,QQQ,SPY
```

Artefactos:

- `data/alpha_store/opening_range_reversion_v1_postmortem_summary.csv`
- `data/alpha_store/opening_range_reversion_v1_postmortem_trade_diagnostics.csv`
- `data/alpha_store/opening_range_reversion_v1_postmortem_monthly.csv`
- `data/alpha_store/opening_range_reversion_v1_postmortem_side_breakdown.csv`
- `data/alpha_store/opening_range_reversion_v1_postmortem.md`

El post-mortem nunca recomienda demo. Las recomendaciones permitidas son `abandon`, `redesign` y `continue_research`.

## Demo

Dry-run de una pasada:

```bash
GENESIS_PROFILE=demo
DEMO_LOOP_EXECUTE=false
python scripts/run_demo_trading_loop.py
```

Ejecucion demo explicita:

```bash
GENESIS_PROFILE=demo
DEMO_LOOP_EXECUTE=true
python scripts/run_demo_trading_loop.py
```

Daemon demo:

```bash
GENESIS_PROFILE=demo
SCALP_DAEMON_EXECUTE=true
python scripts/run_demo_scalping_daemon.py
```

El daemon registra heartbeat y runs en `data/journals/`.

## Arquitectura vigente

```text
src/genesis/
  config/settings.py
  data/
  features/
  alpha/
  strategies/
  backtest/
  llm/regime_detector.py
  portfolio/
  execution/
  utils/

scripts/
  run_data_pipeline.py
  run_feature_pipeline.py
  run_alpha_generation.py
  run_alpha_research.py
  run_regime_analysis.py
  run_backtest_alpha.py
  run_backtest_opening_range_reversion.py
  run_demo_trading_loop.py
  run_demo_scalping_daemon.py
  run_health_check.py
```

Opening Range Reversion ya es la primera estrategia concreta; la Alpha Factory queda como evolucion posterior.
