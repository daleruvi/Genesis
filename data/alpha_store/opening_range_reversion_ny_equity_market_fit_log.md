# ORR Market Fit Validation Log

## QQQ - Fase 2.8

Status: `no_go`

Conclusion: QQQ tiene datos validos, warmup daily valido y cobertura regular-session suficiente. ORR V1 no pasa a demo porque ambas variantes fallan por performance neta negativa.

Datos:

- Intraday: `data/raw/qqq_5m_2025.parquet`
- Daily: `data/raw/qqq_1d_20241201_20260101.parquet`
- Daily warmup: `daily_warmup_ok=True`, `daily_warmup_bars=21`, `daily_required_warmup_bars=14`
- Regular-session coverage: `0.9175`
- Regular bars observadas/esperadas: `19395/21138`
- Sesiones: `250` detectadas, `247` completas, `24` parciales segun denominador daily
- Gaps NY >60m: `3`, en `2025-07-03`, `2025-11-28`, `2025-12-24`
- Invalid sessions ratio: `0.028`
- Signals/trades: `140/140`

Resultado por TP:

| symbol | tp_variant | total_return | profit_factor | expectancy | max_drawdown | decision |
| --- | --- | --- | --- | --- | --- | --- |
| QQQ | midpoint | -15.52% | 0.1546 | -0.0012 | -15.89% | no_go |
| QQQ | opposite_extreme | -12.26% | 0.5391 | -0.0009 | -13.56% | no_go |

Decision: no pasar a demo. La estrategia queda rechazada para QQQ en esta configuracion default `ny_equity`; el fallo es de performance, no de datos.

## Siguiente Validacion - SPY

Repetir el pipeline y backtest con la misma configuracion `ny_equity` y los mismos criterios de decision.

```powershell
.\.venv\Scripts\python.exe scripts\run_data_pipeline.py --provider polygon --market-type stocks --symbol SPY --timeframes "5m" --start-date 2025-01-01 --end-date 2026-01-01 --partition monthly --combine-partitions --output-name spy_5m_2025 --fail-fast
.\.venv\Scripts\python.exe scripts\run_data_pipeline.py --provider polygon --market-type stocks --symbol SPY --timeframes "1d" --start-date 2024-12-01 --end-date 2026-01-01 --output-name spy_1d_20241201_20260101 --fail-fast
.\.venv\Scripts\python.exe scripts\run_backtest_opening_range_reversion.py --config config\strategies\opening_range_reversion_ny_equity.yaml --dataset-path data\raw\spy_5m_2025.parquet --daily-dataset-path data\raw\spy_1d_20241201_20260101.parquet --output-prefix opening_range_reversion_ny_equity_spy --symbol SPY
```

Decision esperada a emitir por `symbol + tp_variant`: `no_data`, `no_go`, `continue_research` o `candidate_for_validation`. No recomendar demo en Fase 2.8.
