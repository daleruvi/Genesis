# Opening Range Reversion Crypto No-Go

## Dataset

- Symbol: BTCUSDT futures UM
- Window: 2025-01-01 to 2026-01-01
- Intraday coverage: 105120 / 105120 5m bars
- Daily warmup: OK, from 2024-12-01 through 2025-12-31
- Relevant NY gaps > 60 minutes: 0

## Result

| tp_variant | total_return | profit_factor | decision |
| --- | --- | --- | --- |
| midpoint | -48.31% | 0.0476 | no_go |
| opposite_extreme | -34.68% | 0.3577 | no_go |

## Conclusion

Opening Range Reversion V1 failed on BTCUSDT futures UM because of performance, not because of data quality.
The synthetic NY crypto opening hypothesis is `no_go`.
The strategy should be validated next on assets with an official market open, starting with liquid ETFs and stocks.
