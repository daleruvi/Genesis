# Opening Range Reversion Validation - opening_range_reversion_ny_equity_spy

## Summary

| tp_variant | symbol | total_return | win_rate | profit_factor | decision | max_drawdown | return_to_drawdown_ratio | expectancy | top_5_trades_pnl_share | top_month_return_share | avg_bars_in_trade | median_bars_in_trade | coverage_ratio_5m | partial_regular_sessions | invalid_sessions_ratio | signals_count | trades_count | signal_to_trade_rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| midpoint | QQQ | -0.128 | 0.2375 | 0.1417 | no_go | -0.13 | -0.9847 | -0.0009 | 0.41 | 1.0 | 2.45 | 1.0 | 0.9175 | 24 | 0.024 | 160 | 160 | 1.0 |
| opposite_extreme | QQQ | -0.0865 | 0.5 | 0.5443 | no_go | -0.0933 | -0.9273 | -0.0006 | 0.2276 | 1.0 | 12.2375 | 4.0 | 0.9175 | 24 | 0.024 | 160 | 160 | 1.0 |

## Decision Inputs

- best_tp_variant: opposite_extreme
- symbol: QQQ
- decision: no_go
- coverage_ratio_5m: 0.9175418677263696
- partial_regular_sessions: 24
- invalid_sessions_ratio: 0.024
- top_5_trades_pnl_share: 0.22759219694023608
- top_5_net_pnl_share: 0.0
- top_month_return_share: 1.0
- adjusted_data: True
- ny_gap_count_gt_60m: 3

## Monthly Performance

| period | tp_variant | trades_count | total_return | win_rate | profit_factor |
| --- | --- | --- | --- | --- | --- |
| 2025-01 | midpoint | 13 | -0.0035 | 0.3846 | 0.4994 |
| 2025-01 | opposite_extreme | 13 | -0.0137 | 0.4615 | 0.4187 |
| 2025-02 | midpoint | 10 | -0.0215 | 0.2 | 0.0307 |
| 2025-02 | opposite_extreme | 10 | -0.0224 | 0.0 | 0.0 |
| 2025-03 | midpoint | 11 | -0.0147 | 0.0909 | 0.0169 |
| 2025-03 | opposite_extreme | 11 | -0.0114 | 0.6364 | 0.4721 |
| 2025-04 | midpoint | 13 | -0.0254 | 0.2308 | 0.1243 |
| 2025-04 | opposite_extreme | 13 | -0.002 | 0.5385 | 0.9068 |
| 2025-05 | midpoint | 16 | -0.0096 | 0.375 | 0.2227 |
| 2025-05 | opposite_extreme | 16 | -0.0162 | 0.4375 | 0.323 |
| 2025-06 | midpoint | 15 | -0.0109 | 0.2 | 0.1511 |
| 2025-06 | opposite_extreme | 15 | -0.001 | 0.5333 | 0.9111 |
| 2025-07 | midpoint | 14 | -0.0109 | 0.2143 | 0.0629 |
| 2025-07 | opposite_extreme | 14 | -0.0063 | 0.5714 | 0.4809 |
| 2025-08 | midpoint | 12 | -0.0052 | 0.3333 | 0.2048 |
| 2025-08 | opposite_extreme | 12 | -0.0023 | 0.5 | 0.7259 |
| 2025-09 | midpoint | 15 | -0.0108 | 0.0667 | 0.0562 |
| 2025-09 | opposite_extreme | 15 | -0.0027 | 0.6 | 0.6042 |
| 2025-10 | midpoint | 15 | -0.0058 | 0.2 | 0.2311 |
| 2025-10 | opposite_extreme | 15 | -0.0094 | 0.6 | 0.5238 |
| 2025-11 | midpoint | 11 | -0.0063 | 0.5455 | 0.45 |
| 2025-11 | opposite_extreme | 11 | 0.0119 | 0.6364 | 2.6297 |
| 2025-12 | midpoint | 15 | -0.0115 | 0.0667 | 0.0177 |
| 2025-12 | opposite_extreme | 15 | -0.0141 | 0.4 | 0.2784 |

## Invalid Sessions

| invalid_reason | count |
| --- | --- |
| or_size_above_max | 6 |

## Relevant 5m Gaps

| session_date | gap_start | gap_end | gap_minutes |
| --- | --- | --- | --- |
| 2025-07-03 | 2025-07-03 17:00:00+00:00 | 2025-07-03 20:00:00+00:00 | 180.0 |
| 2025-11-28 | 2025-11-28 18:00:00+00:00 | 2025-11-28 21:00:00+00:00 | 180.0 |
| 2025-12-24 | 2025-12-24 18:00:00+00:00 | 2025-12-24 21:00:00+00:00 | 180.0 |

## Notes

- Reports prioritize `or_size_vs_daily_atr`; `or_size_vs_atr` is only a compatibility alias.
- TP variants are evaluated from the same base signal and entry.

## Recommendation

no_go
