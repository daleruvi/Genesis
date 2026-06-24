# Opening Range Reversion Validation - opening_range_reversion_ny_equity_qqq

## Summary

| tp_variant | symbol | total_return | win_rate | profit_factor | decision | max_drawdown | return_to_drawdown_ratio | expectancy | top_5_trades_pnl_share | top_month_return_share | avg_bars_in_trade | median_bars_in_trade | coverage_ratio_5m | partial_regular_sessions | invalid_sessions_ratio | signals_count | trades_count | signal_to_trade_rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| midpoint | QQQ | -0.1552 | 0.3 | 0.1546 | no_go | -0.1589 | -0.9768 | -0.0012 | 0.4048 | 1.0 | 4.4214 | 1.0 | 0.9175 | 24 | 0.028 | 140 | 140 | 1.0 |
| opposite_extreme | QQQ | -0.1226 | 0.5429 | 0.5391 | no_go | -0.1356 | -0.9041 | -0.0009 | 0.2314 | 0.6714 | 14.3429 | 5.0 | 0.9175 | 24 | 0.028 | 140 | 140 | 1.0 |

## Decision Inputs

- best_tp_variant: opposite_extreme
- symbol: QQQ
- decision: no_go
- coverage_ratio_5m: 0.9175418677263696
- partial_regular_sessions: 24
- invalid_sessions_ratio: 0.028
- top_5_trades_pnl_share: 0.2313966103984265
- top_5_net_pnl_share: 0.0
- top_month_return_share: 0.6713549806092634
- adjusted_data: True
- ny_gap_count_gt_60m: 3

## Monthly Performance

| period | tp_variant | trades_count | total_return | win_rate | profit_factor |
| --- | --- | --- | --- | --- | --- |
| 2025-01 | midpoint | 11 | -0.0046 | 0.5455 | 0.5977 |
| 2025-01 | opposite_extreme | 11 | -0.0003 | 0.4545 | 0.9932 |
| 2025-02 | midpoint | 6 | -0.0112 | 0.1667 | 0.0218 |
| 2025-02 | opposite_extreme | 6 | -0.0111 | 0.3333 | 0.0968 |
| 2025-03 | midpoint | 13 | -0.0272 | 0.2308 | 0.072 |
| 2025-03 | opposite_extreme | 13 | -0.0375 | 0.3846 | 0.2438 |
| 2025-04 | midpoint | 12 | -0.0467 | 0.1667 | 0.0402 |
| 2025-04 | opposite_extreme | 12 | -0.0512 | 0.4167 | 0.2268 |
| 2025-05 | midpoint | 12 | -0.0063 | 0.4167 | 0.3176 |
| 2025-05 | opposite_extreme | 12 | -0.0105 | 0.5833 | 0.5138 |
| 2025-06 | midpoint | 11 | -0.0032 | 0.4545 | 0.5385 |
| 2025-06 | opposite_extreme | 11 | 0.0116 | 0.7273 | 2.7938 |
| 2025-07 | midpoint | 11 | -0.0063 | 0.3636 | 0.0782 |
| 2025-07 | opposite_extreme | 11 | -0.0025 | 0.7273 | 0.7789 |
| 2025-08 | midpoint | 12 | -0.0091 | 0.3333 | 0.1943 |
| 2025-08 | opposite_extreme | 12 | -0.006 | 0.5833 | 0.5935 |
| 2025-09 | midpoint | 10 | -0.0083 | 0.2 | 0.062 |
| 2025-09 | opposite_extreme | 10 | -0.0036 | 0.6 | 0.6392 |
| 2025-10 | midpoint | 15 | -0.0101 | 0.2667 | 0.2397 |
| 2025-10 | opposite_extreme | 15 | -0.0129 | 0.6 | 0.5339 |
| 2025-11 | midpoint | 11 | -0.0077 | 0.4545 | 0.4393 |
| 2025-11 | opposite_extreme | 11 | 0.0056 | 0.6364 | 1.3509 |
| 2025-12 | midpoint | 16 | -0.026 | 0.0625 | 0.0133 |
| 2025-12 | opposite_extreme | 16 | -0.0101 | 0.4375 | 0.5089 |

## Invalid Sessions

| invalid_reason | count |
| --- | --- |
| or_size_above_max | 7 |

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
