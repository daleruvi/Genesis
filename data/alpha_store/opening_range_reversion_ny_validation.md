# Opening Range Reversion Validation - opening_range_reversion_ny

## Summary

| tp_variant | total_return | win_rate | profit_factor | decision | max_drawdown | return_to_drawdown_ratio | expectancy | top_5_trades_pnl_share | avg_bars_in_trade | median_bars_in_trade | coverage_ratio_5m | invalid_sessions_ratio | signals_count | trades_count | signal_to_trade_rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| midpoint | -0.4831 | 0.0972 | 0.0476 | no-go | -0.4831 | -0.9999 | -0.003 | 0.5509 | 4.0602 | 1.0 | 1.0 | 0.1066 | 216 | 216 | 1.0 |
| opposite_extreme | -0.3468 | 0.3657 | 0.3577 | no-go | -0.3543 | -0.9788 | -0.002 | 0.2377 | 13.1667 | 5.0 | 1.0 | 0.1066 | 216 | 216 | 1.0 |

## Decision Inputs

- best_tp_variant: opposite_extreme
- decision: no-go
- coverage_ratio_5m: 1.0
- invalid_sessions_ratio: 0.10655737704918032
- top_5_trades_pnl_share: 0.23769414254536753
- top_5_net_pnl_share: 0.0
- ny_gap_count_gt_60m: 0

## Monthly Performance

| period | tp_variant | trades_count | total_return | win_rate | profit_factor |
| --- | --- | --- | --- | --- | --- |
| 2025-01 | midpoint | 16 | -0.0427 | 0.125 | 0.0294 |
| 2025-01 | opposite_extreme | 16 | 0.0057 | 0.4375 | 1.2534 |
| 2025-02 | midpoint | 13 | -0.0541 | 0.1538 | 0.0935 |
| 2025-02 | opposite_extreme | 13 | -0.0552 | 0.3077 | 0.2985 |
| 2025-03 | midpoint | 16 | -0.0441 | 0.3125 | 0.1725 |
| 2025-03 | opposite_extreme | 16 | -0.0028 | 0.5 | 0.9443 |
| 2025-04 | midpoint | 16 | -0.0518 | 0.125 | 0.0896 |
| 2025-04 | opposite_extreme | 16 | -0.0502 | 0.4375 | 0.3162 |
| 2025-05 | midpoint | 23 | -0.0805 | 0.0435 | 0.0237 |
| 2025-05 | opposite_extreme | 23 | -0.0691 | 0.2174 | 0.1746 |
| 2025-06 | midpoint | 16 | -0.025 | 0.125 | 0.0361 |
| 2025-06 | opposite_extreme | 16 | -0.0241 | 0.4375 | 0.4326 |
| 2025-07 | midpoint | 18 | -0.0557 | 0.0556 | 0.0066 |
| 2025-07 | opposite_extreme | 18 | -0.0218 | 0.5 | 0.3962 |
| 2025-08 | midpoint | 22 | -0.0396 | 0.0455 | 0.0243 |
| 2025-08 | opposite_extreme | 22 | -0.027 | 0.3636 | 0.3922 |
| 2025-09 | midpoint | 17 | -0.0429 | 0.0 | 0.0 |
| 2025-09 | opposite_extreme | 17 | -0.0498 | 0.1765 | 0.0264 |
| 2025-10 | midpoint | 20 | -0.0641 | 0.05 | 0.0018 |
| 2025-10 | opposite_extreme | 20 | -0.0583 | 0.2 | 0.0659 |
| 2025-11 | midpoint | 21 | -0.0573 | 0.1429 | 0.0789 |
| 2025-11 | opposite_extreme | 21 | -0.0112 | 0.4762 | 0.7207 |
| 2025-12 | midpoint | 18 | -0.0827 | 0.0556 | 0.02 |
| 2025-12 | opposite_extreme | 18 | -0.0513 | 0.3889 | 0.2605 |

## Invalid Sessions

| invalid_reason | count |
| --- | --- |
| or_size_below_min | 36 |
| or_size_above_max | 2 |
| insufficient_opening_range_bars | 1 |

## Relevant 5m Gaps

No gaps above 60 minutes inside the NY validation window.

## Notes

- Reports prioritize `or_size_vs_daily_atr`; `or_size_vs_atr` is only a compatibility alias.
- TP variants are evaluated from the same base signal and entry.

## Recommendation

no-go
