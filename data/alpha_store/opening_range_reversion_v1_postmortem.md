# Opening Range Reversion V1 Post-Mortem

## Executive Summary

Final recommendation: `abandon`.

ORR V1 was validated across BTCUSDT futures UM, QQQ and SPY with reproducible historical data. The failure mode is performance, not infrastructure: all evaluated variants remain below profitability thresholds and no demo trading is recommended.

## What Was Tested

- BTCUSDT futures UM 2025 with Binance Vision historical data.
- QQQ 2025 with Polygon Stocks adjusted data.
- SPY 2025 with Polygon Stocks adjusted data.
- TP variants: `midpoint` and `opposite_extreme`.

## Data Quality

Daily warmup and 5m coverage were sufficient for the tested datasets. Known equity partial sessions/gaps are reported as market-calendar artifacts, not automatic no-data conditions when global coverage is sufficient.

## Result By Market And TP Variant

| symbol | market_type | tp_variant | total_return | win_rate | profit_factor | max_drawdown | expectancy | trades_count | return_to_drawdown_ratio | top_5_trades_pnl_share | decision |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BTCUSDT | crypto_futures | midpoint | -0.4831 | 0.0972 | 0.0476 | -0.4831 | -0.003 | 216 | -0.9999 | 0.5509 | no_go |
| BTCUSDT | crypto_futures | opposite_extreme | -0.3468 | 0.3657 | 0.3577 | -0.3543 | -0.002 | 216 | -0.9788 | 0.2377 | no_go |
| QQQ | equity | midpoint | -0.1552 | 0.3 | 0.1546 | -0.1589 | -0.0012 | 140 | -0.9768 | 0.4048 | no_go |
| QQQ | equity | opposite_extreme | -0.1226 | 0.5429 | 0.5391 | -0.1356 | -0.0009 | 140 | -0.9041 | 0.2314 | no_go |
| SPY | equity | midpoint | -0.128 | 0.2375 | 0.1417 | -0.13 | -0.0009 | 160 | -0.9847 | 0.41 | no_go |
| SPY | equity | opposite_extreme | -0.0865 | 0.5 | 0.5443 | -0.0933 | -0.0006 | 160 | -0.9273 | 0.2276 | no_go |

## Trade Diagnostics

| symbol | market_type | tp_variant | trades_count | signals_count | signal_to_trade_rate | avg_pnl | median_pnl | best_5_trades_sum | worst_5_trades_sum | max_consecutive_losses | long_trades | short_trades | long_return | short_return | best_entry_hour | avg_bars_in_trade | median_bars_in_trade | top_5_trades_pnl_share | breakout_above_count | breakout_below_count | discarded_signals_count | median_confirmation_lag_minutes | median_entry_lag_minutes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BTCUSDT | crypto_futures | midpoint | 216 | 216 | 1.0 | -0.003 | -0.0023 | 0.0181 | -0.0812 | 45 | 100 | 116 | -0.3356 | -0.3218 | 16 | 4.0602 | 1.0 | 0.5509 | 116 | 100 | 0 | 0.0 | 5.0 |
| BTCUSDT | crypto_futures | opposite_extreme | 216 | 216 | 1.0 | -0.002 | -0.0007 | 0.0558 | -0.1039 | 11 | 100 | 116 | -0.2129 | -0.2087 | 16 | 13.1667 | 5.0 | 0.2377 | 116 | 100 | 0 | 0.0 | 5.0 |
| QQQ | equity | midpoint | 140 | 140 | 1.0 | -0.0012 | -0.0006 | 0.0125 | -0.0434 | 9 | 75 | 65 | -0.0926 | -0.0756 | 13 | 4.4214 | 1.0 | 0.4048 | 65 | 75 | 0 | 0.0 | 5.0 |
| QQQ | equity | opposite_extreme | 140 | 140 | 1.0 | -0.0009 | 0.0004 | 0.035 | -0.0817 | 4 | 75 | 65 | -0.0714 | -0.0579 | 13 | 14.3429 | 5.0 | 0.2314 | 65 | 75 | 0 | 0.0 | 5.0 |
| SPY | equity | midpoint | 160 | 160 | 1.0 | -0.0009 | -0.0005 | 0.0093 | -0.0271 | 14 | 76 | 84 | -0.0689 | -0.0678 | 16 | 2.45 | 1.0 | 0.41 | 84 | 76 | 0 | 0.0 | 5.0 |
| SPY | equity | opposite_extreme | 160 | 160 | 1.0 | -0.0006 | -0.0 | 0.0245 | -0.0392 | 11 | 76 | 84 | -0.0318 | -0.0581 | 13 | 12.2375 | 4.0 | 0.2276 | 84 | 76 | 0 | 0.0 | 5.0 |

## Side Breakdown

| symbol | market_type | tp_variant | side | trades_count | total_return | win_rate | profit_factor | expectancy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BTCUSDT | crypto_futures | midpoint | long | 100 | -0.2861 | 0.1 | 0.0391 | -0.0034 |
| BTCUSDT | crypto_futures | midpoint | short | 116 | -0.2759 | 0.0948 | 0.0563 | -0.0028 |
| BTCUSDT | crypto_futures | opposite_extreme | long | 100 | -0.1935 | 0.34 | 0.3396 | -0.0021 |
| BTCUSDT | crypto_futures | opposite_extreme | short | 116 | -0.1901 | 0.3879 | 0.3752 | -0.0018 |
| QQQ | equity | midpoint | long | 75 | -0.0886 | 0.2933 | 0.0999 | -0.0012 |
| QQQ | equity | midpoint | short | 65 | -0.0731 | 0.3077 | 0.2131 | -0.0012 |
| QQQ | equity | opposite_extreme | long | 75 | -0.0695 | 0.5333 | 0.5064 | -0.001 |
| QQQ | equity | opposite_extreme | short | 65 | -0.0571 | 0.5538 | 0.5739 | -0.0009 |
| SPY | equity | midpoint | long | 76 | -0.0667 | 0.2105 | 0.0913 | -0.0009 |
| SPY | equity | midpoint | short | 84 | -0.0657 | 0.2619 | 0.1875 | -0.0008 |
| SPY | equity | opposite_extreme | long | 76 | -0.0315 | 0.5395 | 0.589 | -0.0004 |
| SPY | equity | opposite_extreme | short | 84 | -0.0569 | 0.4643 | 0.5155 | -0.0007 |

## Monthly Performance

| symbol | market_type | period | tp_variant | trades_count | total_return | win_rate | profit_factor |
| --- | --- | --- | --- | --- | --- | --- | --- |
| BTCUSDT | crypto_futures | 2025-01 | midpoint | 16 | -0.0427 | 0.125 | 0.0294 |
| BTCUSDT | crypto_futures | 2025-01 | opposite_extreme | 16 | 0.0057 | 0.4375 | 1.2534 |
| BTCUSDT | crypto_futures | 2025-02 | midpoint | 13 | -0.0541 | 0.1538 | 0.0935 |
| BTCUSDT | crypto_futures | 2025-02 | opposite_extreme | 13 | -0.0552 | 0.3077 | 0.2985 |
| BTCUSDT | crypto_futures | 2025-03 | midpoint | 16 | -0.0441 | 0.3125 | 0.1725 |
| BTCUSDT | crypto_futures | 2025-03 | opposite_extreme | 16 | -0.0028 | 0.5 | 0.9443 |
| BTCUSDT | crypto_futures | 2025-04 | midpoint | 16 | -0.0518 | 0.125 | 0.0896 |
| BTCUSDT | crypto_futures | 2025-04 | opposite_extreme | 16 | -0.0502 | 0.4375 | 0.3162 |
| BTCUSDT | crypto_futures | 2025-05 | midpoint | 23 | -0.0805 | 0.0435 | 0.0237 |
| BTCUSDT | crypto_futures | 2025-05 | opposite_extreme | 23 | -0.0691 | 0.2174 | 0.1746 |
| BTCUSDT | crypto_futures | 2025-06 | midpoint | 16 | -0.025 | 0.125 | 0.0361 |
| BTCUSDT | crypto_futures | 2025-06 | opposite_extreme | 16 | -0.0241 | 0.4375 | 0.4326 |
| BTCUSDT | crypto_futures | 2025-07 | midpoint | 18 | -0.0557 | 0.0556 | 0.0066 |
| BTCUSDT | crypto_futures | 2025-07 | opposite_extreme | 18 | -0.0218 | 0.5 | 0.3962 |
| BTCUSDT | crypto_futures | 2025-08 | midpoint | 22 | -0.0396 | 0.0455 | 0.0243 |
| BTCUSDT | crypto_futures | 2025-08 | opposite_extreme | 22 | -0.027 | 0.3636 | 0.3922 |
| BTCUSDT | crypto_futures | 2025-09 | midpoint | 17 | -0.0429 | 0.0 | 0.0 |
| BTCUSDT | crypto_futures | 2025-09 | opposite_extreme | 17 | -0.0498 | 0.1765 | 0.0264 |
| BTCUSDT | crypto_futures | 2025-10 | midpoint | 20 | -0.0641 | 0.05 | 0.0018 |
| BTCUSDT | crypto_futures | 2025-10 | opposite_extreme | 20 | -0.0583 | 0.2 | 0.0659 |
| BTCUSDT | crypto_futures | 2025-11 | midpoint | 21 | -0.0573 | 0.1429 | 0.0789 |
| BTCUSDT | crypto_futures | 2025-11 | opposite_extreme | 21 | -0.0112 | 0.4762 | 0.7207 |
| BTCUSDT | crypto_futures | 2025-12 | midpoint | 18 | -0.0827 | 0.0556 | 0.02 |
| BTCUSDT | crypto_futures | 2025-12 | opposite_extreme | 18 | -0.0513 | 0.3889 | 0.2605 |
| QQQ | equity | 2025-01 | midpoint | 11 | -0.0046 | 0.5455 | 0.5977 |
| QQQ | equity | 2025-01 | opposite_extreme | 11 | -0.0003 | 0.4545 | 0.9932 |
| QQQ | equity | 2025-02 | midpoint | 6 | -0.0112 | 0.1667 | 0.0218 |
| QQQ | equity | 2025-02 | opposite_extreme | 6 | -0.0111 | 0.3333 | 0.0968 |
| QQQ | equity | 2025-03 | midpoint | 13 | -0.0272 | 0.2308 | 0.072 |
| QQQ | equity | 2025-03 | opposite_extreme | 13 | -0.0375 | 0.3846 | 0.2438 |
| QQQ | equity | 2025-04 | midpoint | 12 | -0.0467 | 0.1667 | 0.0402 |
| QQQ | equity | 2025-04 | opposite_extreme | 12 | -0.0512 | 0.4167 | 0.2268 |
| QQQ | equity | 2025-05 | midpoint | 12 | -0.0063 | 0.4167 | 0.3176 |
| QQQ | equity | 2025-05 | opposite_extreme | 12 | -0.0105 | 0.5833 | 0.5138 |
| QQQ | equity | 2025-06 | midpoint | 11 | -0.0032 | 0.4545 | 0.5385 |
| QQQ | equity | 2025-06 | opposite_extreme | 11 | 0.0116 | 0.7273 | 2.7938 |
| QQQ | equity | 2025-07 | midpoint | 11 | -0.0063 | 0.3636 | 0.0782 |
| QQQ | equity | 2025-07 | opposite_extreme | 11 | -0.0025 | 0.7273 | 0.7789 |
| QQQ | equity | 2025-08 | midpoint | 12 | -0.0091 | 0.3333 | 0.1943 |
| QQQ | equity | 2025-08 | opposite_extreme | 12 | -0.006 | 0.5833 | 0.5935 |
| QQQ | equity | 2025-09 | midpoint | 10 | -0.0083 | 0.2 | 0.062 |
| QQQ | equity | 2025-09 | opposite_extreme | 10 | -0.0036 | 0.6 | 0.6392 |
| QQQ | equity | 2025-10 | midpoint | 15 | -0.0101 | 0.2667 | 0.2397 |
| QQQ | equity | 2025-10 | opposite_extreme | 15 | -0.0129 | 0.6 | 0.5339 |
| QQQ | equity | 2025-11 | midpoint | 11 | -0.0077 | 0.4545 | 0.4393 |
| QQQ | equity | 2025-11 | opposite_extreme | 11 | 0.0056 | 0.6364 | 1.3509 |
| QQQ | equity | 2025-12 | midpoint | 16 | -0.026 | 0.0625 | 0.0133 |
| QQQ | equity | 2025-12 | opposite_extreme | 16 | -0.0101 | 0.4375 | 0.5089 |
| SPY | equity | 2025-01 | midpoint | 13 | -0.0035 | 0.3846 | 0.4994 |
| SPY | equity | 2025-01 | opposite_extreme | 13 | -0.0137 | 0.4615 | 0.4187 |
| SPY | equity | 2025-02 | midpoint | 10 | -0.0215 | 0.2 | 0.0307 |
| SPY | equity | 2025-02 | opposite_extreme | 10 | -0.0224 | 0.0 | 0.0 |
| SPY | equity | 2025-03 | midpoint | 11 | -0.0147 | 0.0909 | 0.0169 |
| SPY | equity | 2025-03 | opposite_extreme | 11 | -0.0114 | 0.6364 | 0.4721 |
| SPY | equity | 2025-04 | midpoint | 13 | -0.0254 | 0.2308 | 0.1243 |
| SPY | equity | 2025-04 | opposite_extreme | 13 | -0.002 | 0.5385 | 0.9068 |
| SPY | equity | 2025-05 | midpoint | 16 | -0.0096 | 0.375 | 0.2227 |
| SPY | equity | 2025-05 | opposite_extreme | 16 | -0.0162 | 0.4375 | 0.323 |
| SPY | equity | 2025-06 | midpoint | 15 | -0.0109 | 0.2 | 0.1511 |
| SPY | equity | 2025-06 | opposite_extreme | 15 | -0.001 | 0.5333 | 0.9111 |
| SPY | equity | 2025-07 | midpoint | 14 | -0.0109 | 0.2143 | 0.0629 |
| SPY | equity | 2025-07 | opposite_extreme | 14 | -0.0063 | 0.5714 | 0.4809 |
| SPY | equity | 2025-08 | midpoint | 12 | -0.0052 | 0.3333 | 0.2048 |
| SPY | equity | 2025-08 | opposite_extreme | 12 | -0.0023 | 0.5 | 0.7259 |
| SPY | equity | 2025-09 | midpoint | 15 | -0.0108 | 0.0667 | 0.0562 |
| SPY | equity | 2025-09 | opposite_extreme | 15 | -0.0027 | 0.6 | 0.6042 |
| SPY | equity | 2025-10 | midpoint | 15 | -0.0058 | 0.2 | 0.2311 |
| SPY | equity | 2025-10 | opposite_extreme | 15 | -0.0094 | 0.6 | 0.5238 |
| SPY | equity | 2025-11 | midpoint | 11 | -0.0063 | 0.5455 | 0.45 |
| SPY | equity | 2025-11 | opposite_extreme | 11 | 0.0119 | 0.6364 | 2.6297 |
| SPY | equity | 2025-12 | midpoint | 15 | -0.0115 | 0.0667 | 0.0177 |
| SPY | equity | 2025-12 | opposite_extreme | 15 | -0.0141 | 0.4 | 0.2784 |

## Observed Patterns

- `opposite_extreme` improves profit factor versus `midpoint` in the tested markets, but remains below 1.0.
- Win rate alone is misleading: QQQ and SPY `opposite_extreme` reach near or above 50% win rate while expectancy remains negative.
- Losses are broad enough across markets and months that this is not a single bad data slice.

## Failure Hypotheses

- The fixed NY opening-range reversion rule may be fading genuine continuation rather than transient opening dislocation.
- The stop/target geometry produces poor payoff after fees/slippage, especially for midpoint exits.
- The same base entry logic does not transfer from synthetic crypto sessions to official equity opens in its current form.

## Final Recommendation

`abandon`

Do not pass ORR V1 to demo. Any further work should be treated as redesign or fresh research, not parameter polishing of the current rule.
