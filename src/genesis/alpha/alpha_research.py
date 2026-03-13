from __future__ import annotations

import pandas as pd

from genesis.alpha.alpha_evaluator import AlphaEvaluator
from genesis.backtest.engine import BacktestEngine
from genesis.backtest.performance import PerformanceAnalyzer


class AlphaResearch:
    def __init__(
        self,
        close: pd.Series,
        fee_rate: float = 0.0005,
        slippage_rate: float = 0.0005,
        train_ratio: float = 0.7,
        periods_per_year: int = 365 * 6,
    ):
        self.close = close.astype(float)
        self.forward_returns = self.close.pct_change().shift(-1)
        self.backtest_engine = BacktestEngine(fee_rate=fee_rate, slippage_rate=slippage_rate)
        self.performance = PerformanceAnalyzer(periods_per_year=periods_per_year)
        self.evaluator = AlphaEvaluator(pd.DataFrame({"close": close}), self.forward_returns)
        self.train_ratio = train_ratio

    def _positions_from_alpha(
        self,
        alpha: pd.Series,
        signal_mode: str,
        lower_q: float,
        upper_q: float,
    ) -> pd.Series:
        alpha = alpha.astype(float)
        if signal_mode == "quantile":
            split_idx = max(1, int(len(alpha.dropna()) * self.train_ratio))
            alpha_train = alpha.dropna().iloc[:split_idx]
            if alpha_train.empty:
                return pd.Series(0.0, index=alpha.index)
            lower = alpha_train.quantile(lower_q)
            upper = alpha_train.quantile(upper_q)
            return alpha.apply(lambda x: 1.0 if x >= upper else (-1.0 if x <= lower else 0.0))

        return self.backtest_engine.signal_to_positions(alpha, mode=signal_mode, lower_q=lower_q, upper_q=upper_q)

    def _score_rankings(self, rankings: pd.DataFrame) -> pd.DataFrame:
        frame = rankings.copy()
        components = {
            "rank_test_sharpe": frame["test_sharpe"].rank(pct=True, ascending=True),
            "rank_test_ic_abs": frame["test_ic_abs"].rank(pct=True, ascending=True),
            "rank_test_total_return": frame["test_total_return"].rank(pct=True, ascending=True),
            "rank_test_profit_factor": frame["test_profit_factor"].rank(pct=True, ascending=True),
            "rank_test_mdd": frame["test_max_drawdown"].abs().rank(pct=True, ascending=False),
            "rank_test_turnover": frame["test_total_turnover"].rank(pct=True, ascending=False),
            "rank_temporal_positive_ratio": frame["temporal_positive_window_ratio"].rank(pct=True, ascending=True),
            "rank_temporal_consistency": frame["temporal_consistency"].rank(pct=True, ascending=True),
            "rank_temporal_sharpe_mean": frame["temporal_sharpe_mean"].rank(pct=True, ascending=True),
        }
        for column, values in components.items():
            frame[column] = values.fillna(0.0)

        frame["score"] = (
            (0.25 * frame["rank_test_sharpe"])
            + (0.20 * frame["rank_test_ic_abs"])
            + (0.15 * frame["rank_test_total_return"])
            + (0.10 * frame["rank_test_profit_factor"])
            + (0.05 * frame["rank_test_mdd"])
            + (0.05 * frame["rank_test_turnover"])
            + (0.10 * frame["rank_temporal_positive_ratio"])
            + (0.05 * frame["rank_temporal_consistency"])
            + (0.05 * frame["rank_temporal_sharpe_mean"])
        )
        return frame.sort_values(["score", "test_sharpe", "test_ic_abs"], ascending=False).reset_index(drop=True)

    def temporal_stability(
        self,
        alpha: pd.Series,
        signal_mode: str = "quantile",
        lower_q: float = 0.2,
        upper_q: float = 0.8,
        window_size: int = 180,
        step_size: int = 90,
    ) -> pd.DataFrame:
        alpha = alpha.astype(float)
        windows: list[dict] = []
        if len(alpha) < window_size:
            return pd.DataFrame(windows)

        for start in range(0, len(alpha) - window_size + 1, step_size):
            end = start + window_size
            alpha_window = alpha.iloc[start:end]
            close_window = self.close.loc[alpha_window.index]
            returns_window = self.forward_returns.loc[alpha_window.index]
            positions = self._positions_from_alpha(
                alpha=alpha_window,
                signal_mode=signal_mode,
                lower_q=lower_q,
                upper_q=upper_q,
            )
            backtest = self.backtest_engine.run(close_window, positions)
            summary = self.performance.summarize(
                backtest["strategy_returns"],
                turnover=backtest["turnover"],
            )
            ic = AlphaEvaluator(
                pd.DataFrame({"close": close_window}),
                returns_window,
            ).information_coefficient(alpha_window)
            windows.append(
                {
                    "window_id": len(windows) + 1,
                    "start_index": alpha_window.index[0],
                    "end_index": alpha_window.index[-1],
                    "window_size": len(alpha_window),
                    "ic": ic,
                    **summary,
                }
            )

        return pd.DataFrame(windows)

    def summarize_temporal_stability(self, stability_windows: pd.DataFrame) -> dict:
        if stability_windows.empty:
            return {
                "temporal_window_count": 0,
                "temporal_positive_window_ratio": 0.0,
                "temporal_sharpe_mean": 0.0,
                "temporal_sharpe_std": 0.0,
                "temporal_ic_mean": 0.0,
                "temporal_total_return_mean": 0.0,
                "temporal_consistency": 0.0,
            }

        positive_ratio = float((stability_windows["total_return"] > 0).mean())
        sharpe_mean = float(stability_windows["sharpe"].mean())
        sharpe_std = float(stability_windows["sharpe"].std(ddof=0))
        ic_mean = float(stability_windows["ic"].fillna(0.0).mean())
        total_return_mean = float(stability_windows["total_return"].mean())
        consistency = positive_ratio * max(0.0, sharpe_mean)
        if sharpe_std > 0:
            consistency = consistency / (1.0 + sharpe_std)

        return {
            "temporal_window_count": int(len(stability_windows)),
            "temporal_positive_window_ratio": positive_ratio,
            "temporal_sharpe_mean": sharpe_mean,
            "temporal_sharpe_std": sharpe_std,
            "temporal_ic_mean": ic_mean,
            "temporal_total_return_mean": total_return_mean,
            "temporal_consistency": float(consistency),
        }

    def evaluate_alphas(
        self,
        alphas: pd.DataFrame,
        signal_mode: str = "quantile",
        lower_q: float = 0.2,
        upper_q: float = 0.8,
        window_size: int = 180,
        step_size: int = 90,
    ) -> pd.DataFrame:
        rows: list[dict] = []

        for alpha_name in alphas.columns:
            alpha = alphas[alpha_name]
            metrics = self.evaluator.evaluate_alpha(alpha)
            positions = self._positions_from_alpha(
                alpha=alpha,
                signal_mode=signal_mode,
                lower_q=lower_q,
                upper_q=upper_q,
            )
            backtest = self.backtest_engine.run(self.close, positions)
            train_df, test_df = self.backtest_engine.train_test_split(backtest, train_ratio=self.train_ratio)
            full_summary = self.performance.summarize(
                backtest["strategy_returns"],
                turnover=backtest["turnover"],
            )
            train_summary = self.performance.summarize(
                train_df["strategy_returns"],
                turnover=train_df["turnover"],
            )
            test_summary = self.performance.summarize(
                test_df["strategy_returns"],
                turnover=test_df["turnover"],
            )

            train_ic = self.evaluator.information_coefficient(alpha.loc[train_df.index])
            test_ic = self.evaluator.information_coefficient(alpha.loc[test_df.index])
            stability_windows = self.temporal_stability(
                alpha=alpha,
                signal_mode=signal_mode,
                lower_q=lower_q,
                upper_q=upper_q,
                window_size=window_size,
                step_size=step_size,
            )
            stability_summary = self.summarize_temporal_stability(stability_windows)

            row = {
                "alpha": alpha_name,
                "full_ic": metrics["IC"],
                "full_alpha_sharpe": metrics["Sharpe"],
                "full_alpha_mdd": metrics["MaxDrawdown"],
                "train_ic": train_ic,
                "test_ic": test_ic,
                "test_ic_abs": abs(test_ic) if pd.notna(test_ic) else 0.0,
            }

            for prefix, summary in (
                ("train", train_summary),
                ("test", test_summary),
                ("full", full_summary),
            ):
                for metric_name, value in summary.items():
                    row[f"{prefix}_{metric_name}"] = value
            row.update(stability_summary)

            rows.append(row)

        rankings = pd.DataFrame(rows)
        if rankings.empty:
            return rankings
        return self._score_rankings(rankings)
