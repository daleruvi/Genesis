from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class AlphaSelector:
    max_correlation: float = 0.8
    top_n: int = 5

    def correlation_matrix(self, alphas: pd.DataFrame) -> pd.DataFrame:
        return alphas.corr().fillna(0.0)

    def select(self, rankings: pd.DataFrame, alphas: pd.DataFrame) -> pd.DataFrame:
        if rankings.empty:
            return rankings.copy()

        corr = self.correlation_matrix(alphas)
        selected_rows: list[pd.Series] = []
        selected_names: list[str] = []

        for _, row in rankings.sort_values("score", ascending=False).iterrows():
            alpha_name = row["alpha"]
            if alpha_name not in corr.columns:
                continue

            if selected_names:
                max_abs_corr = corr.loc[alpha_name, selected_names].abs().max()
                if max_abs_corr >= self.max_correlation:
                    continue

            selected_rows.append(row)
            selected_names.append(alpha_name)

            if len(selected_names) >= self.top_n:
                break

        if not selected_rows:
            return rankings.head(0).copy()

        selected = pd.DataFrame(selected_rows).reset_index(drop=True)
        selected["selection_rank"] = range(1, len(selected) + 1)
        return selected

    def selected_correlation_matrix(self, selected: pd.DataFrame, alphas: pd.DataFrame) -> pd.DataFrame:
        if selected.empty:
            return pd.DataFrame()
        names = selected["alpha"].tolist()
        return self.correlation_matrix(alphas[names])
