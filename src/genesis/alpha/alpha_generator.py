import itertools
from pathlib import Path

import pandas as pd

from genesis.alpha.operators import Operators
from genesis.config.settings import ALPHA_STORE_DIR, ensure_data_dirs


DEFAULT_ALPHA_FEATURES = (
    "returns_1",
    "returns_5",
    "momentum_10",
    "momentum_20",
    "volatility_10",
    "volume_ratio",
)


class AlphaGenerator:
    def __init__(self, df):
        self.df = df
        self.alphas = {}
        self.counter = 0

    def _next_name(self):
        self.counter += 1
        return f"alpha_{self.counter}"

    def generate_pairwise_alphas(self, features):
        for f1, f2 in itertools.combinations(features, 2):
            name = self._next_name()
            alpha = Operators.rank(self.df[f1] - self.df[f2])
            self.alphas[name] = alpha
        return self.alphas

    def generate_momentum_alphas(self, feature):
        for lag in [3, 5, 10, 20]:
            name = self._next_name()
            alpha = Operators.delta(self.df[feature], lag)
            self.alphas[name] = alpha
        return self.alphas

    def generate_volatility_alphas(self, feature):
        for window in [10, 20, 50]:
            name = self._next_name()
            alpha = Operators.zscore(self.df[feature], window)
            self.alphas[name] = alpha
        return self.alphas

    def generate_default_alphas(self):
        self.generate_pairwise_alphas(DEFAULT_ALPHA_FEATURES)
        self.generate_momentum_alphas("close")
        self.generate_volatility_alphas("returns_1")
        return self.alphas

    def build(self):
        return pd.DataFrame(self.alphas)

    def save(self, path: Path | str | None = None):
        alpha_df = self.build()
        ensure_data_dirs()
        path = Path(path) if path is not None else ALPHA_STORE_DIR / "generated_alphas.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        alpha_df.to_parquet(path, engine="pyarrow")
        print(f"alphas saved -> {path}")
        return alpha_df
