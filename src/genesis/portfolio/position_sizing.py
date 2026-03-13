from __future__ import annotations

import math


class PositionSizer:
    def __init__(
        self,
        target_annual_vol: float = 0.6,
        min_multiplier: float = 0.25,
        max_multiplier: float = 1.5,
        min_conviction_scale: float = 0.25,
        drawdown_cutoff: float = 0.10,
        drawdown_floor_scale: float = 0.35,
    ):
        self.target_annual_vol = float(target_annual_vol)
        self.min_multiplier = float(min_multiplier)
        self.max_multiplier = float(max_multiplier)
        self.min_conviction_scale = float(min_conviction_scale)
        self.drawdown_cutoff = float(drawdown_cutoff)
        self.drawdown_floor_scale = float(drawdown_floor_scale)

    def _safe_ratio(self, numerator: float, denominator: float, fallback: float = 1.0) -> float:
        if denominator <= 0 or math.isnan(denominator):
            return fallback
        return numerator / denominator

    def volatility_multiplier(self, realized_annual_vol: float) -> float:
        ratio = self._safe_ratio(self.target_annual_vol, realized_annual_vol, fallback=1.0)
        return min(max(ratio, self.min_multiplier), self.max_multiplier)

    def conviction_multiplier(self, conviction: float) -> float:
        return min(max(abs(float(conviction)), self.min_conviction_scale), 1.0)

    def drawdown_multiplier(self, recent_drawdown: float) -> float:
        drawdown = abs(float(recent_drawdown))
        if drawdown <= 0:
            return 1.0
        if drawdown <= self.drawdown_cutoff:
            return 1.0
        excess = drawdown - self.drawdown_cutoff
        penalty = min(excess / max(self.drawdown_cutoff, 1e-8), 1.0)
        return max(self.drawdown_floor_scale, 1.0 - penalty)

    def size_notional(
        self,
        base_notional_usdt: float,
        conviction: float,
        realized_annual_vol: float,
        recent_drawdown: float = 0.0,
    ) -> dict:
        vol_mult = self.volatility_multiplier(realized_annual_vol)
        conv_mult = self.conviction_multiplier(conviction)
        dd_mult = self.drawdown_multiplier(recent_drawdown)
        final_notional = max(0.0, float(base_notional_usdt) * vol_mult * conv_mult * dd_mult)
        return {
            "base_notional_usdt": float(base_notional_usdt),
            "volatility_multiplier": vol_mult,
            "conviction_multiplier": conv_mult,
            "drawdown_multiplier": dd_mult,
            "final_notional_usdt": final_notional,
        }
