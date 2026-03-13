class RiskManager:
    def __init__(self, max_position_usdt, max_open_positions=1):
        self.max_position_usdt = float(max_position_usdt)
        self.max_open_positions = int(max_open_positions)

    def normalize_target_usdt(self, requested_usdt):
        return min(float(requested_usdt), self.max_position_usdt)

    def count_active_positions(self, positions):
        return sum(1 for position in positions if self._is_non_zero_position(position))

    def can_open_new_position(self, positions, requested_usdt):
        active_count = self.count_active_positions(positions)
        allowed = active_count < self.max_open_positions and requested_usdt > 0
        return {
            "allowed": allowed,
            "active_positions": active_count,
            "target_usdt": self.normalize_target_usdt(requested_usdt),
        }

    def _is_non_zero_position(self, position):
        contracts = position.get("contracts")
        if contracts not in (None, 0, 0.0, "0", "0.0", "0.0000"):
            try:
                return float(contracts) != 0.0
            except Exception:
                return True
        return False
