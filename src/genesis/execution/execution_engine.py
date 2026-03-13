from genesis.data.providers.bingx_client import BingXClient


def side_to_position_side(side):
    return "LONG" if side.lower() == "buy" else "SHORT"


class ExecutionEngine:
    def __init__(self, client=None, demo_trading=True):
        self.client = client or BingXClient(demo_trading=demo_trading, sandbox=False)

    def get_positions(self, symbol):
        return self.client.fetch_positions([symbol])

    def get_active_positions(self, symbol):
        positions = self.get_positions(symbol)
        return [position for position in positions if self._is_non_zero_position(position)]

    def open_position(self, symbol, side, usdt_notional):
        sizing = self.client.usdt_to_amount(symbol, usdt_notional)
        params = {"positionSide": side_to_position_side(side)}
        order = self.client.open_market_position(
            symbol=symbol,
            side=side,
            amount=sizing["amount"],
            params=params,
        )
        return {
            "action": "open",
            "symbol": symbol,
            "side": side,
            "usdt_notional": usdt_notional,
            "amount": sizing["amount"],
            "order": order,
        }

    def close_positions(self, symbol):
        positions = self.get_active_positions(symbol)
        responses = []
        for position in positions:
            responses.append(
                self.client.close_position(
                    symbol=position.get("symbol") or symbol,
                    side=position.get("side"),
                )
            )
        return responses

    def reconcile_signal(self, symbol, signal, usdt_notional, execute=False):
        active_positions = self.get_active_positions(symbol)
        current_side = active_positions[0].get("side") if active_positions else None

        plan = {
            "signal": signal,
            "symbol": symbol,
            "current_side": current_side,
            "has_position": bool(active_positions),
            "actions": [],
        }

        if signal == "flat":
            if active_positions:
                plan["actions"].append("close_positions")
                if execute:
                    plan["close_responses"] = self.close_positions(symbol)
            return plan

        target_side = "long" if signal == "long" else "short"
        open_side = "buy" if signal == "long" else "sell"

        if current_side == target_side:
            plan["actions"].append("hold")
            return plan

        if active_positions:
            plan["actions"].append("close_positions")
            if execute:
                plan["close_responses"] = self.close_positions(symbol)

        plan["actions"].append(f"open_{target_side}")
        if execute:
            plan["open_response"] = self.open_position(symbol, open_side, usdt_notional)

        return plan

    def _is_non_zero_position(self, position):
        contracts = position.get("contracts")
        if contracts not in (None, 0, 0.0, "0", "0.0", "0.0000"):
            try:
                return float(contracts) != 0.0
            except Exception:
                return True
        return False
