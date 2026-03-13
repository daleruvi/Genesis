from _bootstrap import bootstrap

bootstrap()

from genesis.config.settings import BINGX_DEMO_SYMBOL
from genesis.data.providers.bingx_client import BingXClient


def non_zero_position(position):
    contracts = position.get("contracts")
    if contracts not in (None, 0, 0.0, "0", "0.0", "0.0000"):
        try:
            return float(contracts) != 0.0
        except Exception:
            return True

    info = position.get("info") or {}
    for key in ("positionAmt", "availableAmt", "positionQty", "quantity"):
        value = info.get(key)
        if value not in (None, "", "0", "0.0", "0.0000"):
            try:
                return float(value) != 0.0
            except Exception:
                return True
    return False


def main():
    client = BingXClient(demo_trading=True, sandbox=False)

    print("Closing BingX demo positions")
    positions = client.fetch_positions([BINGX_DEMO_SYMBOL])
    active_positions = [position for position in positions if non_zero_position(position)]

    print(f"Active positions found: {len(active_positions)}")
    for position in active_positions:
        print(position)

    if not active_positions:
        print("No open demo positions found.")
        return

    for position in active_positions:
        symbol = position.get("symbol") or BINGX_DEMO_SYMBOL
        side = position.get("side")
        print(f"Closing position on {symbol} side={side}")
        response = client.close_position(symbol=symbol, side=side)
        print(response)

    positions_after = client.fetch_positions([BINGX_DEMO_SYMBOL])
    remaining = [position for position in positions_after if non_zero_position(position)]
    print(f"Remaining active positions: {len(remaining)}")
    for position in remaining:
        print(position)


if __name__ == "__main__":
    main()
