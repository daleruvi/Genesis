from _bootstrap import bootstrap

bootstrap()

from genesis.config.settings import (
    BINGX_DEMO_OPEN_SIDE,
    BINGX_DEMO_SYMBOL,
    BINGX_DEMO_OPEN_USDT,
)
from genesis.data.providers.bingx_client import BingXClient


def build_position_params(side):
    if side.lower() == "buy":
        return {"positionSide": "LONG"}
    return {"positionSide": "SHORT"}


def main():
    client = BingXClient(demo_trading=True, sandbox=False)
    sizing = client.usdt_to_amount(BINGX_DEMO_SYMBOL, BINGX_DEMO_OPEN_USDT)
    amount = sizing["amount"]

    print("Opening BingX demo position")
    print(f"Symbol: {BINGX_DEMO_SYMBOL}")
    print(f"Side: {BINGX_DEMO_OPEN_SIDE}")
    print(f"Configured USDT: {BINGX_DEMO_OPEN_USDT}")
    print(f"Last price: {sizing['price']}")
    print(f"Market min cost: {sizing['min_cost']}")
    print(f"Market min amount: {sizing['min_amount']}")
    print(f"Final amount: {amount}")

    order = client.open_market_position(
        symbol=BINGX_DEMO_SYMBOL,
        side=BINGX_DEMO_OPEN_SIDE,
        amount=amount,
        params=build_position_params(BINGX_DEMO_OPEN_SIDE),
    )

    print("Open position response:")
    print(order)

    positions = client.fetch_positions([BINGX_DEMO_SYMBOL])
    print(f"Positions returned: {len(positions)}")
    for position in positions:
        print(position)


if __name__ == "__main__":
    main()
