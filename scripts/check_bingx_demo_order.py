from _bootstrap import bootstrap

bootstrap()

from genesis.config.settings import (
    BINGX_DEMO_SYMBOL,
    BINGX_DEMO_TEST_SIDE,
    BINGX_DEMO_TEST_USDT,
)
from genesis.data.providers.bingx_client import BingXClient


def main():
    client = BingXClient(demo_trading=True, sandbox=False)
    sizing = client.usdt_to_amount(BINGX_DEMO_SYMBOL, BINGX_DEMO_TEST_USDT)
    amount = sizing["amount"]

    print("Submitting BingX demo test order")
    print(f"Symbol: {BINGX_DEMO_SYMBOL}")
    print(f"Side: {BINGX_DEMO_TEST_SIDE}")
    print(f"Configured USDT: {BINGX_DEMO_TEST_USDT}")
    print(f"Last price: {sizing['price']}")
    print(f"Market min cost: {sizing['min_cost']}")
    print(f"Market min amount: {sizing['min_amount']}")
    print(f"Final test amount: {amount}")

    response = client.create_test_order(
        symbol=BINGX_DEMO_SYMBOL,
        side=BINGX_DEMO_TEST_SIDE,
        amount=amount,
        order_type="market",
    )

    print("Test order response:")
    print(response)


if __name__ == "__main__":
    main()
