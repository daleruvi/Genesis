from _bootstrap import bootstrap

bootstrap()

from genesis.config.settings import BINGX_DEFAULT_SYMBOL
from genesis.data.providers.bingx_client import BingXClient


def main():
    client = BingXClient()

    balance = client.fetch_balance()
    ticker = client.fetch_ticker(BINGX_DEFAULT_SYMBOL)

    print("BingX connection OK")
    print(f"Default symbol: {BINGX_DEFAULT_SYMBOL}")
    print(f"Last price: {ticker.get('last')}")

    top_level_keys = list(balance.keys())[:10]
    print(f"Balance keys: {top_level_keys}")

    try:
        open_orders = client.fetch_open_orders(BINGX_DEFAULT_SYMBOL)
        print(f"Open orders: {len(open_orders)}")
    except Exception as exc:
        print(f"Open orders check failed: {exc}")

    try:
        positions = client.fetch_positions([BINGX_DEFAULT_SYMBOL])
        print(f"Positions returned: {len(positions)}")
    except Exception as exc:
        print(f"Positions check failed: {exc}")


if __name__ == "__main__":
    main()
