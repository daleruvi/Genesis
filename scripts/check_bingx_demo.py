from _bootstrap import bootstrap

bootstrap()

from genesis.config.settings import BINGX_DEMO_SYMBOL
from genesis.data.providers.bingx_client import BingXClient


def print_section(title):
    print()
    print(f"=== {title} ===")


def main():
    client = BingXClient(demo_trading=True, sandbox=False)

    print("BingX demo mode enabled")
    print(f"Demo symbol: {BINGX_DEMO_SYMBOL}")

    print_section("Ticker")
    try:
        ticker = client.fetch_ticker(BINGX_DEMO_SYMBOL)
        print(f"Last price: {ticker.get('last')}")
        print(f"Bid: {ticker.get('bid')}")
        print(f"Ask: {ticker.get('ask')}")
    except Exception as exc:
        print(f"Ticker check failed: {exc}")

    print_section("Virtual USDT Endpoint")
    try:
        vst_info = client.fetch_virtual_usdt_balance()
        print(f"Virtual USDT response keys: {list(vst_info.keys())}")
        print(f"Virtual USDT payload: {vst_info}")
        vst_amount = None
        data = vst_info.get("data")
        if isinstance(data, dict):
            vst_amount = data.get("amount")
        print(f"Virtual USDT amount from getvst: {vst_amount}")
    except Exception as exc:
        print(f"Virtual USDT check failed: {exc}")

    print_section("Demo Account Balance")
    try:
        balance = client.fetch_balance()
        print(f"Balance keys: {list(balance.keys())[:10]}")
        free = balance.get("free", {})
        used = balance.get("used", {})
        total = balance.get("total", {})
        print(f"USDT free: {free.get('USDT')}")
        print(f"USDT used: {used.get('USDT')}")
        print(f"USDT total: {total.get('USDT')}")
    except Exception as exc:
        print(f"Balance check failed: {exc}")

    print_section("Demo Open Orders")
    try:
        open_orders = client.fetch_open_orders(BINGX_DEMO_SYMBOL)
        print(f"Open orders: {len(open_orders)}")
        if open_orders:
            print(open_orders[0])
    except Exception as exc:
        print(f"Open orders check failed: {exc}")

    print_section("Demo Positions")
    try:
        positions = client.fetch_positions([BINGX_DEMO_SYMBOL])
        print(f"Positions returned: {len(positions)}")
        if positions:
            print(positions[0])
    except Exception as exc:
        print(f"Positions check failed: {exc}")

    print_section("Interpretation")
    print("If `getvst` and `fetch_balance()` return different values, they are not reporting the same thing.")
    print("Use `fetch_balance()` as the main account-level reference and treat `getvst` as a product-specific endpoint.")


if __name__ == "__main__":
    main()
