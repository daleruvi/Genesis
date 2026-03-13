from _bootstrap import bootstrap

bootstrap()

from genesis.config.settings import (
    BINGX_DEMO_CYCLE_PRICE_OFFSET_PCT,
    BINGX_DEMO_CYCLE_SIDE,
    BINGX_DEMO_SYMBOL,
    BINGX_DEMO_CYCLE_USDT,
)
from genesis.data.providers.bingx_client import BingXClient


def print_section(title):
    print()
    print(f"=== {title} ===")


def choose_limit_price(last_price, side, offset_pct):
    if side.lower() == "buy":
        return last_price * (1 - offset_pct)
    return last_price * (1 + offset_pct)


def build_position_params(side):
    if side.lower() == "buy":
        return {"positionSide": "LONG"}
    return {"positionSide": "SHORT"}


def main():
    client = BingXClient(demo_trading=True, sandbox=False)

    print("Running BingX demo cycle")
    print(f"Symbol: {BINGX_DEMO_SYMBOL}")
    print(f"Side: {BINGX_DEMO_CYCLE_SIDE}")
    print(f"Configured USDT: {BINGX_DEMO_CYCLE_USDT}")
    print(f"Price offset pct: {BINGX_DEMO_CYCLE_PRICE_OFFSET_PCT}")

    print_section("Balance Before")
    balance_before = client.fetch_balance()
    free_before = (balance_before.get("free") or {}).get("USDT")
    total_before = (balance_before.get("total") or {}).get("USDT")
    print(f"USDT free before: {free_before}")
    print(f"USDT total before: {total_before}")

    print_section("Ticker")
    ticker = client.fetch_ticker(BINGX_DEMO_SYMBOL)
    last_price = float(ticker["last"])
    print(f"Last price: {last_price}")

    sizing = client.usdt_to_amount(BINGX_DEMO_SYMBOL, BINGX_DEMO_CYCLE_USDT, price=last_price)
    amount = sizing["amount"]

    raw_limit_price = choose_limit_price(
        last_price=last_price,
        side=BINGX_DEMO_CYCLE_SIDE,
        offset_pct=BINGX_DEMO_CYCLE_PRICE_OFFSET_PCT,
    )
    price = client.price_to_precision(BINGX_DEMO_SYMBOL, raw_limit_price)

    print_section("Submit Demo Limit Order")
    print(f"Market min cost: {sizing['min_cost']}")
    print(f"Market min amount: {sizing['min_amount']}")
    print(f"Final amount: {amount}")
    print(f"Final limit price: {price}")

    order = client.create_limit_order(
        symbol=BINGX_DEMO_SYMBOL,
        side=BINGX_DEMO_CYCLE_SIDE,
        amount=amount,
        price=price,
        params=build_position_params(BINGX_DEMO_CYCLE_SIDE),
    )
    order_id = order.get("id")
    print(f"Order id: {order_id}")
    print(order)

    print_section("Open Orders After Submit")
    open_orders = client.fetch_open_orders(BINGX_DEMO_SYMBOL)
    print(f"Open orders: {len(open_orders)}")
    matched = None
    for candidate in open_orders:
        if candidate.get("id") == order_id:
            matched = candidate
            break
    print(f"Submitted order present in open orders: {matched is not None}")
    if matched is not None:
        print(matched)

    print_section("Positions After Submit")
    positions = client.fetch_positions([BINGX_DEMO_SYMBOL])
    print(f"Positions returned: {len(positions)}")
    if positions:
        print(positions[0])

    print_section("Cancel If Applicable")
    if matched is not None:
        cancel_response = client.cancel_order(order_id, BINGX_DEMO_SYMBOL)
        print("Cancel response:")
        print(cancel_response)
    else:
        print("No cancelable open order found. Nothing to cancel.")

    print_section("Open Orders After Cancel")
    open_orders_after = client.fetch_open_orders(BINGX_DEMO_SYMBOL)
    print(f"Open orders after cancel: {len(open_orders_after)}")

    print_section("Balance After")
    balance_after = client.fetch_balance()
    free_after = (balance_after.get("free") or {}).get("USDT")
    total_after = (balance_after.get("total") or {}).get("USDT")
    print(f"USDT free after: {free_after}")
    print(f"USDT total after: {total_after}")


if __name__ == "__main__":
    main()
