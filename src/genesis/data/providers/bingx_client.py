from bingx import BingxSync

from genesis.config.settings import (
    BINGX_API_KEY,
    BINGX_DEMO_TRADING,
    BINGX_ENABLE_RATE_LIMIT,
    BINGX_SANDBOX,
    BINGX_SECRET,
    BINGX_TIMEOUT_MS,
)


def build_bingx_config(api_key=None, secret=None, public_only=False):
    config = {
        "enableRateLimit": BINGX_ENABLE_RATE_LIMIT,
        "timeout": BINGX_TIMEOUT_MS,
    }

    if public_only:
        return config

    api_key = api_key or BINGX_API_KEY
    secret = secret or BINGX_SECRET

    if api_key:
        config["apiKey"] = api_key
    if secret:
        config["secret"] = secret

    return config


def create_bingx_sync_client(api_key=None, secret=None, demo_trading=None, sandbox=None, public_only=False):
    use_demo_trading = BINGX_DEMO_TRADING if demo_trading is None else demo_trading
    use_sandbox = BINGX_SANDBOX if sandbox is None else sandbox

    if public_only:
        use_demo_trading = False
        use_sandbox = False

    if use_demo_trading and use_sandbox:
        raise ValueError("BingX demo trading and sandbox mode cannot be enabled at the same time.")

    exchange = BingxSync(build_bingx_config(api_key=api_key, secret=secret, public_only=public_only))

    if use_sandbox and hasattr(exchange, "set_sandbox_mode"):
        exchange.set_sandbox_mode(True)

    if use_demo_trading:
        if hasattr(exchange, "enable_demo_trading") and "demo" in exchange.urls:
            exchange.enable_demo_trading(True)
        elif hasattr(exchange, "set_sandbox_mode") and "test" in exchange.urls:
            # BingX exposes Virtual USDT through the test host in this SDK build.
            exchange.set_sandbox_mode(True)
        else:
            raise NotImplementedError("The current BingX SDK does not expose a usable demo trading endpoint.")

    return exchange


class BingXClient:
    def __init__(self, api_key=None, secret=None, demo_trading=None, sandbox=None):
        self.exchange = create_bingx_sync_client(
            api_key=api_key,
            secret=secret,
            demo_trading=demo_trading,
            sandbox=sandbox,
        )

    def fetch_order_book(self, symbol="BTC/USDT"):
        return self.exchange.fetch_order_book(symbol)

    def fetch_balance(self):
        return self.exchange.fetch_balance()

    def fetch_open_orders(self, symbol=None):
        if symbol is None:
            return self.exchange.fetch_open_orders()
        return self.exchange.fetch_open_orders(symbol)

    def fetch_positions(self, symbols=None):
        if not hasattr(self.exchange, "fetch_positions"):
            raise NotImplementedError("The current BingX client does not expose fetch_positions().")
        if symbols is None:
            return self.exchange.fetch_positions()
        return self.exchange.fetch_positions(symbols)

    def fetch_position(self, symbol, params=None):
        params = {} if params is None else dict(params)
        return self.exchange.fetch_position(symbol, params)

    def fetch_ticker(self, symbol="BTC/USDT"):
        return self.exchange.fetch_ticker(symbol)

    def fetch_market(self, symbol):
        self.exchange.load_markets()
        return self.exchange.market(symbol)

    def amount_to_precision(self, symbol, amount):
        return self.exchange.amount_to_precision(symbol, amount)

    def price_to_precision(self, symbol, price):
        return self.exchange.price_to_precision(symbol, price)

    def usdt_to_amount(self, symbol, usdt_value, price=None):
        market = self.fetch_market(symbol)
        ticker_price = price
        if ticker_price is None:
            ticker_price = float(self.fetch_ticker(symbol)["last"])

        min_amount = ((market.get("limits") or {}).get("amount") or {}).get("min") or 0
        min_cost = ((market.get("limits") or {}).get("cost") or {}).get("min") or 0
        contract_size = market.get("contractSize") or 1

        target_usdt = max(float(usdt_value), float(min_cost or 0))
        raw_amount = target_usdt / (float(ticker_price) * float(contract_size))
        raw_amount = max(raw_amount, float(min_amount or 0))

        amount = self.amount_to_precision(symbol, raw_amount)
        return {
            "amount": amount,
            "target_usdt": target_usdt,
            "min_amount": min_amount,
            "min_cost": min_cost,
            "price": float(ticker_price),
            "contract_size": contract_size,
        }

    def fetch_virtual_usdt_balance(self):
        if hasattr(self.exchange, "swap_v2_private_post_trade_getvst"):
            return self.exchange.swap_v2_private_post_trade_getvst({})
        if hasattr(self.exchange, "swap_v1_private_post_trade_getvst"):
            return self.exchange.swap_v1_private_post_trade_getvst({})
        raise NotImplementedError("The current BingX client does not expose a Virtual USDT endpoint.")

    def create_market_order(self, symbol, side, amount):
        return self.exchange.create_order(
            symbol,
            "market",
            side,
            amount,
        )

    def open_market_position(self, symbol, side, amount, params=None):
        params = {} if params is None else dict(params)
        return self.exchange.create_order(
            symbol,
            "market",
            side,
            amount,
            params=params,
        )

    def create_limit_order(self, symbol, side, amount, price, params=None):
        params = {} if params is None else dict(params)
        return self.exchange.create_order(
            symbol,
            "limit",
            side,
            amount,
            price,
            params=params,
        )

    def create_test_order(self, symbol, side, amount, order_type="market", params=None):
        params = {} if params is None else dict(params)
        params["test"] = True
        return self.exchange.create_order(
            symbol,
            order_type,
            side,
            amount,
            params=params,
        )

    def fetch_order(self, order_id, symbol=None):
        return self.exchange.fetch_order(order_id, symbol)

    def cancel_order(self, order_id, symbol=None):
        return self.exchange.cancel_order(order_id, symbol)

    def close_position(self, symbol, side=None, params=None):
        params = {} if params is None else dict(params)
        return self.exchange.close_position(symbol, side=side, params=params)

    def close_all_positions(self, params=None):
        params = {} if params is None else dict(params)
        return self.exchange.close_all_positions(params=params)
