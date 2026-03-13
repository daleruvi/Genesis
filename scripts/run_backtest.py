from __future__ import absolute_import, division, print_function, unicode_literals

import backtrader as bt
import yfinance as yf


class TestStrategy(bt.Strategy):
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print("%s, %s" % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None
        self.buyprice = None
        self.buycomm = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    "BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f"
                    % (
                        order.executed.price,
                        order.executed.value,
                        order.executed.comm,
                    )
                )
            elif order.issell():
                self.log(
                    "SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f"
                    % (
                        order.executed.price,
                        order.executed.value,
                        order.executed.comm,
                    )
                )
            self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("Order Canceled/Margin/Rejected")

        self.order = None

    def next(self):
        self.log("Close, %.2f" % self.dataclose[0])
        if self.order:
            return

        if not self.position:
            if self.dataclose[0] < self.dataclose[-1]:
                if self.dataclose[-1] < self.dataclose[-2]:
                    self.log("BUY CREATE, %.2f" % self.dataclose[0])
                    self.order = self.buy()
        else:
            if len(self) >= (self.bar_executed + 5):
                self.log("SELL CREATE, %.2f" % self.dataclose[0])
                self.order = self.sell()


class PandasData(bt.feeds.PandasData):
    params = (
        ("datetime", None),
        ("open", "Open"),
        ("high", "High"),
        ("low", "Low"),
        ("close", "Close"),
        ("volume", "Volume"),
        ("openinterest", None),
    )


def main():
    cerebro = bt.Cerebro()
    cerebro.addstrategy(TestStrategy)

    df = yf.download(
        "ORCL",
        start="2000-01-01",
        end="2001-01-01",
        auto_adjust=False,
        progress=False,
        group_by="column",
        actions=False,
    )

    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = df.columns.get_level_values(0)

    df.dropna(inplace=True)
    data = PandasData(dataname=df)
    cerebro.adddata(data)

    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)

    print("Starting Portfolio Value: %.2f" % cerebro.broker.getvalue())
    cerebro.run()
    print("Final Portfolio Value: %.2f" % cerebro.broker.getvalue())


if __name__ == "__main__":
    main()
