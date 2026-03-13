from genesis.backtest.engine import BacktestEngine
from genesis.backtest.performance import PerformanceAnalyzer


class BacktestSimulator:
    def __init__(self, engine=None, performance=None):
        self.engine = engine or BacktestEngine()
        self.performance = performance or PerformanceAnalyzer()

    def simulate(self, close, positions):
        results = self.engine.run(close, positions)
        summary = self.performance.summarize(
            results["strategy_returns"],
            turnover=results["turnover"],
        )
        benchmark = self.performance.summarize(results["benchmark_returns"])
        return {
            "results": results,
            "summary": summary,
            "benchmark": benchmark,
        }
