from __future__ import annotations

import argparse
from pathlib import Path

from genesis.backtest.postmortem import run_orr_postmortem


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate ORR V1 post-mortem artifacts from existing backtest outputs.")
    parser.add_argument("--alpha-store", default="data/alpha_store")
    parser.add_argument("--output-prefix", default="opening_range_reversion_v1_postmortem")
    parser.add_argument("--symbols", default="BTCUSDT,QQQ,SPY")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()]
    paths = run_orr_postmortem(
        alpha_store=Path(args.alpha_store),
        output_prefix=args.output_prefix,
        symbols=symbols,
    )
    print("ORR post-mortem artifacts:")
    for name, path in paths.items():
        print(f"- {name}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
