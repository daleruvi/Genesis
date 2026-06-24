from _bootstrap import bootstrap

bootstrap()

from genesis.utils.health import run_health_checks


def main() -> int:
    report = run_health_checks()
    print(report.render_text())
    return report.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
