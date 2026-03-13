import json
from pathlib import Path

import pandas as pd

from _bootstrap import bootstrap

bootstrap()

from genesis.config.settings import ALPHA_STORE_DIR, DEMO_LOOP_JOURNAL_FILE, ensure_data_dirs


def section(title):
    print(f"\n=== {title} ===")


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def main():
    ensure_data_dirs()
    section("Step 1/4 - Load demo loop journal records")
    journal_path = Path(DEMO_LOOP_JOURNAL_FILE)
    records = _load_jsonl(journal_path)

    if not records:
        print(f"No journal records found at: {journal_path}")
        return

    section("Step 2/4 - Build summary tables")
    frame = pd.DataFrame(records)
    frame["recorded_at"] = pd.to_datetime(frame["recorded_at"], errors="coerce")
    frame["action"] = frame["execution_plan"].apply(
        lambda p: ",".join(p.get("actions", [])) if isinstance(p, dict) else ""
    )

    summary = {
        "runs": int(len(frame)),
        "date_from": str(frame["recorded_at"].min()),
        "date_to": str(frame["recorded_at"].max()),
        "execute_true_ratio": float(frame["execute_orders"].mean()) if "execute_orders" in frame else 0.0,
        "long_ratio": float((frame["signal"] == "long").mean()) if "signal" in frame else 0.0,
        "short_ratio": float((frame["signal"] == "short").mean()) if "signal" in frame else 0.0,
        "flat_ratio": float((frame["signal"] == "flat").mean()) if "signal" in frame else 0.0,
        "avg_conviction": float(frame["conviction"].mean()) if "conviction" in frame else 0.0,
        "fallback_ratio": float(frame["used_fallback"].mean()) if "used_fallback" in frame else 0.0,
    }

    regime_counts = (
        frame["current_regime"].value_counts(dropna=False).rename_axis("regime").reset_index(name="count")
        if "current_regime" in frame
        else pd.DataFrame(columns=["regime", "count"])
    )

    action_counts = (
        frame["action"].value_counts(dropna=False).rename_axis("action").reset_index(name="count")
        if "action" in frame
        else pd.DataFrame(columns=["action", "count"])
    )

    section("Step 3/4 - Save reporting artifacts")
    out_summary = ALPHA_STORE_DIR / "demo_loop_journal_summary.csv"
    out_regimes = ALPHA_STORE_DIR / "demo_loop_regime_counts.csv"
    out_actions = ALPHA_STORE_DIR / "demo_loop_action_counts.csv"
    out_runs = ALPHA_STORE_DIR / "demo_loop_runs_flat.csv"

    pd.DataFrame([summary]).to_csv(out_summary, index=False)
    regime_counts.to_csv(out_regimes, index=False)
    action_counts.to_csv(out_actions, index=False)
    frame.drop(columns=["votes", "execution_plan", "risk_check"], errors="ignore").to_csv(out_runs, index=False)

    section("Step 4/4 - Print summary outputs")
    print("Demo loop journal summary:")
    print(pd.DataFrame([summary]).to_string(index=False))
    print("\nTop regimes:")
    print(regime_counts.head(10).to_string(index=False))
    print("\nTop actions:")
    print(action_counts.head(10).to_string(index=False))
    print(f"\nsummary saved -> {out_summary}")
    print(f"regime counts saved -> {out_regimes}")
    print(f"action counts saved -> {out_actions}")
    print(f"runs table saved -> {out_runs}")


if __name__ == "__main__":
    main()
