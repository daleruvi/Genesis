from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


class JsonlJournal:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: dict) -> Path:
        payload = {"recorded_at": datetime.now(timezone.utc).isoformat(), **record}
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True, default=str) + "\n")
        return self.path
