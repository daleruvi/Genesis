import json
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from genesis.utils.journal import JsonlJournal


class JournalTest(unittest.TestCase):
    def test_append_writes_jsonl_record(self):
        path = PROJECT_ROOT / "tests" / "_tmp_data" / "journal_test.jsonl"
        if path.exists():
            path.unlink()

        journal = JsonlJournal(path)
        journal.append({"signal": "long", "conviction": 0.75})

        self.assertTrue(path.exists())
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload["signal"], "long")
        self.assertIn("recorded_at", payload)


if __name__ == "__main__":
    unittest.main()
