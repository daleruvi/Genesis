import importlib.util
import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def load_pipeline_module():
    spec = importlib.util.spec_from_file_location("run_data_pipeline_for_test", SCRIPTS_DIR / "run_data_pipeline.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FakeLoader:
    def __init__(self):
        self.calls = []

    def fetch_ohlcv_range(self, symbol, timeframe, start, end, limit_per_call):
        self.calls.append((symbol, timeframe, start, end, limit_per_call))
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=2, freq="5min"),
                "open": [1.0, 2.0],
                "high": [2.0, 3.0],
                "low": [0.5, 1.5],
                "close": [1.5, 2.5],
                "volume": [10.0, 20.0],
            }
        )


class TempBuilder:
    def __init__(self, path):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def save_dataset(self, df, name):
        file_path = self.path / f"{name}.parquet"
        df.to_parquet(file_path)
        return file_path


class FakeMonthlyFailThenOkLoader:
    def __init__(self):
        self.calls = []

    def fetch_ohlcv_range(self, symbol, timeframe, start, end, limit_per_call):
        self.calls.append((symbol, timeframe, start, end, limit_per_call))
        if len(self.calls) == 1:
            raise RuntimeError("rate limit persisted for https://api.polygon.io/test?apiKey=secret")
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-02-03 14:30:00", periods=78, freq="5min"),
                "open": [1.0] * 78,
                "high": [2.0] * 78,
                "low": [0.5] * 78,
                "close": [1.5] * 78,
                "volume": [10.0] * 78,
            }
        )


class RunDataPipelineCliTest(unittest.TestCase):
    def test_cli_accepts_binance_vision_provider_with_mock_loader(self):
        pipeline = load_pipeline_module()
        fake_loader = FakeLoader()
        tmp = PROJECT_ROOT / "tests" / "_tmp_data"
        tmp.mkdir(parents=True, exist_ok=True)
        output_file = tmp / "sample_5m.parquet"
        if output_file.exists():
            output_file.unlink()
        argv = [
            "run_data_pipeline.py",
            "--provider",
            "binance_vision",
            "--market-type",
            "futures_um",
            "--symbol",
            "BTCUSDT",
            "--timeframes",
            "5m",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-01-02",
            "--partition",
            "none",
            "--output-name",
            "sample_5m",
            "--fail-fast",
        ]
        with patch.object(sys, "argv", argv):
            with patch.object(pipeline, "build_loader", return_value=fake_loader) as build_loader:
                with patch.object(pipeline, "DatasetBuilder", lambda _: TempBuilder(tmp)):
                    pipeline.main()

        build_loader.assert_called_once_with("binance_vision", "futures_um")
        self.assertEqual(fake_loader.calls[0][0], "BTCUSDT")
        self.assertTrue(output_file.exists())
        output_file.unlink()

    def test_cli_accepts_polygon_stocks_provider_with_mock_loader(self):
        pipeline = load_pipeline_module()
        fake_loader = FakeLoader()
        tmp = PROJECT_ROOT / "tests" / "_tmp_data"
        tmp.mkdir(parents=True, exist_ok=True)
        output_file = tmp / "qqq_5m_2025.parquet"
        if output_file.exists():
            output_file.unlink()
        argv = [
            "run_data_pipeline.py",
            "--provider",
            "polygon",
            "--market-type",
            "stocks",
            "--symbol",
            "QQQ",
            "--timeframes",
            "5m",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-01-02",
            "--partition",
            "none",
            "--output-name",
            "qqq_5m_2025",
            "--fail-fast",
        ]
        with patch.object(sys, "argv", argv):
            with patch.object(pipeline, "build_loader", return_value=fake_loader) as build_loader:
                with patch.object(pipeline, "DatasetBuilder", lambda _: TempBuilder(tmp)):
                    pipeline.main()

        build_loader.assert_called_once_with("polygon", "stocks")
        self.assertEqual(fake_loader.calls[0][0], "QQQ")
        self.assertTrue(output_file.exists())
        output_file.unlink()

    def test_polygon_monthly_failure_continues_without_fail_fast_and_sanitizes(self):
        pipeline = load_pipeline_module()
        fake_loader = FakeMonthlyFailThenOkLoader()
        tmp = PROJECT_ROOT / "tests" / "_tmp_data"
        tmp.mkdir(parents=True, exist_ok=True)
        output_file = tmp / "qqq_5m_2025_202502.parquet"
        combined_file = tmp / "qqq_5m_2025.parquet"
        for path in [output_file, combined_file]:
            if path.exists():
                path.unlink()
        argv = [
            "run_data_pipeline.py",
            "--provider",
            "polygon",
            "--market-type",
            "stocks",
            "--symbol",
            "QQQ",
            "--timeframes",
            "5m",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-03-01",
            "--partition",
            "monthly",
            "--combine-partitions",
            "--output-name",
            "qqq_5m_2025",
        ]
        buffer = io.StringIO()
        with patch.object(sys, "argv", argv):
            with patch.object(pipeline, "build_loader", return_value=fake_loader):
                with patch.object(pipeline, "DatasetBuilder", lambda _: TempBuilder(tmp)):
                    with redirect_stdout(buffer):
                        pipeline.main()

        output = buffer.getvalue()
        self.assertIn("failed_partition", output)
        self.assertIn("apiKey=***REDACTED***", output)
        self.assertNotIn("apiKey=secret", output)
        self.assertTrue(output_file.exists())
        self.assertTrue(combined_file.exists())
        output_file.unlink()
        combined_file.unlink()

    def test_polygon_monthly_failure_aborts_with_fail_fast(self):
        pipeline = load_pipeline_module()
        fake_loader = FakeMonthlyFailThenOkLoader()
        argv = [
            "run_data_pipeline.py",
            "--provider",
            "polygon",
            "--market-type",
            "stocks",
            "--symbol",
            "QQQ",
            "--timeframes",
            "5m",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-03-01",
            "--partition",
            "monthly",
            "--output-name",
            "qqq_5m_2025",
            "--fail-fast",
        ]
        with patch.object(sys, "argv", argv):
            with patch.object(pipeline, "build_loader", return_value=fake_loader):
                with self.assertRaisesRegex(RuntimeError, r"apiKey=\*\*\*REDACTED\*\*\*"):
                    pipeline.main()


if __name__ == "__main__":
    unittest.main()
