import unittest

from genesis.utils.health import HealthCheck, HealthReport


class HealthReportTest(unittest.TestCase):
    def test_healthy_report_returns_zero(self):
        report = HealthReport([HealthCheck("OK", "sample", "ready")])

        self.assertFalse(report.has_failures)
        self.assertEqual(report.exit_code, 0)
        self.assertIn("[OK] sample", report.render_text())

    def test_failure_report_returns_one(self):
        report = HealthReport([HealthCheck("FAIL", "sample", "broken")])

        self.assertTrue(report.has_failures)
        self.assertEqual(report.exit_code, 1)
        self.assertIn("[FAIL] sample", report.render_text())

    def test_warning_report_does_not_fail(self):
        report = HealthReport([HealthCheck("WARN", "sample", "missing optional artifact")])

        self.assertFalse(report.has_failures)
        self.assertEqual(report.exit_code, 0)


if __name__ == "__main__":
    unittest.main()
