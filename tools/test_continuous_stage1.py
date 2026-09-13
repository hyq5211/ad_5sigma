from datetime import datetime, timedelta, timezone
import csv
from pathlib import Path
import tempfile
import unittest

from compare_continuous_stage1 import counter_rates, fs, read_source


class ContinuousStage1Tests(unittest.TestCase):
    def test_distinct_traffic_targets_are_not_merged(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory) / "beida" / "processed"
            folder.mkdir(parents=True)
            with (folder / "traffic_flow_metrics.csv").open("w", newline="", encoding="utf-8") as handle:
                fields = ["timestamp_utc", "flow_type", "source_region", "target_region", "target_domain", "web_flow_error_total"]
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                for domain in ("web1", "web2"):
                    writer.writerow(dict(zip(fields, ["2026-01-01 00:00:00", "web", "beida", "beida", domain, 10])))
            original, identified, audit = read_source(Path(directory), {"beida": "beida"}, "traffic")
            self.assertEqual(len(original), 1)
            self.assertEqual(len(identified), 2)
            self.assertEqual(audit["legacy_duplicate_timestamps"], 1)

    def test_counter_reset_missing_interval_and_first_sample(self):
        time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        key = ("node", "traffic.web.city.web_flow_error_total")
        series = {key: [(time, 100), (time + timedelta(minutes=1), 106),
                        (time + timedelta(minutes=2), 1), (time + timedelta(minutes=3), 4),
                        (time + timedelta(minutes=10), 10)]}
        result, audit = counter_rates(series, guard_gaps=True)
        self.assertEqual([v for _, v in result[key]], [0.1, 0.05])
        self.assertEqual(audit["counter_resets"], 1)
        self.assertEqual(audit["counter_intervals_over_5min"], 1)

    def test_zero_variance_requires_effect_floor(self):
        time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        metric = "traffic.web.city.web_flow_error_total"
        values = [(time + timedelta(minutes=i), 0.0) for i in range(20)]
        values += [(time + timedelta(minutes=20), 0.1)]
        series = {("node", metric): values}
        self.assertEqual(fs._detect_metric_segments(series, 5, timedelta(minutes=5)), [])
        segments = fs._detect_metric_segments(series, 5, timedelta(minutes=5), zero_floors={metric: 1/60})
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0]["start"], time + timedelta(minutes=20))
        self.assertLess(segments[0]["magnitude"], 100)
        values[-1] = (time + timedelta(minutes=20), 0.001)
        self.assertEqual(fs._detect_metric_segments(series, 5, timedelta(minutes=5), zero_floors={metric: 1/60}), [])

    def test_long_segments_are_counted_in_diagnostics(self):
        time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        metric = "node.cpu_usage"
        values = [(time + timedelta(minutes=i), 0.0) for i in range(20)]
        values += [(time + timedelta(minutes=i), 90.0) for i in range(20, 55)]
        diagnostics = {}
        self.assertEqual(fs._detect_metric_segments({("node", metric): values}, 6,
            timedelta(minutes=5), zero_floors={metric: 1}, diagnostics=diagnostics), [])
        self.assertEqual(diagnostics["discarded_long_segments"], 1)

    def test_nonzero_variance_is_unchanged(self):
        time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        metric = "node.cpu_usage"
        values = [(time + timedelta(minutes=i), float(i % 2)) for i in range(20)]
        values.append((time + timedelta(minutes=20), 90.0))
        series = {("node", metric): values}
        original = fs._detect_metric_segments(series, 6, timedelta(minutes=5))
        repaired = fs._detect_metric_segments(series, 6, timedelta(minutes=5), zero_floors={metric: 1000})
        self.assertEqual(original, repaired)


if __name__ == "__main__":
    unittest.main()
