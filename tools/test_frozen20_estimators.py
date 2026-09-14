from datetime import datetime, timedelta, timezone
import unittest

from run_frozen20_multisource_ad import five_sigma as fs
from compare_frozen20_estimators import configured_floors
from compare_traffic_boundaries import trace_evidence


class EstimatorTests(unittest.TestCase):
    def test_scaled_mad_and_mean_std(self):
        self.assertEqual(fs._baseline_center_scale([0, 1, 2, 3, 100], "mad"), (2, 1.4826))
        self.assertGreater(fs._baseline_center_scale([0, 1, 2, 3, 100], "mean_std")[1], 30)

    def test_default_unchanged(self):
        time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        series = {("n", "node.cpu_usage"): [(time+timedelta(minutes=i), float(i%2) if i < 20 else 10) for i in range(25)]}
        a = fs._detect_metric_segments(series, 5, timedelta(minutes=5), include_baseline=True)
        b = fs._detect_metric_segments(series, 5, timedelta(minutes=5), baseline_estimator="mean_std", include_baseline=True)
        self.assertEqual(a, b)

    def test_mad_zero_floor_and_frozen_snapshot(self):
        time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        metric = "node.cpu_usage"
        series = {("n", metric): [(time+timedelta(minutes=i), 0 if i < 20 else 10) for i in range(23)]}
        diagnostics = {}
        segments = fs._detect_metric_segments(series, 5, timedelta(minutes=5), baseline_estimator="mad",
                    zero_floors={metric: 1}, include_baseline=True, diagnostics=diagnostics)
        self.assertEqual(segments[0]["baseline"], {"mean": 0, "std": 0})
        self.assertEqual(len(segments[0]["points"]), 3)
        self.assertGreater(diagnostics["zero_scale_baselines"], 0)

    def test_mad_rejects_other_baseline_modes(self):
        with self.assertRaises(ValueError):
            fs._detect_metric_segments({}, 5, timedelta(minutes=5), baseline_estimator="mad", baseline_mode="rolling69")

    def test_zero_mad_without_floor_is_explicitly_not_detected(self):
        time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        metric = "node.disk_read_rate"
        values = [0]*19 + [10, 100]
        series = {("n", metric): [(time+timedelta(minutes=i), value) for i, value in enumerate(values)]}
        mean_std = fs._detect_metric_segments(series, 5, timedelta(minutes=5))
        mad = fs._detect_metric_segments(series, 5, timedelta(minutes=5), baseline_estimator="mad")
        self.assertTrue(mean_std)
        self.assertEqual(mad, [])

    def test_positive_scale_floor_blocks_small_changes(self):
        self.assertEqual(fs._anomaly_threshold(.01, 5, 1, "all_scales"), 1)
        self.assertEqual(fs._anomaly_threshold(.01, 5, 1, "zero_only"), .05)
        time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        metric = "node.cpu_usage"
        values = [0, .02]*10 + [.2]
        series = {("n", metric): [(time+timedelta(minutes=i), value) for i, value in enumerate(values)]}
        old = fs._detect_metric_segments(series, 5, timedelta(minutes=5), zero_floors={metric: 1})
        guarded = fs._detect_metric_segments(series, 5, timedelta(minutes=5), zero_floors={metric: 1}, scale_floor_mode="all_scales")
        self.assertTrue(old)
        self.assertEqual(guarded, [])

    def test_zero_mad_std_fallback_restores_detection(self):
        time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        series = {("n", "node.disk_read_rate"): [(time+timedelta(minutes=i), value) for i, value in enumerate([0]*19+[10,100])]}
        diagnostics = {}
        segments = fs._detect_metric_segments(series, 5, timedelta(minutes=5), baseline_estimator="mad",
                    scale_floor_mode="all_scales", mad_zero_fallback="std", include_baseline=True, diagnostics=diagnostics)
        self.assertTrue(segments)
        self.assertEqual(segments[0]["baseline"]["mean"], 0)
        self.assertGreater(segments[0]["baseline"]["std"], 0)
        self.assertGreater(diagnostics["mad_std_fallback_uses"], 0)

    def test_weak_boundary_uses_same_absolute_floor(self):
        time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        samples = [(time+timedelta(minutes=i), value) for i, value in enumerate([2,.2,0,0])]
        times = [t for t, _ in samples]
        guarded = trace_evidence(samples, times, time, {"mean": 0, "std": .01}, 1, 2.5, 1,
                                  time, time+timedelta(minutes=4), floor_mode="all_scales")
        self.assertEqual(guarded, [])

    def test_field_floor_units_and_default_configuration(self):
        keys = {("n", "traffic.auth.city.auth_flow_observed_qps"): [], ("n", "node.disk_read_rate"): []}
        self.assertEqual(configured_floors(keys, False), {})
        guarded = configured_floors(keys, True)
        self.assertEqual(guarded["traffic.auth.city.auth_flow_observed_qps"], .05)
        self.assertEqual(guarded["node.disk_read_rate"], 65536)


if __name__ == "__main__":
    unittest.main()
