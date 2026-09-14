from datetime import datetime, timedelta, timezone
import unittest

from run_frozen20_multisource_ad import five_sigma as fs


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


if __name__ == "__main__":
    unittest.main()
