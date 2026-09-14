from datetime import datetime, timedelta, timezone
import unittest

from run_frozen20_multisource_ad import five_sigma as fs


class TrustedHistoryTests(unittest.TestCase):
    def series(self, values, offset=0):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        return [(start+timedelta(minutes=i+offset), v) for i,v in enumerate(values)]

    def detect(self, samples, **kwargs):
        return fs._detect_metric_segments({("n", "node.cpu_usage"): samples}, 5, timedelta(minutes=5),
            baseline_mode="trusted20", zero_floors={"node.cpu_usage": 1}, include_baseline=True, **kwargs)

    def test_frozen_baseline_excludes_fault_and_delays_admission(self):
        samples = self.series([0,1]*20+[10]*10+[0,1]*15+[100]*2)
        diagnostic = {}
        segments = self.detect(samples, diagnostics=diagnostic)
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0]["baseline"], {"mean": .5, "std": .5})
        self.assertEqual(len(segments[0]["points"]), 10)
        self.assertLess(segments[1]["baseline"]["mean"], 1)
        self.assertGreater(diagnostic["trusted_quarantined_pending_points"], 0)
        self.assertGreaterEqual(diagnostic["trusted_recovery_held_points"], 3)

    def test_expired_cache_falls_back_or_warms_up(self):
        samples = self.series([0,1]*20) + self.series([0,1]*20, 120)
        diagnostic = {}
        self.detect(samples, diagnostics=diagnostic)
        self.assertGreater(diagnostic["trusted_cache_expired_points"], 0)
        self.assertGreater(diagnostic["trusted_fallback_used"], 0)
        self.assertGreater(diagnostic["trusted_warmup_skipped_points"], 12)

    def test_future_values_do_not_change_completed_event_baseline(self):
        first = self.series([0,1]*20+[10]*3+[0,1]*15)
        a = self.detect(first+self.series([0,1]*10, len(first)))
        b = self.detect(first+self.series([100]*10, len(first)))
        self.assertEqual(a[0], b[0])

    def test_invalid_cache_configuration(self):
        with self.assertRaises(ValueError):
            self.detect([], trusted_cache_points=5)
        with self.assertRaises(ValueError):
            self.detect([], trusted_delay=timedelta(0))

    def test_cache_expiry_does_not_silently_change_inherited_freeze_rule(self):
        samples = self.series([0,1]*20+[10]*2) + self.series([10], 160)
        diagnostic = {}
        segments = self.detect(samples, diagnostics=diagnostic)
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0]["baseline"], segments[1]["baseline"])
        self.assertGreater(diagnostic["trusted_cache_expired_points"], 0)
        self.assertGreater(diagnostic["trusted_frozen_age_sum_seconds"], 60*60)


if __name__ == "__main__":
    unittest.main()
