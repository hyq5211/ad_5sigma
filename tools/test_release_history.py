from datetime import datetime, timedelta, timezone
import unittest

from run_frozen20_multisource_ad import five_sigma as fs


class ReleaseHistoryTests(unittest.TestCase):
    def samples(self, values, offset=0):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        return [(start+timedelta(minutes=i+offset), v) for i, v in enumerate(values)]

    def detect(self, samples, policy="before_current", diagnostics=None):
        return fs._detect_metric_segments({("n", "node.cpu_usage"): samples}, 5,
            timedelta(minutes=5), zero_floors={"node.cpu_usage": 1}, include_baseline=True,
            release_policy=policy, diagnostics=diagnostics)

    def test_release_before_anomalous_current_uses_new_history(self):
        samples = self.samples([0, 1]*20+[10]+[0, 1, 0, 1, 0]+[10])
        legacy = self.detect(samples, "legacy")
        audit = {}
        new = self.detect(samples, diagnostics=audit)
        self.assertEqual(len(legacy), 2)
        self.assertEqual(len(new), 1)
        self.assertEqual(audit["release_contains_alerts"], 1)
        self.assertEqual(audit["release_normal_12_19"], 1)
        self.assertEqual(audit["release_immediate_evaluations"], 1)

    def test_missing_history_does_not_reuse_expired_frozen_baseline(self):
        samples = self.samples([0, 1]*20+[10])+self.samples([10], 160)
        audit = {}
        new = self.detect(samples, diagnostics=audit)
        self.assertEqual(len(new), 1)
        self.assertEqual(audit["release_normal_0"], 1)
        self.assertEqual(audit["release_missing_minutes_sum"], 20)
        self.assertEqual(audit["release_insufficient_points"], 1)

    def test_exact_gap_is_not_released(self):
        samples = self.samples([0, 1]*20+[10]+[0, 1, 0, 1]+[10])
        audit = {}
        new = self.detect(samples, diagnostics=audit)
        self.assertEqual(len(new), 1)
        self.assertEqual(len(new[0]["points"]), 2)
        self.assertEqual(audit.get("release_count", 0), 0)

    def test_diagnostics_do_not_change_default_segments(self):
        samples = self.samples([0, 1]*20+[10]*3+[0, 1]*20+[20]*2)
        self.assertEqual(self.detect(samples, "legacy"), self.detect(samples, "legacy", {}))

    def test_current_and_future_cannot_enter_released_baseline(self):
        samples = self.samples([0, 1]*20+[10]+[0, 1, 0, 1, 0]+[100])
        audit = {}
        new = self.detect(samples, diagnostics=audit)
        self.assertEqual(len(new), 2)
        history = [v for _, v in samples[26:46]]
        self.assertEqual(new[1]["baseline"]["mean"], sum(history)/20)
        self.assertEqual(audit["freeze_baseline_count"], 2)
        self.assertEqual(audit["freeze_baseline_contains_alerts"], 1)

    def test_duplicate_samples_do_not_fill_twenty_minutes(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        samples = [(start+timedelta(minutes=i//4), i%2) for i in range(40)]
        samples.append((start+timedelta(minutes=10), 10))
        audit = {}
        self.detect(samples, diagnostics=audit)
        self.assertEqual(audit["freeze_baseline_count"], 1)
        self.assertEqual(audit["freeze_baseline_missing_minutes_sum"], 10)
        self.assertEqual(audit["freeze_baseline_normal_lt20"], 1)

    def test_invalid_policy(self):
        with self.assertRaises(ValueError):
            self.detect([], "bad")


if __name__ == "__main__":
    unittest.main()
