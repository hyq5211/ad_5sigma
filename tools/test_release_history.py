from datetime import datetime, timedelta, timezone
import unittest

from run_frozen20_multisource_ad import five_sigma as fs


class ReleaseHistoryTests(unittest.TestCase):
    def samples(self, values, offset=0):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        return [(start+timedelta(minutes=i+offset), v) for i, v in enumerate(values)]

    def detect(self, samples, policy="before_current", diagnostics=None, **kwargs):
        return fs._detect_metric_segments({("n", "node.cpu_usage"): samples}, 5,
            timedelta(minutes=5), zero_floors={"node.cpu_usage": 1}, include_baseline=True,
            release_policy=policy, diagnostics=diagnostics, **kwargs)

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

    def test_exclusion_restores_low_amplitude_second_segment(self):
        samples = self.samples([0, 1]*20+[10]+[0, 1, 0, 1, 0]+[10])
        audit = {}
        new = self.detect(samples, diagnostics=audit, release_history="exclude_alerts")
        self.assertEqual(len(new), 2)
        self.assertEqual(audit["freeze_baseline_contains_alerts"], 0)
        self.assertGreater(audit["excluded_alert_samples_sum"], 0)

    def test_previous_normal_baseline_supplements_insufficient_recovery(self):
        samples = self.samples([0, 1]*20+[10]*15+[0, 1, 0, 1, 0]+[10])
        clean_audit, supplement_audit = {}, {}
        clean = self.detect(samples, diagnostics=clean_audit, release_history="exclude_alerts")
        supplemented = self.detect(samples, diagnostics=supplement_audit, release_history="normal_supplement")
        self.assertEqual(len(clean), 1)
        self.assertEqual(len(supplemented), 2)
        self.assertEqual(supplement_audit["supplemented_freezes"], 1)
        self.assertEqual(supplement_audit["supplemented_samples_sum"], 15)
        self.assertEqual(supplement_audit["filtered_selected_lt20"], 0)
        self.assertLess(supplemented[-1]["baseline"]["mean"], 1)

    def test_expired_old_normal_samples_are_not_reused(self):
        samples = self.samples([0, 1]*20+[10])+self.samples([10], 160)
        audit = {}
        self.assertEqual(len(self.detect(samples, diagnostics=audit, release_history="normal_supplement")), 1)
        self.assertEqual(audit.get("supplemented_freezes", 0), 0)

    def test_unknown_warmup_samples_never_supplement_old_normal_pool(self):
        samples = self.samples([0, 1]*6+[10]*15+[0, 1, 0, 1, 0]+[10])
        audit = {}
        segments = self.detect(samples, diagnostics=audit, release_history="normal_supplement")
        self.assertEqual(len(segments), 1)
        self.assertEqual(audit.get("supplemented_freezes", 0), 0)

    def test_filtered_baseline_excludes_current_and_future(self):
        first = self.samples([0, 1]*20+[10]*15+[0, 1, 0, 1, 0]+[10])
        a = self.detect(first+self.samples([0, 1]*5, len(first)), release_history="normal_supplement")
        b = self.detect(first+self.samples([100]*5, len(first)), release_history="normal_supplement")
        self.assertEqual(a[1]["baseline"], b[1]["baseline"])

    def test_invalid_filtered_history(self):
        with self.assertRaises(ValueError):
            self.detect([], "legacy", release_history="exclude_alerts")
        with self.assertRaises(ValueError):
            self.detect([], release_history="bad")
        with self.assertRaises(ValueError):
            self.detect([], release_history="normal_supplement", supplement_max_age=timedelta(minutes=10))


if __name__ == "__main__":
    unittest.main()
