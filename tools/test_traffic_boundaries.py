from datetime import datetime, timedelta, timezone
import unittest

from compare_traffic_boundaries import cap_to_evidence, extend_windows, trace_evidence
from run_frozen20_multisource_ad import five_sigma as fs


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def test_recovery_and_direction(self):
        samples = [(self.start+timedelta(minutes=i), v) for i, v in enumerate([8, 4, 0, 4, 0, 0, 4])]
        times = [t for t, _ in samples]
        evidence = trace_evidence(samples, times, times[0], {"mean": 0, "std": 1}, 0, 3, 1,
                                  self.start, self.start+timedelta(minutes=20))
        self.assertEqual(evidence, [times[1], times[3]])
        samples[1] = (times[1], -4)
        evidence = trace_evidence(samples, times, times[0], {"mean": 0, "std": 1}, 0, 3, 1,
                                  self.start, self.start+timedelta(minutes=20))
        self.assertEqual(evidence, [])

    def test_missing_samples_stop_extension(self):
        samples = [(self.start, 8), (self.start+timedelta(minutes=5), 4)]
        self.assertEqual(trace_evidence(samples, [t for t, _ in samples], self.start,
            {"mean": 0, "std": 1}, 0, 3, 1, self.start, self.start+timedelta(minutes=20)), [])

    def test_zero_variance_floor_is_not_relaxed(self):
        samples = [(self.start+timedelta(minutes=i), v) for i, v in enumerate([0.1, 0.01, 0.01, 0.1])]
        self.assertEqual(trace_evidence(samples, [t for t, _ in samples], self.start,
            {"mean": 0, "std": 0}, 1/60, 2.5, 1, self.start, self.start+timedelta(minutes=20)), [])

    def test_cap_preserves_core_and_uses_evidence(self):
        start, end = self.start+timedelta(minutes=20), self.start+timedelta(minutes=25)
        left = [self.start+timedelta(minutes=i) for i in range(20)]
        right = [self.start+timedelta(minutes=i) for i in range(25, 45)]
        a, b, limited = cap_to_evidence(start, end, left, right)
        self.assertTrue(limited)
        self.assertLessEqual(b-a, timedelta(minutes=30))
        self.assertLessEqual(a, start)
        self.assertGreaterEqual(b, end)
        self.assertIn(a, left+[start])
        self.assertIn(b, [t+timedelta(minutes=1) for t in right]+[end])

    def test_baseline_snapshots_do_not_change_detector(self):
        metric = "node.cpu_usage"
        series = {("node", metric): [(self.start+timedelta(minutes=i), float(i%2)) for i in range(20)]}
        series["node", metric].append((self.start+timedelta(minutes=20), 10.0))
        original = fs._detect_metric_segments(series, 5, timedelta(minutes=5))
        captured = fs._detect_metric_segments(series, 5, timedelta(minutes=5), include_baseline=True)
        self.assertEqual(captured[0].pop("baseline"), {"mean": 0.5, "std": 0.5})
        self.assertEqual(original, captured)

    def test_neighbor_guards_preserve_both_cores(self):
        key = ("node", "traffic.web.city.latency")
        samples = [(self.start+timedelta(minutes=i), 8 if i in (20, 25) else 4) for i in range(40)]
        windows = []
        for index in (20, 25):
            time = samples[index][0]
            point = {"time": time, "node": key[0], "metric": key[1], "magnitude": 8}
            segment = {"start": time, "node": key[0], "metric": key[1], "points": [point],
                       "baseline": {"mean": 0, "std": 1}}
            windows.append({"start": time, "end": time+timedelta(minutes=1), "items": [segment]})
        extended, _ = extend_windows(windows, {key: samples}, {}, 3, self.start,
                                     self.start+timedelta(minutes=40))
        self.assertEqual(len(extended), len(windows))
        self.assertLessEqual(extended[0]["end"], extended[1]["start"])
        for core, result in zip(windows, extended):
            self.assertLessEqual(result["start"], core["start"])
            self.assertGreaterEqual(result["end"], core["end"])


if __name__ == "__main__":
    unittest.main()
