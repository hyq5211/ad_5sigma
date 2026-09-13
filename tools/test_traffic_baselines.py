from datetime import datetime, timedelta, timezone
import unittest

from run_frozen20_multisource_ad import five_sigma as fs


class BaselineTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.key = ("node", "traffic.web.city.web_flow_error_total")

    def samples(self, values):
        return {self.key: [(self.start + timedelta(minutes=i), v) for i, v in enumerate(values)]}

    def detect(self, series, mode, **kwargs):
        return fs._detect_metric_segments(series, kwargs.pop("sigma", 5), timedelta(minutes=5),
            zero_floors={self.key[1]: 1/60}, baseline_mode=mode, **kwargs)

    def test_rolling_excludes_current_and_future(self):
        short = self.detect(self.samples([0]*20 + [100]), "rolling69")
        longer = self.detect(self.samples([0]*20 + [100] + [10000]*10), "rolling69")
        time = self.start + timedelta(minutes=20)
        point = next(p for s in longer for p in s["points"] if p["time"] == time)
        self.assertEqual(short[0]["points"][0], point)

    def test_rolling_expires_old_samples(self):
        series = {self.key: [(self.start, 10000)] +
            [(self.start + timedelta(minutes=i), 0) for i in range(70, 82)] +
            [(self.start + timedelta(minutes=82), 100)]}
        segments = self.detect(series, "rolling69")
        self.assertEqual(segments[0]["start"], self.start + timedelta(minutes=82))

    def test_whole_block_can_dilute_anomaly(self):
        series = self.samples([0]*20 + [100])
        self.assertEqual(len(self.detect(series, "frozen20")), 1)
        self.assertEqual(self.detect(series, "block292", block_start=self.start,
            block_end=self.start + timedelta(minutes=21), block_count=1), [])

    def test_block_boundaries_do_not_force_event_split(self):
        series = self.samples([0]*19 + [100, 100] + [0]*19)
        segments = self.detect(series, "block292", sigma=3, block_start=self.start,
            block_end=self.start + timedelta(minutes=40), block_count=2)
        self.assertEqual(len(segments), 1)
        self.assertEqual(len(segments[0]["points"]), 2)

    def test_invalid_block_range(self):
        with self.assertRaises(ValueError):
            self.detect(self.samples([0]*20), "block292")


if __name__ == "__main__":
    unittest.main()
