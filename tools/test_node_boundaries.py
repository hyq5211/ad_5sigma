from datetime import datetime, timedelta, timezone
import unittest

from compare_node_boundaries import extend_resource, validate_output
from run_frozen20_multisource_ad import _utc


class ResourceBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.key = ("node", "node.cpu_usage")

    def test_frozen_baseline_direction_and_recovery(self):
        samples = [(self.time+timedelta(minutes=i), value) for i, value in enumerate([0, 0, 4, 9, 9, 4, 0, 0, 4])]
        start, end = samples[3][0], samples[4][0]+timedelta(minutes=1)
        evidence = [(self.key, {"mean": 0, "std": 1}, [samples[3][0], samples[4][0]])]
        a, b, _ = extend_resource(start, end, evidence, {self.key: samples}, {}, 3,
                                   self.time, self.time+timedelta(minutes=10))
        self.assertEqual(a, samples[2][0])
        self.assertEqual(b, samples[5][0]+timedelta(minutes=1))

    def test_zero_variance_floor_is_not_relaxed(self):
        samples = [(self.time+timedelta(minutes=i), v) for i, v in enumerate([0, .5, 2, .5, 0])]
        start, end = samples[2][0], samples[2][0]+timedelta(minutes=1)
        evidence = [(self.key, {"mean": 0, "std": 0}, [start])]
        a, b, _ = extend_resource(start, end, evidence, {self.key: samples}, {self.key[1]: 1}, 3,
                                   self.time, self.time+timedelta(minutes=5))
        self.assertEqual((a, b), (start, end))

    def test_validation_traffic_immutable_and_resource_core_preserved(self):
        traffic = {"window_id": "traffic", "start": _utc(self.time),
                   "end": _utc(self.time+timedelta(minutes=5))}
        resource = {"window_id": "resource", "start": _utc(self.time+timedelta(minutes=20)),
                    "end": _utc(self.time+timedelta(minutes=25))}
        extended = {**resource, "start": _utc(self.time+timedelta(minutes=18))}
        validate_output([traffic, resource], [traffic, extended], {"resource"})
        with self.assertRaises(RuntimeError):
            validate_output([traffic, resource], [{**traffic, "end": extended["start"]}, extended], {"resource"})
        with self.assertRaises(RuntimeError):
            validate_output([traffic, resource], [traffic, {**resource, "start": resource["end"]}], {"resource"})


if __name__ == "__main__":
    unittest.main()
