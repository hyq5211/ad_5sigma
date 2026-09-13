from datetime import datetime, timedelta, timezone
import unittest

from compare_node_family_fusion import classify, select_additions


class NodeFamilyTests(unittest.TestCase):
    def test_cpu_requires_direction_and_pressure(self):
        flag = {"cpu_usage": {"delta": 40, "mean": 50, "magnitude": 8}}
        self.assertIn("cpu", classify({"cpu_usage": 90}, flag))
        self.assertEqual(classify({"cpu_usage": 10}, flag), {})
        flag["cpu_usage"]["delta"] = -40
        self.assertEqual(classify({"cpu_usage": 90}, flag), {})
        self.assertEqual(classify({"cpu_usage": 90}, {}), {})

    def test_memory_requires_drop(self):
        flag = {"memory_available_ratio": {"delta": -.5, "mean": .55, "magnitude": 8}}
        self.assertIn("memory", classify({"memory_available_ratio": .05}, flag))
        flag["memory_available_ratio"]["delta"] = .5
        self.assertEqual(classify({"memory_available_ratio": .05}, flag), {})

    def test_selection_keeps_traffic_and_merges_node_overlap(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        def candidate(a, b):
            return {"start": start+timedelta(minutes=a), "end": start+timedelta(minutes=b),
                    "score": 10, "families": {"cpu"}, "nodes": {"n"}, "points": []}
        traffic = [candidate(0, 10)]
        selected, audit = select_additions([candidate(12, 14), candidate(30, 35), candidate(34, 38)], traffic)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["end"], start+timedelta(minutes=38))
        self.assertEqual(traffic[0]["end"], start+timedelta(minutes=10))
        self.assertEqual(audit["overlap_or_within_5min_of_traffic"], 1)


if __name__ == "__main__":
    unittest.main()
