import unittest
from datetime import datetime, timedelta, timezone

from compare_node_relative_pressure import PROFILES, relative_classify, build_records
from compare_node_family_fusion import select_additions


class RelativeTests(unittest.TestCase):
    def test_memory_relative_drop_without_extreme_exhaustion(self):
        values = {"memory_available_ratio": .75}
        flags = {"memory_available_ratio": {"mean": .92, "delta": -.17, "magnitude": 8}}
        self.assertIn("memory", relative_classify(values, flags, PROFILES["conservative"]))
        flags["memory_available_ratio"]["delta"] = .17
        self.assertEqual(relative_classify(values, flags, PROFILES["balanced"]), {})

    def test_stable_high_space_is_not_an_event(self):
        self.assertEqual(relative_classify({"filesystem_used_ratio": .85}, {}, PROFILES["balanced"]), {})
        flags = {"filesystem_used_ratio": {"delta": .01, "magnitude": 20, "mean": .84}}
        self.assertEqual(relative_classify({"filesystem_used_ratio": .85}, flags, PROFILES["balanced"]), {})

    def test_process_requires_relative_and_absolute_change(self):
        flags = {"process_count": {"mean": 130, "delta": 140, "magnitude": 8}}
        self.assertIn("process", relative_classify({"process_count": 270}, flags, PROFILES["conservative"]))
        flags["process_count"]["mean"] = 250
        self.assertEqual(relative_classify({"process_count": 270}, flags, PROFILES["balanced"]), {})

    def test_build_preserves_reference_and_rejects_overlap(self):
        record = {"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:10:00Z"}
        self.assertEqual(build_records([record], [], "test"), [record])
        with self.assertRaises(RuntimeError):
            build_records([record, record], [], "test")

    def test_new_disk_reference_blocks_nearby_memory_candidate(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        memory = {"start": start, "end": start+timedelta(minutes=10), "score": 20,
                  "families": {"memory"}, "nodes": {"n"}, "points": []}
        disk = {"start": start+timedelta(minutes=12), "end": start+timedelta(minutes=20)}
        selected, audit = select_additions([memory], [disk])
        self.assertEqual(selected, [])
        self.assertEqual(audit["overlap_or_within_5min_of_traffic"], 1)


if __name__ == "__main__":
    unittest.main()
