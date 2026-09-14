from collections import Counter
from datetime import datetime, timedelta, timezone
import unittest

from ad_event_review import EventEvidence, merge_candidates, review_long_segments, protect_reference_records
from compare_traffic_boundaries import extend_windows
from run_frozen20_multisource_ad import _build_windows, five_sigma as fs


class EventReviewTests(unittest.TestCase):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def time(self, minute):
        return self.start+timedelta(minutes=minute)

    def segment(self, offsets, metric="traffic.m", values=None, node="n"):
        values = values or [20]*len(offsets)
        points = [{"time": self.time(i), "node": node, "metric": metric, "magnitude": abs(v-10)}
                  for i, v in zip(offsets, values)]
        return {"start": points[0]["time"], "end": points[-1]["time"]+timedelta(minutes=1),
            "node": node, "metric": metric, "source": "traffic", "points": points,
            "magnitude": max(p["magnitude"] for p in points), "baseline": {"mean": 10, "std": 1}}

    def window(self, segments):
        return {"start": min(s["start"] for s in segments), "end": max(s["end"] for s in segments),
            "items": segments, "nodes": {s["node"] for s in segments},
            "metrics": {s["metric"] for s in segments}, "sources": {"traffic"},
            "segments": len(segments), "score": 10}

    def evidence(self, segments, gap_value=None, end=120):
        series = {}
        for segment in segments:
            key = segment["node"], segment["metric"]
            samples = series.setdefault(key, {self.time(i): gap_value for i in range(end)} if gap_value is not None else {})
            for point in segment["points"]:
                samples[point["time"]] = 10+point["magnitude"]
        return EventEvidence({k: sorted(v.items()) for k, v in series.items()}, {})

    def test_merge_same_series_with_missing_gap_observations(self):
        a, b = self.segment([0, 1]), self.segment([6, 7])
        audit = Counter()
        merged = merge_candidates([self.window([a]), self.window([b])], self.evidence([a, b]), 5, audit)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["end"], self.time(8))
        self.assertEqual(audit["merged_pairs"], 1)

    def test_three_normal_minutes_veto_merge(self):
        a, b = self.segment([0, 1]), self.segment([6, 7])
        audit = Counter()
        self.assertEqual(len(merge_candidates([self.window([a]), self.window([b])], self.evidence([a, b], 10), 5, audit)), 2)
        self.assertEqual(audit["reject_normal_recovery"], 1)

    def test_unrelated_series_not_merged(self):
        a, b = self.segment([0, 1]), self.segment([6, 7], node="other")
        audit = Counter()
        self.assertEqual(len(merge_candidates([self.window([a]), self.window([b])], self.evidence([a, b]), 5, audit)), 2)
        self.assertEqual(audit["reject_no_directed_series"], 1)

    def test_opposite_directions_not_merged(self):
        a, b = self.segment([0, 1]), self.segment([6, 7], values=[0, 0])
        evidence = self.evidence([a, b])
        evidence.series["n", "traffic.m"][-2:] = [(self.time(6), 0), (self.time(7), 0)]
        audit = Counter()
        self.assertEqual(len(merge_candidates([self.window([a]), self.window([b])], evidence, 5, audit)), 2)

    def test_long_gap_requires_weak_support(self):
        a, b = self.segment([0, 1]), self.segment([9, 10])
        audit = Counter()
        self.assertEqual(len(merge_candidates([self.window([a]), self.window([b])], self.evidence([a, b]), 8, audit)), 2)
        self.assertEqual(audit["reject_long_gap_without_weak_support"], 1)
        self.assertEqual(len(merge_candidates([self.window([a]), self.window([b])], self.evidence([a, b], 13), 8, Counter())), 1)

    def test_merge_cannot_exceed_thirty_minutes(self):
        a, b = self.segment([0, 1]), self.segment([28, 31])
        audit = Counter()
        self.assertEqual(len(merge_candidates([self.window([a]), self.window([b])], self.evidence([a, b]), 30, audit)), 2)
        self.assertEqual(audit["reject_span_over30"], 1)

    def test_merge_occurs_before_candidate_suppression(self):
        segments = [self.segment([0, 1], f"traffic.m{i}") for i in range(3)]
        segments += [self.segment([6, 7], f"traffic.m{i}") for i in range(3)]
        evidence = self.evidence(segments, 13)
        original = _build_windows(segments, limit=500, global_gap_minutes=3)
        changed = _build_windows(segments, limit=500, global_gap_minutes=3,
            candidate_transform=lambda candidates: merge_candidates(candidates, evidence, 5, Counter()))
        self.assertEqual(original[0]["end"], self.time(2))
        self.assertEqual(changed[0]["end"], self.time(8))

    def test_compact_raised_cores_recovered_from_long_segment(self):
        values = [16]*40
        for i in (10, 11, 30, 31):
            values[i] = 20
        problem = self.segment(list(range(40)), values=values)
        audit = Counter()
        recovered = review_long_segments([problem], [], self.evidence([problem]), 6, audit)
        self.assertEqual(len(recovered), 2)
        self.assertEqual([len(s["points"]) for s in recovered], [2, 2])
        self.assertEqual(recovered[0]["baseline"], problem["baseline"])

    def test_sustained_high_regime_not_forced_into_thirty_minute_windows(self):
        problem = self.segment(list(range(40)))
        audit = Counter()
        self.assertEqual(review_long_segments([problem], [], self.evidence([problem]), 6, audit), [])
        self.assertEqual(audit["unresolved_core_over30"], 1)

    def test_unsupported_singleton_not_recovered(self):
        values = [16]*40
        values[20] = 18
        problem = self.segment(list(range(40)), values=values)
        audit = Counter()
        self.assertEqual(review_long_segments([problem], [], self.evidence([problem]), 7, audit), [])
        self.assertEqual(audit["reject_unsupported_singleton"], 1)

    def test_problem_collection_does_not_change_default_detection(self):
        samples = [(self.time(i), 9+i%2*2) for i in range(40)]
        samples += [(self.time(i), 20) for i in range(40, 80)]
        series = {("n", "traffic.m"): samples}
        original = fs._detect_metric_segments(series, 5, timedelta(minutes=5), include_baseline=True)
        problems = []
        unchanged = fs._detect_metric_segments(series, 5, timedelta(minutes=5), include_baseline=True, problem_segments=problems)
        self.assertEqual(original, unchanged)
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0]["baseline"], {"mean": 10, "std": 1})

    def test_equal_magnitude_opposite_same_time_samples_are_ambiguous(self):
        segment = self.segment([0])
        evidence = EventEvidence({("n", "traffic.m"): [(self.time(0), 0), (self.time(0), 20)]}, {})
        self.assertEqual(evidence.direction(segment, segment["points"][0]), 0)

    def test_review_edges_do_not_expand_through_confirmed_subcore_valleys(self):
        values = [16]*40
        values[10:12] = [20, 20]
        problem = self.segment(list(range(40)), values=values)
        evidence = self.evidence([problem])
        recovered = review_long_segments([problem], [], evidence, 6, Counter())
        windows, _ = extend_windows([self.window(recovered)], evidence.series, {}, 2.5,
            self.start, self.time(120))
        self.assertEqual(windows[0]["start"], self.time(10))
        self.assertEqual(windows[0]["end"], self.time(12))

    def test_singletons_require_independent_nodes_and_series(self):
        values = [16]*40
        values[20] = 22
        problem = self.segment(list(range(40)), values=values)
        support = [self.segment([20], "traffic.other", node=n) for n in ("n2", "n3")]
        recovered = review_long_segments([problem], support, self.evidence([problem, *support]), 7, Counter())
        self.assertEqual(len(recovered), 1)

    def test_direction_changes_without_subcore_valleys_do_not_create_events(self):
        problem = self.segment(list(range(40)))
        evidence = self.evidence([problem])
        evidence.series["n", "traffic.m"] = [(self.time(i), 20 if i%4 < 2 else 0) for i in range(40)]
        audit = Counter()
        self.assertEqual(review_long_segments([problem], [], evidence, 6, audit), [])
        self.assertGreater(audit["reject_no_bounded_valleys"], 0)

    def record(self, start, end):
        return {"start": self.time(start).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "end": self.time(end).isoformat(timespec="milliseconds").replace("+00:00", "Z"), "points": [], "diagnostics": {"score": 1}}

    def test_protection_restores_lost_reference_and_partial_edges(self):
        reference = [self.record(0, 10), self.record(40, 50)]
        proposals = [self.record(2, 12)]
        protected = protect_reference_records(reference, proposals, "p", Counter())
        self.assertEqual(protected[0]["start"], reference[0]["start"])
        self.assertEqual(protected[0]["end"], proposals[0]["end"])
        self.assertEqual(protected[1]["start"], reference[1]["start"])

    def test_protection_reverts_union_over_thirty_minutes(self):
        reference = [self.record(0, 20)]
        proposals = [self.record(15, 40)]
        audit = Counter()
        protected = protect_reference_records(reference, proposals, "p", audit)
        self.assertEqual(protected[0]["end"], reference[0]["end"])
        self.assertEqual(audit["reverted_over30_components"], 1)


if __name__ == "__main__":
    unittest.main()
