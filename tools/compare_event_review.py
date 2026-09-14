from collections import Counter
from datetime import timedelta
import argparse
import gc
import json
import math
from pathlib import Path

from ad_event_review import EventEvidence, merge_candidates, review_long_segments, protect_reference_records
from compare_continuous_stage1 import counter_rates, stats, write_json, write_windows
from compare_frozen20_estimators import configured_floors, fuse, window_stats, write_records
from compare_node_family_fusion import read_nodes, node_candidates
from compare_node_relative_pressure import PROFILES, relative_classify
from compare_traffic_boundaries import extend_windows
from run_frozen20_multisource_ad import _build_windows, five_sigma as fs, load_config


CONFIGS = {"legacy": (None, None), "merge_gap5": (5, None), "merge_gap8": (8, None),
           "merge_gap10": (10, None), "review_sigma6": (None, 6), "review_sigma7": (None, 7),
           "merge5_review6": (5, 6)}


def read_records(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("outputs/event_review_compare"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[1]
    reference = read_records(repo/"outputs/frozen20_guard_compare/traffic_mean_std_node_mean_std_guard_windows.jsonl")
    traffic_reference = read_records(repo/"outputs/traffic_boundary_compare/C_boundary_sigma2_5_windows.jsonl")
    summary = {"reference_AD": 14.894193170538037, "submitted": False,
        "fixed": {"baseline": "legacy frozen20 mean/population std", "traffic_sigma": 5,
                  "metric_gap_minutes": 5, "global_gap_minutes": 3, "weak_sigma": 2.5,
                  "traffic_floor": "legacy zero-scale only", "node": "legacy8sigma all-scale floors and staged additions",
                  "global_bucket": "unchanged filled segment intervals", "NMS": "unchanged, after optional merge"},
        "merge_rule": "same directed series; gap5/8/10; reject >=3 fully normal minutes; gaps>5 require2 weak-support minutes; total<=30min",
        "review_rule": "original magnitudes>6/7; distinct unanimous-direction minutes; gap<=2min; observed two-minute sub-core valleys on both sides unless parent edge; weak edges bounded by valleys; density>=0.5; singleton>10 and >=3series/>=2nodes; still-long cores unresolved",
        "traffic": {}, "review": {}, "variants": {}}
    print("Reading full traffic", flush=True)
    original = fs._read_series(args.data_root, load_config()["region_aliases"], sources={"traffic"})
    for values in original.values():
        values.sort(key=lambda p: p[0])
    start = min(t for values in original.values() for t, _ in values)
    series, summary["counter_audit"] = counter_rates(original)
    del original
    floors = configured_floors(series, False)
    problems, detection = [], {}
    segments = fs._detect_metric_segments(series, 5, timedelta(minutes=5), zero_floors=floors,
        include_baseline=True, diagnostics=detection, problem_segments=problems)
    evidence = EventEvidence(series, floors)
    summary["detection"] = {**stats(segments), **detection,
        "problem_segments": len(problems), "problem_by_metric": dict(Counter(p["metric"] for p in problems))}
    recovered = {}
    for sigma in (6, 7):
        audit = Counter()
        recovered[sigma] = review_long_segments(problems, segments, evidence, sigma, audit)
        summary["review"][str(sigma)] = dict(audit)
        print({"review_sigma": sigma, **audit}, flush=True)
    traffic = {}
    for name, (merge_gap, sigma) in CONFIGS.items():
        aggregation, merge_audit = {}, Counter()
        transform = None if merge_gap is None else lambda candidates: merge_candidates(candidates, evidence, merge_gap, merge_audit)
        selected = segments if sigma is None else segments+recovered[sigma]
        cores = _build_windows(selected, limit=500, global_gap_minutes=3,
            diagnostics=aggregation, candidate_transform=transform)
        windows, edges = extend_windows(cores, series, floors, 2.5, start, start+timedelta(days=14))
        path = args.output/("traffic_"+name+"_windows.jsonl")
        write_windows(path, windows, "traffic_"+name)
        traffic[name] = read_records(path)
        summary["traffic"][name] = {**aggregation, "merge_audit": dict(merge_audit), **edges,
            **window_stats(traffic[name], traffic_reference),
            "selected_review_core_windows": sum(any("review_core_sigma" in s for s in w["items"]) for w in cores)}
        if name == "legacy" and (len(windows) != len(traffic_reference)
            or summary["traffic"][name]["old_intervals_not_preserved"]):
            raise RuntimeError("Legacy traffic reproduction failed")
        print({"traffic": name, **summary["traffic"][name]}, flush=True)
        del cores, windows
    del evidence, series, floors, segments, problems, recovered, selected, transform
    gc.collect()
    nodes, rows = read_nodes(args.data_root, load_config()["region_aliases"])
    summary["node_rows"], summary["nodes"] = rows, len(nodes)
    original_pressure, relative_pressure = [], []
    node_diagnostic = Counter()
    for i, (node, data) in enumerate(sorted(nodes.items()), 1):
        current = {(node, "node."+field): [(t, v) for t, v in zip(data["times"], values) if math.isfinite(v)]
                   for field, values in data["fields"].items()}
        diagnostic = {}
        segments = fs._detect_metric_segments(current, 8, timedelta(minutes=5),
            zero_floors=configured_floors(current, True), scale_floor_mode="all_scales",
            include_baseline=True, diagnostics=diagnostic)
        pressure, _ = node_candidates(node, data, 8, metric_segments=segments)
        relative, _ = node_candidates(node, data, 8, metric_segments=segments,
            classifier=lambda values, flags: relative_classify(values, flags, PROFILES["balanced"]))
        original_pressure.extend(pressure)
        relative_pressure.extend(relative)
        node_diagnostic.update(diagnostic)
        if i % 8 == 0 or i == len(nodes):
            print(f"Node {i}/{len(nodes)}", flush=True)
    summary["node_detection"] = dict(node_diagnostic)
    for name in CONFIGS:
        label = "traffic_"+name+"_node_legacy"
        records, audit = fuse(traffic[name], original_pressure, relative_pressure, label)
        summary["variants"][name] = {**window_stats(records, reference), **audit}
        if name == "legacy" and (len(records) != len(reference)
            or summary["variants"][name]["old_intervals_not_preserved"]
            or summary["variants"][name]["new_or_changed_intervals"]):
            raise RuntimeError("Best345 reproduction failed")
        parsed = [(fs._time({"timestamp": r["start"]}), fs._time({"timestamp": r["end"]})) for r in records]
        if any(not timedelta(minutes=1) <= end-start <= timedelta(minutes=30) for start, end in parsed):
            raise RuntimeError("Output duration violation")
        if any(a[1] > b[0] for a, b in zip(parsed, parsed[1:])):
            raise RuntimeError("Output windows overlap")
        write_records(args.output/(label+"_windows.jsonl"), records)
        print({"variant": name, **summary["variants"][name]}, flush=True)
    protected_audit = Counter()
    label = "traffic_merge_gap5_protected_node_legacy"
    proposals = read_records(args.output/"traffic_merge_gap5_node_legacy_windows.jsonl")
    protected = protect_reference_records(reference, proposals, label, protected_audit)
    summary["variants"]["merge_gap5_protected"] = {
        **window_stats(protected, reference), "protection_audit": dict(protected_audit)}
    write_records(args.output/(label+"_windows.jsonl"), protected)
    print({"variant": "merge_gap5_protected", **summary["variants"]["merge_gap5_protected"]}, flush=True)
    summary["best345_reproduction_exact"] = True
    write_json(args.output/"summary.json", summary)


if __name__ == "__main__":
    main()
