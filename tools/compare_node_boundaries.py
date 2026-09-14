from __future__ import annotations

import argparse
from collections import Counter
from datetime import timedelta
import json
import math
from pathlib import Path

from compare_continuous_stage1 import floors_for, write_json
from compare_node_family_fusion import read_nodes, node_candidates, select_additions
from compare_node_relative_pressure import PROFILES, relative_classify
from compare_traffic_boundaries import trace_evidence, cap_to_evidence, summarize, prepare_submission
from run_frozen20_multisource_ad import _utc, five_sigma as fs, load_config


PRIMARY = {"cpu": {"cpu_usage"}, "disk_io": {"disk_io_util"},
           "memory": {"memory_available_ratio"},
           "disk_space": {"filesystem_used_ratio", "inode_used_ratio"},
           "process": {"process_count", "open_fd_ratio"}}
MINUTE = timedelta(minutes=1)
MAX_DURATION = timedelta(minutes=30)


def interval(record):
    return (fs._time({"timestamp": record["start"]}), fs._time({"timestamp": record["end"]}))


def extend_resource(start, end, evidence, series, floors, sigma, lower, upper):
    left, right = [], []
    for key, baseline, strong in evidence:
        samples = series[key]
        times = [t for t, _ in samples]
        left.extend(trace_evidence(samples, times, min(strong), baseline,
                                  floors.get(key[1], 0), sigma, -1, lower, upper))
        right.extend(trace_evidence(samples, times, max(strong), baseline,
                                   floors.get(key[1], 0), sigma, 1, lower, upper-MINUTE))
    return cap_to_evidence(start, end, left, right)


def validate_output(reference, records, resources):
    if len(reference) != len(records):
        raise RuntimeError("Window count changed")
    for old, new in zip(reference, records):
        if old["window_id"] != new["window_id"]:
            raise RuntimeError("Window identity changed")
        a, b = interval(old)
        c, d = interval(new)
        if old["window_id"] not in resources:
            if old != new:
                raise RuntimeError("Traffic record changed")
        elif not c <= a < b <= d or d-c > MAX_DURATION:
            raise RuntimeError("Resource core changed or exceeded 30min")
    if any(interval(a)[1] > interval(b)[0] for a, b in zip(records, records[1:])):
        raise RuntimeError("Overlapping windows")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("outputs/node_boundary_compare"))
    parser.add_argument("--prepare-submissions", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[1]
    read = lambda path: [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    reference = read(repo/"outputs/node_memory_compare/best338_memory_balanced_memory_windows.jsonl")
    traffic = read(repo/"outputs/traffic_boundary_compare/C_boundary_sigma2_5_windows.jsonl")
    traffic_ids = {r["window_id"] for r in traffic}
    resources = {r["window_id"] for r in reference if r["window_id"] not in traffic_ids}
    if len(reference) != 342 or len(resources) != 26:
        raise RuntimeError("Unexpected best342 reference")
    nodes, rows = read_nodes(args.data_root, load_config()["region_aliases"])
    original, relative = [], []
    series, floors, point_baselines = {}, {}, {}
    needed = {(p["node"], p["metric"]) for r in reference if r["window_id"] in resources for p in r["points"]}
    for record in reference:
        if record["window_id"] in resources:
            record_nodes = {p["node"] for p in record["points"]}
            fields = set().union(*(PRIMARY[f] for f in record["diagnostics"]["families"]))
            needed.update((node, "node."+field) for node in record_nodes for field in fields)
    for i, (node, data) in enumerate(sorted(nodes.items()), 1):
        current = {(node, "node."+f): [(t, v) for t, v in zip(data["times"], values) if math.isfinite(v)]
                   for f, values in data["fields"].items()}
        current_floors = floors_for(current)
        segments = fs._detect_metric_segments(current, 8, timedelta(minutes=5),
                    zero_floors=current_floors, include_baseline=True)
        old, _ = node_candidates(node, data, 8, metric_segments=segments)
        new, _ = node_candidates(node, data, 8, metric_segments=segments,
            classifier=lambda values, flags: relative_classify(values, flags, PROFILES["balanced"]))
        original.extend(old)
        relative.extend(new)
        for key in needed:
            if key in current:
                series[key] = current[key]
                if key[1] in current_floors:
                    floors[key[1]] = current_floors[key[1]]
        for segment in segments:
            key = segment["node"], segment["metric"]
            if key in needed:
                for point in segment["points"]:
                    point_baselines[(*key, point["time"])] = segment["baseline"]
        if i % 8 == 0 or i == len(nodes):
            print(f"Reconstructed {i}/{len(nodes)} nodes", flush=True)
    fixed = [{"start": interval(r)[0], "end": interval(r)[1]} for r in traffic]
    selected, _ = select_additions(original, fixed)
    disk, _ = select_additions([p for p in relative if "disk_space" in p["families"]], fixed+selected)
    memory, _ = select_additions([p for p in relative if "memory" in p["families"]], fixed+selected+disk)
    candidates = {(p["start"], p["end"]): p for p in selected+disk+memory}
    expected = Counter(interval(r) for r in reference if r["window_id"] in resources)
    if expected != Counter((p["start"], p["end"]) for p in selected+disk+memory):
        raise RuntimeError("Original resource interval reproduction failed")
    output, changes = [], []
    counters = Counter()
    for i, record in enumerate(reference):
        if record["window_id"] not in resources:
            output.append(record)
            continue
        start, end = interval(record)
        candidate = candidates[(start, end)]
        lower, upper = start-MAX_DURATION, end+MAX_DURATION
        if i:
            previous_end = interval(reference[i-1])[1]
            lower = max(lower, previous_end+(start-previous_end)/2)
        if i+1 < len(reference):
            next_start = interval(reference[i+1])[0]
            upper = min(upper, end+(next_start-end)/2)
        grouped = {}
        fields = set().union(*(PRIMARY[f] for f in candidate["families"]))
        for point in candidate["points"]:
            key = point["node"], point["metric"]
            time = fs._time({"timestamp": point["time"]})
            if key[1].split(".")[-1] not in fields:
                continue
            baseline = point_baselines.get((*key, time))
            if baseline is None or not start <= time < end:
                raise RuntimeError("Primary evidence baseline missing")
            identity = (*key, baseline["mean"], baseline["std"])
            entry = grouped.setdefault(identity, [key, baseline, []])
            entry[2].append(time)
        if not grouped:
            raise RuntimeError("No primary resource evidence")
        new_start, new_end, limited = extend_resource(start, end, list(grouped.values()),
                                                       series, floors, 3, lower, upper)
        counters["duration_cap_limited"] += limited
        output.append({**record, "start": _utc(new_start), "end": _utc(new_end)})
        if (new_start, new_end) != (start, end):
            changes.append({"window_id": record["window_id"], "families": sorted(candidate["families"]),
                            "old_start": _utc(start), "old_end": _utc(end),
                            "new_start": _utc(new_start), "new_end": _utc(new_end),
                            "left_minutes": (start-new_start).total_seconds()/60,
                            "right_minutes": (new_end-end).total_seconds()/60})
    validate_output(reference, output, resources)
    as_windows = lambda records: [{"start": interval(r)[0], "end": interval(r)[1]} for r in records]
    resource_old = [r for r in reference if r["window_id"] in resources]
    resource_new = [r for r in output if r["window_id"] in resources]
    summary = {"reference_AD": 14.55902988063768, "rows": rows, "submitted": False,
               "resource_reproduction_exact": True, "traffic_records_unchanged": len(traffic),
               "sigma": 3, "primary_metrics_only": True, "changes": changes, **counters,
               "control": summarize(as_windows(reference), as_windows(reference)),
               "variant": summarize(as_windows(reference), as_windows(output)),
               "resource_control": summarize(as_windows(resource_old), as_windows(resource_old)),
               "resource_variant": summarize(as_windows(resource_old), as_windows(resource_new))}
    label = "best342_node_boundary_sigma3"
    path = args.output/(label+"_windows.jsonl")
    with path.open("w", encoding="utf-8") as handle:
        for record in output:
            handle.write(json.dumps(record, ensure_ascii=False)+"\n")
    if args.prepare_submissions:
        prepare_submission(repo, path, label)
    write_json(args.output/"summary.json", summary)
    print({"variant": summary["variant"], "resources": summary["resource_variant"]}, flush=True)


if __name__ == "__main__":
    main()
