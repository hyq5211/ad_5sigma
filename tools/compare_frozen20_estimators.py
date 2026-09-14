from __future__ import annotations

import argparse
from collections import Counter
from datetime import timedelta
import gc
import json
import math
from pathlib import Path
import statistics

from compare_continuous_stage1 import counter_rates, floors_for, stats, write_json, write_windows
from compare_node_family_fusion import read_nodes, node_candidates, select_additions
from compare_node_relative_pressure import PROFILES, relative_classify, build_records
from compare_traffic_baselines import intervals
from compare_traffic_boundaries import extend_windows, prepare_submission
from run_frozen20_multisource_ad import _build_windows, _utc, five_sigma as fs, load_config


def write_records(path, records):
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False)+"\n")


def window_stats(records, reference):
    parse = lambda value: fs._time({"timestamp": value})
    lengths = [(parse(r["end"])-parse(r["start"])).total_seconds()/60 for r in records]
    pairs = lambda rows: {(r["start"], r["end"]) for r in rows}
    old, new = pairs(reference), pairs(records)
    return {"windows": len(records), "mean_minutes": statistics.mean(lengths) if lengths else 0,
            "median_minutes": statistics.median(lengths) if lengths else 0,
            "at_most_3min": sum(v <= 3 for v in lengths), "at_least_25min": sum(v >= 25 for v in lengths),
            "exact_shared_intervals": len(old & new), "new_or_changed_intervals": len(new-old),
            "old_intervals_not_preserved": len(old-new)}


def fuse(traffic, original, relative, label):
    fixed = [{"start": fs._time({"timestamp": r["start"]}),
              "end": fs._time({"timestamp": r["end"]})} for r in traffic]
    node, node_audit = select_additions(original, fixed)
    disk, disk_audit = select_additions([p for p in relative if "disk_space" in p["families"]], fixed+node)
    memory, memory_audit = select_additions([p for p in relative if "memory" in p["families"]], fixed+node+disk)
    records = build_records(traffic, node+disk+memory, label)
    return records, {"node_cpu_io_added": len(node), "disk_space_added": len(disk), "memory_added": len(memory),
                     "node_audit": node_audit, "disk_audit": disk_audit, "memory_audit": memory_audit}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("outputs/frozen20_estimator_compare"))
    parser.add_argument("--prepare-submissions", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[1]
    read = lambda path: [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    reference = read(repo/"outputs/node_memory_compare/best338_memory_balanced_memory_windows.jsonl")
    traffic_reference = read(repo/"outputs/traffic_boundary_compare/C_boundary_sigma2_5_windows.jsonl")
    summary = {"reference_AD": 14.55902988063768, "submitted": False,
        "fixed": {"baseline_minutes": 20, "metric_gap_minutes": 5, "global_gap_minutes": 3,
                  "traffic_sigma": 5, "node_sigma": 8, "traffic_boundary_sigma": 2.5,
                  "node_boundaries": "original; no weak extension", "MAD_scale": "1.4826 * raw MAD",
                  "floors": "same existing zero-scale fallback; no new positive-scale clamp",
                  "node_selection": "same staged CPU/IO, disk-space, memory selection"},
        "traffic": {}, "node": {}, "variants": {}}
    print("Reading full traffic", flush=True)
    original = fs._read_series(args.data_root, load_config()["region_aliases"], sources={"traffic"})
    for values in original.values():
        values.sort(key=lambda p: p[0])
    start = min(t for values in original.values() for t, _ in values)
    series, audit = counter_rates(original)
    del original
    summary["counter_audit"] = audit
    floors = floors_for(series)
    traffic_records = {}
    for estimator in ("mean_std", "mad"):
        print(f"Traffic estimator={estimator}", flush=True)
        diagnostic, aggregation = {}, {}
        segments = fs._detect_metric_segments(series, 5, timedelta(minutes=5), zero_floors=floors,
            diagnostics=diagnostic, include_baseline=True, baseline_estimator=estimator)
        cores = _build_windows(segments, limit=500, global_gap_minutes=3, diagnostics=aggregation)
        if estimator == "mean_std" and [(_utc(w["start"]), _utc(w["end"])) for w in cores] != intervals(repo/"outputs/continuous_stage1/C_zero_variance_traffic_windows.jsonl"):
            raise RuntimeError("Mean/std traffic core reproduction failed")
        windows, edge_audit = extend_windows(cores, series, floors, 2.5, start, start+timedelta(days=14))
        path = args.output/f"traffic_{estimator}_windows.jsonl"
        write_windows(path, windows, "traffic_"+estimator)
        traffic_records[estimator] = read(path)
        if estimator == "mean_std" and window_stats(traffic_records[estimator], traffic_reference)["old_intervals_not_preserved"]:
            raise RuntimeError("Mean/std traffic boundary reproduction failed")
        summary["traffic"][estimator] = {**stats(segments), **diagnostic, **aggregation, **edge_audit,
            "kept_anomaly_points": sum(len(s["points"]) for s in segments),
            **window_stats(traffic_records[estimator], traffic_reference)}
        print({"traffic": estimator, **{k:v for k,v in summary["traffic"][estimator].items() if k != "by_metric"}}, flush=True)
        del segments, cores, windows
        gc.collect()
    del series, floors
    gc.collect()
    nodes, rows = read_nodes(args.data_root, load_config()["region_aliases"])
    summary["node_rows"], summary["nodes"] = rows, len(nodes)
    old_proposals = {name: [] for name in traffic_records}
    relative_proposals = {name: [] for name in traffic_records}
    diagnostics = {name: Counter() for name in traffic_records}
    metric_counts = {name: Counter() for name in traffic_records}
    for i, (node, data) in enumerate(sorted(nodes.items()), 1):
        current = {(node, "node."+field): [(t,v) for t,v in zip(data["times"], values) if math.isfinite(v)]
                   for field, values in data["fields"].items()}
        current_floors = floors_for(current)
        for estimator in traffic_records:
            diagnostic = {}
            segments = fs._detect_metric_segments(current, 8, timedelta(minutes=5), zero_floors=current_floors,
                include_baseline=True, diagnostics=diagnostic, baseline_estimator=estimator)
            old, _ = node_candidates(node, data, 8, metric_segments=segments)
            relative, _ = node_candidates(node, data, 8, metric_segments=segments,
                classifier=lambda values, flags: relative_classify(values, flags, PROFILES["balanced"]))
            old_proposals[estimator].extend(old)
            relative_proposals[estimator].extend(relative)
            diagnostics[estimator].update(diagnostic)
            diagnostics[estimator].update({"kept_segments": len(segments),
                "kept_anomaly_points": sum(len(s["points"]) for s in segments),
                "singleton_segments": sum(len(s["points"]) == 1 for s in segments)})
            metric_counts[estimator].update(s["metric"] for s in segments)
        if i % 8 == 0 or i == len(nodes):
            print(f"Node {i}/{len(nodes)}: " + str({e: len(old_proposals[e])+len(relative_proposals[e]) for e in traffic_records}), flush=True)
    for estimator in traffic_records:
        summary["node"][estimator] = {**diagnostics[estimator], "by_metric": dict(metric_counts[estimator]),
            "pressure_candidates": len(old_proposals[estimator])+len(relative_proposals[estimator])}
    for traffic_estimator, node_estimator in (("mean_std", "mean_std"), ("mad", "mean_std"), ("mean_std", "mad"), ("mad", "mad")):
        label = f"traffic_{traffic_estimator}_node_{node_estimator}"
        records, fusion_audit = fuse(traffic_records[traffic_estimator], old_proposals[node_estimator],
                                    relative_proposals[node_estimator], label)
        current_stats = window_stats(records, reference)
        if traffic_estimator == node_estimator == "mean_std" and (current_stats["old_intervals_not_preserved"] or current_stats["new_or_changed_intervals"] or len(records) != len(reference)):
            raise RuntimeError("Best342 full mean/std reproduction failed")
        path = args.output/(label+"_windows.jsonl")
        write_records(path, records)
        summary["variants"][label] = {**current_stats, **fusion_audit}
        if args.prepare_submissions and (traffic_estimator != "mean_std" or node_estimator != "mean_std"):
            prepare_submission(repo, path, label)
        print({"variant": label, **summary["variants"][label]}, flush=True)
    summary["mean_std_best342_reproduced"] = True
    write_json(args.output/"summary.json", summary)


if __name__ == "__main__":
    main()
