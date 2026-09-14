from __future__ import annotations

import argparse
from collections import Counter
from datetime import timedelta
import gc
import json
import math
from pathlib import Path

from compare_continuous_stage1 import counter_rates, stats, write_json, write_windows
from compare_frozen20_estimators import configured_floors, window_stats, write_records, fuse
from compare_node_family_fusion import read_nodes, node_candidates
from compare_node_relative_pressure import PROFILES, relative_classify
from compare_traffic_boundaries import extend_windows, prepare_submission
from run_frozen20_multisource_ad import _build_windows, _utc, five_sigma as fs, load_config


CONFIGS = {"legacy": {"baseline_mode": "frozen20"},
           "trusted_delay2": {"baseline_mode": "trusted20", "trusted_delay": timedelta(minutes=2)},
           "trusted_delay5": {"baseline_mode": "trusted20", "trusted_delay": timedelta(minutes=5)}}


def cache_statistics(diagnostic):
    used = diagnostic.get("trusted_cache_used", 0)
    fallback = diagnostic.get("trusted_fallback_used", 0)
    frozen = diagnostic.get("trusted_frozen_points", 0)
    return {"fallback_fraction": fallback/(used+fallback) if used+fallback else None,
            "mean_oldest_cache_age_minutes": diagnostic.get("trusted_cache_age_sum_seconds", 0)/used/60 if used else None,
            "mean_frozen_age_minutes": diagnostic.get("trusted_frozen_age_sum_seconds", 0)/frozen/60 if frozen else None}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("outputs/trusted_history_compare"))
    parser.add_argument("--prepare-submissions", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[1]
    read = lambda path: [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    reference = read(repo/"outputs/frozen20_guard_compare/traffic_mean_std_node_mean_std_guard_windows.jsonl")
    traffic_reference = read(repo/"outputs/traffic_boundary_compare/C_boundary_sigma2_5_windows.jsonl")
    summary = {"reference_AD": 14.894193170538037, "submitted": False,
        "fixed": {"estimator": "mean/population std", "cache_points": 20, "minimum_points": 12,
                  "cache_max_age_minutes": 60, "recovery_samples": 3, "metric_gap_minutes": 5,
                  "global_gap_minutes": 3, "traffic_sigma": 5, "node_sigma": 8,
                  "traffic_floor": "legacy zero-scale fallback", "node_floor": "current all-scale guards",
                  "fallback": "prior 20min raw history; never current/future", "traffic_weak_sigma": 2.5},
        "traffic": {}, "node": {}, "variants": {}}
    original = fs._read_series(args.data_root, load_config()["region_aliases"], sources={"traffic"})
    for samples in original.values():
        samples.sort(key=lambda p: p[0])
    start = min(t for samples in original.values() for t, _ in samples)
    series, audit = counter_rates(original)
    del original
    floors = configured_floors(series, False)
    summary["counter_audit"] = audit
    traffic_records = {}
    for name, config in CONFIGS.items():
        print(f"Traffic {name}", flush=True)
        diagnostic, aggregation = {}, {}
        segments = fs._detect_metric_segments(series, 5, timedelta(minutes=5), zero_floors=floors,
            include_baseline=True, diagnostics=diagnostic, **config)
        cores = _build_windows(segments, limit=500, global_gap_minutes=3, diagnostics=aggregation)
        windows, edges = extend_windows(cores, series, floors, 2.5, start, start+timedelta(days=14))
        path = args.output/("traffic_"+name+"_windows.jsonl")
        write_windows(path, windows, "traffic_"+name)
        traffic_records[name] = read(path)
        summary["traffic"][name] = {**stats(segments), **diagnostic, **aggregation, **edges,
            "kept_anomaly_points": sum(len(s["points"]) for s in segments),
            **window_stats(traffic_records[name], traffic_reference), **cache_statistics(diagnostic)}
        if name == "legacy" and (len(windows) != len(traffic_reference) or summary["traffic"][name]["old_intervals_not_preserved"]):
            raise RuntimeError("Legacy traffic reproduction failed")
        print({"traffic": name, **{k:v for k,v in summary["traffic"][name].items() if k != "by_metric"}}, flush=True)
        del segments, cores, windows
        gc.collect()
    del series, floors
    gc.collect()
    nodes, rows = read_nodes(args.data_root, load_config()["region_aliases"])
    summary["node_rows"], summary["nodes"] = rows, len(nodes)
    old = {name: [] for name in CONFIGS}
    relative = {name: [] for name in CONFIGS}
    diagnostics = {name: Counter() for name in CONFIGS}
    metrics = {name: Counter() for name in CONFIGS}
    for i, (node, data) in enumerate(sorted(nodes.items()), 1):
        current = {(node, "node."+f): [(t,v) for t,v in zip(data["times"], values) if math.isfinite(v)]
                   for f, values in data["fields"].items()}
        current_floors = configured_floors(current, True)
        for name, config in CONFIGS.items():
            diagnostic = {}
            segments = fs._detect_metric_segments(current, 8, timedelta(minutes=5), zero_floors=current_floors,
                scale_floor_mode="all_scales", include_baseline=True, diagnostics=diagnostic, **config)
            candidates, _ = node_candidates(node, data, 8, metric_segments=segments)
            pressure, _ = node_candidates(node, data, 8, metric_segments=segments,
                classifier=lambda values, flags: relative_classify(values, flags, PROFILES["balanced"]))
            old[name].extend(candidates)
            relative[name].extend(pressure)
            diagnostics[name].update(diagnostic)
            diagnostics[name].update({"kept_segments": len(segments),
                "kept_anomaly_points": sum(len(s["points"]) for s in segments),
                "singleton_segments": sum(len(s["points"]) == 1 for s in segments)})
            metrics[name].update(s["metric"] for s in segments)
        if i % 8 == 0 or i == len(nodes):
            print(f"Node {i}/{len(nodes)}: " + str({n: len(old[n])+len(relative[n]) for n in CONFIGS}), flush=True)
    for name in CONFIGS:
        summary["node"][name] = {**diagnostics[name], **cache_statistics(diagnostics[name]),
            "by_metric": dict(metrics[name]), "pressure_candidates": len(old[name])+len(relative[name])}
    combinations = [("legacy", "legacy")]
    for name in ("trusted_delay2", "trusted_delay5"):
        combinations += [(name, "legacy"), ("legacy", name), (name, name)]
    for traffic_name, node_name in combinations:
        label = f"traffic_{traffic_name}_node_{node_name}"
        records, audit = fuse(traffic_records[traffic_name], old[node_name], relative[node_name], label)
        summary["variants"][label] = {**window_stats(records, reference), **audit}
        if traffic_name == node_name == "legacy" and (len(records) != len(reference)
            or summary["variants"][label]["old_intervals_not_preserved"]
            or summary["variants"][label]["new_or_changed_intervals"]):
            raise RuntimeError("Current best345 reproduction failed")
        path = args.output/(label+"_windows.jsonl")
        write_records(path, records)
        if args.prepare_submissions and (traffic_name != "legacy" or node_name != "legacy"):
            prepare_submission(repo, path, label)
        print({"variant": label, **summary["variants"][label]}, flush=True)
    summary["best345_reproduction_exact"] = True
    write_json(args.output/"summary.json", summary)


if __name__ == "__main__":
    main()
