from __future__ import annotations

import argparse
from array import array
from collections import Counter, defaultdict
import csv
from datetime import timedelta
import json
import math
from pathlib import Path
import statistics

from compare_continuous_stage1 import floors_for, write_json
from compare_traffic_boundaries import prepare_submission
from run_frozen20_multisource_ad import _utc, five_sigma as fs, load_config


FIELDS = ("cpu_usage", "load1", "memory_available_ratio", "swap_used_ratio", "disk_io_util",
          "disk_read_rate", "disk_write_rate", "filesystem_used_ratio", "inode_used_ratio",
          "open_fd_ratio", "process_count")
MINUTE = timedelta(minutes=1)
RULES = {
    "cpu": {"minimum_usage": 65, "minimum_rise": 25, "severe_usage": 85, "support_load": 1},
    "memory": {"maximum_available": .20, "minimum_drop": .10, "severe_available": .10, "support_swap": .02},
    "disk_io": {"minimum_util": 60, "minimum_rise": 20, "severe_util": 85, "support_rate": 1_000_000},
    "disk_space": {"minimum_used": .90, "minimum_rise": .05},
    "process": {"minimum_fd": .25, "minimum_fd_rise": .10, "severe_fd": .50,
                "minimum_processes": 400, "minimum_process_rise": 100, "severe_processes": 1000},
}


def read_nodes(root, aliases):
    nodes = {}
    rows = 0
    for path in sorted(root.rglob("node_metrics*.csv")):
        if path.parent.name != "processed":
            continue
        city = fs._city(path, aliases)
        print(f"Reading node: {path.parent.parent.name}", flush=True)
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                node, time = fs._node_id(row, city), fs._time(row)
                if node is None or time is None:
                    continue
                if node not in nodes:
                    nodes[node] = {"times": [], "fields": {f: array("d") for f in FIELDS}}
                data = nodes[node]
                if data["times"] and time <= data["times"][-1]:
                    raise ValueError(f"Duplicate or unordered timestamp in {node}")
                data["times"].append(time)
                for field in FIELDS:
                    number = fs._number(row.get(field))
                    data["fields"][field].append(number if number is not None else math.nan)
                rows += 1
    return nodes, rows


def distribution(nodes):
    output = {}
    for field in FIELDS:
        samples = sorted(v for data in nodes.values() for v in data["fields"][field][::10] if math.isfinite(v))
        output[field] = {"sampled_q50": samples[len(samples)//2],
                         "sampled_q95": samples[int((len(samples)-1)*.95)],
                         "sampled_q99": samples[int((len(samples)-1)*.99)],
                         "sampled_max": samples[-1]} if samples else {}
    return output


def classify(values, flags):
    families = {}

    def up(field, minimum, rise):
        return field in flags and values[field] >= minimum and flags[field]["delta"] >= rise

    def add(family, primary, support, severe):
        metrics = [primary]+[f for f in support if f in flags and flags[f]["delta"] > 0]
        families[family] = {"metrics": metrics, "paired": len(metrics) >= 2, "severe": severe,
                            "score": min(flags[primary]["magnitude"], 30)+(3 if len(metrics) >= 2 else 0)}

    cpu = RULES["cpu"]
    if up("cpu_usage", cpu["minimum_usage"], cpu["minimum_rise"]):
        severe = values["cpu_usage"] >= cpu["severe_usage"]
        support = "load1" in flags and flags["load1"]["delta"] > 0 and values["load1"] >= cpu["support_load"]
        if severe or support:
            add("cpu", "cpu_usage", ["load1"] if support else [], severe)
    memory = RULES["memory"]
    if "memory_available_ratio" in flags and values["memory_available_ratio"] <= memory["maximum_available"] and flags["memory_available_ratio"]["delta"] <= -memory["minimum_drop"]:
        severe = values["memory_available_ratio"] <= memory["severe_available"]
        support = up("swap_used_ratio", memory["support_swap"], .01)
        if severe or support:
            add("memory", "memory_available_ratio", ["swap_used_ratio"] if support else [], severe)
    io = RULES["disk_io"]
    if up("disk_io_util", io["minimum_util"], io["minimum_rise"]):
        support = [f for f in ("disk_read_rate", "disk_write_rate") if up(f, io["support_rate"], io["support_rate"]*.5)]
        severe = values["disk_io_util"] >= io["severe_util"]
        if severe or support:
            add("disk_io", "disk_io_util", support, severe)
    space = RULES["disk_space"]
    for field in ("filesystem_used_ratio", "inode_used_ratio"):
        if up(field, space["minimum_used"], space["minimum_rise"]):
            add("disk_space", field, [], values[field] >= .95)
    process = RULES["process"]
    fd = up("open_fd_ratio", process["minimum_fd"], process["minimum_fd_rise"])
    proc = up("process_count", process["minimum_processes"], process["minimum_process_rise"])
    if proc:
        proc = values["process_count"] >= flags["process_count"]["mean"]*1.5
    severe_fd = fd and values["open_fd_ratio"] >= process["severe_fd"]
    severe_proc = proc and values["process_count"] >= max(process["severe_processes"], flags["process_count"]["mean"]*3)
    if severe_fd or severe_proc or fd and proc:
        primary = "open_fd_ratio" if fd else "process_count"
        add("process", primary, ["process_count"] if fd and proc else [], severe_fd or severe_proc)
    return families


def node_candidates(node, samples, sigma, classifier=classify, metric_segments=None):
    times = samples["times"]
    lookup = {t: i for i, t in enumerate(times)}
    series = {(node, "node."+field): [(t, v) for t, v in zip(times, samples["fields"][field]) if math.isfinite(v)] for field in FIELDS}
    diagnostic = {}
    segments = metric_segments
    if segments is None:
        segments = fs._detect_metric_segments(series, sigma, timedelta(minutes=5), zero_floors=floors_for(series),
                                             diagnostics=diagnostic, include_baseline=True)
    flags = defaultdict(dict)
    for segment in segments:
        field = segment["metric"].split(".")[-1]
        for point in segment["points"]:
            value = samples["fields"][field][lookup[point["time"]]]
            flags[point["time"]][field] = {"mean": segment["baseline"]["mean"],
                "delta": value-segment["baseline"]["mean"], "magnitude": point["magnitude"]}
    grouped = defaultdict(list)
    for time, state in sorted(flags.items()):
        i = lookup[time]
        values = {field: samples["fields"][field][i] for field in FIELDS}
        for family, evidence in classifier(values, state).items():
            grouped[family].append({"time": time, **evidence})
    candidates = []
    skipped = Counter()

    def flush(family, buffer):
        if not buffer:
            return
        # Require repeated pressure, or a severe one-minute event with paired metrics.
        if len(buffer) < 2 and not (buffer[0]["severe"] and buffer[0]["paired"]):
            skipped["isolated_pressure"] += 1
            return
        if buffer[-1]["time"]-buffer[0]["time"]+MINUTE > timedelta(minutes=30):
            skipped["long_pressure"] += 1
            return
        points = [{"time": _utc(p["time"]), "node": node, "metric": "node."+field,
                   "magnitude": flags[p["time"]][field]["magnitude"]} for p in buffer for field in p["metrics"]]
        candidates.append({"start": buffer[0]["time"], "end": buffer[-1]["time"]+MINUTE,
            "score": statistics.mean(p["score"] for p in buffer)+min(len(buffer), 10)*.2,
            "points": points, "families": {family}, "nodes": {node}})
    for family, observations in grouped.items():
        buffer = []
        for observation in observations:
            if buffer and observation["time"]-buffer[-1]["time"] > timedelta(minutes=2):
                flush(family, buffer)
                buffer = []
            buffer.append(observation)
        flush(family, buffer)
    return candidates, {"raw_metric_segments": len(segments), "pressure_minutes": sum(len(v) for v in grouped.values()),
                        **diagnostic, **skipped}


def gap(a, b):
    return max(a["start"]-b["end"], b["start"]-a["end"])


def select_additions(candidates, traffic):
    groups = []
    for candidate in sorted(candidates, key=lambda c: c["start"]):
        if groups and candidate["start"] < groups[-1]["end"]:
            group = groups[-1]
            group["end"] = max(group["end"], candidate["end"])
            group["score"] = max(group["score"], candidate["score"])
            group["families"] |= candidate["families"]
            group["nodes"] |= candidate["nodes"]
            group["points"].extend(candidate["points"])
        else:
            groups.append({**candidate, "families": set(candidate["families"]), "nodes": set(candidate["nodes"]), "points": list(candidate["points"])})
    audit = Counter()
    selected = []
    for group in sorted(groups, key=lambda c: -c["score"]):
        if group["end"]-group["start"] > timedelta(minutes=30):
            audit["long_groups"] += 1
        elif any(gap(group, window) < timedelta(minutes=5) for window in traffic):
            audit["overlap_or_within_5min_of_traffic"] += 1
        elif any(gap(group, old) < timedelta(minutes=20) for old in selected):
            audit["node_time_conflicts"] += 1
        else:
            selected.append(group)
    return sorted(selected, key=lambda c: c["start"]), {"merged_groups": len(groups), **audit}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("outputs/node_family_compare"))
    parser.add_argument("--prepare-submissions", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[1]
    reference = repo/"outputs/traffic_boundary_compare/C_boundary_sigma2_5_windows.jsonl"
    with reference.open(encoding="utf-8") as handle:
        traffic_records = [json.loads(line) for line in handle]
    traffic = [{"start": fs._time({"timestamp": r["start"]}), "end": fs._time({"timestamp": r["end"]})} for r in traffic_records]
    nodes, rows = read_nodes(args.data_root, load_config()["region_aliases"])
    summary = {"reference_AD": 11.71012791890643, "reference_windows": len(traffic), "input_rows": rows,
               "nodes": len(nodes), "sampled_distribution": distribution(nodes), "rules": RULES,
               "same_traffic_intervals": True, "no_ground_truth": True, "variants": {}}
    print(summary["sampled_distribution"], flush=True)
    proposals = {6: [], 8: []}
    audits = {6: Counter(), 8: Counter()}
    for i, (node, data) in enumerate(sorted(nodes.items()), 1):
        for sigma in (6, 8):
            candidates, audit = node_candidates(node, data, sigma)
            proposals[sigma].extend(candidates)
            audits[sigma].update(audit)
        if i % 8 == 0 or i == len(nodes):
            print(f"Processed {i}/{len(nodes)} nodes: sigma6={len(proposals[6])}, sigma8={len(proposals[8])} pressure segments", flush=True)
    for sigma in (6, 8):
        selected, audit = select_additions(proposals[sigma], traffic)
        label = f"traffic25_node_family_sigma{sigma}"
        records = list(traffic_records)
        for i, candidate in enumerate(selected, 1):
            points = sorted(candidate["points"], key=lambda p: -p["magnitude"])[:30]
            records.append({"window_id": f"{label}_node_{i:06d}", "start": _utc(candidate["start"]),
                "end": _utc(candidate["end"]), "points": points,
                "diagnostics": {"sources": ["node"], "families": sorted(candidate["families"]),
                    "scope": points[0]["node"], "anchor_metric": points[0]["metric"], "score": candidate["score"]}})
        records.sort(key=lambda r: r["start"])
        original_intervals = Counter((r["start"], r["end"]) for r in traffic_records)
        fused_intervals = Counter((r["start"], r["end"]) for r in records)
        if original_intervals - fused_intervals:
            raise RuntimeError("Fusion changed or removed a traffic interval")
        if any(a["end"] > b["start"] for a, b in zip(records, records[1:])):
            raise RuntimeError("Fusion created overlapping windows")
        path = args.output/(label+"_windows.jsonl")
        with path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False)+"\n")
        lengths = [(c["end"]-c["start"]).total_seconds()/60 for c in selected]
        summary["variants"][str(sigma)] = {**audits[sigma], "family_segments": len(proposals[sigma]),
            "source_families": dict(Counter(f for p in proposals[sigma] for f in p["families"])), **audit,
            "added_windows": len(selected), "fused_windows": len(records),
            "added_family_counts": dict(Counter(f for p in selected for f in p["families"])),
            "added_mean_minutes": round(statistics.mean(lengths), 3) if lengths else 0,
            "added_median_minutes": statistics.median(lengths) if lengths else 0,
            "short_added_windows": sum(d <= 3 for d in lengths)}
        if args.prepare_submissions:
            prepare_submission(repo, path, label)
        write_json(args.output/"summary.json", summary)
        print({"sigma": sigma, **summary["variants"][str(sigma)]}, flush=True)


if __name__ == "__main__":
    main()
