from __future__ import annotations

import argparse
from collections import Counter
import json
import math
from pathlib import Path
import statistics

from compare_node_family_fusion import read_nodes, node_candidates, select_additions
from compare_traffic_boundaries import prepare_submission
from run_frozen20_multisource_ad import _utc, five_sigma as fs, load_config
from compare_continuous_stage1 import write_json


PROFILES = {
    "conservative": {"memory_drop": .15, "memory_max_available": .80,
                     "space_rise": .08, "space_min_used": .80,
                     "process_rise": 80, "process_ratio": 1.8, "process_min": 240},
    "balanced": {"memory_drop": .08, "memory_max_available": .85,
                 "space_rise": .05, "space_min_used": .75,
                 "process_rise": 50, "process_ratio": 1.5, "process_min": 200},
}


def relative_classify(values, flags, rules):
    result = {}

    def add(family, primary, support=()):
        metrics = [primary] + list(support)
        result[family] = {"metrics": metrics, "paired": len(metrics) > 1,
                          "severe": False,
                          "score": min(flags[primary]["magnitude"], 30) + (3 if len(metrics) > 1 else 0)}

    field = "memory_available_ratio"
    if field in flags and values[field] <= rules["memory_max_available"] and flags[field]["delta"] <= -rules["memory_drop"]:
        support = ["swap_used_ratio"] if "swap_used_ratio" in flags and flags["swap_used_ratio"]["delta"] >= .001 else []
        add("memory", field, support)
    for field in ("filesystem_used_ratio", "inode_used_ratio"):
        if field in flags and values[field] >= rules["space_min_used"] and flags[field]["delta"] >= rules["space_rise"]:
            if "disk_space" not in result or flags[field]["magnitude"] > result["disk_space"]["score"]:
                add("disk_space", field)
    field = "process_count"
    if field in flags and values[field] >= rules["process_min"] and flags[field]["delta"] >= rules["process_rise"] and values[field] >= flags[field]["mean"] * rules["process_ratio"]:
        support = [f for f in ("load1", "cpu_usage", "open_fd_ratio")
                   if f in flags and flags[f]["delta"] >= {"load1": .3, "cpu_usage": 5, "open_fd_ratio": .001}[f]]
        add("process", field, support)
    return result


def full_distribution(nodes):
    output = {}
    for field in ("memory_available_ratio", "swap_used_ratio", "filesystem_used_ratio",
                  "inode_used_ratio", "open_fd_ratio", "process_count"):
        values = sorted(v for data in nodes.values() for v in data["fields"][field] if math.isfinite(v))
        output[field] = {"count": len(values), "min": values[0], "max": values[-1],
                         **{f"q{q}": values[int((len(values)-1)*q/100)] for q in (1, 50, 95, 99)}} if values else {}
    return output


def build_records(reference, selected, label):
    records = list(reference)
    for i, candidate in enumerate(selected, 1):
        points = sorted(candidate["points"], key=lambda p: -p["magnitude"])[:30]
        records.append({"window_id": f"{label}_new_{i:06d}", "start": _utc(candidate["start"]),
                        "end": _utc(candidate["end"]), "points": points,
                        "diagnostics": {"sources": ["node"], "families": sorted(candidate["families"]),
                                        "scope": points[0]["node"], "anchor_metric": points[0]["metric"],
                                        "score": candidate["score"]}})
    records.sort(key=lambda r: r["start"])
    original = Counter((r["start"], r["end"]) for r in reference)
    if original - Counter((r["start"], r["end"]) for r in records):
        raise RuntimeError("Reference intervals changed")
    if any(a["end"] > b["start"] for a, b in zip(records, records[1:])):
        raise RuntimeError("Overlapping intervals")
    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=Path("outputs/node_relative_compare"))
    parser.add_argument("--prepare-submissions", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[1]
    reference_path = repo / "outputs/node_family_compare/traffic25_node_family_sigma8_windows.jsonl"
    reference = [json.loads(l) for l in reference_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    fixed = [{"start": fs._time({"timestamp": r["start"]}), "end": fs._time({"timestamp": r["end"]})} for r in reference]
    nodes, rows = read_nodes(args.data_root, load_config()["region_aliases"])
    summary = {"reference_AD": 13.500592373195124, "reference_windows": len(reference),
               "rows": rows, "nodes": len(nodes), "full_distribution": full_distribution(nodes),
               "profiles": PROFILES, "sigma": 8, "submitted": False, "variants": {}}
    print(summary["full_distribution"], flush=True)
    proposals = {name: [] for name in PROFILES}
    audits = {name: Counter() for name in PROFILES}
    for i, (node, data) in enumerate(sorted(nodes.items()), 1):
        for name, rules in PROFILES.items():
            candidates, audit = node_candidates(node, data, 8,
                classifier=lambda values, flags: relative_classify(values, flags, rules))
            proposals[name].extend(candidates)
            audits[name].update(audit)
        if i % 8 == 0 or i == len(nodes):
            print(f"Processed {i}/{len(nodes)}: " + str({n: len(p) for n, p in proposals.items()}), flush=True)
    # Isolate each indicator family so an eventual submission has a single change.
    for name in PROFILES:
        for family in ("memory", "disk_space", "process", "all"):
            candidates = [p for p in proposals[name] if family == "all" or family in p["families"]]
            selected, audit = select_additions(candidates, fixed)
            label = f"node_relative_{name}_{family}"
            records = build_records(reference, selected, label)
            path = args.output / (label + "_windows.jsonl")
            with path.open("w", encoding="utf-8") as handle:
                for record in records:
                    handle.write(json.dumps(record, ensure_ascii=False)+"\n")
            lengths = [(p["end"]-p["start"]).total_seconds()/60 for p in selected]
            summary["variants"][label] = {"family_candidates": len(candidates), **audit,
                "added_windows": len(selected), "total_windows": len(records),
                "added_family_counts": dict(Counter(f for p in selected for f in p["families"])),
                "mean_minutes": statistics.mean(lengths) if lengths else 0,
                "short_windows": sum(v <= 3 for v in lengths), "fixed_intervals_preserved": True,
                "detector_audit": dict(audits[name])}
            if selected and args.prepare_submissions:
                prepare_submission(repo, path, label)
            print({"variant": label, **summary["variants"][label]}, flush=True)
    write_json(args.output / "summary.json", summary)


if __name__ == "__main__":
    main()
