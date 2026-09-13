from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys

from run_frozen20_multisource_ad import five_sigma as fs, load_config, _build_windows, _utc, SOURCE_WEIGHT


VARIANTS = ("A_original", "B_counter_rate", "C_zero_variance", "D_series_identity")


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def read_source(root, aliases, source):
    legacy = defaultdict(list)
    identified = defaultdict(list)
    audit = Counter()
    for path in sorted(root.rglob("*.csv")):
        if path.parent.name != "processed" or fs._source(path) != source:
            continue
        print(f"Reading {path.name}: {path.parent.parent.name}", flush=True)
        city = fs._city(path, aliases)
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                audit["rows"] += 1
                time = fs._time(row)
                node = fs._traffic_node(row, aliases) if source == "traffic" else fs._node_id(row, city)
                if time is None or node is None:
                    audit["invalid_identity_or_time"] += 1
                    continue
                identity = ""
                if source == "traffic":
                    fields = ("series_key", "flow_type", "source_ip", "target_region", "target_domain", "protocol")
                    identity = hashlib.sha256(json.dumps([row.get(k, "") for k in fields]).encode()).hexdigest()[:16]
                for field, value in fs._numeric_fields(row, source):
                    key = fs._metric_key(source, field, row)
                    legacy[node, key].append((time, value))
                    if source == "traffic":
                        stable_key = key + ".series_" + identity
                        identified[node, stable_key].append((time, value))
    for values in legacy.values():
        values.sort(key=lambda item: item[0])
        audit["legacy_duplicate_timestamps"] += len(values) - len({t for t, _ in values})
    clean = {}
    for key, values in (identified if source == "traffic" else legacy).items():
        if len(values) == len({t for t, _ in values}):
            clean[key] = values
            continue
        by_time = defaultdict(list)
        for time, value in values:
            by_time[time].append(value)
        audit["identified_duplicate_timestamps"] += len(values) - len(by_time)
        audit["conflicting_duplicate_timestamps"] += sum(len(set(v)) > 1 for v in by_time.values())
        # Conflicting duplicates have no reliable value; preserve a missing sample.
        clean[key] = sorted((t, v[0]) for t, v in by_time.items() if len(set(v)) == 1)
    print(f"{source} input audit: {dict(audit)}", flush=True)
    return dict(legacy), clean, dict(audit)


def counter_rates(series, *, guard_gaps=False):
    transformed = {}
    audit = Counter()
    for (node, metric), values in series.items():
        field = metric.split(".series_", 1)[0].split(".")[-1]
        if not field.endswith("_total"):
            transformed[node, metric] = values
            continue
        result = []
        previous = None
        for time, value in values:
            if previous is not None:
                dt = (time - previous[0]).total_seconds()
                delta = value - previous[1]
                if dt <= 0:
                    audit["nonpositive_counter_intervals"] += 1
                elif delta < 0:
                    audit["counter_resets"] += 1
                elif guard_gaps and dt > 300:
                    audit["counter_intervals_over_5min"] += 1
                else:
                    result.append((time, delta / dt))
            previous = time, value
        transformed[node, metric] = result
    return transformed, dict(audit)


def floors_for(series):
    floors = {}
    for _, metric in series:
        field = metric.split(".series_", 1)[0].split(".")[-1]
        if field.endswith("_total"):
            floors[metric] = 1 / 60
        elif "ratio" in field or "loss_rate" in field:
            floors[metric] = 0.01
        elif "latency" in field or "jitter" in field:
            floors[metric] = 0.005
        elif field in {"cpu_usage", "disk_io_util"}:
            floors[metric] = 1.0
        elif field in {"load1", "load5"}:
            floors[metric] = 0.5
        elif field == "process_count":
            floors[metric] = 5.0
    return floors


def stats(segments):
    lengths = [(s["end"] - s["start"]).total_seconds() / 60 for s in segments]
    metrics = Counter(s["metric"].split(".series_", 1)[0].split(".")[-1] for s in segments)
    return {"segments": len(segments), "mean_minutes": round(statistics.mean(lengths), 3) if lengths else 0,
            "singleton_segments": sum(len(s["points"]) == 1 for s in segments),
            "by_metric": dict(metrics.most_common())}


def write_windows(path, windows, label):
    with path.open("w", encoding="utf-8") as handle:
        for index, window in enumerate(windows, 1):
            points = {}
            for segment in window["items"]:
                for p in segment["points"]:
                    if window["start"] <= p["time"] < window["end"]:
                        points[p["node"], p["metric"], p["time"]] = p
            selected = sorted(points.values(), key=lambda p: -p["magnitude"])[:30]
            record = {"window_id": f"{label}_{index:06d}", "start": _utc(window["start"]),
                      "end": _utc(window["end"]), "points": [{**p, "time": _utc(p["time"])} for p in selected],
                      "diagnostics": {"sources": sorted(window["sources"]), "score": window["score"],
                                      "scope": selected[0]["node"] if selected else "",
                                      "anchor_metric": selected[0]["metric"] if selected else "", "families": ["cpu"]}}
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("outputs/continuous_stage1"))
    parser.add_argument("--prepare-submissions", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    report = {"fixed_parameters": {"baseline_minutes": 20, "metric_gap_minutes": 5,
                                   "global_gap_minutes": 3, "limit": 500, "sigma": {"traffic": 5, "node": 6}},
              "source_audit": {}, "variants": {v: {} for v in VARIANTS},
              "historical_traffic_only": {"submission_id": "1789156640101", "AD": 3.8561178953376585,
                                          "windows": 304, "note": "Historical score; exact reproduction must be verified."}}
    combined = {v: [] for v in VARIANTS}
    node_tuning = {}
    aliases = load_config()["region_aliases"]
    for source, sigma in (("traffic", 5), ("node", 6)):
        original, identified, audit = read_source(args.data_root, aliases, source)
        rates, counter_audit = counter_rates(original)
        stable_rates, stable_audit = counter_rates(identified, guard_gaps=True)
        detection_cache = {}
        report["source_audit"][source] = {**audit, "rate_processing": counter_audit, "stable_rate_processing": stable_audit}
        for variant, series in zip(VARIANTS, (original, rates, rates, stable_rates)):
            diag = {}
            # Node has no counters or traffic identity expansion. Reuse identical
            # detector inputs rather than retaining redundant segment copies.
            cache_key = (source, variant in VARIANTS[2:]) if source == "node" and not audit.get("legacy_duplicate_timestamps") else (variant,)
            if cache_key in detection_cache:
                segments, diag = detection_cache[cache_key]
            else:
                segments = fs._detect_metric_segments(series, sigma, timedelta(minutes=5),
                            zero_floors=floors_for(series) if variant in VARIANTS[2:] else None, diagnostics=diag)
                detection_cache[cache_key] = segments, diag
            print(f"Detected {variant} {source}: {len(segments)} segments", flush=True)
            report["variants"][variant][source] = {**stats(segments), **diag}
            windows = _build_windows(segments, limit=500, global_gap_minutes=3)
            write_windows(args.output / f"{variant}_{source}_windows.jsonl", windows, variant + "_" + source)
            report["variants"][variant][source]["windows"] = len(windows)
            combined[variant].extend(segments)
            print(f"{variant} {source}: {len(segments)} segments, {len(windows)} windows", flush=True)
            write_json(args.output / "report.partial.json", report)
        if source == "node":
            for threshold in (7, 8):
                seg = fs._detect_metric_segments(stable_rates, threshold, timedelta(minutes=5), zero_floors=floors_for(stable_rates))
                node_tuning[f"sigma{threshold}"] = seg
                report.setdefault("node_sigma_sensitivity", {})[str(threshold)] = stats(seg)
    for variant, segments in combined.items():
        windows = _build_windows(segments, limit=500, global_gap_minutes=3)
        write_windows(args.output / f"{variant}_fused_windows.jsonl", windows, variant)
        report["variants"][variant]["fusion"] = {"windows": len(windows),
            "mean_minutes": round(statistics.mean((w["end"]-w["start"]).total_seconds()/60 for w in windows), 3) if windows else 0,
            "source_combinations": dict(Counter("+".join(sorted(w["sources"])) for w in windows))}
    traffic = [s for s in combined["D_series_identity"] if s["source"] == "traffic"]
    for label, node in node_tuning.items():
        windows = _build_windows(traffic + node, limit=500, global_gap_minutes=3)
        write_windows(args.output / f"D_node_{label}_windows.jsonl", windows, "D_node_" + label)
        report["node_sigma_sensitivity"][label.removeprefix("sigma")]["fused_windows"] = len(windows)
    node = [s for s in combined["D_series_identity"] if s["source"] == "node"]
    old_weight = SOURCE_WEIGHT["node"]
    SOURCE_WEIGHT["node"] = 0.3
    windows = _build_windows(traffic + node, limit=500, global_gap_minutes=3)
    SOURCE_WEIGHT["node"] = old_weight
    write_windows(args.output / "D_node_weight03_windows.jsonl", windows, "D_node_weight03")
    report["node_weight_sensitivity"] = {"node_weight": 0.3, "fused_windows": len(windows)}
    write_json(args.output / "report.json", report)
    if args.prepare_submissions:
        repo = Path(__file__).resolve().parents[1]
        for path in sorted(args.output.glob("*_windows.jsonl")):
            if "fused" not in path.name and "_traffic_" not in path.name and not path.name.startswith("D_node_"):
                continue
            rule = path.with_name(path.stem.replace("_windows", "_rule") + ".jsonl")
            submit = path.with_name(path.stem.replace("_windows", "_submit") + ".jsonl")
            subprocess.run([sys.executable, str(repo / "ad_detection/scripts/make_v4_rule_submission.py"),
                "--repo", str(repo), "--v4-windows", str(path), "--output", str(rule), "--prediction-prefix", path.stem], check=True)
            subprocess.run([sys.executable, str(repo / "ad_detection/scripts/convert_submission_category_names.py"), str(rule), str(submit)], check=True)
            subprocess.run([sys.executable, str(repo / "ad_detection/scripts/validate_submission.py"), str(submit)], check=True)
    historical = Path(__file__).resolve().parents[1] / "outputs/frozen20_traffic_only_5sigma_windows.jsonl"
    if historical.exists():
        def intervals(path):
            return [(r["start"], r["end"]) for r in (json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line)]
        report["historical_traffic_only"]["exact_intervals_reproduced"] = intervals(historical) == intervals(args.output / "A_original_traffic_windows.jsonl")
        write_json(args.output / "report.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
