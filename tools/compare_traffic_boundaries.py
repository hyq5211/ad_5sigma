from __future__ import annotations

import argparse
from bisect import bisect_left, bisect_right
from datetime import timedelta
import json
from pathlib import Path
import statistics
import subprocess
import sys

from compare_continuous_stage1 import counter_rates, floors_for, write_json, write_windows
from compare_traffic_baselines import intervals
from run_frozen20_multisource_ad import _build_windows, _utc, five_sigma as fs, load_config


MINUTE = timedelta(minutes=1)
MAX_DURATION = timedelta(minutes=30)
MAX_SAMPLE_GAP = timedelta(minutes=2)
RECOVERY_SAMPLES = 2


def trace_evidence(samples, times, anchor, baseline, floor, sigma, step, lower, upper, *, floor_mode="zero_only"):
    index = bisect_left(times, anchor)
    if index == len(times) or times[index] != anchor:
        raise ValueError("Strong anchor missing from original series")
    mean, std = baseline["mean"], baseline["std"]
    direction = 1 if samples[index][1] > mean else -1
    evidence = []
    previous = anchor
    recovered = 0
    index += step
    while 0 <= index < len(samples):
        time, value = samples[index]
        if time < lower or time >= upper or abs(time - previous) > MAX_SAMPLE_GAP:
            break
        previous = time
        delta = value - mean
        threshold = fs._anomaly_threshold(std, sigma, floor, floor_mode)
        weak = threshold > 0 and delta * direction > threshold
        if weak:
            recovered = 0
            evidence.append(time)
        else:
            recovered += 1
            if recovered >= RECOVERY_SAMPLES:
                break
        index += step
    return evidence


def cap_to_evidence(start, end, left, right):
    left = sorted({start, *(t for t in left if t < start)})
    right = sorted({end, *(t + MINUTE for t in right if t + MINUTE > end)})
    raw_start, raw_end = left[0], right[-1]
    if raw_end - raw_start <= MAX_DURATION:
        return raw_start, raw_end, False
    extra = MAX_DURATION - (end - start)
    if extra < timedelta(0):
        raise ValueError("Core window exceeds duration limit")
    left_length, right_length = start - raw_start, raw_end - end
    left_budget = extra * (left_length / (left_length + right_length))
    selected_start = left[bisect_left(left, start-left_budget)]
    selected_end = right[bisect_right(right, selected_start+MAX_DURATION)-1]
    selected_start = left[bisect_left(left, selected_end-MAX_DURATION)]
    return selected_start, selected_end, True


def extend_windows(windows, series, floors, sigma, dataset_start, dataset_end, *, floor_mode="zero_only"):
    times = {key: [t for t, _ in samples] for key, samples in series.items()}
    output = []
    counters = {"duration_cap_limited": 0, "no_internal_strong_evidence": 0}
    for i, window in enumerate(windows):
        lower = max(dataset_start, window["start"]-MAX_DURATION)
        upper = min(dataset_end, window["end"]+MAX_DURATION)
        # Split only the unused interval between unchanged neighboring cores.
        if i:
            lower = max(lower, windows[i-1]["end"]+(window["start"]-windows[i-1]["end"])/2)
        if i+1 < len(windows):
            upper = min(upper, window["end"]+(windows[i+1]["start"]-window["end"])/2)
        left, right = [], []
        unique = {(s["node"], s["metric"], s["start"]): s for s in window["items"]}
        internal = 0
        for segment in unique.values():
            strong = [p["time"] for p in segment["points"] if window["start"] <= p["time"] < window["end"]]
            if not strong:
                continue
            internal += 1
            key = segment["node"], segment["metric"]
            segment_lower = max(lower, segment.get("review_lower", lower))
            segment_upper = min(upper, segment.get("review_upper", upper))
            left.extend(trace_evidence(series[key], times[key], min(strong), segment["baseline"],
                        floors.get(key[1], 0), sigma, -1, segment_lower, segment_upper, floor_mode=floor_mode))
            # An evidence sample occupies its original one-minute interval.
            right.extend(trace_evidence(series[key], times[key], max(strong), segment["baseline"],
                         floors.get(key[1], 0), sigma, 1, segment_lower, segment_upper-MINUTE, floor_mode=floor_mode))
        if not internal:
            counters["no_internal_strong_evidence"] += 1
        start, end, limited = cap_to_evidence(window["start"], window["end"], left, right)
        counters["duration_cap_limited"] += limited
        output.append({**window, "start": start, "end": end})
    if any(a["end"] > b["start"] for a, b in zip(output, output[1:])):
        raise RuntimeError("Boundary extension introduced overlapping windows")
    if any(not (w["start"] <= core["start"] < core["end"] <= w["end"]) or w["end"]-w["start"] > MAX_DURATION
           for core, w in zip(windows, output)):
        raise RuntimeError("Boundary extension changed the core or exceeded 30 minutes")
    return output, counters


def summarize(cores, windows):
    lengths = [(w["end"]-w["start"]).total_seconds()/60 for w in windows]
    left = [(c["start"]-w["start"]).total_seconds()/60 for c, w in zip(cores, windows)]
    right = [(w["end"]-c["end"]).total_seconds()/60 for c, w in zip(cores, windows)]
    return {"windows": len(windows), "changed_windows": sum(l > 0 or r > 0 for l, r in zip(left, right)),
            "left_extended_windows": sum(l > 0 for l in left), "right_extended_windows": sum(r > 0 for r in right),
            "mean_minutes": round(statistics.mean(lengths), 3), "median_minutes": statistics.median(lengths),
            "at_most_3min": sum(d <= 3 for d in lengths), "at_least_25min": sum(d >= 25 for d in lengths),
            "at_30min": sum(d == 30 for d in lengths), "mean_left_extension_minutes": round(statistics.mean(left), 3),
            "mean_right_extension_minutes": round(statistics.mean(right), 3),
            "extension_over_10min_windows": sum(l+r > 10 for l, r in zip(left, right)),
            "total_minutes": sum(lengths), "overlaps": 0, "cores_preserved": True}


def prepare_submission(repo, path, label):
    rule, submit = path.with_name(label+"_rule.jsonl"), path.with_name(label+"_submit.jsonl")
    subprocess.run([sys.executable, str(repo/"ad_detection/scripts/make_v4_rule_submission.py"),
                    "--repo", str(repo), "--v4-windows", str(path), "--output", str(rule),
                    "--prediction-prefix", label], check=True)
    subprocess.run([sys.executable, str(repo/"ad_detection/scripts/convert_submission_category_names.py"),
                    str(rule), str(submit)], check=True)
    subprocess.run([sys.executable, str(repo/"ad_detection/scripts/validate_submission.py"), str(submit)], check=True)
    with submit.open(encoding="utf-8") as handle:
        submitted = [(r["start_time"], r["end_time"]) for r in (json.loads(line) for line in handle)]
    if submitted != intervals(path):
        raise RuntimeError("Submission formatter changed AD intervals")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("outputs/traffic_boundary_compare"))
    parser.add_argument("--prepare-submissions", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[1]
    print("Reading full traffic dataset", flush=True)
    original = fs._read_series(args.data_root, load_config()["region_aliases"], sources={"traffic"})
    for samples in original.values():
        samples.sort(key=lambda p: p[0])
    dataset_start = min(t for samples in original.values() for t, _ in samples)
    dataset_end = dataset_start+timedelta(days=14)
    series, audit = counter_rates(original)
    floors = floors_for(series)
    segments = fs._detect_metric_segments(series, 5, timedelta(minutes=5), zero_floors=floors, include_baseline=True)
    cores = _build_windows(segments, limit=500, global_gap_minutes=3)
    reference = repo/"outputs/continuous_stage1/C_zero_variance_traffic_windows.jsonl"
    if [(_utc(w["start"]), _utc(w["end"])) for w in cores] != intervals(reference):
        raise RuntimeError("C core interval reproduction failed")
    summary = {"fixed": {"trigger_sigma": 5, "baseline_minutes": 20, "metric_gap_minutes": 5,
                "global_gap_minutes": 3, "recovery_samples": RECOVERY_SAMPLES, "max_sample_gap_minutes": 2,
                "max_window_minutes": 30, "same_direction_evidence": True, "new_windows_allowed": False,
                "zero_variance_floor": "same as C; not relaxed", "nearby_window_guard": "midpoint between original cores"},
               "counter_audit": audit, "C_exact_reproduction": True, "reference_AD": 9.535391740361836,
               "control": summarize(cores, cores), "variants": {}, "submitted": False}
    for sigma in (3.0, 2.5):
        print(f"Tracing boundary sigma={sigma}", flush=True)
        windows, diagnostics = extend_windows(cores, series, floors, sigma, dataset_start, dataset_end)
        label = "C_boundary_sigma"+str(sigma).replace(".", "_")
        path = args.output/(label+"_windows.jsonl")
        write_windows(path, windows, label)
        summary["variants"][str(sigma)] = {**summarize(cores, windows), **diagnostics}
        if args.prepare_submissions:
            prepare_submission(repo, path, label)
        write_json(args.output/"summary.json", summary)
        print(summary["variants"][str(sigma)], flush=True)


if __name__ == "__main__":
    main()
