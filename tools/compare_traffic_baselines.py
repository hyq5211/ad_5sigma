from __future__ import annotations

import argparse
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys

from compare_continuous_stage1 import counter_rates, floors_for, stats, write_json, write_windows
from run_frozen20_multisource_ad import _build_windows, _utc, five_sigma as fs, load_config


def intervals(path):
    with path.open(encoding="utf-8") as handle:
        return [(r["start"], r["end"]) for r in (json.loads(line) for line in handle if line.strip())]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("outputs/traffic_baseline_compare"))
    parser.add_argument("--prepare-submissions", action="store_true")
    parser.add_argument("--modes", nargs="+", choices=("frozen20", "rolling69", "block292"),
                        default=("frozen20", "rolling69", "block292"))
    parser.add_argument("--global-gap-minutes", type=int, default=3)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[1]
    print("Reading full traffic dataset", flush=True)
    original = fs._read_series(args.data_root, load_config()["region_aliases"], sources={"traffic"})
    for samples in original.values():
        samples.sort(key=lambda p: p[0])
    start = min(t for samples in original.values() for t, _ in samples)
    end = start + timedelta(days=14)
    series, rate_audit = counter_rates(original)
    floors = floors_for(series)
    reference = repo / "outputs/continuous_stage1/C_zero_variance_traffic_windows.jsonl"
    reference_intervals = set(intervals(reference))
    summary = {"fixed": {"sigma": 5, "metric_gap_minutes": 5, "global_gap_minutes": args.global_gap_minutes,
                          "window_limit": 500, "zero_variance_floors": "same as C",
                          "counter_processing": "same as C; no new gap guard",
                          "variance": "population variance; divide by sample count"},
               "blocks": {"start": _utc(start), "end_exclusive": _utc(end), "count": 292,
                          "minutes_per_block": 14 * 24 * 60 / 292,
                          "uses_future_samples": True, "forces_one_event_per_block": False},
               "reference": {"AD": 9.535391740361836, "submission_id": "1789285354387"},
               "counter_audit": rate_audit, "variants": {}}
    for mode in args.modes:
        print(f"Detecting {mode}", flush=True)
        diagnostics = {}
        segments = fs._detect_metric_segments(series, 5, timedelta(minutes=5), zero_floors=floors,
                        diagnostics=diagnostics, baseline_mode=mode, block_start=start, block_end=end)
        if mode == "frozen20":
            control = _build_windows(segments, limit=500, global_gap_minutes=3)
            if [(_utc(w["start"]), _utc(w["end"])) for w in control] != intervals(reference):
                raise RuntimeError("C reference interval reproduction failed; stop before generating new submissions")
            summary["reference"]["exact_reproduction"] = True
        windows = _build_windows(segments, limit=500, global_gap_minutes=args.global_gap_minutes)
        label = "C_" + mode + (f"_gap{args.global_gap_minutes}" if args.global_gap_minutes != 3 else "")
        path = args.output / f"{label}_windows.jsonl"
        write_windows(path, windows, label)
        lengths = [(w["end"] - w["start"]).total_seconds() / 60 for w in windows]
        current = set(intervals(path))
        summary["variants"][mode] = {**stats(segments), **diagnostics, "windows": len(windows),
            "mean_window_minutes": round(statistics.mean(lengths), 3) if lengths else 0,
            "median_window_minutes": statistics.median(lengths) if lengths else 0,
            "windows_at_most_3min": sum(d <= 3 for d in lengths),
            "windows_at_least_25min": sum(d >= 25 for d in lengths),
            "exact_intervals_shared_with_C": len(reference_intervals & current),
            "new_or_changed_intervals": len(current - reference_intervals),
            "reference_intervals_not_preserved": len(reference_intervals - current)}
        if args.prepare_submissions and (mode != "frozen20" or args.global_gap_minutes != 3):
            rule = path.with_name(f"{label}_rule.jsonl")
            submit = path.with_name(f"{label}_submit.jsonl")
            subprocess.run([sys.executable, str(repo / "ad_detection/scripts/make_v4_rule_submission.py"),
                            "--repo", str(repo), "--v4-windows", str(path), "--output", str(rule),
                            "--prediction-prefix", label], check=True)
            subprocess.run([sys.executable, str(repo / "ad_detection/scripts/convert_submission_category_names.py"),
                            str(rule), str(submit)], check=True)
            subprocess.run([sys.executable, str(repo / "ad_detection/scripts/validate_submission.py"), str(submit)], check=True)
            with submit.open(encoding="utf-8") as handle:
                submitted = [(r["start_time"], r["end_time"]) for r in (json.loads(line) for line in handle)]
            if submitted != intervals(path):
                raise RuntimeError("Submission formatter changed AD intervals")
            summary["variants"][mode]["submission_sha256"] = hashlib.sha256(submit.read_bytes()).hexdigest()
        write_json(args.output / "summary.json", summary)
        print(json.dumps({"mode": mode, **{k: v for k, v in summary["variants"][mode].items() if k != "by_metric"}}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
