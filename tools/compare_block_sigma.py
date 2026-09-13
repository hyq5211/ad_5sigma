from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path
import statistics

from compare_continuous_stage1 import counter_rates, floors_for, stats, write_json, write_windows
from compare_traffic_baselines import intervals
from run_frozen20_multisource_ad import _build_windows, _utc, five_sigma as fs, load_config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("outputs/block_sigma_compare"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[1]
    print("Reading full traffic dataset", flush=True)
    original = fs._read_series(args.data_root, load_config()["region_aliases"], sources={"traffic"})
    for samples in original.values():
        samples.sort(key=lambda sample: sample[0])
    start = min(t for samples in original.values() for t, _ in samples)
    end = start + timedelta(days=14)
    series, audit = counter_rates(original)
    floors = floors_for(series)
    reference5 = repo / "outputs/traffic_baseline_compare/C_block292_windows.jsonl"
    frozen = set(intervals(repo / "outputs/continuous_stage1/C_zero_variance_traffic_windows.jsonl"))
    summary = {"fixed": {"source": "traffic only", "baseline": "whole block",
                "block_start": _utc(start), "block_end_exclusive": _utc(end), "blocks": 292,
                "block_minutes": 14*24*60/292, "metric_gap_minutes": 5,
                "global_gap_minutes": 3, "window_limit": 500, "minimum_baseline_samples": 12,
                "input_processing": "same as C; counters to rates; zero variance floors unchanged"},
               "counter_audit": audit, "variants": {},
               "no_ground_truth": True, "submitted": False}
    reference_intervals = None
    for sigma in (5.0, 4.5, 4.0):
        print(f"Detecting block292 sigma={sigma}", flush=True)
        detector_audit = {}
        segments = fs._detect_metric_segments(series, sigma, timedelta(minutes=5), zero_floors=floors,
            diagnostics=detector_audit, baseline_mode="block292", block_start=start, block_end=end)
        aggregation_audit = {}
        windows = _build_windows(segments, limit=500, global_gap_minutes=3, diagnostics=aggregation_audit)
        label = "block292_sigma" + str(sigma).replace(".", "_")
        path = args.output / f"{label}_windows.jsonl"
        write_windows(path, windows, label)
        current = set(intervals(path))
        if sigma == 5:
            if intervals(path) != intervals(reference5):
                raise RuntimeError("5sigma interval reproduction failed")
            summary["five_sigma_exact_reproduction"] = True
            reference_intervals = current
        lengths = [(w["end"]-w["start"]).total_seconds()/60 for w in windows]
        source_stats = stats(segments)
        result = {**source_stats, **detector_audit, "aggregation": aggregation_audit,
            "anomaly_points_in_kept_segments": sum(len(s["points"]) for s in segments),
            "singleton_segment_percent": round(100*source_stats["singleton_segments"]/len(segments), 2) if segments else 0,
            "windows": len(windows),
            "mean_window_minutes": round(statistics.mean(lengths), 3) if lengths else 0,
            "median_window_minutes": statistics.median(lengths) if lengths else 0,
            "windows_at_most_3min": sum(d <= 3 for d in lengths),
            "windows_at_least_5min": sum(d >= 5 for d in lengths),
            "windows_at_least_10min": sum(d >= 10 for d in lengths),
            "maximum_window_minutes": max(lengths, default=0),
            "total_selected_window_minutes": sum(lengths),
            "exact_intervals_shared_with_sigma5": len(current & reference_intervals),
            "new_or_changed_intervals_vs_sigma5": len(current-reference_intervals),
            "sigma5_intervals_not_preserved": len(reference_intervals-current),
            "exact_intervals_shared_with_frozen_C": len(current & frozen),
            "ideal_two_level_max_fault_minutes": (14*24*60/292)/(sigma*sigma+1)}
        summary["variants"][str(sigma)] = result
        write_json(args.output / "summary.json", summary)
        print({"sigma": sigma, "segments": len(segments), "windows": len(windows),
               "mean_minutes": result["mean_window_minutes"], "median_minutes": result["median_window_minutes"],
               "singleton_segment_percent": result["singleton_segment_percent"],
               "raw_candidates": aggregation_audit["raw_candidates"]}, flush=True)


if __name__ == "__main__":
    main()
