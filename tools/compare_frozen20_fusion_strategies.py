from __future__ import annotations

import argparse
from datetime import timedelta, timezone
import json
from pathlib import Path
import sys


REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "bian_mas" / "implementation"))

from settings import load_config
import anomaly_detector.five_sigma as five_sigma


SOURCE_SIGMA = {"traffic": 5.0, "node": 6.0, "scrape": 6.0}


def _utc(value) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _city(node: str) -> str:
    return node.split("-", 1)[0]


def _role(node: str) -> str:
    for role in ("service-vm-1", "service-vm-2", "service-vm-3", "traffic-vm", "br-1", "br-2", "cr-1", "cr-2", "fw"):
        if node.endswith("-" + role):
            return role
    return node.split("-", 1)[1] if "-" in node else node


def _topology_score(nodes: set[str], metrics: set[str]) -> float:
    roles = {_role(node) for node in nodes}
    access = {"traffic-vm", "service-vm-1", "service-vm-2", "service-vm-3"} & roles
    score = min(len(metrics), 8) * 0.4
    if access:
        score += 1.0
    if "fw" in roles:
        score += 1.5
    if {"cr-1", "cr-2"} & roles:
        score += 1.0
    if {"br-1", "br-2"} & roles:
        score += 0.8
    if access and "fw" in roles:
        score += 2.5
    if "fw" in roles and {"cr-1", "cr-2"} & roles:
        score += 2.0
    return score


def _detect(data_root: Path, aliases: dict[str, str], source: str) -> list[dict]:
    series = five_sigma._read_series(data_root, aliases, sources={source})
    return five_sigma._detect_metric_segments(series, SOURCE_SIGMA[source], timedelta(minutes=5))


def _bucket_segments(segments: list[dict]) -> dict:
    buckets = {}
    for segment in segments:
        current = segment["start"].replace(second=0, microsecond=0)
        end = segment["end"].replace(second=0, microsecond=0)
        while current < end:
            bucket = buckets.setdefault(current, {"segments": 0, "nodes": set(), "metrics": set(), "score": 0.0, "items": []})
            bucket["segments"] += 1
            bucket["nodes"].add(segment["node"])
            bucket["metrics"].add(segment["metric"])
            bucket["score"] += min(segment["magnitude"], 30.0)
            bucket["items"].append(segment)
            current += timedelta(minutes=1)
    return buckets


def _make_windows(active: list[tuple], *, gap_minutes: int = 3) -> list[dict]:
    active.sort(key=lambda item: item[0])
    raw = []
    current = None
    for minute, score, bucket, label in active:
        if current is None or minute - current["last"] > timedelta(minutes=gap_minutes) or minute - current["start"] >= timedelta(minutes=29):
            if current is not None:
                raw.append(current)
            current = {"start": minute, "last": minute, "score": 0.0, "nodes": set(), "metrics": set(), "items": [], "labels": set(), "segments": 0}
        current["last"] = minute
        current["score"] += score
        current["nodes"] |= bucket["nodes"]
        current["metrics"] |= bucket["metrics"]
        current["items"].extend(bucket["items"])
        current["labels"].add(label)
        current["segments"] += bucket["segments"]
    if current is not None:
        raw.append(current)
    return [
        {
            "start": item["start"],
            "end": item["last"] + timedelta(minutes=1),
            "score": item["score"],
            "nodes": item["nodes"],
            "metrics": item["metrics"],
            "items": item["items"],
            "labels": item["labels"],
            "segments": item["segments"],
        }
        for item in raw
    ]


def _nms(windows: list[dict], limit: int) -> list[dict]:
    kept = []
    for window in sorted(windows, key=lambda item: item["score"], reverse=True):
        if all(
            abs((window["start"] - selected["start"]).total_seconds()) > 20 * 60
            and not (window["start"] < selected["end"] and selected["start"] < window["end"])
            for selected in kept
        ):
            kept.append(window)
            if len(kept) >= limit:
                break
    return sorted(kept, key=lambda item: item["start"])


def _strategy_traffic_anchor(segments_by_source: dict[str, list[dict]], limit: int) -> list[dict]:
    traffic_buckets = _bucket_segments(segments_by_source["traffic"])
    support_segments = segments_by_source["node"] + segments_by_source["scrape"]
    support_buckets = _bucket_segments(support_segments)
    active = []
    for minute, bucket in traffic_buckets.items():
        if bucket["segments"] < 2:
            continue
        support_score = 0.0
        support_nodes = set()
        support_metrics = set()
        support_items = []
        for delta in range(-2, 3):
            nearby = support_buckets.get(minute + timedelta(minutes=delta))
            if not nearby:
                continue
            support_score += 0.35 * nearby["score"]
            support_nodes |= nearby["nodes"]
            support_metrics |= nearby["metrics"]
            support_items.extend(nearby["items"][:20])
        nodes = bucket["nodes"] | support_nodes
        metrics = bucket["metrics"] | support_metrics
        score = 3.0 * bucket["score"] + support_score + 4.0 * _topology_score(nodes, metrics)
        merged = {
            "segments": bucket["segments"],
            "nodes": nodes,
            "metrics": metrics,
            "score": score,
            "items": bucket["items"] + support_items,
        }
        active.append((minute, score, merged, "traffic_anchor"))
    return _nms(_make_windows(active), limit)


def _source_windows(source: str, segments: list[dict]) -> list[dict]:
    buckets = _bucket_segments(segments)
    active = []
    for minute, bucket in buckets.items():
        if source == "traffic":
            keep = bucket["segments"] >= 2
            weight = 3.0
        elif source == "node":
            keep = bucket["segments"] >= 8 and len(bucket["nodes"]) >= 5 and len(bucket["metrics"]) >= 5
            weight = 1.0
        else:
            keep = bucket["segments"] >= 3
            weight = 0.5
        if keep:
            score = weight * bucket["score"] + _topology_score(bucket["nodes"], bucket["metrics"])
            active.append((minute, score, bucket, source))
    return _make_windows(active)


def _strategy_per_source_then_fuse(segments_by_source: dict[str, list[dict]], limit: int) -> list[dict]:
    candidates = []
    for source, segments in segments_by_source.items():
        for window in _source_windows(source, segments):
            window["source"] = source
            if source == "traffic":
                window["score"] *= 2.0
            elif source == "node":
                window["score"] *= 0.7
            else:
                window["score"] *= 0.4
            candidates.append(window)
    return _nms(candidates, limit)


def _write_windows(path: Path, windows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for index, window in enumerate(windows, 1):
            unique = {}
            for item in window["items"]:
                if not (window["start"] <= item["start"] < window["end"]):
                    continue
                key = (item["node"], item["metric"], item["start"])
                if key not in unique or item["magnitude"] > unique[key]["magnitude"]:
                    unique[key] = item
            points = sorted(unique.values(), key=lambda item: -item["magnitude"])[:30]
            top = points[0] if points else None
            record = {
                "window_id": f"{path.stem}_{index:06d}",
                "start": _utc(window["start"]),
                "end": _utc(window["end"]),
                "points": [
                    {"time": _utc(point["start"]), "node": point["node"], "metric": point["metric"], "magnitude": point["magnitude"]}
                    for point in points
                ],
                "diagnostics": {
                    "scope": top["node"] if top else "",
                    "anchor_metric": top["metric"] if top else "",
                    "families": ["cpu"],
                    "labels": sorted(window.get("labels", {window.get("source", "unknown")})),
                    "segments": window["segments"],
                    "nodes": len(window["nodes"]),
                    "metrics": len(window["metrics"]),
                    "score": round(window["score"], 3),
                },
            }
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def _summary(windows: list[dict]) -> dict:
    durations = [(window["end"] - window["start"]).total_seconds() / 60.0 for window in windows]
    labels = {}
    for window in windows:
        key = ",".join(sorted(window.get("labels", {window.get("source", "unknown")})))
        labels[key] = labels.get(key, 0) + 1
    return {
        "windows": len(windows),
        "duration_avg": round(sum(durations) / len(durations), 2) if durations else None,
        "duration_max": max(durations) if durations else None,
        "labels": labels,
        "fixed_04xx": sum(1 for window in windows if window["start"].hour == 4 and window["start"].minute <= 5),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/frozen20_fusion_compare"))
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()

    config = load_config()
    segments_by_source = {}
    source_stats = {}
    for source in ("traffic", "node", "scrape"):
        segments = _detect(args.data_root, config["region_aliases"], source)
        segments_by_source[source] = segments
        durations = [(segment["end"] - segment["start"]).total_seconds() / 60.0 for segment in segments]
        source_stats[source] = {
            "segments": len(segments),
            "duration_avg": round(sum(durations) / len(durations), 2) if durations else None,
            "duration_max": max(durations) if durations else None,
            "sigma": SOURCE_SIGMA[source],
        }

    anchor = _strategy_traffic_anchor(segments_by_source, args.limit)
    fused = _strategy_per_source_then_fuse(segments_by_source, args.limit)
    anchor_path = args.output_dir / "traffic_anchor_windows.jsonl"
    fused_path = args.output_dir / "per_source_fused_windows.jsonl"
    _write_windows(anchor_path, anchor)
    _write_windows(fused_path, fused)
    print(json.dumps({
        "source_stats": source_stats,
        "traffic_anchor": {**_summary(anchor), "output": str(anchor_path)},
        "per_source_fused": {**_summary(fused), "output": str(fused_path)},
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
