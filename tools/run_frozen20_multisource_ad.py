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


SOURCE_SIGMA = {
    "traffic": 5.0,
    "interface": 7.0,
    "node": 6.0,
    "scrape": 6.0,
}

SOURCE_WEIGHT = {
    "traffic": 3.0,
    "interface": 2.5,
    "node": 1.0,
    "scrape": 0.5,
}


def _utc(value) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _city(node: str) -> str:
    return node.split("-", 1)[0]


def _role(node: str) -> str:
    for role in ("service-vm-1", "service-vm-2", "service-vm-3", "traffic-vm", "br-1", "br-2", "cr-1", "cr-2", "fw"):
        if node.endswith("-" + role):
            return role
    return node.split("-", 1)[1] if "-" in node else node


def _topology_bonus(nodes: set[str], metrics: set[str]) -> float:
    roles = {_role(node) for node in nodes}
    access = {"traffic-vm", "service-vm-1", "service-vm-2", "service-vm-3"} & roles
    has_fw = "fw" in roles
    has_cr = bool({"cr-1", "cr-2"} & roles)
    has_br = bool({"br-1", "br-2"} & roles)
    score = min(len(metrics), 8) * 0.3
    if access:
        score += 1.0
    if has_fw:
        score += 1.0
    if has_cr:
        score += 0.8
    if has_br:
        score += 0.6
    if access and has_fw:
        score += 2.0
    if has_fw and has_cr:
        score += 1.5
    if has_cr and has_br:
        score += 1.0
    if access and has_fw and (has_cr or has_br):
        score += 2.5
    return score


def _detect_source_segments(data_root: Path, aliases: dict[str, str], source: str, metric_gap_minutes: int) -> list[dict]:
    series = five_sigma._read_series(data_root, aliases, sources={source})
    return five_sigma._detect_metric_segments(
        series,
        SOURCE_SIGMA[source],
        timedelta(minutes=metric_gap_minutes),
    )


def _build_windows(segments: list[dict], *, limit: int, global_gap_minutes: int) -> list[dict]:
    buckets: dict = {}
    for segment in segments:
        start = segment["start"].replace(second=0, microsecond=0)
        end = segment["end"].replace(second=0, microsecond=0)
        source = segment.get("source", segment["metric"].split(".", 1)[0])
        current = start
        while current < end:
            bucket = buckets.setdefault(current, {
                "score": 0.0,
                "segments": 0,
                "nodes": set(),
                "metrics": set(),
                "sources": set(),
                "items": [],
            })
            bucket["score"] += SOURCE_WEIGHT.get(source, 1.0) * min(segment["magnitude"], 30.0)
            bucket["segments"] += 1
            bucket["nodes"].add(segment["node"])
            bucket["metrics"].add(segment["metric"])
            bucket["sources"].add(source)
            bucket["items"].append(segment)
            current += timedelta(minutes=1)

    active = []
    for minute, bucket in buckets.items():
        source_score = (
            3.0 * ("traffic" in bucket["sources"])
            + 2.5 * ("interface" in bucket["sources"])
            + 1.0 * ("node" in bucket["sources"])
            + 0.5 * ("scrape" in bucket["sources"])
        )
        score = bucket["score"] + 8.0 * source_score + _topology_bonus(bucket["nodes"], bucket["metrics"])
        if (
            ("traffic" in bucket["sources"] and bucket["segments"] >= 3)
            or ("interface" in bucket["sources"] and bucket["segments"] >= 4)
            or (len(bucket["sources"]) >= 2 and bucket["segments"] >= 5)
            or (len(bucket["nodes"]) >= 5 and bucket["segments"] >= 8)
        ):
            active.append((minute, score, bucket))
    active.sort(key=lambda item: item[0])

    raw = []
    current_window = None
    for minute, score, bucket in active:
        if current_window is None or minute - current_window["last"] > timedelta(minutes=global_gap_minutes) or minute - current_window["start"] >= timedelta(minutes=29):
            if current_window is not None:
                raw.append(current_window)
            current_window = {
                "start": minute,
                "last": minute,
                "score": 0.0,
                "segments": 0,
                "nodes": set(),
                "metrics": set(),
                "sources": set(),
                "items": [],
            }
        current_window["last"] = minute
        current_window["score"] += score
        current_window["segments"] += bucket["segments"]
        current_window["nodes"] |= bucket["nodes"]
        current_window["metrics"] |= bucket["metrics"]
        current_window["sources"] |= bucket["sources"]
        current_window["items"].extend(bucket["items"])
    if current_window is not None:
        raw.append(current_window)

    candidates = []
    for window in raw:
        candidates.append({
            "start": window["start"],
            "end": window["last"] + timedelta(minutes=1),
            "score": window["score"],
            "segments": window["segments"],
            "nodes": window["nodes"],
            "metrics": window["metrics"],
            "sources": window["sources"],
            "items": window["items"],
        })

    kept = []
    for candidate in sorted(candidates, key=lambda item: item["score"], reverse=True):
        if all(
            abs((candidate["start"] - selected["start"]).total_seconds()) > 20 * 60
            and not (candidate["start"] < selected["end"] and selected["start"] < candidate["end"])
            for selected in kept
        ):
            kept.append(candidate)
            if len(kept) >= limit:
                break
    return sorted(kept, key=lambda item: item["start"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("outputs/frozen20_multisource_windows.jsonl"))
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--sources", default="traffic,interface,node,scrape")
    parser.add_argument("--metric-gap-minutes", type=int, default=5)
    parser.add_argument("--global-gap-minutes", type=int, default=3)
    args = parser.parse_args()

    config = load_config()
    all_segments = []
    stats = {}
    sources = [item.strip() for item in args.sources.split(",") if item.strip()]
    for source in sources:
        segments = _detect_source_segments(args.data_root, config["region_aliases"], source, args.metric_gap_minutes)
        all_segments.extend(segments)
        durations = [(segment["end"] - segment["start"]).total_seconds() / 60.0 for segment in segments]
        stats[source] = {
            "segments": len(segments),
            "duration_avg": round(sum(durations) / len(durations), 2) if durations else None,
            "duration_max": max(durations) if durations else None,
            "sigma": SOURCE_SIGMA[source],
        }

    windows = _build_windows(all_segments, limit=args.limit, global_gap_minutes=args.global_gap_minutes)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for index, window in enumerate(windows, 1):
            unique = {}
            for item in window["items"]:
                key = (item["node"], item["metric"], item["start"])
                if key not in unique or item["magnitude"] > unique[key]["magnitude"]:
                    unique[key] = item
            points = sorted(unique.values(), key=lambda item: -item["magnitude"])[:30]
            top = points[0] if points else None
            record = {
                "window_id": f"frozen20_multisource_{index:06d}",
                "start": _utc(window["start"]),
                "end": _utc(window["end"]),
                "points": [
                    {
                        "time": _utc(point["start"]),
                        "node": point["node"],
                        "metric": point["metric"],
                        "magnitude": point["magnitude"],
                    }
                    for point in points
                ],
                "diagnostics": {
                    "scope": top["node"] if top else "",
                    "anchor_metric": top["metric"] if top else "",
                    "families": ["cpu"],
                    "sources": sorted(window["sources"]),
                    "segments": window["segments"],
                    "nodes": len(window["nodes"]),
                    "metrics": len(window["metrics"]),
                    "score": round(window["score"], 3),
                },
            }
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")

    durations = [(window["end"] - window["start"]).total_seconds() / 60.0 for window in windows]
    print(json.dumps({
        "source_stats": stats,
        "windows": len(windows),
        "duration_avg": round(sum(durations) / len(durations), 2) if durations else None,
        "duration_max": max(durations) if durations else None,
        "metric_gap_minutes": args.metric_gap_minutes,
        "global_gap_minutes": args.global_gap_minutes,
        "output": str(args.output),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
