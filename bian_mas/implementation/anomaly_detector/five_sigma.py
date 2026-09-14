from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
import csv
import math
from statistics import median
from pathlib import Path
from typing import Any


BASELINE_WINDOW = timedelta(minutes=20)
DEFAULT_EVENT_GAP = timedelta(minutes=5)
MAX_METRIC_EVENT_DURATION = timedelta(minutes=30)
MIN_BASELINE_POINTS = 12


def _number(value: str) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _time(row: dict[str, str]) -> datetime | None:
    for key in ("timestamp", "timestamp_utc", "minute_utc", "first_seen"):
        value = row.get(key, "").strip().strip('"')
        if value:
            value = value.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(value)
                return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
            except ValueError:
                continue
    return None


def _city(path: Path, aliases: dict[str, str]) -> str | None:
    for alias, city in aliases.items():
        if alias in path.as_posix().lower():
            return city
    return None


def _node_id(row: dict[str, str], city: str | None) -> str | None:
    raw = row.get("node") or row.get("node_key") or ""
    raw = raw.strip().strip('"').lower()
    role = next((token for token in ("br-1", "br-2", "cr-1", "cr-2", "traffic-vm", "service-vm-1", "service-vm-2", "service-vm-3", "fw") if token in raw), None)
    if role is None or city is None:
        return None
    return f"{city}-{role}"


def _source(path: Path) -> str:
    if "traffic_flow" in path.name:
        return "traffic"
    if "interface_metrics" in path.name:
        return "interface"
    if "routing_metrics" in path.name:
        return "routing"
    if "node_metrics" in path.name:
        return "node"
    if "scrape_health" in path.name:
        return "scrape"
    return path.stem.split("_")[0]


def _traffic_node(row: dict[str, str], aliases: dict[str, str]) -> str | None:
    region = (row.get("source_region") or "").strip().strip('"').lower()
    city = aliases.get(region, region)
    return f"{city}-traffic-vm" if city in set(aliases.values()) else None


def _metric_key(source: str, metric: str, row: dict[str, str]) -> str:
    if source == "interface":
        interface = (row.get("interface_id") or row.get("if_role") or "").strip().strip('"').lower()
        if interface:
            return f"{source}.{interface}.{metric}"
    if source == "traffic":
        flow = (row.get("flow_type") or "").strip().strip('"').lower() or "flow"
        target = (row.get("target_region") or "").strip().strip('"').lower() or "target"
        return f"{source}.{flow}.{target}.{metric}"
    return f"{source}.{metric}"


def _numeric_fields(row: dict[str, str], source: str = "") -> list[tuple[str, float]]:
    ignored = {"id", "port", "collector_port", "protocol", "src_port", "dst_port", "flow_record_count"}
    preferred = {
        "traffic": {
            "dns_flow_batches_failed_total",
            "dns_flow_batches_timeout_total",
            "dns_flow_error_total",
            "dns_flow_latency_mean_seconds",
            "dns_flow_latency_p95_seconds",
            "dns_flow_observed_qps",
            "web_flow_batches_failed_total",
            "web_flow_batches_timeout_total",
            "web_flow_error_total",
            "web_flow_latency_mean_seconds",
            "web_flow_latency_p95_seconds",
            "web_flow_observed_qps",
            "auth_flow_batches_failed_total",
            "auth_flow_batches_timeout_total",
            "auth_flow_error_total",
            "auth_flow_latency_mean_seconds",
            "auth_flow_latency_p95_seconds",
            "auth_flow_observed_qps",
            "elephant_flow_latency_mean_seconds",
            "elephant_flow_latency_p95_seconds",
            "elephant_flow_loss_rate",
            "elephant_flow_jitter_seconds",
            "elephant_flow_retransmits_total",
            "elephant_flow_throughput_bps",
        },
        "interface": {
            "rx_drop_rate",
            "tx_drop_rate",
            "rx_error_rate",
            "tx_error_rate",
            "rx_bytes_rate",
            "tx_bytes_rate",
            "rx_packets_rate",
            "tx_packets_rate",
            "carrier_changes",
        },
    }
    result = []
    for key, value in row.items():
        if key in ignored or key.endswith("_port") or key in {"timestamp", "timestamp_utc", "prometheus_sample_time_utc", "minute_utc"}:
            continue
        if source in preferred and key not in preferred[source]:
            continue
        number = _number(value)
        if number is not None:
            result.append((key, number))
    return result[:32]


def _mean_std(values: list[float]) -> tuple[float, float]:
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return mean, math.sqrt(variance)


def _baseline_center_scale(values: list[float], estimator: str) -> tuple[float, float]:
    if estimator == "mean_std":
        return _mean_std(values)
    if estimator == "mad":
        center = median(values)
        return center, 1.4826 * median([abs(value-center) for value in values])
    raise ValueError(f"Unknown baseline estimator: {estimator}")


def _is_anomaly(value: float, mean: float, std: float, sigma: float) -> bool:
    return std > 1e-12 and abs(value - mean) > sigma * std


def _anomaly_threshold(std: float, sigma: float, floor: float, mode: str) -> float:
    if mode == "all_scales":
        return max(sigma * std if std > 1e-12 else 0.0, floor)
    if mode == "zero_only":
        return floor if std <= 1e-12 else sigma * std
    raise ValueError(f"Unknown floor mode: {mode}")


def _read_series(
    root: Path,
    aliases: dict[str, str],
    sources: set[str] | None = None,
) -> dict[tuple[str, str], list[tuple[datetime, float]]]:
    series: dict[tuple[str, str], list[tuple[datetime, float]]] = defaultdict(list)
    for path in sorted(root.rglob("*.csv")):
        if (
            path.parent.name != "processed"
            or "frr_syslog" in path.name
            or "netflow" in path.name
        ):
            continue
        source = _source(path)
        if sources is not None and source not in sources:
            continue
        city = _city(path, aliases)
        try:
            handle = path.open(newline="", encoding="utf-8-sig", errors="replace")
        except OSError:
            continue
        with handle:
            reader = csv.DictReader(handle)
            for row in reader:
                timestamp = _time(row)
                node = _traffic_node(row, aliases) if source == "traffic" else _node_id(row, city)
                if timestamp is None or node is None:
                    continue
                for metric, value in _numeric_fields(row, source):
                    series[(node, _metric_key(source, metric, row))].append((timestamp, value))
    return series


def _detect_points(
    series: dict[tuple[str, str], list[tuple[datetime, float]]],
    sigma: float,
) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []

    def flush_metric_event(buffer: list[dict[str, Any]]) -> None:
        if not buffer:
            return
        if buffer[-1]["time"] - buffer[0]["time"] <= MAX_METRIC_EVENT_DURATION:
            points.extend(buffer)

    for (node, metric), values in series.items():
        values.sort(key=lambda item: item[0])
        if len(values) < MIN_BASELINE_POINTS + 1:
            continue

        baseline_window: deque[tuple[datetime, float]] = deque()
        frozen_baseline: tuple[float, float] | None = None
        metric_event_points: list[dict[str, Any]] = []
        last_anomaly_time: datetime | None = None

        for timestamp, value in values:
            if frozen_baseline is None:
                window_start = timestamp - BASELINE_WINDOW
                while baseline_window and baseline_window[0][0] < window_start:
                    baseline_window.popleft()
                if len(baseline_window) < MIN_BASELINE_POINTS:
                    baseline_window.append((timestamp, value))
                    continue

                baseline_values = [item[1] for item in baseline_window]
                mean, std = _mean_std(baseline_values)
                if _is_anomaly(value, mean, std, sigma):
                    frozen_baseline = (mean, std)
                    last_anomaly_time = timestamp
                    metric_event_points = [{"time": timestamp, "node": node, "metric": metric, "magnitude": abs(value - mean) / max(std, 1e-12)}]
                else:
                    baseline_window.append((timestamp, value))
                continue

            mean, std = frozen_baseline
            if _is_anomaly(value, mean, std, sigma):
                last_anomaly_time = timestamp
                metric_event_points.append({"time": timestamp, "node": node, "metric": metric, "magnitude": abs(value - mean) / max(std, 1e-12)})
                continue

            if last_anomaly_time is not None and timestamp - last_anomaly_time > BASELINE_WINDOW:
                flush_metric_event(metric_event_points)
                frozen_baseline = None
                metric_event_points = []
                last_anomaly_time = None
                baseline_window.clear()
            baseline_window.append((timestamp, value))
        flush_metric_event(metric_event_points)
    points.sort(key=lambda item: item["time"])
    return points


def _detect_metric_segments(
    series: dict[tuple[str, str], list[tuple[datetime, float]]],
    sigma: float,
    event_gap: timedelta,
    *,
    zero_floors: dict[str, float] | None = None,
    diagnostics: dict[str, Any] | None = None,
    baseline_mode: str = "frozen20",
    block_start: datetime | None = None,
    block_end: datetime | None = None,
    block_count: int = 292,
    include_baseline: bool = False,
    baseline_estimator: str = "mean_std",
    scale_floor_mode: str = "zero_only",
    mad_zero_fallback: str = "none",
) -> list[dict[str, Any]]:
    if scale_floor_mode not in {"zero_only", "all_scales"}:
        raise ValueError(f"Unknown floor mode: {scale_floor_mode}")
    if mad_zero_fallback not in {"none", "std"}:
        raise ValueError(f"Unknown MAD fallback: {mad_zero_fallback}")
    if mad_zero_fallback != "none" and baseline_estimator != "mad":
        raise ValueError("MAD fallback requires MAD estimator")
    if baseline_estimator not in {"mean_std", "mad"}:
        raise ValueError(f"Unknown baseline estimator: {baseline_estimator}")
    if baseline_estimator != "mean_std" and baseline_mode != "frozen20":
        raise ValueError("Robust estimator comparison requires frozen20 mode")
    if baseline_mode not in {"frozen20", "rolling69", "block292"}:
        raise ValueError(f"Unknown baseline mode: {baseline_mode}")
    if include_baseline and baseline_mode != "frozen20":
        raise ValueError("Baseline snapshots require frozen20 mode")
    if baseline_mode == "block292" and (
        block_start is None or block_end is None or block_end <= block_start or block_count <= 0
    ):
        raise ValueError("Block baseline requires a positive time range and block count")
    segments: list[dict[str, Any]] = []

    def keep_segment(buffer: list[dict[str, Any]]) -> None:
        if not buffer:
            return
        start = buffer[0]["time"]
        end = buffer[-1]["time"]
        if end - start > MAX_METRIC_EVENT_DURATION:
            if diagnostics is not None:
                diagnostics["discarded_long_segments"] = diagnostics.get("discarded_long_segments", 0) + 1
            return
        top_point = max(buffer, key=lambda item: item["magnitude"])
        source = top_point["metric"].split(".", 1)[0]
        segments.append({
            "start": start,
            "end": end + timedelta(minutes=1),
            "node": top_point["node"],
            "metric": top_point["metric"],
            "source": source,
            "points": list(buffer),
            "magnitude": top_point["magnitude"],
        })
        if include_baseline:
            segments[-1]["baseline"] = {"mean": frozen_baseline[0], "std": frozen_baseline[1]}

    for (node, metric), values in series.items():
        floor = (zero_floors or {}).get(metric, 0.0)

        def anomalous(value: float, mean: float, std: float) -> bool:
            threshold = _anomaly_threshold(std, sigma, floor, scale_floor_mode)
            return threshold > 0 and abs(value - mean) > threshold

        def magnitude(value: float, mean: float, std: float) -> float:
            scale = floor / sigma if std <= 1e-12 and floor > 0 else std
            if scale_floor_mode == "all_scales":
                scale = max(scale, floor / sigma)
            return abs(value - mean) / max(scale, 1e-12)

        values.sort(key=lambda item: item[0])
        if len(values) < MIN_BASELINE_POINTS + 1:
            continue

        if baseline_mode != "frozen20":
            candidate_points: list[dict[str, Any]] = []

            def evaluate_point(timestamp: datetime, value: float, mean: float, std: float) -> None:
                if not anomalous(value, mean, std):
                    return
                if candidate_points and timestamp - candidate_points[-1]["time"] > event_gap:
                    keep_segment(candidate_points)
                    candidate_points.clear()
                candidate_points.append({"time": timestamp, "node": node, "metric": metric,
                                         "magnitude": magnitude(value, mean, std)})

            if baseline_mode == "rolling69":
                history: deque[tuple[datetime, float]] = deque()
                for timestamp, value in values:
                    cutoff = timestamp - timedelta(minutes=69)
                    while history and history[0][0] < cutoff:
                        history.popleft()
                    if len(history) >= MIN_BASELINE_POINTS:
                        mean, std = _mean_std([v for _, v in history])
                        evaluate_point(timestamp, value, mean, std)
                    # Include prior anomalies in future baselines, never the current sample.
                    history.append((timestamp, value))
            else:
                blocks: dict[int, list[tuple[datetime, float]]] = defaultdict(list)
                span_us = (block_end - block_start) // timedelta(microseconds=1)
                for timestamp, value in values:
                    if not block_start <= timestamp < block_end:
                        if diagnostics is not None:
                            diagnostics["outside_block_range"] = diagnostics.get("outside_block_range", 0) + 1
                        continue
                    offset_us = (timestamp - block_start) // timedelta(microseconds=1)
                    blocks[offset_us * block_count // span_us].append((timestamp, value))
                for block in sorted(blocks):
                    samples = blocks[block]
                    if len(samples) < MIN_BASELINE_POINTS:
                        if diagnostics is not None:
                            diagnostics["insufficient_baseline_blocks"] = diagnostics.get("insufficient_baseline_blocks", 0) + 1
                        continue
                    mean, std = _mean_std([v for _, v in samples])
                    for timestamp, value in samples:
                        evaluate_point(timestamp, value, mean, std)
                # Deliberately do not split events at arbitrary block boundaries.
            keep_segment(candidate_points)
            continue

        baseline_window: deque[tuple[datetime, float]] = deque()
        frozen_baseline: tuple[float, float] | None = None
        last_anomaly_time: datetime | None = None
        current_segment: list[dict[str, Any]] = []

        for timestamp, value in values:
            if frozen_baseline is None:
                window_start = timestamp - BASELINE_WINDOW
                while baseline_window and baseline_window[0][0] < window_start:
                    baseline_window.popleft()
                if len(baseline_window) < MIN_BASELINE_POINTS:
                    baseline_window.append((timestamp, value))
                    continue

                baseline_values = [item[1] for item in baseline_window]
                mean, std = _baseline_center_scale(baseline_values, baseline_estimator)
                if baseline_estimator == "mad" and std <= 1e-12:
                    ordinary_scale = _mean_std(baseline_values)[1]
                    if diagnostics is not None:
                        diagnostics["raw_mad_zero_baselines"] = diagnostics.get("raw_mad_zero_baselines", 0) + 1
                        if ordinary_scale > 1e-12:
                            diagnostics["mad_zero_with_nonzero_std_baselines"] = diagnostics.get("mad_zero_with_nonzero_std_baselines", 0) + 1
                    if mad_zero_fallback == "std" and ordinary_scale > 1e-12:
                        std = ordinary_scale
                        if diagnostics is not None:
                            diagnostics["mad_std_fallback_uses"] = diagnostics.get("mad_std_fallback_uses", 0) + 1
                if diagnostics is not None:
                    diagnostics["baseline_evaluations"] = diagnostics.get("baseline_evaluations", 0) + 1
                    if std <= 1e-12:
                        diagnostics["zero_scale_baselines"] = diagnostics.get("zero_scale_baselines", 0) + 1
                    elif floor > 0 and sigma * std < floor:
                        diagnostics["small_scale_below_floor_baselines"] = diagnostics.get("small_scale_below_floor_baselines", 0) + 1
                if anomalous(value, mean, std):
                    frozen_baseline = (mean, std)
                    last_anomaly_time = timestamp
                    current_segment = [{"time": timestamp, "node": node, "metric": metric, "magnitude": magnitude(value, mean, std)}]
                else:
                    baseline_window.append((timestamp, value))
                continue

            mean, std = frozen_baseline
            if anomalous(value, mean, std):
                point = {"time": timestamp, "node": node, "metric": metric, "magnitude": magnitude(value, mean, std)}
                if last_anomaly_time is None or timestamp - last_anomaly_time <= event_gap:
                    current_segment.append(point)
                else:
                    keep_segment(current_segment)
                    current_segment = [point]
                last_anomaly_time = timestamp
                continue

            if last_anomaly_time is not None and timestamp - last_anomaly_time > event_gap:
                keep_segment(current_segment)
                current_segment = []
                frozen_baseline = None
                last_anomaly_time = None
                baseline_window.clear()
            baseline_window.append((timestamp, value))
        keep_segment(current_segment)

    segments.sort(key=lambda item: (item["start"], item["end"], item["node"], item["metric"]))
    return segments


def _aggregate_events(points: list[dict[str, Any]], event_gap: timedelta) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for point in points:
        if not events or point["time"] - events[-1]["end"] > event_gap:
            events.append({"start": point["time"], "end": point["time"], "points": []})
        event = events[-1]
        event["end"] = max(event["end"], point["time"])
        event["points"].append(point)
    for event in events:
        event["end"] = event["end"] + timedelta(minutes=1)
        event["points"].sort(key=lambda item: (-item["magnitude"], item["node"], item["metric"]))
    return events


def detect(
    root: Path,
    aliases: dict[str, str],
    sigma: float = 5.0,
    event_gap: timedelta = DEFAULT_EVENT_GAP,
) -> list[dict[str, Any]]:
    series = _read_series(root, aliases)
    points = _detect_points(series, sigma)
    return _aggregate_events(points, event_gap)
