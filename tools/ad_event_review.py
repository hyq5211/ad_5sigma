from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import timedelta

from run_frozen20_multisource_ad import five_sigma as fs


MINUTE = timedelta(minutes=1)
MAX_DURATION = timedelta(minutes=30)


def _longest_run(minutes):
    longest = run = 0
    previous = None
    for minute in sorted(minutes):
        run = run+1 if previous is not None and minute-previous == MINUTE else 1
        longest = max(longest, run)
        previous = minute
    return longest


class EventEvidence:
    def __init__(self, series, floors, sigma=5):
        self.series, self.floors, self.sigma = series, floors, sigma
        self.times = {key: [t for t, _ in values] for key, values in series.items()}

    def direction(self, segment, point):
        key = point["node"], point["metric"]
        times, samples = self.times[key], self.series[key]
        left, right = bisect_left(times, point["time"]), bisect_right(times, point["time"])
        baseline = segment["baseline"]
        scale = baseline["std"]
        floor = self.floors.get(key[1], 0)
        if scale <= 1e-12 and floor > 0:
            scale = floor/self.sigma
        scale = max(scale, 1e-12)
        errors = [(abs(abs(v-baseline["mean"])/scale-point["magnitude"]),
                   1 if v > baseline["mean"] else -1) for _, v in samples[left:right]]
        if not errors:
            return 0
        best = min(error for error, _ in errors)
        signs = {sign for error, sign in errors if error-best <= 1e-6}
        # Same-time flows with equal magnitudes but opposite signs are not safe merge evidence.
        return next(iter(signs)) if len(signs) == 1 else 0

    def signature(self, window):
        result = {}
        for segment in {id(s): s for s in window["items"]}.values():
            for point in segment["points"]:
                if not window["start"] <= point["time"] < window["end"]:
                    continue
                direction = self.direction(segment, point)
                if not direction:
                    continue
                key = point["node"], point["metric"], direction
                if key not in result or point["magnitude"] > result[key][0]:
                    result[key] = point["magnitude"], segment["baseline"]
        return result

    def gap_state(self, left, right, left_signature, right_signature):
        common = left_signature.keys() & right_signature.keys()
        all_normal = None
        weak_minutes = set()
        for node, metric, direction in common:
            key = node, metric
            times = self.times[key]
            begin, end = bisect_left(times, left["end"]), bisect_left(times, right["start"])
            baselines = (left_signature[node, metric, direction][1], right_signature[node, metric, direction][1])
            thresholds = [fs._anomaly_threshold(b["std"], 2.5, self.floors.get(metric, 0), "zero_only")
                          for b in baselines]
            by_minute = defaultdict(list)
            for time, value in self.series[key][begin:end]:
                by_minute[time.replace(second=0, microsecond=0)].append(value)
            normal = set()
            for minute, values in by_minute.items():
                if all(threshold > 0 and abs(v-b["mean"]) <= threshold
                       for v in values for b, threshold in zip(baselines, thresholds)):
                    normal.add(minute)
                if any(all(threshold > 0 and direction*(v-b["mean"]) > threshold
                           for b, threshold in zip(baselines, thresholds)) for v in values):
                    weak_minutes.add(minute)
            all_normal = normal if all_normal is None else all_normal & normal
        return _longest_run(all_normal or ()), len(weak_minutes)


def merge_candidates(candidates, evidence, gap_minutes, audit):
    if gap_minutes < 0:
        raise ValueError("Merge gap cannot be negative")
    output = []
    signature = None
    for candidate in sorted(candidates, key=lambda c: c["start"]):
        current_signature = evidence.signature(candidate)
        if output:
            previous = output[-1]
            gap = candidate["start"]-previous["end"]
            if timedelta(0) <= gap <= timedelta(minutes=gap_minutes):
                audit["nearby_pairs"] += 1
                if candidate["end"]-previous["start"] > MAX_DURATION:
                    audit["reject_span_over30"] += 1
                elif not signature.keys() & current_signature.keys():
                    audit["reject_no_directed_series"] += 1
                else:
                    normal_run, weak = evidence.gap_state(previous, candidate, signature, current_signature)
                    if normal_run >= 3:
                        audit["reject_normal_recovery"] += 1
                    elif gap > timedelta(minutes=5) and weak < 2:
                        audit["reject_long_gap_without_weak_support"] += 1
                    else:
                        previous = {**previous, "end": candidate["end"],
                            "score": previous["score"]+candidate["score"],
                            "segments": previous["segments"]+candidate["segments"],
                            "items": previous["items"]+candidate["items"]}
                        for field in ("nodes", "metrics", "sources"):
                            previous[field] = output[-1][field] | candidate[field]
                        output[-1] = previous
                        # Only the latest core can establish the next merge relation.
                        signature = current_signature
                        audit["merged_pairs"] += 1
                        audit["bridged_minutes_sum"] += gap.total_seconds()/60
                        continue
        output.append(candidate)
        signature = current_signature
    audit["input_candidates"], audit["output_candidates"] = len(candidates), len(output)
    return output


def review_long_segments(problems, retained, evidence, core_sigma, audit):
    if core_sigma <= evidence.sigma:
        raise ValueError("Review cores must be stricter than the original detector")
    corroboration = defaultdict(set)
    for segment in [*retained, *problems]:
        for point in segment["points"]:
            corroboration[point["time"].replace(second=0, microsecond=0)].add((point["node"], point["metric"]))
    recovered = []
    for problem in problems:
        before = len(recovered)
        buffer = []
        current_direction = None
        by_minute = defaultdict(list)
        for point in problem["points"]:
            if point["magnitude"] > core_sigma:
                by_minute[point["time"].replace(second=0, microsecond=0)].append(point)
        key = problem["node"], problem["metric"]
        times = evidence.times[key]
        begin, finish = bisect_left(times, problem["start"]), bisect_left(times, problem["end"])
        scale = problem["baseline"]["std"]
        if scale <= 1e-12 and evidence.floors.get(key[1], 0) > 0:
            scale = evidence.floors[key[1]]/evidence.sigma
        scores = {}
        for time, value in evidence.series[key][begin:finish]:
            minute = time.replace(second=0, microsecond=0)
            score = abs(value-problem["baseline"]["mean"])/max(scale, 1e-12)
            scores[minute] = max(scores.get(minute, 0), score)
        valleys = {minute for minute, score in scores.items() if score <= core_sigma}
        first_minute = problem["start"].replace(second=0, microsecond=0)
        last_minute = (problem["end"]-MINUTE).replace(second=0, microsecond=0)

        def flush():
            if not buffer:
                return
            audit["raised_core_clusters"] += 1
            start, end = buffer[0]["time"], buffer[-1]["time"]+MINUTE
            if end-start > MAX_DURATION:
                audit["unresolved_core_over30"] += 1
                return
            minutes = {p["time"].replace(second=0, microsecond=0) for p in buffer}
            first, last = min(minutes), max(minutes)
            left_pairs = [m for m in valleys if first-5*MINUTE <= m-MINUTE and m < first and m-MINUTE in valleys]
            right_pairs = [m for m in valleys if last < m and m+MINUTE <= last+5*MINUTE and m+MINUTE in valleys]
            if (first != first_minute and not left_pairs) or (last != last_minute and not right_pairs):
                audit["reject_no_bounded_valleys"] += 1
                return
            if len(minutes) < 2:
                point = max(buffer, key=lambda p: p["magnitude"])
                support = corroboration[next(iter(minutes))]
                if point["magnitude"] <= 10 or len(support) < 3 or len({n for n, _ in support}) < 2:
                    audit["reject_unsupported_singleton"] += 1
                    return
            density = len(minutes)/((end-start).total_seconds()/60)
            if density < .5:
                audit["reject_sparse_core"] += 1
                return
            recovered.append({**problem, "start": start, "end": end, "points": list(buffer),
                "magnitude": max(p["magnitude"] for p in buffer), "review_core_sigma": core_sigma,
                "review_lower": min(start, max(left_pairs)+MINUTE) if left_pairs else start,
                "review_upper": max(end, min(right_pairs)) if right_pairs else end})

        for minute, points in sorted(by_minute.items()):
            directions = {evidence.direction(problem, point) for point in points}
            if 0 in directions or len(directions) != 1:
                audit["mixed_or_ambiguous_core_minutes"] += 1
                continue
            direction = next(iter(directions))
            point = max(points, key=lambda p: p["magnitude"])
            if buffer and (point["time"]-buffer[-1]["time"] > 2*MINUTE or direction != current_direction):
                flush()
                buffer = []
            buffer.append(point)
            current_direction = direction
        flush()
        audit["problem_segments_with_recovered_core"] += int(len(recovered) > before)
    audit["problem_segments"] = len(problems)
    audit["problem_anomaly_points"] = sum(len(s["points"]) for s in problems)
    audit["recovered_segments"] = len(recovered)
    audit["recovered_points"] = sum(len(s["points"]) for s in recovered)
    audit["unrecovered_problem_segments"] = len(problems)-audit["problem_segments_with_recovered_core"]
    return recovered


def protect_reference_records(reference, proposals, label, audit):
    entries = []
    for source, records in (("reference", reference), ("proposal", proposals)):
        for record in records:
            entries.append((fs._time({"timestamp": record["start"]}),
                            fs._time({"timestamp": record["end"]}), source, record))
    groups = []
    group_end = None
    for entry in sorted(entries, key=lambda e: (e[0], e[1])):
        if group_end is None or entry[0] >= group_end:
            groups.append([])
            group_end = entry[1]
        groups[-1].append(entry)
        group_end = max(group_end, entry[1])
    output = []
    for group in groups:
        old = [entry for entry in group if entry[2] == "reference"]
        new = [entry for entry in group if entry[2] == "proposal"]
        if not old or all(any(c <= a and b <= d for c, d, _, _ in new) for a, b, _, _ in old):
            output.extend(record for _, _, _, record in new)
        elif not new:
            audit["restored_reference_windows"] += len(old)
            output.extend(record for _, _, _, record in old)
        else:
            start, end = min(e[0] for e in group), max(e[1] for e in group)
            if end-start > MAX_DURATION:
                audit["reverted_over30_components"] += 1
                output.extend(record for _, _, _, record in old)
                continue
            strongest = max((r for _, _, _, r in group), key=lambda r: r.get("diagnostics", {}).get("score", 0))
            points = {}
            for _, _, _, record in group:
                for point in record.get("points", []):
                    key = point["node"], point["metric"], point["time"]
                    if key not in points or point["magnitude"] > points[key]["magnitude"]:
                        points[key] = point
            utc = lambda t: t.isoformat(timespec="milliseconds").replace("+00:00", "Z")
            output.append({**strongest, "start": utc(start), "end": utc(end),
                "points": sorted(points.values(), key=lambda p: -p["magnitude"])[:30],
                "diagnostics": {**strongest.get("diagnostics", {}), "reference_protection": "observed-boundary union"}})
            audit["expanded_protection_components"] += 1
    output.sort(key=lambda r: r["start"])
    for i, record in enumerate(output, 1):
        output[i-1] = {**record, "window_id": f"{label}_{i:06d}"}
    parsed = [(fs._time({"timestamp": r["start"]}), fs._time({"timestamp": r["end"]})) for r in output]
    if any(a[1] > b[0] for a, b in zip(parsed, parsed[1:])):
        raise RuntimeError("Reference protection introduced overlap")
    if any(not MINUTE <= end-start <= MAX_DURATION for start, end in parsed):
        raise RuntimeError("Reference protection duration violation")
    if any(not any(start <= a and b <= end for start, end in parsed)
           for a, b, source, _ in entries if source == "reference"):
        raise RuntimeError("Reference protection failed to preserve a window")
    audit["reference_windows"], audit["protected_output_windows"] = len(reference), len(output)
    return output
