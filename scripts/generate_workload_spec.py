#!/usr/bin/env python3
"""
Generate a downscaled fine-grained `--workload-spec` JSON for ChangeDataGenerator from a real
Hudi table's workload profile (see README.md "Fine-Grained Workload Spec" and
src/main/scala/ai/onehouse/lakeloader/configs/FineGrainedWorkloadSpec.scala for the target
format and its validation rules).

Inputs (from --profile-dir):
  - workload-profile.json       summary stats, incl. latestPartitionSizes for every touched
                                 partition and per-commit arrays (partitionsTouchedPerCommit,
                                 updatesPerCommit, updateRatio, commitCount).
  - commit-partition-stats.json exact per-commit, per-partition {inserts, updates, deletes}.

Output: a JSON file with `bootstrap` (N daily partitions, uniform size) and `commits` (one
object per original commit). Each commit distributes updates across a weighted-sampled subset
of the bootstrap partitions (per-partition counts vary with that partition's activity weight,
not a flat share) and inserts across the most-recently-active partitions only -- no new
partitions are opened, so the table stays fixed at N partitions, but total insert volume is
capped by reusing the real table's relative growth ratio (total inserts / total records) over
its own 20-commit window, scaled to the bootstrap size instead of an arbitrary number.
"""

import argparse
import json
import math
import random
import statistics
from datetime import date, datetime, timedelta

DATE_FMT_SLASH = "%Y/%m/%d"
DATE_FMT_DASH = "%Y-%m-%d"


def load_profile(profile_dir):
    with open(f"{profile_dir}/workload-profile.json") as f:
        profile = json.load(f)
    with open(f"{profile_dir}/commit-partition-stats.json") as f:
        commit_stats = json.load(f)
    return profile, commit_stats


def build_update_weights(profile, commit_stats):
    """Sum updates per partition date across all commits; union with every partition date seen
    in latestPartitionSizes (weight 0 if it was never updated). Returns a dict of
    'yyyy/MM/dd' -> total update count."""
    weights = {}
    for entry in profile["latestPartitionSizes"]:
        weights[entry["partition"]] = 0
    for commit in commit_stats:
        for p in commit["partitions"]:
            weights[p["partition"]] = weights.get(p["partition"], 0) + p["updates"]
    return weights


def build_buckets(weights, dense_count, window_cap):
    """Order partitions ascending by date, drop the most recent one (assumed partially filled),
    then collapse into buckets: the `dense_count` most recent remaining dates are kept
    individually; everything older is grouped into windows that double in size (2, 4, 8, ...)
    capped at `window_cap`, each bucket weight = mean update count of its window. Returns the
    final bucket list ordered oldest -> newest, each item a float weight."""
    dates_sorted = sorted(weights.keys(), key=lambda d: datetime.strptime(d, DATE_FMT_SLASH))
    dates_sorted = dates_sorted[:-1]  # drop the latest (possibly partially-filled) partition

    dense = dates_sorted[-dense_count:]
    rest = dates_sorted[:-dense_count]

    rest_buckets = []
    i = 0
    w = 2
    while i < len(rest):
        window = rest[i : i + w]
        rest_buckets.append(statistics.mean(weights[d] for d in window))
        i += len(window)
        if w < window_cap:
            w *= 2

    dense_buckets = [float(weights[d]) for d in dense]
    return rest_buckets + dense_buckets


def weighted_sample_without_replacement(population, weights, k, rng):
    """Efraimidis-Spirakis weighted random sampling without replacement (stdlib-only, no
    numpy): assign each item a key = U ** (1 / weight) for U ~ Uniform(0, 1), then take the
    top-k by key. Falls back to weight=1e-9 for zero-weight items so they remain eligible
    (with low priority) rather than raising on a zero-division."""
    keyed = []
    for item, weight in zip(population, weights):
        w = weight if weight > 0 else 1e-9
        u = rng.random()
        keyed.append((u ** (1.0 / w), item))
    keyed.sort(key=lambda t: t[0], reverse=True)
    return [item for _, item in keyed[:k]]


def scale_to_range(values, lo, hi):
    """Min-max scale a list of numbers into [lo, hi], rounding to int. Constant input maps to
    the midpoint."""
    vmin, vmax = min(values), max(values)
    if vmax == vmin:
        mid = round((lo + hi) / 2)
        return [mid] * len(values)
    return [
        round(lo + (v - vmin) / (vmax - vmin) * (hi - lo))
        for v in values
    ]


def distribute_proportionally(total, weights, min_each=0):
    """Split an integer `total` across `weights` proportionally (largest-remainder method), so
    the per-item counts actually vary with weight instead of every item getting the same flat
    share. If `min_each` > 0, every item is guaranteed at least that floor (e.g. so a partition
    chosen as an update target doesn't get rounded down to 0 and silently drop out) and only the
    remaining budget above that floor is weight-distributed. Returns ints summing to exactly
    `total` (or all zero if total <= 0)."""
    if total <= 0 or not weights:
        return [0] * len(weights)
    n = len(weights)
    floor_total = min_each * n
    if floor_total >= total:
        # Not enough budget to give everyone the floor: fall back to plain proportional split.
        min_each = 0
        floor_total = 0
    remaining = total - floor_total
    wsum = sum(weights)
    if wsum <= 0:
        weights = [1] * n
        wsum = n
    raw = [remaining * w / wsum for w in weights]
    floors = [math.floor(x) for x in raw]
    remainder = remaining - sum(floors)
    order = sorted(range(n), key=lambda i: raw[i] - floors[i], reverse=True)
    for i in range(remainder):
        floors[order[i % len(order)]] += 1
    return [f + min_each for f in floors]


def build_spec(profile, commit_stats, args):
    weights_by_date = build_update_weights(profile, commit_stats)
    bucket_weights = build_buckets(weights_by_date, args.dense_count, args.window_cap)
    n = len(bucket_weights)

    records_per_partition = math.floor(
        (args.partition_size_gb * 1024**3) / args.record_size_bytes
    )
    end_date = date.today()
    start_date = end_date - timedelta(days=n - 1)
    partition_dates = [
        (start_date + timedelta(days=i)).strftime(DATE_FMT_DASH) for i in range(n)
    ]
    total_weight = sum(bucket_weights)
    normalized_weights = (
        [w / total_weight for w in bucket_weights]
        if total_weight > 0
        else [1.0 / n] * n
    )
    date_to_weight = dict(zip(partition_dates, normalized_weights))

    bootstrap = {
        "startDate": partition_dates[0],
        "endDate": partition_dates[-1],
        "totalRecords": n * records_per_partition,
    }

    touched_per_commit = profile["partitionsTouchedPerCommit"]
    updates_per_commit = profile["updatesPerCommit"]
    inserts_per_commit = profile["insertsPerCommit"]
    update_ratio = profile["updateRatio"]
    commit_count = profile["commitCount"]
    assert len(touched_per_commit) == commit_count
    assert len(updates_per_commit) == commit_count
    assert len(inserts_per_commit) == commit_count

    touch_counts = scale_to_range(touched_per_commit, args.min_touch, args.max_touch)
    median_updates = statistics.median(updates_per_commit)
    baseline_updates = max(1, round(update_ratio * records_per_partition))

    # Total insert volume across the run, capped so the table doesn't blow past its fixed
    # bootstrap size: reuse the *same relative growth ratio* the real table saw over its
    # 20-commit window (total inserts / estimated total records), applied to our bootstrap
    # size instead of an arbitrary absolute number. Shaped per-commit by insertsPerCommit, and
    # landed only on the most-recently-active partitions (mimicking late-arriving data for
    # recent days) rather than spread across all 99 -- no new partitions are opened.
    total_inserts_original = sum(inserts_per_commit)
    growth_ratio = total_inserts_original / profile["estimatedTotalRecords"]
    total_insert_budget = round(growth_ratio * bootstrap["totalRecords"])

    insert_target_count = min(args.insert_target_count, n)
    insert_targets = partition_dates[-insert_target_count:]
    insert_target_weights = normalized_weights[-insert_target_count:]

    rng = random.Random(args.seed)
    commits = []
    for i in range(commit_count):
        touch_count = min(touch_counts[i], n)
        selected_dates = weighted_sample_without_replacement(
            partition_dates, normalized_weights, touch_count, rng
        )
        selected_weights = [date_to_weight[d] for d in selected_dates]

        intensity = updates_per_commit[i] / median_updates if median_updates else 1.0
        total_update_budget = max(touch_count, round(baseline_updates * intensity * touch_count))
        update_counts = distribute_proportionally(total_update_budget, selected_weights, min_each=1)

        insert_budget_i = round(total_insert_budget * inserts_per_commit[i] / total_inserts_original)
        insert_counts = distribute_proportionally(insert_budget_i, insert_target_weights)

        ops = {}
        for d, cnt in zip(selected_dates, update_counts):
            if cnt > 0:
                ops.setdefault(d, {})["updates"] = cnt
        for d, cnt in zip(insert_targets, insert_counts):
            if cnt > 0:
                ops.setdefault(d, {})["inserts"] = cnt
        commits.append(ops)

    spec = {"bootstrap": bootstrap, "commits": commits}
    summary = {
        "bootstrapPartitions": n,
        "recordsPerPartition": records_per_partition,
        "bootstrapTotalRecords": bootstrap["totalRecords"],
        "dateRange": [bootstrap["startDate"], bootstrap["endDate"]],
        "growthRatio": growth_ratio,
        "totalInsertBudget": total_insert_budget,
        "commitTouchCounts": [len(c) for c in commits],
        "commitTotalUpdates": [
            sum(v.get("updates", 0) for v in c.values()) for c in commits
        ],
        "commitTotalInserts": [
            sum(v.get("inserts", 0) for v in c.values()) for c in commits
        ],
    }
    return spec, summary


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--profile-dir", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--partition-size-gb", type=float, default=1.0)
    p.add_argument("--record-size-bytes", type=int, default=6144)
    p.add_argument("--dense-count", type=int, default=14)
    p.add_argument("--window-cap", type=int, default=8)
    p.add_argument("--min-touch", type=int, default=20)
    p.add_argument("--max-touch", type=int, default=30)
    p.add_argument(
        "--insert-target-count",
        type=int,
        default=5,
        help="Number of most-recently-active bootstrap partitions that receive inserts each "
        "commit (mimics late-arriving data for recent days without opening new partitions).",
    )
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--max-commits",
        type=int,
        default=None,
        help="Truncate to only the first N commits (e.g. for a quick --number-rounds-less "
        "smoke test: totalRounds = 1 bootstrap + N). Bootstrap is unaffected. Default: keep "
        "all commits from the source profile.",
    )
    return p.parse_args()


def main():
    args = parse_args()
    profile, commit_stats = load_profile(args.profile_dir)
    spec, summary = build_spec(profile, commit_stats, args)

    if args.max_commits is not None:
        spec["commits"] = spec["commits"][: args.max_commits]
        for key in ("commitTouchCounts", "commitTotalUpdates", "commitTotalInserts"):
            summary[key] = summary[key][: args.max_commits]

    with open(args.output, "w") as f:
        json.dump(spec, f, indent=2)

    print(f"Wrote workload spec to {args.output}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
