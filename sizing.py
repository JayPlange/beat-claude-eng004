#!/usr/bin/env python3
"""Capacity and cost model for the proposed pipeline.

Every input carries a source label. Change an input and re-run; nothing is
hard-coded downstream.

    python3 sizing.py              # base case
    python3 sizing.py --spike 20   # changed constraint: 20x spikes
    python3 sizing.py --kb 2       # changed constraint: 2 KB events
"""
import argparse
import math

HOURS_PER_MONTH = 730

INPUTS = {
    # name: (value, label, note)
    "events_per_day":        (50_000_000, "Assumed", "given in brief: ~50M events/day"),
    "spike_multiplier":      (10, "Assumed", "given in brief: 10x spikes"),
    "diurnal_peak_factor":   (3, "Assumed", "busiest hour vs daily mean; not in brief"),
    "avg_event_kb":          (1.0, "Assumed", "typical web event JSON incl. properties"),
    "compression_ratio":     (5, "Assumed", "JSON to Parquet/ClickHouse columnar"),
    "retention_months":      (13, "Assumed", "year-on-year comparison window"),
    "shard_records_per_s":   (1000, "Benchmarked", "Kinesis per-shard write limit (AWS docs)"),
    "shard_mb_per_s":        (1.0, "Benchmarked", "Kinesis per-shard write limit (AWS docs)"),
    "shard_headroom":        (1.3, "Assumed", "pre-scaled headroom above sized spike"),
    "collector_rps_per_task":(1500, "Assumed", "1 vCPU task; to be load-tested in week 2"),
    # Unit prices: AWS us-east-1 list prices as understood at time of writing. Re-verify in the
    # AWS Pricing Calculator before committing; they are the weakest inputs here.
    "usd_per_shard_hour":    (0.015, "Assumed", "Kinesis provisioned shard-hour"),
    "usd_per_m_put_units":   (0.014, "Assumed", "Kinesis PUT payload units"),
    "usd_per_task_hour":     (0.05, "Assumed", "Fargate 1 vCPU / 2 GB"),
    "usd_firehose_per_gb":   (0.047, "Assumed", "Firehose ingest + Parquet conversion"),
    "usd_s3_per_gb_month":   (0.023, "Assumed", "S3 Standard"),
    "usd_redis_month":       (600, "Assumed", "ElastiCache, 2 nodes, primary + replica"),
    "usd_olap_month":        (6000, "Assumed", "ClickHouse Cloud on AWS, upper end of plausible"),
    "usd_alb_month":         (300, "Assumed", "ALB + LCUs"),
    "usd_misc_month":        (3000, "Assumed", "NAT, cross-AZ transfer, CloudWatch, KMS, backups"),
    "budget_month":          (50_000, "Assumed", "given in brief: $50K/month ceiling"),
}


def model(i):
    v = {k: x[0] for k, x in i.items()}
    out = []
    mean_rps = v["events_per_day"] / 86_400
    peak_rps = mean_rps * v["diurnal_peak_factor"]
    spike_rps = peak_rps * v["spike_multiplier"]
    spike_mb = spike_rps * v["avg_event_kb"] / 1000
    shards = math.ceil(max(spike_rps / v["shard_records_per_s"], spike_mb / v["shard_mb_per_s"]) * v["shard_headroom"])
    normal_shards = math.ceil(max(peak_rps / v["shard_records_per_s"], peak_rps * v["avg_event_kb"] / 1000 / v["shard_mb_per_s"]) * v["shard_headroom"])
    collector_tasks_spike = math.ceil(spike_rps / v["collector_rps_per_task"])
    collector_tasks_normal = max(3, math.ceil(peak_rps / v["collector_rps_per_task"]))  # 3 = one per AZ
    gb_month = v["events_per_day"] * 30 * v["avg_event_kb"] / 1_000_000
    stored_gb = gb_month / v["compression_ratio"] * v["retention_months"]

    out += [("Mean events/s", mean_rps, "Estimated"), ("Busy-hour events/s", peak_rps, "Estimated"),
            ("Spike events/s (sized for)", spike_rps, "Estimated"), ("Spike MB/s", spike_mb, "Estimated"),
            ("Kinesis shards, normal", normal_shards, "Estimated"),
            ("Kinesis shards, always on (spike-sized)", shards, "Estimated"),
            ("Collector tasks, normal / spike", f"{collector_tasks_normal} / {collector_tasks_spike}", "Estimated"),
            ("Raw GB/month", gb_month, "Estimated"), ("Stored GB at full retention", stored_gb, "Estimated")]

    # Decision: keep spike-sized capacity ALWAYS on. Spikes are not always predictable, resharding
    # takes minutes, and the model shows always-on spike capacity costs little next to the ceiling.
    shard_cost = shards * HOURS_PER_MONTH * v["usd_per_shard_hour"]
    put_cost = v["events_per_day"] * 30 / 1e6 * v["usd_per_m_put_units"]
    # Collectors: spike-sized floor always on (autoscaling above it is a bonus, not the plan).
    collector_cost = collector_tasks_spike * HOURS_PER_MONTH * v["usd_per_task_hour"]
    consumer_cost = max(2, math.ceil(shards / 4)) * 2 * HOURS_PER_MONTH * v["usd_per_task_hour"]  # 2 vCPU tasks, 4 shards each
    firehose_cost = gb_month * v["usd_firehose_per_gb"]
    s3_cost = stored_gb * v["usd_s3_per_gb_month"]
    lines = [("Kinesis shards", shard_cost), ("Kinesis PUT units", put_cost), ("Collector (Fargate)", collector_cost),
             ("Stream consumers (Fargate)", consumer_cost), ("Firehose to S3 Parquet", firehose_cost),
             ("S3 storage at full retention", s3_cost), ("Redis (ElastiCache)", v["usd_redis_month"]),
             ("OLAP store (ClickHouse Cloud)", v["usd_olap_month"]), ("ALB", v["usd_alb_month"]),
             ("Misc: NAT, transfer, monitoring", v["usd_misc_month"])]
    total = sum(c for _, c in lines)
    return out, lines, total, v["budget_month"]


def fmt(x):
    return f"{x:,.0f}" if isinstance(x, (int, float)) else str(x)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spike", type=float)
    ap.add_argument("--kb", type=float)
    ap.add_argument("--events", type=float)
    a = ap.parse_args()
    i = dict(INPUTS)
    if a.spike:
        i["spike_multiplier"] = (a.spike, "Assumed", "changed constraint")
    if a.kb:
        i["avg_event_kb"] = (a.kb, "Assumed", "changed constraint")
    if a.events:
        i["events_per_day"] = (a.events, "Assumed", "changed constraint")
    sizing, lines, total, budget = model(i)

    print("## Inputs\n\n| Input | Value | Label | Note |\n|---|---|---|---|")
    for k, (val, label, note) in i.items():
        print(f"| {k} | {val:,} | [{label}] | {note} |")
    print("\n## Sizing\n\n| Quantity | Value | Label |\n|---|---|---|")
    for name, val, label in sizing:
        print(f"| {name} | {fmt(val)} | [{label}] |")
    print("\n## Monthly cost (USD)\n\n| Line | USD | Label |\n|---|---|---|")
    for name, c in lines:
        print(f"| {name} | {c:,.0f} | [Estimated] |")
    print(f"| **Total** | **{total:,.0f}** | [Estimated] |")
    print(f"| Budget ceiling | {budget:,.0f} | [Assumed] given in brief |")
    print(f"| Headroom | {budget - total:,.0f} ({(budget - total) / budget:.0%}) | [Estimated] |")


if __name__ == "__main__":
    main()
