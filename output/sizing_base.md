## Inputs

| Input | Value | Label | Note |
|---|---|---|---|
| events_per_day | 50,000,000 | [Assumed] | given in brief: ~50M events/day |
| spike_multiplier | 10 | [Assumed] | given in brief: 10x spikes |
| diurnal_peak_factor | 3 | [Assumed] | busiest hour vs daily mean; not in brief |
| avg_event_kb | 1.0 | [Assumed] | typical web event JSON incl. properties |
| compression_ratio | 5 | [Assumed] | JSON to Parquet/ClickHouse columnar |
| retention_months | 13 | [Assumed] | year-on-year comparison window |
| shard_records_per_s | 1,000 | [Benchmarked] | Kinesis per-shard write limit (AWS docs) |
| shard_mb_per_s | 1.0 | [Benchmarked] | Kinesis per-shard write limit (AWS docs) |
| shard_headroom | 1.3 | [Assumed] | pre-scaled headroom above sized spike |
| collector_rps_per_task | 1,500 | [Assumed] | 1 vCPU task; to be load-tested in week 2 |
| usd_per_shard_hour | 0.015 | [Assumed] | Kinesis provisioned shard-hour |
| usd_per_m_put_units | 0.014 | [Assumed] | Kinesis PUT payload units |
| usd_per_task_hour | 0.05 | [Assumed] | Fargate 1 vCPU / 2 GB |
| usd_firehose_per_gb | 0.047 | [Assumed] | Firehose ingest + Parquet conversion |
| usd_s3_per_gb_month | 0.023 | [Assumed] | S3 Standard |
| usd_redis_month | 600 | [Assumed] | ElastiCache, 2 nodes, primary + replica |
| usd_olap_month | 6,000 | [Assumed] | ClickHouse Cloud on AWS, upper end of plausible |
| usd_alb_month | 300 | [Assumed] | ALB + LCUs |
| usd_misc_month | 3,000 | [Assumed] | NAT, cross-AZ transfer, CloudWatch, KMS, backups |
| budget_month | 50,000 | [Assumed] | given in brief: $50K/month ceiling |

## Sizing

| Quantity | Value | Label |
|---|---|---|
| Mean events/s | 579 | [Estimated] |
| Busy-hour events/s | 1,736 | [Estimated] |
| Spike events/s (sized for) | 17,361 | [Estimated] |
| Spike MB/s | 17 | [Estimated] |
| Kinesis shards, normal | 3 | [Estimated] |
| Kinesis shards, always on (spike-sized) | 23 | [Estimated] |
| Collector tasks, normal / spike | 3 / 12 | [Estimated] |
| Raw GB/month | 1,500 | [Estimated] |
| Stored GB at full retention | 3,900 | [Estimated] |

## Monthly cost (USD)

| Line | USD | Label |
|---|---|---|
| Kinesis shards | 252 | [Estimated] |
| Kinesis PUT units | 21 | [Estimated] |
| Collector (Fargate) | 438 | [Estimated] |
| Stream consumers (Fargate) | 438 | [Estimated] |
| Firehose to S3 Parquet | 70 | [Estimated] |
| S3 storage at full retention | 90 | [Estimated] |
| Redis (ElastiCache) | 600 | [Estimated] |
| OLAP store (ClickHouse Cloud) | 6,000 | [Estimated] |
| ALB | 300 | [Estimated] |
| Misc: NAT, transfer, monitoring | 3,000 | [Estimated] |
| **Total** | **11,209** | [Estimated] |
| Budget ceiling | 50,000 | [Assumed] given in brief |
| Headroom | 38,791 (78%) | [Estimated] |
