## Inputs

| Input | Value | Label | Note |
|---|---|---|---|
| events_per_day | 50,000,000 | [Assumed] | given in brief: ~50M events/day |
| spike_multiplier | 20.0 | [Assumed] | changed constraint |
| diurnal_peak_factor | 3 | [Assumed] | busiest hour vs daily mean; not in brief |
| avg_event_kb | 2.0 | [Assumed] | changed constraint |
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
| Spike events/s (sized for) | 34,722 | [Estimated] |
| Spike MB/s | 69 | [Estimated] |
| Kinesis shards, normal | 5 | [Estimated] |
| Kinesis shards, always on (spike-sized) | 91 | [Estimated] |
| Collector tasks, normal / spike | 3 / 24 | [Estimated] |
| Raw GB/month | 3,000 | [Estimated] |
| Stored GB at full retention | 7,800 | [Estimated] |

## Monthly cost (USD)

| Line | USD | Label |
|---|---|---|
| Kinesis shards | 996 | [Estimated] |
| Kinesis PUT units | 21 | [Estimated] |
| Collector (Fargate) | 876 | [Estimated] |
| Stream consumers (Fargate) | 1,679 | [Estimated] |
| Firehose to S3 Parquet | 141 | [Estimated] |
| S3 storage at full retention | 179 | [Estimated] |
| Redis (ElastiCache) | 600 | [Estimated] |
| OLAP store (ClickHouse Cloud) | 6,000 | [Estimated] |
| ALB | 300 | [Estimated] |
| Misc: NAT, transfer, monitoring | 3,000 | [Estimated] |
| **Total** | **13,793** | [Estimated] |
| Budget ceiling | 50,000 | [Assumed] given in brief |
| Headroom | 36,207 (72%) | [Estimated] |
