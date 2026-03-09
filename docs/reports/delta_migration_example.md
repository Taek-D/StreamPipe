# Delta Migration Example

- Generated at (UTC): 2026-03-09T03:27:12.542270+00:00
- Source path: `data/processed/batch_taxi_metrics_jan/summary`
- Target path: `data/processed/delta/day4/batch_summary_delta_example`
- Input rows: `1`
- Output rows: `1`
- Latest version: `0`
- Partition by: `none`

## History

| Version | Operation | Timestamp |
|---|---|---|
| 0 | WRITE | 2026-03-09T12:27:07.808000 |

## Schema

| Name | Type | Nullable |
|---|---|---|
| row_count | bigint | True |
| active_days | bigint | True |
| min_pickup_ts | timestamp | True |
| max_pickup_ts | timestamp | True |
| avg_passenger_count | double | True |
| avg_trip_distance | double | True |
| active_pickup_boroughs | bigint | True |
| avg_total_amount | double | True |
| sum_total_amount | double | True |
| sum_tip_amount | double | True |
| avg_tip_pct | double | True |
