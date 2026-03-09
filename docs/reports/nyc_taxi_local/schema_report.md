# NYC Taxi Schema Report

- Generated at (UTC): 2026-03-08T13:02:54.386673+00:00
- Input path: `data/raw/nyc_taxi`
- Dataset variant: `yellow`
- Pickup timestamp column: `tpep_pickup_datetime`
- File count: `1`
- Total rows: `3`
- Total size (MB): `0.0`

## Recommended analysis columns

| Role | Column | Why |
|---|---|---|
| pickup_timestamp | `tpep_pickup_datetime` | 시간대별 승차 패턴 분석 기준 시각 |
| passenger_count | `passenger_count` | 수요 강도와 탑승 인원 분석 |
| trip_distance | `trip_distance` | 거리별 수익/수요 패턴 분석 |
| total_amount | `total_amount` | 총 매출 집계 |

## Schema

| Column | Type |
|---|---|
| `tpep_pickup_datetime` | `string` |
| `passenger_count` | `int64` |
| `total_amount` | `double` |
| `trip_distance` | `double` |

## Files

| File | Rows | Size (MB) |
|---|---:|---:|
| `data/raw/nyc_taxi/sample.parquet` | 3 | 0.0 |
