# NYC Taxi Schema Report

- Generated at (UTC): 2026-03-08T13:10:01.952949+00:00
- Input path: `data/raw/nyc_taxi/yellow/2023`
- Dataset variant: `yellow`
- Pickup timestamp column: `tpep_pickup_datetime`
- File count: `1`
- Total rows: `3066766`
- Total size (MB): `45.46`

## Recommended analysis columns

| Role | Column | Why |
|---|---|---|
| pickup_timestamp | `tpep_pickup_datetime` | 시간대별 승차 패턴 분석 기준 시각 |
| dropoff_timestamp | `tpep_dropoff_datetime` | 이동 시간 및 체류 시간 계산 |
| pickup_zone | `PULocationID` | 지역별 수요/매출 집계 |
| dropoff_zone | `DOLocationID` | 도착 지역 분석 및 이동 패턴 파악 |
| passenger_count | `passenger_count` | 수요 강도와 탑승 인원 분석 |
| trip_distance | `trip_distance` | 거리별 수익/수요 패턴 분석 |
| payment_type | `payment_type` | 결제 방식별 팁/매출 비교 |
| fare_amount | `fare_amount` | 기본 운임 분석 |
| tip_amount | `tip_amount` | 팁 비율 분석 |
| total_amount | `total_amount` | 총 매출 집계 |

## Schema

| Column | Type |
|---|---|
| `VendorID` | `int64` |
| `tpep_pickup_datetime` | `timestamp[us]` |
| `tpep_dropoff_datetime` | `timestamp[us]` |
| `passenger_count` | `double` |
| `trip_distance` | `double` |
| `RatecodeID` | `double` |
| `store_and_fwd_flag` | `string` |
| `PULocationID` | `int64` |
| `DOLocationID` | `int64` |
| `payment_type` | `int64` |
| `fare_amount` | `double` |
| `extra` | `double` |
| `mta_tax` | `double` |
| `tip_amount` | `double` |
| `tolls_amount` | `double` |
| `improvement_surcharge` | `double` |
| `total_amount` | `double` |
| `congestion_surcharge` | `double` |
| `airport_fee` | `double` |

## Files

| File | Rows | Size (MB) |
|---|---:|---:|
| `data/raw/nyc_taxi/yellow/2023/yellow_tripdata_2023-01.parquet` | 3066766 | 45.46 |
