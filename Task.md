# Task.md

## 프로젝트 요약
- **프로젝트명**: StreamPipe
- **목표**: NYC Taxi 대용량 배치 분석 + 클릭스트림 실시간 스트리밍 분석 + Batch vs Streaming 비교 + 이상 탐지 대시보드 구현
- **현재 상태**: Day 1~6 전체 완료 (2026-03-09)

## 지금까지 완료
- [x] 요구사항 문서 확인
- [x] 저장소 기본 구조 생성
- [x] 핵심 설정 파일 추가 (`README.md`, `AGENTS.md`, `docker-compose.yml`, `requirements.txt`, `.env.example`)
- [x] 기본 Streamlit 앱 골격 추가
- [x] `src/common/`, `src/batch/`, `src/streaming/`, `src/quality/`, `src/simulator/` 기본 구조 생성
- [x] `.venv` 환경 생성 및 requirements 설치 완료
- [x] 로컬 JDK 준비 및 PySpark 로컬 실행 가능 상태 확인
- [x] docker-compose / Makefile / README 실행 경로 정합화 완료
- [x] Kafka producer 옵션이 포함된 clickstream 시뮬레이터 확장 완료
- [x] Kafka / Batch / Streaming 실행 골격 코드 추가
- [x] Kafka producer / batch / streaming 실제 스모크 테스트 완료
- [x] NYC Taxi 공식 parquet 다운로드/스키마 리포트 CLI 추가
- [x] 실제 NYC Taxi January 2023 파일 다운로드 및 스키마 검증 완료
- [x] zone lookup join 기반 borough / zone batch 집계 추가
- [x] payment type 기준 tip ratio batch 집계 추가
- [x] 데이터 품질 요약 / issue row 산출 추가
- [x] Delta Lake 저장 경로 연결 및 실제 저장 검증 완료
- [x] pandas vs PySpark benchmark CLI / 리포트 추가
- [x] streaming funnel / anomaly alert 로직 추가
- [x] memory sink 기반 streaming demo / 검증 리포트 추가
- [x] Batch vs Streaming 비교 리포트 CLI / 기준 정의 추가
- [x] partitioning effect benchmark CLI / 리포트 추가
- [x] Streamlit 대시보드에 batch / streaming / benchmark / quality 탭 연결
- [x] Overview 탭 아키텍처 다이어그램 추가
- [x] Parquet → Delta migration 예제 CLI / 리포트 추가
- [x] Delta time travel / schema evolution demo CLI / 리포트 추가
- [x] DQ 규칙에 null / duplicate 보강
- [x] 아키텍처 문서 보강

## 최근 검증 결과 (2026-03-09)
- [x] compose config 검증 성공
- [x] `streampipe-kafka` healthy 확인
- [x] `streampipe-spark-master`, `streampipe-spark-worker` running 확인
- [x] `clickstream.events` 토픽 생성 성공
- [x] Kafka producer 실제 전송 성공 (offset 증가 확인)
- [x] 로컬 batch 실행 성공 (`data/processed/batch_hourly_pickups_local`)
- [x] compose Spark cluster batch 실행 성공 (`data/processed/batch_hourly_pickups_compose`)
- [x] streaming console sink 실제 소비/집계 출력 확인
- [x] `yellow_tripdata_2023-01.parquet` 다운로드 성공 (`data/raw/nyc_taxi/yellow/2023/`)
- [x] 실제 스키마 리포트 생성 성공 (`docs/reports/nyc_taxi_2023_yellow_jan/`)
- [x] 실제 NYC Taxi January 2023 데이터 batch smoke test 성공 (`row_count=3,066,766`)
- [x] borough / zone / payment tip ratio batch analytics 저장 성공 (`data/processed/batch_taxi_metrics_jan/`)
- [x] quality summary / quality issue rows 저장 성공
- [x] Delta Lake 저장 성공 (`data/processed/delta/batch_taxi_metrics_jan_delta/`)
- [x] pandas vs PySpark benchmark 리포트 생성 성공 (`data/benchmarks/pandas_vs_spark_jan2023.*`)
- [x] streaming funnel / anomaly demo 리포트 생성 성공 (`docs/reports/streaming_demo_result_*.json`)
- [x] Batch vs Streaming 비교 리포트 생성 성공 (`data/benchmarks/batch_vs_streaming_jan2023.*`)
- [x] Delta migration 예제 리포트 생성 성공 (`docs/reports/delta_migration_example.*`)
- [x] Delta time travel demo 리포트 생성 성공 (`docs/reports/delta_time_travel_demo.*`)
- [x] 최신 streaming demo 리포트 재생성 성공 (`docs/reports/streaming_demo_result_20260309.*`)
- [x] 최신 partitioning effect benchmark 리포트 생성 성공 (`data/benchmarks/partitioning_effect_demo.*`)
- [x] Delta demo 리포트에 OPTIMIZE / ZORDER 결과 반영 확인
- [x] Streamlit 앱 import 검증 성공 (`python -c "import app"`)
- [x] Streamlit headless health check 성공 (`http://127.0.0.1:8501/_stcore/health = ok`)
- [x] Streamlit AppTest 검증 성공 (`0 exceptions`, `0 warnings`)
- [x] streaming demo 기준 결과 확인 (`total_sessions=10`, alerts=`traffic_spike`,`low_purchase_conversion`,`cart_abandonment_risk`)
- [x] `.venv` 기준 테스트 50개 통과

## Day 1 - 환경 설정 + 데이터 준비
- [x] Docker Compose 기동 확인 (Spark / Kafka)
- [x] Kafka 토픽 생성 (`clickstream.events`)
- [x] PySpark 로컬 실행 확인
- [x] 클릭스트림 시뮬레이터를 Kafka Producer 옵션으로 확장
- [x] NYC Taxi 2023 데이터 다운로드 경로 정리
- [x] 스키마 확인 및 필요 컬럼 선정
- [x] 실제 원천 데이터 적재 자동화 스크립트 추가

## Day 2 - Batch 처리 파이프라인
- [x] `src/batch/` 에 배치 분석 엔트리포인트 추가
- [x] Parquet 저장 경로 연결 및 저장 검증
- [x] 시간대별 / 지역별 / 팁 비율 분석 구현
- [x] 데이터 품질 검사 로직 초안 작성
- [x] Delta Lake 저장 경로 연결
- [x] pandas vs PySpark 비교 실험 설계

## Day 3 - Streaming 파이프라인
- [x] `src/streaming/` 에 Spark Structured Streaming 엔트리포인트 추가
- [x] Kafka consumer + JSON 파싱 + watermark/window 집계 스타터 구현
- [x] streaming console sink 실제 실행 검증
- [x] 실시간 퍼널/트래픽 이상 탐지 로직 추가
- [x] Batch vs Streaming 지연시간/처리량 비교 기준 정의
- [x] memory sink 결과 활용 예시 추가

## Day 4 - Delta Lake + 데이터 품질
- [x] Parquet → Delta 마이그레이션 예제 작성
- [x] Time Travel / Schema Evolution 데모 추가
- [x] DQ 규칙(null, 범위, 중복) 구현
- [x] 아키텍처 문서 보강

## Day 5 - Streamlit 대시보드
- [x] Overview 탭 고도화
- [x] Batch 결과 시각화 연결
- [x] 실시간 모니터링 탭 연결
- [x] 성능 벤치마크 탭 연결
- [x] 데이터 품질 리포트 탭 연결

## Day 6 - 포트폴리오 정리
- [x] README 실행 가이드 최종화
- [x] 아키텍처 다이어그램 정리 (Mermaid + HTML dashboard)
- [x] GitHub 업로드용 결과 요약 작성 (README 벤치마크 테이블)
- [ ] Notion 포트폴리오 문안 초안 작성

## 메모
- 현재 compose 기준 Kafka 기본 포트는 `9092`
- 로컬 PySpark 실행은 `.jdk` + `.venv` 기준으로 검증 완료
- NYC Taxi 실제 적재 경로는 `data/raw/nyc_taxi/yellow/2023/`
- 스키마 리포트는 `docs/reports/nyc_taxi_2023_yellow_jan/` 에 생성됨
- Day 1 요구사항은 2026-03-08 기준 모두 완료
- Day 2 batch 결과 디렉터리 예시: `data/processed/batch_taxi_metrics_jan/`
- Delta 결과 디렉터리 예시: `data/processed/delta/batch_taxi_metrics_jan_delta/`
- DQ summary에서 `issue_rows=120,637`, `pickup_year_mismatch_rows=38` 검증 완료
- Delta migration report 경로: `docs/reports/delta_migration_example.json`, `docs/reports/delta_migration_example.md`
- Delta time travel demo report 경로: `docs/reports/delta_time_travel_demo.json`, `docs/reports/delta_time_travel_demo.md`
- Delta time travel demo 기준 결과: `version_0_rows=3`, `latest_rows=5`, `added_columns=channel`
- 최신 Delta demo에서 `optimization.executed=true`, `zorder_columns=event_date,channel` 확인
- benchmark report 경로: `data/benchmarks/pandas_vs_spark_jan2023.json`, `data/benchmarks/pandas_vs_spark_jan2023.md`
- benchmark 기준 결과: `full` 구간에서 pandas `5.6518s`, PySpark `6.6087s`
- Batch vs Streaming report 경로: `data/benchmarks/batch_vs_streaming_jan2023.json`, `data/benchmarks/batch_vs_streaming_jan2023.md`
- Batch vs Streaming 기준 정의:
  - batch latency = bounded aggregate 완료 시간
  - streaming latency = event burst → sink materialization 시간
  - throughput 비교는 `rows/s` 와 `events/s` 를 분리해서 해석
- streaming demo 리포트: `docs/reports/streaming_demo_result_*.json`, `docs/reports/streaming_demo_result_*.md`
- 최신 streaming demo 리포트: `docs/reports/streaming_demo_result_20260309.json`, `docs/reports/streaming_demo_result_20260309.md`
- streaming demo 기준 결과: `page_to_purchase_rate=10.0`, `cart_to_purchase_rate=16.6667`, `severity=high`
- partitioning effect report 경로: `data/benchmarks/partitioning_effect_demo.json`, `data/benchmarks/partitioning_effect_demo.md`
- Streamlit runtime 검증: health check `ok`, AppTest `0 exceptions / 0 warnings`
- `make` 명령이 없는 환경에서는 `README.md`의 직접 실행 명령 사용
