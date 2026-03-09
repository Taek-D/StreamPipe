# AGENTS.md

## 프로젝트 목적
- 이 저장소는 **StreamPipe**: PySpark + Kafka + Delta Lake + Streamlit 기반의 실시간 이벤트 로그 분석 파이프라인 프로젝트다.
- 최상위 요구사항의 기준 문서는 `## ⚡ 프로젝트 4 실시간 이벤트 로그 분석 파이프라인.md` 이다.
- 작업 우선순위와 진행 체크리스트는 `Task.md` 를 기준으로 관리한다.

## 작업 원칙
- 재사용 가능한 로직은 `src/` 아래에 둔다. 노트북은 실험/데모용으로만 사용한다.
- 배치 로직은 `src/batch/`, 스트리밍 로직은 `src/streaming/`, 데이터 품질 로직은 `src/quality/`, 시뮬레이터는 `src/simulator/`, 공통 유틸은 `src/common/` 에 둔다.
- 설정값은 `.env` 또는 `configs/settings.yaml` 에서 관리하고, 로컬 절대 경로를 코드에 하드코딩하지 않는다.
- 대용량 원천 데이터, 체크포인트, 실행 산출물은 `data/` 아래에 두고 Git에는 커밋하지 않는다.
- 폴더 구조나 실행 방법이 바뀌면 `README.md` 와 `Task.md` 를 함께 갱신한다.

## 기본 검증 명령
- `docker compose up -d`
- `make topic-create`
- `python -m pytest`
- `streamlit run app.py`

## 산출물 우선순위
1. Day 1: Docker + Kafka + Spark 환경, 클릭스트림 시뮬레이터
2. Day 2: NYC Taxi Batch 파이프라인
3. Day 3: Structured Streaming 분석
4. Day 4: Delta Lake + 데이터 품질
5. Day 5: Streamlit 대시보드
6. Day 6: README / GitHub / Notion 정리
