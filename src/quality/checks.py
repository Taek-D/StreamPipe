from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

REQUIRED_EVENT_FIELDS = (
    'event_id',
    'user_id',
    'session_id',
    'event_type',
    'page',
    'timestamp',
)
TAXI_NEGATIVE_NUMERIC_FIELDS = (
    'fare_amount',
    'tip_amount',
    'total_amount',
    'trip_distance',
)
DEFAULT_MAX_PASSENGER_COUNT = 8
DEFAULT_MAX_TRIP_DISTANCE = 200.0
DEFAULT_MAX_TIP_PCT = 100.0
UNKNOWN_LOCATION_VALUES = {'', 'Unknown', 'N/A', None}
TAXI_PICKUP_TIMESTAMP_CANDIDATES = ('pickup_ts', 'tpep_pickup_datetime', 'pickup_datetime', 'lpep_pickup_datetime')
TAXI_DROPOFF_TIMESTAMP_CANDIDATES = ('tpep_dropoff_datetime', 'dropoff_datetime', 'lpep_dropoff_datetime')
TAXI_PICKUP_LOCATION_ID_CANDIDATES = ('PULocationID', 'PUlocationID')
TAXI_DROPOFF_LOCATION_ID_CANDIDATES = ('DOLocationID', 'DOlocationID')
REQUIRED_TAXI_FIELD_GROUPS = (
    TAXI_PICKUP_TIMESTAMP_CANDIDATES,
    ('passenger_count',),
    ('trip_distance',),
    ('fare_amount',),
    ('total_amount',),
)
DUPLICATE_TAXI_SIGNATURE_GROUPS = (
    ('VendorID', 'vendor_id'),
    TAXI_PICKUP_TIMESTAMP_CANDIDATES,
    TAXI_DROPOFF_TIMESTAMP_CANDIDATES,
    TAXI_PICKUP_LOCATION_ID_CANDIDATES,
    TAXI_DROPOFF_LOCATION_ID_CANDIDATES,
    ('passenger_count',),
    ('trip_distance',),
    ('fare_amount',),
    ('total_amount',),
)


def validate_required_fields(
    record: Mapping[str, object],
    required_fields: tuple[str, ...] = REQUIRED_EVENT_FIELDS,
) -> list[str]:
    return [field for field in required_fields if record.get(field) in (None, '')]


def detect_negative_values(
    record: Mapping[str, object],
    numeric_fields: tuple[str, ...] = ('fare_amount', 'tip_amount', 'total_amount'),
) -> dict[str, float]:
    issues: dict[str, float] = {}

    for field in numeric_fields:
        value = record.get(field)
        if isinstance(value, (int, float)) and value < 0:
            issues[field] = float(value)

    return issues


def build_quality_report(records: Iterable[Mapping[str, object]]) -> dict[str, object]:
    total_records = 0
    missing_required = 0
    negative_value_rows = 0

    for record in records:
        total_records += 1

        if validate_required_fields(record):
            missing_required += 1

        if detect_negative_values(record):
            negative_value_rows += 1

    return {
        'total_records': total_records,
        'missing_required_rows': missing_required,
        'negative_value_rows': negative_value_rows,
    }


def _to_float(value: object) -> float | None:
    if value in (None, ''):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: object) -> datetime | None:
    if value in (None, ''):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except ValueError:
        return None


def _resolve_first_present_value(record: Mapping[str, object], candidates: tuple[str, ...]) -> object | None:
    for candidate in candidates:
        if candidate in record:
            value = record.get(candidate)
            if value not in (None, ''):
                return value
            return None
    return None


def has_missing_taxi_required_fields(record: Mapping[str, object]) -> bool:
    return any(_resolve_first_present_value(record, candidates) in (None, '') for candidates in REQUIRED_TAXI_FIELD_GROUPS)


def _normalize_duplicate_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        return round(value, 6)
    return value


def build_taxi_duplicate_key(record: Mapping[str, object]) -> tuple[tuple[str, object], ...] | None:
    signature_parts: list[tuple[str, object]] = []

    for candidates in DUPLICATE_TAXI_SIGNATURE_GROUPS:
        for candidate in candidates:
            if candidate not in record:
                continue
            value = record.get(candidate)
            if value in (None, ''):
                continue
            signature_parts.append((candidate, _normalize_duplicate_value(value)))
            break

    if len(signature_parts) < 5:
        return None
    return tuple(signature_parts)


def resolve_taxi_duplicate_signature_columns(columns: Iterable[str]) -> list[str]:
    available = set(columns)
    signature_columns: list[str] = []

    for candidates in DUPLICATE_TAXI_SIGNATURE_GROUPS:
        for candidate in candidates:
            if candidate in available:
                signature_columns.append(candidate)
                break

    return signature_columns if len(signature_columns) >= 5 else []


def detect_taxi_record_issues(
    record: Mapping[str, object],
    *,
    pickup_year: int | None = None,
    max_passenger_count: int = DEFAULT_MAX_PASSENGER_COUNT,
    max_trip_distance: float = DEFAULT_MAX_TRIP_DISTANCE,
    max_tip_pct: float = DEFAULT_MAX_TIP_PCT,
    is_duplicate: bool = False,
) -> list[str]:
    issues: list[str] = []

    if has_missing_taxi_required_fields(record):
        issues.append('missing_required_fields')

    pickup_ts = _parse_datetime(_resolve_first_present_value(record, TAXI_PICKUP_TIMESTAMP_CANDIDATES))
    if pickup_ts is None:
        issues.append('missing_pickup_timestamp')
    elif pickup_year is not None and pickup_ts.year != pickup_year:
        issues.append('pickup_year_mismatch')

    negative_values = detect_negative_values(record, TAXI_NEGATIVE_NUMERIC_FIELDS)
    if negative_values:
        issues.append('negative_numeric_values')

    passenger_count = _to_float(record.get('passenger_count'))
    if passenger_count is not None and (passenger_count <= 0 or passenger_count > max_passenger_count):
        issues.append('invalid_passenger_count')

    trip_distance = _to_float(record.get('trip_distance'))
    if trip_distance is not None and trip_distance > max_trip_distance:
        issues.append('invalid_trip_distance')

    tip_pct = _to_float(record.get('tip_pct'))
    if tip_pct is not None and (tip_pct < 0 or tip_pct > max_tip_pct):
        issues.append('invalid_tip_pct')

    pickup_zone = record.get('pickup_zone')
    pickup_borough = record.get('pickup_borough')
    if pickup_zone in UNKNOWN_LOCATION_VALUES or pickup_borough in UNKNOWN_LOCATION_VALUES:
        issues.append('unresolved_pickup_zone')

    if is_duplicate:
        issues.append('duplicate_taxi_record')

    return issues


def build_taxi_quality_report(
    records: Iterable[Mapping[str, object]],
    *,
    pickup_year: int | None = None,
    max_passenger_count: int = DEFAULT_MAX_PASSENGER_COUNT,
    max_trip_distance: float = DEFAULT_MAX_TRIP_DISTANCE,
    max_tip_pct: float = DEFAULT_MAX_TIP_PCT,
) -> dict[str, Any]:
    records_list = list(records)
    total_records = 0
    issue_rows = 0
    issue_counter: Counter[str] = Counter()
    duplicate_counts: Counter[tuple[tuple[str, object], ...]] = Counter(
        key for key in (build_taxi_duplicate_key(record) for record in records_list) if key is not None
    )

    for record in records_list:
        total_records += 1
        duplicate_key = build_taxi_duplicate_key(record)
        issues = detect_taxi_record_issues(
            record,
            pickup_year=pickup_year,
            max_passenger_count=max_passenger_count,
            max_trip_distance=max_trip_distance,
            max_tip_pct=max_tip_pct,
            is_duplicate=duplicate_key is not None and duplicate_counts[duplicate_key] > 1,
        )
        if issues:
            issue_rows += 1
            issue_counter.update(issues)

    return {
        'total_records': total_records,
        'issue_rows': issue_rows,
        'clean_rows': total_records - issue_rows,
        'issue_types': dict(sorted(issue_counter.items())),
    }


def build_taxi_quality_flag_columns(
    dataframe: 'DataFrame',
    *,
    pickup_year: int | None = None,
    max_passenger_count: int = DEFAULT_MAX_PASSENGER_COUNT,
    max_trip_distance: float = DEFAULT_MAX_TRIP_DISTANCE,
    max_tip_pct: float = DEFAULT_MAX_TIP_PCT,
) -> dict[str, 'Column']:
    from functools import reduce
    from operator import and_, or_

    from pyspark.sql import functions as F
    from pyspark.sql import Window

    columns = set(dataframe.columns)
    flags: dict[str, Column] = {}

    flags['missing_pickup_timestamp'] = F.col('pickup_ts').isNull() if 'pickup_ts' in columns else F.lit(True)

    required_checks: list[Column] = []
    for field_name in ('pickup_ts', 'passenger_count', 'trip_distance', 'fare_amount', 'total_amount'):
        if field_name in columns:
            required_checks.append(F.col(field_name).isNull())
    if required_checks:
        flags['missing_required_fields'] = reduce(or_, required_checks)

    negative_checks = [F.col(field) < 0 for field in TAXI_NEGATIVE_NUMERIC_FIELDS if field in columns]
    if negative_checks:
        flags['negative_numeric_values'] = reduce(or_, negative_checks)

    if 'passenger_count' in columns:
        flags['invalid_passenger_count'] = (F.col('passenger_count') <= 0) | (
            F.col('passenger_count') > F.lit(float(max_passenger_count))
        )

    if 'trip_distance' in columns:
        flags['invalid_trip_distance'] = F.col('trip_distance') > F.lit(max_trip_distance)

    if 'tip_pct' in columns:
        flags['invalid_tip_pct'] = (F.col('tip_pct') < 0) | (F.col('tip_pct') > F.lit(max_tip_pct))

    if pickup_year is not None and 'pickup_ts' in columns:
        flags['pickup_year_mismatch'] = F.year(F.col('pickup_ts')) != F.lit(pickup_year)

    unresolved_checks: list[Column] = []
    if 'pickup_zone' in columns:
        unresolved_checks.append(F.col('pickup_zone').isin('Unknown', 'N/A', ''))
    if 'pickup_borough' in columns:
        unresolved_checks.append(F.col('pickup_borough').isin('Unknown', 'N/A', ''))
    if unresolved_checks:
        flags['unresolved_pickup_zone'] = reduce(or_, unresolved_checks)

    duplicate_signature_columns = resolve_taxi_duplicate_signature_columns(columns)
    if duplicate_signature_columns:
        partition_window = Window.partitionBy(*duplicate_signature_columns)
        non_null_checks = [F.col(column_name).isNotNull() for column_name in duplicate_signature_columns]
        flags['duplicate_taxi_record'] = reduce(and_, non_null_checks) & (F.count('*').over(partition_window) > F.lit(1))

    return flags


def append_taxi_quality_flags(
    dataframe: 'DataFrame',
    *,
    pickup_year: int | None = None,
    max_passenger_count: int = DEFAULT_MAX_PASSENGER_COUNT,
    max_trip_distance: float = DEFAULT_MAX_TRIP_DISTANCE,
    max_tip_pct: float = DEFAULT_MAX_TIP_PCT,
) -> 'DataFrame':
    from pyspark.sql import functions as F

    flags = build_taxi_quality_flag_columns(
        dataframe,
        pickup_year=pickup_year,
        max_passenger_count=max_passenger_count,
        max_trip_distance=max_trip_distance,
        max_tip_pct=max_tip_pct,
    )

    flagged = dataframe
    issue_name_expressions: list[Column] = []

    for issue_name, condition in flags.items():
        column_name = f'dq_{issue_name}'
        flagged = flagged.withColumn(column_name, condition)
        issue_name_expressions.append(F.when(F.col(column_name), F.lit(issue_name)))

    flagged = flagged.withColumn('_dq_issue_candidates', F.array(*issue_name_expressions))
    flagged = flagged.withColumn('dq_issue_names', F.expr('filter(_dq_issue_candidates, x -> x is not null)'))
    flagged = flagged.drop('_dq_issue_candidates')
    flagged = flagged.withColumn('dq_issue_count', F.size(F.col('dq_issue_names')))
    flagged = flagged.withColumn('dq_has_issue', F.col('dq_issue_count') > 0)
    return flagged


def compute_taxi_quality_summary(
    dataframe: 'DataFrame',
    *,
    pickup_year: int | None = None,
    max_passenger_count: int = DEFAULT_MAX_PASSENGER_COUNT,
    max_trip_distance: float = DEFAULT_MAX_TRIP_DISTANCE,
    max_tip_pct: float = DEFAULT_MAX_TIP_PCT,
) -> 'DataFrame':
    from pyspark.sql import functions as F

    flagged = dataframe
    if 'dq_has_issue' not in dataframe.columns:
        flagged = append_taxi_quality_flags(
            dataframe,
            pickup_year=pickup_year,
            max_passenger_count=max_passenger_count,
            max_trip_distance=max_trip_distance,
            max_tip_pct=max_tip_pct,
        )

    flag_names = [
        column_name.removeprefix('dq_')
        for column_name in flagged.columns
        if column_name.startswith('dq_') and column_name not in {'dq_issue_names', 'dq_issue_count', 'dq_has_issue'}
    ]

    aggregations: list[Column] = [
        F.count('*').alias('total_rows'),
        F.sum(F.col('dq_has_issue').cast('int')).alias('issue_rows'),
    ]
    for name in flag_names:
        aggregations.append(F.sum(F.col(f'dq_{name}').cast('int')).alias(f'{name}_rows'))

    summary = flagged.agg(*aggregations)
    return (
        summary.withColumn('clean_rows', F.col('total_rows') - F.col('issue_rows'))
        .withColumn(
            'issue_rate_pct',
            F.when(F.col('total_rows') > 0, (F.col('issue_rows') / F.col('total_rows')) * F.lit(100.0)),
        )
    )


def select_taxi_quality_issue_rows(dataframe: 'DataFrame') -> 'DataFrame':
    from pyspark.sql import functions as F

    if 'dq_has_issue' not in dataframe.columns:
        dataframe = append_taxi_quality_flags(dataframe)

    preferred_columns = [
        'pickup_ts',
        'pickup_date',
        'pickup_hour',
        'pickup_borough',
        'pickup_zone',
        'dropoff_borough',
        'dropoff_zone',
        'PULocationID',
        'DOLocationID',
        'payment_type',
        'payment_type_label',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'tip_amount',
        'total_amount',
        'tip_pct',
        'dq_issue_count',
        'dq_issue_names',
    ]
    selected_columns = [column_name for column_name in preferred_columns if column_name in dataframe.columns]
    return dataframe.filter(F.col('dq_has_issue')).select(*selected_columns).orderBy(F.desc('dq_issue_count'), 'pickup_ts')
