from src.quality.checks import (
    build_taxi_duplicate_key,
    build_quality_report,
    build_taxi_quality_report,
    detect_negative_values,
    detect_taxi_record_issues,
    has_missing_taxi_required_fields,
    resolve_taxi_duplicate_signature_columns,
    validate_required_fields,
)


def test_validate_required_fields_returns_missing_keys() -> None:
    record = {
        'event_id': 'evt-1',
        'user_id': 'user-1',
        'session_id': '',
        'event_type': 'page_view',
        'page': 'home',
        'timestamp': None,
    }

    assert validate_required_fields(record) == ['session_id', 'timestamp']


def test_detect_negative_values_finds_negative_numbers() -> None:
    record = {'fare_amount': -10.5, 'tip_amount': 2.0, 'total_amount': -8.5}

    assert detect_negative_values(record) == {
        'fare_amount': -10.5,
        'total_amount': -8.5,
    }


def test_build_quality_report_aggregates_counts() -> None:
    report = build_quality_report(
        [
            {
                'event_id': 'evt-1',
                'user_id': 'user-1',
                'session_id': 'sess-1',
                'event_type': 'page_view',
                'page': 'home',
                'timestamp': '2026-03-08T00:00:00+00:00',
            },
            {
                'event_id': 'evt-2',
                'user_id': 'user-2',
                'session_id': '',
                'event_type': 'purchase',
                'page': 'checkout',
                'timestamp': '2026-03-08T00:00:01+00:00',
                'fare_amount': -1.0,
            },
        ]
    )

    assert report == {
        'total_records': 2,
        'missing_required_rows': 1,
        'negative_value_rows': 1,
    }


def test_detect_taxi_record_issues_finds_expected_flags() -> None:
    record = {
        'tpep_pickup_datetime': '2022-12-31T23:59:59+00:00',
        'passenger_count': 0,
        'trip_distance': 250.0,
        'fare_amount': -10.0,
        'tip_amount': 5.0,
        'total_amount': 12.0,
        'tip_pct': 150.0,
        'pickup_borough': 'Unknown',
        'pickup_zone': 'Unknown',
    }

    assert detect_taxi_record_issues(record, pickup_year=2023) == [
        'pickup_year_mismatch',
        'negative_numeric_values',
        'invalid_passenger_count',
        'invalid_trip_distance',
        'invalid_tip_pct',
        'unresolved_pickup_zone',
    ]


def test_build_taxi_quality_report_aggregates_issue_counts() -> None:
    report = build_taxi_quality_report(
        [
            {
                'tpep_pickup_datetime': '2023-01-01T00:00:00+00:00',
                'passenger_count': 1,
                'trip_distance': 3.2,
                'fare_amount': 10.0,
                'tip_amount': 2.0,
                'total_amount': 14.0,
                'tip_pct': 20.0,
                'pickup_borough': 'Manhattan',
                'pickup_zone': 'Midtown Center',
            },
            {
                'tpep_pickup_datetime': '2024-01-01T00:00:00+00:00',
                'passenger_count': 12,
                'trip_distance': 10.0,
                'fare_amount': 20.0,
                'tip_amount': 1.0,
                'total_amount': 25.0,
                'tip_pct': 5.0,
                'pickup_borough': 'Queens',
                'pickup_zone': 'JFK Airport',
            },
            {
                'tpep_pickup_datetime': None,
                'passenger_count': 1,
                'trip_distance': -1.0,
                'fare_amount': 10.0,
                'tip_amount': 0.0,
                'total_amount': 9.0,
                'tip_pct': 0.0,
                'pickup_borough': 'Unknown',
                'pickup_zone': 'Unknown',
            },
        ],
        pickup_year=2023,
    )

    assert report == {
        'total_records': 3,
        'issue_rows': 2,
        'clean_rows': 1,
        'issue_types': {
            'invalid_passenger_count': 1,
            'missing_required_fields': 1,
            'missing_pickup_timestamp': 1,
            'negative_numeric_values': 1,
            'pickup_year_mismatch': 1,
            'unresolved_pickup_zone': 1,
        },
    }


def test_has_missing_taxi_required_fields_detects_null_metrics() -> None:
    assert has_missing_taxi_required_fields(
        {
            'tpep_pickup_datetime': '2023-01-01T00:00:00+00:00',
            'passenger_count': 1,
            'trip_distance': 2.5,
            'fare_amount': None,
            'total_amount': 10.0,
        }
    )


def test_build_taxi_duplicate_key_uses_composite_signature() -> None:
    key = build_taxi_duplicate_key(
        {
            'VendorID': 1,
            'tpep_pickup_datetime': '2023-01-01T00:00:00+00:00',
            'tpep_dropoff_datetime': '2023-01-01T00:10:00+00:00',
            'PULocationID': 161,
            'DOLocationID': 236,
            'passenger_count': 1,
            'trip_distance': 3.2,
            'fare_amount': 12.5,
            'total_amount': 18.0,
        }
    )

    assert key == (
        ('VendorID', 1),
        ('tpep_pickup_datetime', '2023-01-01T00:00:00+00:00'),
        ('tpep_dropoff_datetime', '2023-01-01T00:10:00+00:00'),
        ('PULocationID', 161),
        ('DOLocationID', 236),
        ('passenger_count', 1),
        ('trip_distance', 3.2),
        ('fare_amount', 12.5),
        ('total_amount', 18.0),
    )


def test_build_taxi_quality_report_counts_duplicate_rows() -> None:
    records = [
        {
            'VendorID': 1,
            'tpep_pickup_datetime': '2023-01-01T00:00:00+00:00',
            'tpep_dropoff_datetime': '2023-01-01T00:10:00+00:00',
            'PULocationID': 161,
            'DOLocationID': 236,
            'passenger_count': 1,
            'trip_distance': 3.2,
            'fare_amount': 12.5,
            'tip_amount': 2.5,
            'total_amount': 18.0,
            'tip_pct': 20.0,
            'pickup_borough': 'Manhattan',
            'pickup_zone': 'Midtown Center',
        },
        {
            'VendorID': 1,
            'tpep_pickup_datetime': '2023-01-01T00:00:00+00:00',
            'tpep_dropoff_datetime': '2023-01-01T00:10:00+00:00',
            'PULocationID': 161,
            'DOLocationID': 236,
            'passenger_count': 1,
            'trip_distance': 3.2,
            'fare_amount': 12.5,
            'tip_amount': 2.5,
            'total_amount': 18.0,
            'tip_pct': 20.0,
            'pickup_borough': 'Manhattan',
            'pickup_zone': 'Midtown Center',
        },
        {
            'VendorID': 2,
            'tpep_pickup_datetime': '2023-01-01T01:00:00+00:00',
            'tpep_dropoff_datetime': '2023-01-01T01:20:00+00:00',
            'PULocationID': 48,
            'DOLocationID': 68,
            'passenger_count': 1,
            'trip_distance': 4.1,
            'fare_amount': 18.0,
            'tip_amount': 4.0,
            'total_amount': 24.0,
            'tip_pct': 22.2,
            'pickup_borough': 'Manhattan',
            'pickup_zone': 'Clinton East',
        },
    ]

    report = build_taxi_quality_report(records, pickup_year=2023)

    assert report == {
        'total_records': 3,
        'issue_rows': 2,
        'clean_rows': 1,
        'issue_types': {
            'duplicate_taxi_record': 2,
        },
    }


def test_resolve_taxi_duplicate_signature_columns_returns_expected_order() -> None:
    columns = [
        'VendorID',
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'total_amount',
    ]

    assert resolve_taxi_duplicate_signature_columns(columns) == columns
