import pytest

from src.streaming.demo import (
    build_baseline_session_series,
    build_demo_anomaly_rows,
    build_demo_event_payload,
    build_demo_scenario_events,
    build_timing_summary,
    summarize_demo_events,
    summarize_baseline_session_series,
)
from src.streaming.main import AlertThresholds


def test_build_demo_event_payload_sets_expected_fields() -> None:
    payload = build_demo_event_payload(
        session_number=3,
        event_type="add_to_cart",
        timestamp="2026-03-08T14:15:55+00:00",
        scenario_prefix="validation",
    )

    assert payload == {
        "event_id": "validation-event-03-add_to_cart",
        "user_id": "validation-user-03",
        "session_id": "validation-session-03",
        "event_type": "add_to_cart",
        "page": "cart",
        "product_id": "SKU-0003",
        "timestamp": "2026-03-08T14:15:55+00:00",
    }


def test_build_demo_scenario_events_creates_expected_event_mix() -> None:
    events = build_demo_scenario_events(
        page_view_sessions=4,
        add_to_cart_sessions=2,
        purchase_sessions=1,
        timestamp="2026-03-08T14:15:55+00:00",
        scenario_prefix="validation",
    )

    assert len(events) == 11
    assert sum(event["event_type"] == "page_view" for event in events) == 4
    assert sum(event["event_type"] == "product_view" for event in events) == 4
    assert sum(event["event_type"] == "add_to_cart" for event in events) == 2
    assert sum(event["event_type"] == "purchase" for event in events) == 1


def test_summarize_demo_events_returns_counts() -> None:
    events = build_demo_scenario_events(
        page_view_sessions=3,
        add_to_cart_sessions=2,
        purchase_sessions=1,
        timestamp="2026-03-08T14:15:55+00:00",
        scenario_prefix="validation",
    )

    summary = summarize_demo_events(events)

    assert summary == {
        "generated_event_count": 9,
        "generated_session_count": 3,
        "generated_user_count": 3,
        "event_type_counts": {
            "add_to_cart": 2,
            "page_view": 3,
            "product_view": 3,
            "purchase": 1,
        },
    }


def test_build_timing_summary_calculates_end_to_end_rates() -> None:
    timing = build_timing_summary(
        event_count=27,
        session_count=10,
        publish_seconds=0.5,
        processing_seconds=1.5,
    )

    assert timing == {
        "publish_seconds": 0.5,
        "processing_seconds": 1.5,
        "end_to_end_seconds": 2.0,
        "events_per_second": 13.5,
        "sessions_per_second": 5.0,
    }


def test_build_baseline_session_series_creates_deterministic_recent_hour_pattern() -> None:
    baseline = build_baseline_session_series(baseline_total_sessions=4, lookback_windows=8)

    assert baseline == [3, 4, 5, 4, 3, 4, 5, 4]


def test_summarize_baseline_session_series_returns_mean_and_stddev() -> None:
    summary = summarize_baseline_session_series([3, 4, 5, 4])

    assert summary == {
        "lookback_window_count": 4,
        "recent_hour_total_sessions": [3, 4, 5, 4],
        "mean_total_sessions": 4,
        "stddev_total_sessions": 0.7071,
    }


def test_build_demo_anomaly_rows_uses_zscore_for_traffic_alerts() -> None:
    thresholds = AlertThresholds(
        min_baseline_sessions=5,
        min_purchase_from_page_rate=12.0,
        min_purchase_from_cart_rate=30.0,
        traffic_zscore_threshold=2.0,
    )

    anomaly_rows = build_demo_anomaly_rows(
        [
            {
                "window_start": "2026-03-09T00:00:00+00:00",
                "window_end": "2026-03-09T00:05:00+00:00",
                "total_sessions": 10,
                "page_view_sessions": 10,
                "add_to_cart_sessions": 6,
                "purchase_sessions": 1,
                "page_to_purchase_rate": 10.0,
                "cart_to_purchase_rate": 16.6667,
            }
        ],
        thresholds=thresholds,
        baseline_sessions=[3, 4, 5, 4, 3, 4, 5, 4, 3, 4, 5, 4],
    )

    assert anomaly_rows == [
        {
            "window_start": "2026-03-09T00:00:00+00:00",
            "window_end": "2026-03-09T00:05:00+00:00",
            "total_sessions": 10,
            "page_view_sessions": 10,
            "add_to_cart_sessions": 6,
            "purchase_sessions": 1,
            "page_to_purchase_rate": 10.0,
            "cart_to_purchase_rate": 16.6667,
            "baseline_window_count": 12,
            "baseline_session_mean": 4,
            "baseline_session_stddev": 0.7071,
            "traffic_z_score": 8.4854,
            "alert_types": ["traffic_spike", "low_purchase_conversion", "cart_abandonment_risk"],
            "alert_count": 3,
            "severity": "high",
            "anomaly_model": "zscore_vs_recent_hour",
        }
    ]


@pytest.mark.parametrize(
    ("page_view_sessions", "add_to_cart_sessions", "purchase_sessions"),
    [
        (0, 0, 0),
        (3, 4, 1),
        (3, 2, 3),
    ],
)
def test_build_demo_scenario_events_validates_session_ranges(
    page_view_sessions: int,
    add_to_cart_sessions: int,
    purchase_sessions: int,
) -> None:
    with pytest.raises(ValueError):
        build_demo_scenario_events(
            page_view_sessions=page_view_sessions,
            add_to_cart_sessions=add_to_cart_sessions,
            purchase_sessions=purchase_sessions,
        )
