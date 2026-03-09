from src.dashboard.data_loader import normalize_streaming_report


def test_normalize_streaming_report_backfills_scenario_summary_and_timing() -> None:
    report = normalize_streaming_report(
        {
            "config": {"query_name": "clickstream_demo", "page_view_sessions": 10},
            "event_metrics": [
                {"event_type": "page_view", "event_count": 10, "window_end": "2026-03-09T00:05:00+00:00"},
                {"event_type": "purchase", "event_count": 1, "window_end": "2026-03-09T00:05:00+00:00"},
            ],
            "funnel_metrics": [
                {
                    "window_start": "2026-03-09T00:00:00+00:00",
                    "window_end": "2026-03-09T00:05:00+00:00",
                    "total_sessions": 10,
                    "total_users": 10,
                }
            ],
            "query_progress": [{"trigger_execution_ms": 1500}],
        }
    )

    assert report == {
        "config": {"query_name": "clickstream_demo", "page_view_sessions": 10},
        "event_metrics": [
            {"event_type": "page_view", "event_count": 10, "window_end": "2026-03-09T00:05:00+00:00"},
            {"event_type": "purchase", "event_count": 1, "window_end": "2026-03-09T00:05:00+00:00"},
        ],
        "funnel_metrics": [
            {
                "window_start": "2026-03-09T00:00:00+00:00",
                "window_end": "2026-03-09T00:05:00+00:00",
                "total_sessions": 10,
                "total_users": 10,
            }
        ],
        "query_progress": [{"trigger_execution_ms": 1500}],
        "scenario_summary": {
            "generated_event_count": 11,
            "generated_session_count": 10,
            "generated_user_count": 10,
            "event_type_counts": {"page_view": 10, "purchase": 1},
        },
        "timing": {
            "publish_seconds": None,
            "processing_seconds": 1.5,
            "end_to_end_seconds": 1.5,
            "events_per_second": 7.3333,
            "sessions_per_second": 6.6667,
        },
    }
