from src.streaming.main import AlertThresholds, build_query_name, calculate_rate, calculate_z_score, evaluate_anomaly_flags


def test_build_query_name_adds_suffix() -> None:
    assert build_query_name("clickstream_metrics", "events") == "clickstream_metrics_events"


def test_calculate_rate_returns_none_for_zero_denominator() -> None:
    assert calculate_rate(5, 0) is None
    assert calculate_rate(2, 8) == 25.0


def test_calculate_z_score_applies_stddev_floor() -> None:
    assert calculate_z_score(10, 4, 0, stddev_floor=2.0) == 3.0
    assert calculate_z_score(10, None, 1.0) is None


def test_evaluate_anomaly_flags_detects_zscore_spike_and_conversion_risk() -> None:
    thresholds = AlertThresholds(
        spike_threshold=10,
        drop_threshold=1,
        min_baseline_sessions=5,
        min_purchase_from_page_rate=12.0,
        min_purchase_from_cart_rate=30.0,
        traffic_zscore_threshold=2.0,
        traffic_baseline_std_floor=0.5,
    )

    alerts = evaluate_anomaly_flags(
        total_sessions=10,
        page_view_sessions=10,
        add_to_cart_sessions=6,
        purchase_sessions=1,
        thresholds=thresholds,
        baseline_mean=4.0,
        baseline_stddev=1.0,
    )

    assert alerts == [
        "traffic_spike",
        "low_purchase_conversion",
        "cart_abandonment_risk",
    ]


def test_evaluate_anomaly_flags_detects_low_traffic_only() -> None:
    thresholds = AlertThresholds(spike_threshold=20, drop_threshold=2, min_baseline_sessions=5)

    alerts = evaluate_anomaly_flags(
        total_sessions=2,
        page_view_sessions=2,
        add_to_cart_sessions=1,
        purchase_sessions=1,
        thresholds=thresholds,
    )

    assert alerts == ["traffic_drop"]


def test_evaluate_anomaly_flags_detects_zscore_drop() -> None:
    thresholds = AlertThresholds(traffic_zscore_threshold=2.0, traffic_baseline_std_floor=0.5)

    alerts = evaluate_anomaly_flags(
        total_sessions=1,
        page_view_sessions=1,
        add_to_cart_sessions=0,
        purchase_sessions=0,
        thresholds=thresholds,
        baseline_mean=6.0,
        baseline_stddev=2.0,
    )

    assert alerts == ["traffic_drop"]
