from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.dashboard.data_loader import load_dashboard_snapshot

st.set_page_config(page_title="StreamPipe", page_icon="⚡", layout="wide")


@st.cache_data(show_spinner=False)
def get_dashboard_snapshot() -> dict:
    return load_dashboard_snapshot()


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        :root {
            --canvas: #f4efe6;
            --card: rgba(255, 251, 244, 0.88);
            --ink: #1b2a41;
            --muted: #6c757d;
            --accent: #b85c38;
            --accent-soft: #f2c49b;
            --line: rgba(27, 42, 65, 0.12);
        }
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(184, 92, 56, 0.12), transparent 35%),
                radial-gradient(circle at top right, rgba(27, 42, 65, 0.08), transparent 30%),
                linear-gradient(180deg, #f8f4ec 0%, var(--canvas) 100%);
        }
        h1, h2, h3 {
            font-family: Georgia, "Times New Roman", serif;
            color: var(--ink);
            letter-spacing: -0.02em;
        }
        .stCaption, .stMarkdown, .stMetricLabel, .stMetricValue {
            color: var(--ink);
        }
        div[data-testid="stMetric"] {
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 18px;
            padding: 14px 16px;
            box-shadow: 0 10px 30px rgba(27, 42, 65, 0.05);
        }
        div[data-testid="stTabs"] button {
            font-weight: 700;
        }
        .source-note {
            padding: 12px 14px;
            border-radius: 14px;
            background: rgba(255, 255, 255, 0.55);
            border: 1px solid var(--line);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def metric_value(value: object, *, digits: int = 1, suffix: str = "") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    if isinstance(value, int):
        return f"{value:,}{suffix}"
    if isinstance(value, float):
        return f"{value:,.{digits}f}{suffix}"
    return f"{value}{suffix}"


def first_row_value(frame: pd.DataFrame, column: str) -> object:
    if frame.empty or column not in frame.columns:
        return None
    return frame.iloc[0][column]


def build_hourly_chart(hourly_df: pd.DataFrame):
    if hourly_df.empty or not {"pickup_date", "pickup_hour", "trip_count"}.issubset(hourly_df.columns):
        return None

    chart_df = hourly_df.copy()
    chart_df["pickup_date"] = pd.to_datetime(chart_df["pickup_date"])
    chart_df["hour_bucket"] = chart_df["pickup_date"] + pd.to_timedelta(chart_df["pickup_hour"], unit="h")
    fig = px.line(
        chart_df.sort_values("hour_bucket"),
        x="hour_bucket",
        y="trip_count",
        title="Hourly pickup volume",
        markers=True,
        color_discrete_sequence=["#b85c38"],
    )
    fig.update_layout(margin=dict(l=20, r=20, t=60, b=20), height=360)
    return fig


def build_ranked_bar_chart(
    dataframe: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    color: str,
    limit: int = 10,
):
    if dataframe.empty or x not in dataframe.columns or y not in dataframe.columns:
        return None

    chart_df = dataframe.sort_values(y, ascending=False).head(limit)
    fig = px.bar(
        chart_df,
        x=x,
        y=y,
        title=title,
        color_discrete_sequence=[color],
    )
    fig.update_layout(margin=dict(l=20, r=20, t=60, b=20), height=360)
    return fig


def build_event_mix_chart(streaming_report: dict):
    event_metrics = pd.DataFrame(streaming_report.get("event_metrics", []))
    if event_metrics.empty or not {"event_type", "event_count"}.issubset(event_metrics.columns):
        return None
    if "window_end" in event_metrics.columns:
        latest_window_end = event_metrics["window_end"].astype(str).max()
        event_metrics = event_metrics[event_metrics["window_end"].astype(str) == latest_window_end]
    event_metrics = event_metrics.groupby("event_type", as_index=False)["event_count"].sum()
    fig = px.bar(
        event_metrics.sort_values("event_type"),
        x="event_type",
        y="event_count",
        title="Streaming event mix",
        color="event_type",
        color_discrete_sequence=["#1b2a41", "#b85c38", "#d9a066", "#4f6d7a"],
    )
    fig.update_layout(showlegend=False, margin=dict(l=20, r=20, t=60, b=20), height=320)
    return fig


def build_funnel_chart(streaming_report: dict):
    funnel_rows = streaming_report.get("funnel_metrics", [])
    if not funnel_rows:
        return None

    funnel = sorted(funnel_rows, key=lambda row: str(row.get("window_end") or row.get("window_start") or ""))[-1]
    chart_df = pd.DataFrame(
        [
            {"stage": "page_view", "sessions": funnel.get("page_view_sessions", 0)},
            {"stage": "product_view", "sessions": funnel.get("product_view_sessions", 0)},
            {"stage": "add_to_cart", "sessions": funnel.get("add_to_cart_sessions", 0)},
            {"stage": "purchase", "sessions": funnel.get("purchase_sessions", 0)},
        ]
    )
    fig = px.funnel(chart_df, x="sessions", y="stage", title="Session funnel", color_discrete_sequence=["#b85c38"])
    fig.update_layout(margin=dict(l=20, r=20, t=60, b=20), height=320)
    return fig


def build_pandas_vs_spark_chart(report: dict):
    results = pd.DataFrame(report.get("results", []))
    if results.empty or not {"engine", "row_limit_label", "duration_seconds"}.issubset(results.columns):
        return None

    fig = px.bar(
        results,
        x="row_limit_label",
        y="duration_seconds",
        color="engine",
        barmode="group",
        title="pandas vs PySpark runtime",
        color_discrete_map={"pandas": "#b85c38", "pyspark": "#1b2a41"},
    )
    fig.update_layout(margin=dict(l=20, r=20, t=60, b=20), height=360)
    return fig


def build_batch_vs_streaming_chart(report: dict):
    comparison_rows = pd.DataFrame(report.get("comparison_rows", []))
    if comparison_rows.empty or not {"variant", "latency_seconds", "pipeline"}.issubset(comparison_rows.columns):
        return None

    fig = px.bar(
        comparison_rows,
        x="variant",
        y="latency_seconds",
        color="pipeline",
        barmode="group",
        title="Freshness latency snapshot",
        color_discrete_map={"batch": "#1b2a41", "streaming": "#b85c38"},
    )
    fig.update_layout(margin=dict(l=20, r=20, t=60, b=20), height=360)
    return fig


def build_partitioning_effect_chart(report: dict):
    comparison_rows = pd.DataFrame(report.get("comparison_rows", []))
    if comparison_rows.empty or not {"layout", "avg_seconds"}.issubset(comparison_rows.columns):
        return None

    fig = px.bar(
        comparison_rows,
        x="layout",
        y="avg_seconds",
        color="layout",
        title="Partition pruning benchmark",
        color_discrete_sequence=["#1b2a41", "#b85c38", "#d9a066"],
    )
    fig.update_layout(showlegend=False, margin=dict(l=20, r=20, t=60, b=20), height=360)
    return fig


def render_architecture_diagram() -> None:
    st.markdown(
        """
        <div style="padding: 18px; border: 1px solid rgba(27,42,65,0.12); border-radius: 18px; background: rgba(255,255,255,0.55);">
            <div style="font-weight: 700; margin-bottom: 14px; color: #1b2a41;">Architecture Overview</div>
            <div style="display: flex; flex-wrap: wrap; align-items: center; gap: 10px; line-height: 1.2;">
                <div style="padding: 12px 14px; border-radius: 14px; background: #fff7ef; border: 1px solid rgba(184,92,56,0.18); min-width: 120px;">
                    <strong>Sources</strong><br/>NYC Taxi + Clickstream
                </div>
                <div style="font-size: 24px; color: #b85c38;">→</div>
                <div style="padding: 12px 14px; border-radius: 14px; background: #ffffff; border: 1px solid rgba(27,42,65,0.12); min-width: 120px;">
                    <strong>Kafka</strong><br/>Event transport
                </div>
                <div style="font-size: 24px; color: #b85c38;">→</div>
                <div style="padding: 12px 14px; border-radius: 14px; background: #ffffff; border: 1px solid rgba(27,42,65,0.12); min-width: 140px;">
                    <strong>Spark</strong><br/>Batch + Structured Streaming
                </div>
                <div style="font-size: 24px; color: #b85c38;">→</div>
                <div style="padding: 12px 14px; border-radius: 14px; background: #fff7ef; border: 1px solid rgba(184,92,56,0.18); min-width: 120px;">
                    <strong>Delta / Parquet</strong><br/>Metrics + quality
                </div>
                <div style="font-size: 24px; color: #b85c38;">→</div>
                <div style="padding: 12px 14px; border-radius: 14px; background: #ffffff; border: 1px solid rgba(27,42,65,0.12); min-width: 140px;">
                    <strong>Dashboard</strong><br/>Streamlit + Plotly
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_source_notes(snapshot: dict) -> None:
    st.markdown(
        f"""
        <div class="source-note">
            <strong>Connected outputs</strong><br/>
            Batch benchmark: {snapshot.get('pandas_vs_spark_path') or '-'}<br/>
            Streaming demo: {snapshot.get('streaming_demo_path') or '-'}<br/>
            Batch vs Streaming: {snapshot.get('batch_vs_streaming_path') or 'generated in-app from available reports'}<br/>
            Partitioning effect: {snapshot.get('partitioning_report_path') or '-'}
        </div>
        """,
        unsafe_allow_html=True,
    )


apply_theme()
snapshot = get_dashboard_snapshot()
batch_tables = snapshot["batch_tables"]
summary_df = batch_tables["summary"]
hourly_df = batch_tables["hourly"]
borough_df = batch_tables["borough_summary"]
zone_df = batch_tables["zone_summary"]
payment_df = batch_tables["payment_tip_summary"]
quality_df = batch_tables["quality_summary"]
quality_issue_df = batch_tables["quality_issue_rows"]
streaming_report = snapshot["streaming_demo_report"] or {}
pandas_vs_spark_report = snapshot["pandas_vs_spark_report"] or {}
batch_vs_streaming_report = snapshot["batch_vs_streaming_report"] or {}
partitioning_report = snapshot["partitioning_report"] or {}

st.title("StreamPipe")
st.caption("Batch and streaming analytics outputs wired into one inspection dashboard.")

metric_columns = st.columns(4)
metric_columns[0].metric("Batch rows", metric_value(first_row_value(summary_df, "row_count"), digits=0))
metric_columns[1].metric("Total revenue", metric_value(first_row_value(summary_df, "sum_total_amount"), suffix=" USD"))
metric_columns[2].metric(
    "Streaming events",
    metric_value((streaming_report.get("scenario_summary") or {}).get("generated_event_count"), digits=0),
)
metric_columns[3].metric(
    "Streaming alerts",
    metric_value(sum((row.get("alert_count") or 0) for row in streaming_report.get("anomaly_alerts", [])), digits=0),
)

overview_tab, batch_tab, streaming_tab, benchmark_tab, quality_tab = st.tabs(
    ["Overview", "Batch", "Streaming", "Benchmark", "Quality"]
)

with overview_tab:
    render_architecture_diagram()
    st.caption("Source → Kafka → Spark → Delta/Parquet → Streamlit flow used in the current project layout.")

    left, right = st.columns([1.3, 1.0])
    with left:
        st.subheader("Pipeline snapshot")
        overview_rows = pd.DataFrame(
            [
                {
                    "section": "Batch summary",
                    "status": "ready" if not summary_df.empty else "missing",
                    "path": batch_tables["paths"].get("summary"),
                },
                {
                    "section": "Streaming demo",
                    "status": "ready" if streaming_report else "missing",
                    "path": snapshot.get("streaming_demo_path"),
                },
                {
                    "section": "pandas vs PySpark",
                    "status": "ready" if pandas_vs_spark_report else "missing",
                    "path": snapshot.get("pandas_vs_spark_path"),
                },
                {
                    "section": "Batch vs Streaming",
                    "status": "ready" if batch_vs_streaming_report else "missing",
                    "path": snapshot.get("batch_vs_streaming_path") or "generated at runtime",
                },
                {
                    "section": "Partitioning effect",
                    "status": "ready" if partitioning_report else "missing",
                    "path": snapshot.get("partitioning_report_path"),
                },
            ]
        )
        st.dataframe(overview_rows, width="stretch", hide_index=True)

        if not summary_df.empty:
            summary_view = summary_df.T.reset_index()
            summary_view.columns = ["metric", "value"]
            summary_view["value"] = summary_view["value"].astype(str)
            st.subheader("Batch headline metrics")
            st.dataframe(summary_view, width="stretch", hide_index=True)

    with right:
        render_source_notes(snapshot)
        streaming_timing = streaming_report.get("timing") or {}
        batch_comparison_rows = pd.DataFrame(batch_vs_streaming_report.get("comparison_rows", []))
        st.subheader("Decision cues")
        st.write(
            "Batch latency here means time-to-final-output for a bounded workload, while streaming latency tracks how fast one event burst is materialized into sink results."
        )
        if streaming_timing:
            st.metric("Streaming end-to-end", metric_value(streaming_timing.get("end_to_end_seconds"), suffix=" s"))
        if not batch_comparison_rows.empty:
            best_batch = batch_comparison_rows[batch_comparison_rows["pipeline"] == "batch"].sort_values(
                "latency_seconds", ascending=True
            )
            if not best_batch.empty:
                st.metric("Fastest batch snapshot", metric_value(best_batch.iloc[0]["latency_seconds"], suffix=" s"))

with batch_tab:
    st.subheader("Batch analytics")
    top_left, top_right = st.columns(2)
    with top_left:
        hourly_chart = build_hourly_chart(hourly_df)
        if hourly_chart is not None:
            st.plotly_chart(hourly_chart, width="stretch")
        else:
            st.info("Hourly batch output not found.")

    with top_right:
        borough_chart = build_ranked_bar_chart(
            borough_df,
            x="pickup_borough",
            y="sum_total_amount",
            title="Revenue by pickup borough",
            color="#1b2a41",
        )
        if borough_chart is not None:
            st.plotly_chart(borough_chart, width="stretch")
        else:
            st.info("Borough summary not found.")

    bottom_left, bottom_right = st.columns(2)
    with bottom_left:
        zone_chart = build_ranked_bar_chart(
            zone_df,
            x="pickup_zone",
            y="sum_total_amount",
            title="Top pickup zones by revenue",
            color="#d9a066",
        )
        if zone_chart is not None:
            st.plotly_chart(zone_chart, width="stretch")
        else:
            st.info("Zone summary not found.")

    with bottom_right:
        payment_chart = build_ranked_bar_chart(
            payment_df,
            x="payment_type_label",
            y="tip_to_fare_pct",
            title="Tip-to-fare ratio by payment type",
            color="#b85c38",
        )
        if payment_chart is not None:
            st.plotly_chart(payment_chart, width="stretch")
        else:
            st.info("Payment tip summary not found.")

with streaming_tab:
    st.subheader("Streaming monitoring")
    if not streaming_report:
        st.warning("Streaming demo report not found.")
    else:
        timing = streaming_report.get("timing") or {}
        scenario = streaming_report.get("scenario_summary") or {}
        metrics = st.columns(4)
        metrics[0].metric("Generated events", metric_value(scenario.get("generated_event_count"), digits=0))
        metrics[1].metric("Generated sessions", metric_value(scenario.get("generated_session_count"), digits=0))
        metrics[2].metric("End-to-end", metric_value(timing.get("end_to_end_seconds"), suffix=" s"))
        metrics[3].metric("Events / sec", metric_value(timing.get("events_per_second")))

        event_col, funnel_col = st.columns(2)
        with event_col:
            event_chart = build_event_mix_chart(streaming_report)
            if event_chart is not None:
                st.plotly_chart(event_chart, width="stretch")

        with funnel_col:
            funnel_chart = build_funnel_chart(streaming_report)
            if funnel_chart is not None:
                st.plotly_chart(funnel_chart, width="stretch")

        anomaly_df = pd.DataFrame(streaming_report.get("anomaly_alerts", []))
        if not anomaly_df.empty:
            st.subheader("Anomaly alerts")
            st.dataframe(anomaly_df, width="stretch", hide_index=True)

        query_progress_df = pd.DataFrame(streaming_report.get("query_progress", []))
        if not query_progress_df.empty:
            st.subheader("Query progress")
            st.dataframe(query_progress_df, width="stretch", hide_index=True)

with benchmark_tab:
    st.subheader("Benchmark comparisons")
    benchmark_left, benchmark_right = st.columns(2)

    with benchmark_left:
        if pandas_vs_spark_report:
            pandas_vs_spark_chart = build_pandas_vs_spark_chart(pandas_vs_spark_report)
            if pandas_vs_spark_chart is not None:
                st.plotly_chart(pandas_vs_spark_chart, width="stretch")
            comparison_rows = pd.DataFrame(pandas_vs_spark_report.get("comparison_rows", []))
            if not comparison_rows.empty:
                st.dataframe(comparison_rows, width="stretch", hide_index=True)
        else:
            st.info("pandas vs PySpark benchmark report not found.")

    with benchmark_right:
        if batch_vs_streaming_report:
            batch_vs_streaming_chart = build_batch_vs_streaming_chart(batch_vs_streaming_report)
            if batch_vs_streaming_chart is not None:
                st.plotly_chart(batch_vs_streaming_chart, width="stretch")
            st.dataframe(
                pd.DataFrame(batch_vs_streaming_report.get("comparison_rows", [])),
                width="stretch",
                hide_index=True,
            )
            definitions_df = pd.DataFrame(batch_vs_streaming_report.get("metric_definitions", []))
            if not definitions_df.empty:
                st.caption("Metric definitions")
                st.dataframe(definitions_df, width="stretch", hide_index=True)
        else:
            st.info("Batch vs Streaming report not found.")

    st.subheader("Partitioning effect")
    if partitioning_report:
        partitioning_chart = build_partitioning_effect_chart(partitioning_report)
        partition_left, partition_right = st.columns([1.1, 0.9])
        with partition_left:
            if partitioning_chart is not None:
                st.plotly_chart(partitioning_chart, width="stretch")
        with partition_right:
            st.dataframe(
                pd.DataFrame(partitioning_report.get("comparison_rows", [])),
                width="stretch",
                hide_index=True,
            )
    else:
        st.info("Partitioning effect benchmark report not found.")

with quality_tab:
    st.subheader("Data quality")
    quality_left, quality_right = st.columns([0.9, 1.1])
    with quality_left:
        if quality_df.empty:
            st.info("Quality summary output not found.")
        else:
            st.dataframe(quality_df, width="stretch", hide_index=True)

    with quality_right:
        if quality_issue_df.empty:
            st.info("Quality issue sample not found.")
        else:
            st.dataframe(quality_issue_df, width="stretch", hide_index=True)
