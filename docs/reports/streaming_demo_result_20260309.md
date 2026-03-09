# Streaming Funnel And Anomaly Demo

- Generated at (UTC): 2026-03-09T05:15:54.625086+00:00
- Topic: `clickstream.events`
- Query name: `clickstream_demo`

## Scenario summary

- Generated events: `27`
- Generated sessions: `10`
- Event mix: `{"add_to_cart": 6, "page_view": 10, "product_view": 10, "purchase": 1}`

## Timing

- Publish seconds: `0.1729`
- Processing seconds: `8.7496`
- End-to-end seconds: `8.9225`
- Events / sec: `3.0261`

## Z-score baseline

- Lookback windows: `12`
- Mean total sessions: `4`
- Stddev total sessions: `0.7071`
- Recent hour session series: `[3, 4, 5, 4, 3, 4, 5, 4, 3, 4, 5, 4]`

## Alerts

- Severity: `high`
- Alert types: `['traffic_spike', 'low_purchase_conversion', 'cart_abandonment_risk']`
- Traffic z-score: `8.4854`

## Report files

- JSON: `docs/reports/streaming_demo_result_20260309.json`
- Markdown: `docs/reports/streaming_demo_result_20260309.md`
