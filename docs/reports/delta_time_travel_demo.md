# Delta Time Travel And Schema Evolution Demo

- Generated at (UTC): 2026-03-09T05:17:59.769938+00:00
- Target path: `data/processed/delta/day4/time_travel_orders_demo`

## Version comparison

| Version | Rows | Columns |
|---|---:|---|
| 0 | 3 | amount, event_date, order_id, user_id |
| 3 | 5 | amount, event_date, order_id, user_id, channel |

## Schema evolution

- Added columns: `channel`
- Removed columns: `none`

## History

| Version | Operation | Timestamp |
|---|---|---|
| 0 | WRITE | 2026-03-09T14:17:48.293000 |
| 1 | WRITE | 2026-03-09T14:17:53.227000 |
| 2 | OPTIMIZE | 2026-03-09T14:17:55.628000 |
| 3 | OPTIMIZE | 2026-03-09T14:17:58.335000 |

## OPTIMIZE and ZORDER

- Supported: `True`
- Executed: `True`
- Skipped: `False`
- ZORDER columns: `event_date, channel`
