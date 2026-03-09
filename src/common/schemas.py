from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

CLICKSTREAM_EVENT_SCHEMA = StructType(
    [
        StructField('event_id', StringType(), False),
        StructField('user_id', StringType(), False),
        StructField('session_id', StringType(), False),
        StructField('event_type', StringType(), False),
        StructField('page', StringType(), False),
        StructField('product_id', StringType(), True),
        StructField('timestamp', StringType(), False),
    ]
)
