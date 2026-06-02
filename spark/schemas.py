from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_date", StringType(), True),
    StructField("event_timestamp", TimestampType(), False),
    StructField("actor1_country", StringType(), True),
    StructField("actor2_country", StringType(), True),
    StructField("cameo_code", StringType(), True),
    StructField("cameo_root_code", StringType(), True),
    StructField("goldstein_score", DoubleType(), True),
    StructField("num_articles", IntegerType(), True),
    StructField("avg_tone", DoubleType(), True),
])

MARKET_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", LongType(), True),
])
