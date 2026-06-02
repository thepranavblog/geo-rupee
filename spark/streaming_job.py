import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import (
    EVENT_TOPIC,
    EVENT_WATERMARK_THRESHOLD,
    JOIN_WINDOW_DURATION,
    KAFKA_BOOTSTRAP_SERVERS,
    MARKET_TOPIC,
    MARKET_WATERMARK_THRESHOLD,
    RELEVANT_COUNTRIES,
    S3_BRONZE_GDELT_PATH,
    S3_BRONZE_MARKET_PATH,
    S3_CHECKPOINT_PATH,
    S3_SILVER_CORRELATED_PATH,
    TRIGGER_INTERVAL,
)
from schemas import EVENT_SCHEMA, MARKET_SCHEMA


class GeopoliticalMarketStreamJob:
    def __init__(self):
        self.spark_session = None
        self.kafka_bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.event_topic = EVENT_TOPIC
        self.market_topic = MARKET_TOPIC
        self.s3_output_path = S3_SILVER_CORRELATED_PATH
        self.checkpoint_path = S3_CHECKPOINT_PATH
        self.relevant_countries = RELEVANT_COUNTRIES

    def create_spark_session(self) -> SparkSession:
        self.spark_session = (
            SparkSession.builder
            .appName("GeopoliticalRupeeImpact")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4",
            )
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_path)
            .getOrCreate()
        )
        self.spark_session.sparkContext.setLogLevel("WARN")
        return self.spark_session

    def read_event_stream(self) -> DataFrame:
        raw = (
            self.spark_session.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", self.event_topic)
            .option("startingOffsets", "latest")
            .load()
        )
        parsed = (
            raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"))
            .select("data.*")
            .filter(
                F.col("actor1_country").isin(self.relevant_countries)
                | F.col("actor2_country").isin(self.relevant_countries)
            )
            .withWatermark("event_timestamp", EVENT_WATERMARK_THRESHOLD)
        )
        return parsed

    def read_market_stream(self) -> DataFrame:
        raw = (
            self.spark_session.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", self.market_topic)
            .option("startingOffsets", "latest")
            .load()
        )
        parsed = (
            raw.select(F.from_json(F.col("value").cast("string"), MARKET_SCHEMA).alias("data"))
            .select("data.*")
            .withWatermark("timestamp", MARKET_WATERMARK_THRESHOLD)
        )
        return parsed

    def join_streams(self, event_stream: DataFrame, market_stream: DataFrame) -> DataFrame:
        join_condition = (
            (market_stream["timestamp"] >= event_stream["event_timestamp"])
            & (market_stream["timestamp"] <= event_stream["event_timestamp"] + F.expr(f"INTERVAL {JOIN_WINDOW_DURATION}"))
        )
        return event_stream.join(market_stream, join_condition, "left_outer")

    def enrich(self, joined_stream: DataFrame) -> DataFrame:
        # Compute rupee baseline and percentage changes per event
        enriched = (
            joined_stream
            .groupBy(
                "event_id", "event_timestamp", "actor1_country", "actor2_country",
                "cameo_code", "cameo_root_code", "goldstein_score", "num_articles", "avg_tone",
            )
            .agg(
                F.first("close").alias("rupee_at_event"),
                F.avg(
                    F.when(
                        (F.col("timestamp") >= F.col("event_timestamp"))
                        & (F.col("timestamp") <= F.col("event_timestamp") + F.expr("INTERVAL 15 minutes")),
                        F.col("close"),
                    )
                ).alias("rupee_15min"),
                F.avg(
                    F.when(
                        (F.col("timestamp") >= F.col("event_timestamp"))
                        & (F.col("timestamp") <= F.col("event_timestamp") + F.expr("INTERVAL 1 hour")),
                        F.col("close"),
                    )
                ).alias("rupee_1hr"),
                F.avg(
                    F.when(
                        (F.col("timestamp") >= F.col("event_timestamp"))
                        & (F.col("timestamp") <= F.col("event_timestamp") + F.expr("INTERVAL 2 hours")),
                        F.col("close"),
                    )
                ).alias("rupee_2hr"),
            )
            .withColumn(
                "change_15min_pct",
                (F.col("rupee_15min") - F.col("rupee_at_event")) / F.col("rupee_at_event") * 100,
            )
            .withColumn(
                "change_1hr_pct",
                (F.col("rupee_1hr") - F.col("rupee_at_event")) / F.col("rupee_at_event") * 100,
            )
            .withColumn(
                "change_2hr_pct",
                (F.col("rupee_2hr") - F.col("rupee_at_event")) / F.col("rupee_at_event") * 100,
            )
            .drop("rupee_15min", "rupee_1hr", "rupee_2hr")
        )
        return enriched

    def write_bronze(self, event_stream: DataFrame, market_stream: DataFrame):
        (
            event_stream.writeStream
            .format("parquet")
            .option("path", S3_BRONZE_GDELT_PATH)
            .option("checkpointLocation", f"{S3_CHECKPOINT_PATH}gdelt-raw/")
            .partitionBy("event_date")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )
        (
            market_stream
            .withColumn("date", F.to_date("timestamp"))
            .writeStream
            .format("parquet")
            .option("path", S3_BRONZE_MARKET_PATH)
            .option("checkpointLocation", f"{S3_CHECKPOINT_PATH}market-raw/")
            .partitionBy("date")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )

    def write_silver(self, enriched_stream: DataFrame):
        (
            enriched_stream
            .withColumn("date", F.to_date("event_timestamp"))
            .writeStream
            .format("parquet")
            .option("path", self.s3_output_path)
            .option("checkpointLocation", f"{self.checkpoint_path}correlated/")
            .partitionBy("date")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .outputMode("append")
            .start()
        )

    def run(self):
        self.create_spark_session()
        event_stream = self.read_event_stream()
        market_stream = self.read_market_stream()
        self.write_bronze(event_stream, market_stream)
        joined = self.join_streams(event_stream, market_stream)
        enriched = self.enrich(joined)
        self.write_silver(enriched)
        self.spark_session.streams.awaitAnyTermination()


if __name__ == "__main__":
    GeopoliticalMarketStreamJob().run()
