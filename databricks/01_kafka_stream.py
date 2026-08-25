"""
Databricks Structured Streaming — Step 01: Kafka Stream Ingestion & Schema Enforcement.

Reads raw telemetry JSON events from Apache Kafka topic 'server_telemetry',
enforces an explicit PySpark schema, parses timestamps, and prepares a structured stream.
"""

import os
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


def get_telemetry_schema() -> StructType:
    """
    Returns explicit PySpark schema for raw data center telemetry.
    
    Excludes any alert/decision fields (health_status, alert, severity, sms_sent)
    to maintain raw telemetry integrity for downstream analytics.
    """
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("rack_id", StringType(), True),
        StructField("cpu_utilization", DoubleType(), True),
        StructField("memory_utilization", DoubleType(), True),
        StructField("disk_utilization", DoubleType(), True),
        StructField("network_in", DoubleType(), True),
        StructField("network_out", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("power_consumption", DoubleType(), True),
        StructField("fan_speed", DoubleType(), True),
        StructField("disk_read", DoubleType(), True),
        StructField("disk_write", DoubleType(), True),
    ])


def read_kafka_telemetry_stream(
    spark: SparkSession,
    bootstrap_servers: str = "localhost:9092",
    topic: str = "server_telemetry",
    starting_offsets: str = "earliest",
) -> DataFrame:
    """
    Reads structured streaming DataFrame from Kafka topic.
    
    Args:
        spark: Active SparkSession instance.
        bootstrap_servers: Kafka broker bootstrap endpoint (e.g., '10.0.1.5:9092' or EC2 Public IP).
        topic: Target Kafka topic (default: 'server_telemetry').
        starting_offsets: Offset start policy ('earliest' or 'latest').

    Returns:
        Structured PySpark DataFrame with parsed, typed telemetry columns.
    """
    schema = get_telemetry_schema()

    # Read streaming bytes from Kafka topic
    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Cast key/value from bytes to string and deserialize JSON payload
    parsed_df = (
        raw_kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        )
        .select(
            to_timestamp(col("data.timestamp")).alias("timestamp"),
            col("data.server_id").alias("server_id"),
            col("data.rack_id").alias("rack_id"),
            col("data.cpu_utilization").alias("cpu_utilization"),
            col("data.memory_utilization").alias("memory_utilization"),
            col("data.disk_utilization").alias("disk_utilization"),
            col("data.network_in").alias("network_in"),
            col("data.network_out").alias("network_out"),
            col("data.temperature").alias("temperature"),
            col("data.power_consumption").alias("power_consumption"),
            col("data.fan_speed").alias("fan_speed"),
            col("data.disk_read").alias("disk_read"),
            col("data.disk_write").alias("disk_write"),
        )
    )

    return parsed_df


def main():
    """Main execution block when run inside Databricks environment."""
    # Attempt to retrieve widget parameters if running in Databricks workspace
    try:
        import dbutils  # type: ignore
        bootstrap_servers = dbutils.widgets.get("bootstrap_servers")
        topic = dbutils.widgets.getOrDefault("topic", "server_telemetry")
        starting_offsets = dbutils.widgets.getOrDefault("starting_offsets", "earliest")
    except Exception:
        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        topic = os.environ.get("KAFKA_TOPIC", "server_telemetry")
        starting_offsets = os.environ.get("STARTING_OFFSETS", "earliest")

    spark = SparkSession.builder.appName("DataCenterTelemetryKafkaStream").getOrCreate()
    
    print(f"Connecting to Kafka stream at {bootstrap_servers}, topic: {topic}, offsets: {starting_offsets}")
    telemetry_stream = read_kafka_telemetry_stream(
        spark=spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        starting_offsets=starting_offsets,
    )

    # Display streaming query in Databricks interactive notebook
    query = (
        telemetry_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
