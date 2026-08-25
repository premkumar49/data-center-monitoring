"""
Databricks Structured Streaming — Step 03: Real-Time Streaming Analytics.

Performs real-time windowed aggregations over validated telemetry stream using PySpark:
  1. Average CPU utilization by server
  2. Average memory utilization by server
  3. Average temperature by rack
  4. Maximum CPU utilization by server
  5. Maximum temperature by rack
  6. Average power consumption by server
"""

from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, window, avg, max as spark_max, round as spark_round


def compute_server_cpu_analytics(valid_df: DataFrame, window_duration: str = "10 minutes", slide_duration: str = "1 minute") -> DataFrame:
    """
    Computes 10-minute windowed Average and Maximum CPU utilization grouped by server.
    Uses watermark to handle late-arriving events fault-tolerantly.
    """
    return (
        valid_df
        .withWatermark("timestamp", "10 minutes")
        .groupBy(
            window(col("timestamp"), window_duration, slide_duration),
            col("server_id"),
            col("rack_id")
        )
        .agg(
            spark_round(avg("cpu_utilization"), 2).alias("avg_cpu_utilization"),
            spark_round(spark_max("cpu_utilization"), 2).alias("max_cpu_utilization")
        )
    )


def compute_server_memory_power_analytics(valid_df: DataFrame, window_duration: str = "10 minutes", slide_duration: str = "1 minute") -> DataFrame:
    """
    Computes 10-minute windowed Average Memory and Power consumption grouped by server.
    """
    return (
        valid_df
        .withWatermark("timestamp", "10 minutes")
        .groupBy(
            window(col("timestamp"), window_duration, slide_duration),
            col("server_id")
        )
        .agg(
            spark_round(avg("memory_utilization"), 2).alias("avg_memory_utilization"),
            spark_round(avg("power_consumption"), 2).alias("avg_power_consumption")
        )
    )


def compute_rack_thermal_analytics(valid_df: DataFrame, window_duration: str = "10 minutes", slide_duration: str = "1 minute") -> DataFrame:
    """
    Computes 10-minute windowed Average and Maximum Temperature grouped by rack.
    """
    return (
        valid_df
        .withWatermark("timestamp", "10 minutes")
        .groupBy(
            window(col("timestamp"), window_duration, slide_duration),
            col("rack_id")
        )
        .agg(
            spark_round(avg("temperature"), 2).alias("avg_rack_temperature"),
            spark_round(spark_max("temperature"), 2).alias("max_rack_temperature")
        )
    )


def compute_all_streaming_aggregations(valid_df: DataFrame, window_duration: str = "10 minutes") -> Dict[str, DataFrame]:
    """
    Returns a dictionary of all 6 required streaming analytics DataFrames.
    """
    return {
        "server_cpu": compute_server_cpu_analytics(valid_df, window_duration=window_duration),
        "server_memory_power": compute_server_memory_power_analytics(valid_df, window_duration=window_duration),
        "rack_thermal": compute_rack_thermal_analytics(valid_df, window_duration=window_duration),
    }


def write_streaming_analytics(
    analytics_df: DataFrame,
    checkpoint_location: str,
    output_mode: str = "update",
    format_type: str = "console"
):
    """
    Starts a Structured Streaming write query with fault-tolerant checkpointing.
    
    Args:
        analytics_df: Windowed aggregation DataFrame.
        checkpoint_location: Path for storing Spark streaming metadata & offset checkpoints.
        output_mode: Output mode ('update', 'complete', or 'append').
        format_type: Target sink format ('console', 'delta', 'memory').
    """
    return (
        analytics_df.writeStream
        .format(format_type)
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )
