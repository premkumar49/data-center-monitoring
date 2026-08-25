"""
Databricks Structured Streaming — Step 02: Telemetry Validation & Quarantine.

Validates incoming telemetry against physical hard bounds defined in Step 1.
Separates data stream into valid telemetry and quarantined invalid records.
"""

from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr


# Physical Hard Bounds constants matching Step 1 specification
HARD_BOUNDS = {
    "cpu_min": 0.0, "cpu_max": 100.0,
    "mem_min": 0.0, "mem_max": 100.0,
    "disk_min": 0.0, "disk_max": 100.0,
    "temp_min": 18.0, "temp_max": 50.0,
    "net_in_min": 0.0, "net_in_max": 1000.0,
    "net_out_min": 0.0, "net_out_max": 1000.0,
    "power_min": 250.0, "power_max": 850.0,
    "fan_min": 2000.0, "fan_max": 7000.0,
    "disk_read_min": 0.0, "disk_read_max": 800.0,
    "disk_write_min": 0.0, "disk_write_max": 600.0,
}


def build_validation_expression():
    """
    Constructs PySpark boolean Column expression enforcing strict inclusive hard bounds
    and non-null constraint checks on infrastructure telemetry.
    """
    return (
        col("server_id").isNotNull() & (col("server_id") != "") &
        col("rack_id").isNotNull() & (col("rack_id") != "") &
        col("timestamp").isNotNull() &
        (col("cpu_utilization") >= HARD_BOUNDS["cpu_min"]) & (col("cpu_utilization") <= HARD_BOUNDS["cpu_max"]) &
        (col("memory_utilization") >= HARD_BOUNDS["mem_min"]) & (col("memory_utilization") <= HARD_BOUNDS["mem_max"]) &
        (col("disk_utilization") >= HARD_BOUNDS["disk_min"]) & (col("disk_utilization") <= HARD_BOUNDS["disk_max"]) &
        (col("temperature") >= HARD_BOUNDS["temp_min"]) & (col("temperature") <= HARD_BOUNDS["temp_max"]) &
        (col("network_in") >= HARD_BOUNDS["net_in_min"]) & (col("network_in") <= HARD_BOUNDS["net_in_max"]) &
        (col("network_out") >= HARD_BOUNDS["net_out_min"]) & (col("network_out") <= HARD_BOUNDS["net_out_max"]) &
        (col("power_consumption") >= HARD_BOUNDS["power_min"]) & (col("power_consumption") <= HARD_BOUNDS["power_max"]) &
        (col("fan_speed") >= HARD_BOUNDS["fan_min"]) & (col("fan_speed") <= HARD_BOUNDS["fan_max"]) &
        (col("disk_read") >= HARD_BOUNDS["disk_read_min"]) & (col("disk_read") <= HARD_BOUNDS["disk_read_max"]) &
        (col("disk_write") >= HARD_BOUNDS["disk_write_min"]) & (col("disk_write") <= HARD_BOUNDS["disk_write_max"])
    )


def validate_telemetry_stream(telemetry_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Splits input telemetry DataFrame into valid records and quarantined invalid records.
    
    Args:
        telemetry_df: Raw structured PySpark DataFrame containing parsed telemetry.

    Returns:
        Tuple of (valid_df, quarantined_df).
    """
    is_valid_col = build_validation_expression()

    # Annotate with validation status column for explicit separation
    annotated_df = telemetry_df.withColumn("is_valid", is_valid_col)

    valid_df = annotated_df.filter(col("is_valid") == True).drop("is_valid")
    quarantined_df = annotated_df.filter(col("is_valid") == False)

    return valid_df, quarantined_df
