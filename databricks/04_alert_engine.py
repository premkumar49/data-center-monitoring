"""
Databricks Structured Streaming — Step 04: Stateful Alert & Incident Detection Engine.

Processes validated telemetry streams, executes per-(server_id, incident_type) state machines,
and writes derived incident events to downstream sinks (Delta Lake / Kafka incident_events topic).
"""

import os
from typing import List, Dict, Any, Iterator
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
)

from databricks.alert_rules import AlertEngineConfig
from databricks.incident_engine import StatefulIncidentEngine


def get_incident_event_schema() -> StructType:
    """Explicit PySpark schema for derived incident stream."""
    return StructType([
        StructField("incident_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("rack_id", StringType(), True),
        StructField("incident_type", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("status", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("first_seen", StringType(), True),
        StructField("last_seen", StringType(), True),
        StructField("current_value", DoubleType(), True),
        StructField("threshold", DoubleType(), True),
        StructField("notification_required", BooleanType(), True),
        StructField("message", StringType(), True),
    ])


class StreamingIncidentProcessor:
    """
    Micro-batch / stateful wrapper for executing StatefulIncidentEngine inside Spark Structured Streaming.
    """

    def __init__(self, config: Optional[AlertEngineConfig] = None):
        self.engine = StatefulIncidentEngine(config=config)

    def process_microbatch(self, microbatch_df: DataFrame, batch_id: int) -> List[dict]:
        """
        Executes stateful incident evaluation for a single micro-batch.
        Collects micro-batch rows sorted by timestamp to ensure chronological evaluation.
        """
        records = microbatch_df.sort("timestamp").collect()
        batch_incident_events: List[dict] = []

        for row in records:
            rec_dict = row.asDict()
            # Convert timestamp to ISO string if parsed as datetime/timestamp
            if hasattr(rec_dict["timestamp"], "isoformat"):
                rec_dict["timestamp"] = rec_dict["timestamp"].isoformat() + "Z"
            
            events = self.engine.process_record(rec_dict)
            batch_incident_events.extend(events)

        return batch_incident_events


def run_alert_engine_stream(
    valid_telemetry_df: DataFrame,
    checkpoint_location: str = "/tmp/delta/checkpoints/alert_engine",
    output_sink: str = "console",
):
    """
    Runs Structured Streaming query for stateful alert & incident detection.
    
    Args:
        valid_telemetry_df: Validated streaming telemetry DataFrame.
        checkpoint_location: Path for offset and state checkpointing.
        output_sink: Sink format ('console', 'delta', 'memory').
    """
    processor = StreamingIncidentProcessor()

    def foreach_batch_function(df: DataFrame, batch_id: int):
        events = processor.process_microbatch(df, batch_id)
        if events:
            spark = df.sparkSession
            schema = get_incident_event_schema()
            events_df = spark.createDataFrame(events, schema=schema)
            events_df.show(truncate=False)

    return (
        valid_telemetry_df.writeStream
        .foreachBatch(foreach_batch_function)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )
