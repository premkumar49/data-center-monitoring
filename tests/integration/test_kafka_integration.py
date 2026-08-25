"""
Integration test for Kafka telemetry producer.

Attempts connection to a live Kafka broker (localhost:9092 by default).
Skips gracefully if Kafka broker is unavailable.
"""

import socket
import pytest

from producer.simulator import DataCenterSimulator
from producer.kafka_producer import KafkaTelemetryProducer


def is_kafka_available(host: str = "127.0.0.1", port: int = 9092, timeout: float = 0.5) -> bool:
    """Check if Kafka TCP port is reachable."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.mark.skipif(
    not is_kafka_available("127.0.0.1", 9092),
    reason="Kafka broker is not reachable at localhost:9092",
)
def test_live_kafka_producer_send():
    """Live integration test: send 5 telemetry events to local Kafka broker."""
    sim = DataCenterSimulator(seed=42)
    records = sim.generate_step()[:5]

    producer = KafkaTelemetryProducer(bootstrap_servers="localhost:9092", topic="server_telemetry")
    producer.connect()

    for record in records:
        meta = producer.send(record, sync=True)
        assert meta.topic == "server_telemetry"
        assert meta.partition >= 0
        assert meta.offset >= 0

    producer.flush()
    producer.close()
