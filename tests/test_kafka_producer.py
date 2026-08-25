"""
Unit tests for Kafka telemetry producer.

Verifies producer configuration defaults, CLI parsing, mock-based message sending,
key assignment (server_id), JSON schema integrity, and absence of alert field leakage.
Executes without requiring a live Kafka broker.
"""

import json
from unittest.mock import MagicMock
import pytest

from producer.models import TelemetryRecord
from producer.simulator import DataCenterSimulator
from producer.kafka_producer import KafkaTelemetryProducer, parse_args


def test_default_producer_configuration():
    """Verify default topic is 'server_telemetry' and default broker is 'localhost:9092'."""
    producer = KafkaTelemetryProducer()
    assert producer.bootstrap_servers == "localhost:9092"
    assert producer.topic == "server_telemetry"


def test_cli_argument_parsing():
    """Verify CLI argument parsing for bootstrap server, topic, mode, scenario, and seed."""
    args = parse_args([
        "--bootstrap-server", "192.168.1.100:9092",
        "--topic", "custom_telemetry",
        "--test",
        "--records", "50",
        "--interval", "2.5",
        "--racks", "10",
        "--servers-per-rack", "5",
        "--seed", "42",
        "--scenario", "cpu_overload",
        "--disable-incidents"
    ])
    assert args.bootstrap_server == "192.168.1.100:9092"
    assert args.topic == "custom_telemetry"
    assert args.test is True
    assert args.records == 50
    assert args.interval == 2.5
    assert args.racks == 10
    assert args.servers_per_rack == 5
    assert args.seed == 42
    assert args.scenario == "cpu_overload"
    assert args.disable_incidents is True


def test_telemetry_record_serialization_and_key_assignment():
    """
    Verify TelemetryRecord can be serialized correctly,
    the Kafka message key is server_id bytes, and value is UTF-8 JSON bytes.
    """
    sim = DataCenterSimulator(seed=42)
    record = sim.generate_step()[0]

    mock_kafka = MagicMock()
    mock_future = MagicMock()
    mock_metadata = MagicMock()
    mock_metadata.topic = "server_telemetry"
    mock_metadata.partition = 0
    mock_metadata.offset = 100
    mock_future.get.return_value = mock_metadata
    mock_kafka.send.return_value = mock_future

    producer = KafkaTelemetryProducer(producer_instance=mock_kafka)
    result_meta = producer.send(record, sync=True)

    expected_key_bytes = record.server_id.encode("utf-8")
    expected_value_bytes = record.to_json().encode("utf-8")

    mock_kafka.send.assert_called_once_with(
        "server_telemetry", key=expected_key_bytes, value=expected_value_bytes
    )
    assert result_meta == mock_metadata
    assert result_meta.offset == 100


def test_json_payload_schema_and_no_alert_field_leakage():
    """
    Verify JSON payload contains all expected telemetry fields and
    does not contain health_status, alert, severity, or sms_sent.
    """
    sim = DataCenterSimulator(seed=123)
    record = sim.generate_step()[0]

    json_str = record.to_json()
    payload = json.loads(json_str)

    expected_keys = {
        "timestamp",
        "server_id",
        "rack_id",
        "cpu_utilization",
        "memory_utilization",
        "disk_utilization",
        "network_in",
        "network_out",
        "temperature",
        "power_consumption",
        "fan_speed",
        "disk_read",
        "disk_write",
    }
    forbidden_keys = {"health_status", "alert", "severity", "sms_sent"}

    assert set(payload.keys()) == expected_keys
    for forbidden in forbidden_keys:
        assert forbidden not in payload, f"Forbidden field '{forbidden}' found in Kafka JSON payload"


def test_send_batch_and_producer_lifecycle():
    """Verify batch sending, flushing, and closing on mock producer."""
    sim = DataCenterSimulator(seed=77)
    records = sim.generate_step()[:5]

    mock_kafka = MagicMock()
    mock_future = MagicMock()
    mock_future.get.return_value = MagicMock(topic="server_telemetry", partition=0, offset=1)
    mock_kafka.send.return_value = mock_future

    producer = KafkaTelemetryProducer(producer_instance=mock_kafka)
    results = producer.send_batch(records, sync=True)

    assert len(results) == 5
    assert mock_kafka.send.call_count == 5

    producer.flush()
    mock_kafka.flush.assert_called_once()

    producer.close()
    mock_kafka.close.assert_called_once()
    assert producer.producer is None


def test_kafka_unavailable_connection_error():
    """Verify connection failure raises ConnectionError with clear description."""
    producer = KafkaTelemetryProducer(bootstrap_servers="127.0.0.1:59999")
    with pytest.raises(ConnectionError) as exc_info:
        producer.connect()
    assert "Unable to connect to Kafka at 127.0.0.1:59999" in str(exc_info.value)
