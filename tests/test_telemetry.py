"""
Unit tests for raw telemetry validation, schema integrity, JSON serialization,
topology scaling, inclusive hard bounds, and physical correlations.
"""

import re
import pytest

from producer.server_config import SimulationConfig, HARD_BOUNDS
from producer.models import TelemetryRecord
from producer.simulator import DataCenterSimulator


def test_topology_and_scalability():
    """Verify default topology (5x4=20) and scalable topology (20x10=200)."""
    # Default 20 servers
    sim20 = DataCenterSimulator(SimulationConfig(num_racks=5, servers_per_rack=4), seed=42)
    records20 = sim20.generate_step()
    assert len(records20) == 20
    server_ids20 = {r.server_id for r in records20}
    rack_ids20 = {r.rack_id for r in records20}
    assert len(server_ids20) == 20
    assert len(rack_ids20) == 5

    # Scaled 200 servers
    sim200 = DataCenterSimulator(SimulationConfig(num_racks=20, servers_per_rack=10), seed=42)
    records200 = sim200.generate_step()
    assert len(records200) == 200
    server_ids200 = {r.server_id for r in records200}
    rack_ids200 = {r.rack_id for r in records200}
    assert len(server_ids200) == 200
    assert len(rack_ids200) == 20


def test_identifier_formats():
    """Verify server_id and rack_id format standards."""
    sim = DataCenterSimulator(seed=100)
    records = sim.generate_step()
    for rec in records:
        assert re.match(r"^SRV\d{3,}$", rec.server_id), f"Invalid server_id: {rec.server_id}"
        assert re.match(r"^RACK\d{2,}$", rec.rack_id), f"Invalid rack_id: {rec.rack_id}"


def test_required_fields_and_no_alert_leakage():
    """
    Verify every record contains all 13 raw metric fields and
    ensures raw telemetry DOES NOT leak alert/health fields.
    """
    sim = DataCenterSimulator(seed=42)
    records = sim.generate_step()
    required_keys = {
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

    for rec in records:
        rec_dict = rec.to_dict()
        assert set(rec_dict.keys()) == required_keys
        for forbidden in forbidden_keys:
            assert forbidden not in rec_dict, f"Forbidden alert field '{forbidden}' found in telemetry"


def test_inclusive_hard_bounds():
    """Verify all 10 metrics strictly stay within inclusive hard bounds across 500 records."""
    sim = DataCenterSimulator(SimulationConfig(num_racks=5, servers_per_rack=4), seed=77)
    for _ in range(25):
        records = sim.generate_step()
        for rec in records:
            assert 0.0 <= rec.cpu_utilization <= 100.0
            assert 0.0 <= rec.memory_utilization <= 100.0
            assert 0.0 <= rec.disk_utilization <= 100.0
            assert 18.0 <= rec.temperature <= 50.0
            assert 0.0 <= rec.network_in <= 1000.0
            assert 0.0 <= rec.network_out <= 1000.0
            assert 250.0 <= rec.power_consumption <= 850.0
            assert 2000.0 <= rec.fan_speed <= 7000.0
            assert 0.0 <= rec.disk_read <= 800.0
            assert 0.0 <= rec.disk_write <= 600.0


def test_json_serialization_roundtrip():
    """Verify to_json() and from_json() serialization/deserialization integrity."""
    sim = DataCenterSimulator(seed=123)
    rec = sim.generate_step()[0]

    json_str = rec.to_json()
    deserialized = TelemetryRecord.from_json(json_str)

    assert deserialized.timestamp == rec.timestamp
    assert deserialized.server_id == rec.server_id
    assert deserialized.rack_id == rec.rack_id
    assert pytest.approx(deserialized.cpu_utilization, 0.01) == rec.cpu_utilization
    assert pytest.approx(deserialized.memory_utilization, 0.01) == rec.memory_utilization
    assert pytest.approx(deserialized.disk_utilization, 0.01) == rec.disk_utilization
    assert pytest.approx(deserialized.network_in, 0.01) == rec.network_in
    assert pytest.approx(deserialized.network_out, 0.01) == rec.network_out
    assert pytest.approx(deserialized.temperature, 0.01) == rec.temperature
    assert pytest.approx(deserialized.power_consumption, 0.01) == rec.power_consumption
    assert pytest.approx(deserialized.fan_speed, 1.0) == rec.fan_speed
    assert pytest.approx(deserialized.disk_read, 0.01) == rec.disk_read
    assert pytest.approx(deserialized.disk_write, 0.01) == rec.disk_write


def test_temporal_continuity_normal_operation():
    """Verify metrics do not jump wildly from one step to another in normal mode."""
    sim = DataCenterSimulator(SimulationConfig(enable_incidents=False), seed=555)

    step1 = {r.server_id: r for r in sim.generate_step()}
    step2 = {r.server_id: r for r in sim.generate_step()}

    for sid in step1:
        cpu1, cpu2 = step1[sid].cpu_utilization, step2[sid].cpu_utilization
        temp1, temp2 = step1[sid].temperature, step2[sid].temperature

        # Maximum step-to-step delta under normal autoregressive walk
        assert abs(cpu2 - cpu1) < 15.0, f"CPU jumped too sharply: {cpu1} -> {cpu2}"
        assert abs(temp2 - temp1) < 5.0, f"Temperature jumped too sharply: {temp1} -> {temp2}"


def test_reproducibility():
    """Verify identical outputs when using the same seed."""
    sim1 = DataCenterSimulator(seed=999)
    sim2 = DataCenterSimulator(seed=999)

    ts = "2026-08-25T10:00:00Z"
    recs1 = sim1.generate_step(custom_timestamp=ts)
    recs2 = sim2.generate_step(custom_timestamp=ts)

    for r1, r2 in zip(recs1, recs2):
        assert r1.to_dict() == r2.to_dict()
