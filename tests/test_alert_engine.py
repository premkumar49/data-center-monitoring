"""
Comprehensive Unit Tests for Step 6 Stateful Alert & Incident Detection Engine.

Tests warning/critical/recovery thresholds across CPU, Memory, Disk, Temperature, and Network.
Verifies temporal confirmation counts, hysteresis bounds, incident deduplication, unique IDs,
multi-incident independence, state timeouts, notification eligibility tagging, and raw telemetry integrity.
"""

from datetime import datetime, timedelta, timezone
import pytest

from producer.models import TelemetryRecord
from producer.simulator import DataCenterSimulator
from databricks.alert_rules import AlertEngineConfig, IncidentType, Severity, IncidentStatus, EventType
from databricks.incident_engine import StatefulIncidentEngine


def create_mock_telemetry_record(
    server_id: str = "SRV001",
    rack_id: str = "RACK01",
    cpu: float = 40.0,
    memory: float = 50.0,
    disk: float = 45.0,
    temperature: float = 24.0,
    net_in: float = 200.0,
    net_out: float = 150.0,
    timestamp: str = "2026-08-25T10:00:00Z",
) -> dict:
    """Utility helper to build raw telemetry record dict."""
    return {
        "timestamp": timestamp,
        "server_id": server_id,
        "rack_id": rack_id,
        "cpu_utilization": cpu,
        "memory_utilization": memory,
        "disk_utilization": disk,
        "network_in": net_in,
        "network_out": net_out,
        "temperature": temperature,
        "power_consumption": 350.0,
        "fan_speed": 3000.0,
        "disk_read": 100.0,
        "disk_write": 50.0,
    }


def test_normal_telemetry_produces_no_incidents():
    """Verify normal telemetry within operational baselines produces zero incidents."""
    engine = StatefulIncidentEngine()
    rec = create_mock_telemetry_record(cpu=40.0, memory=50.0, temperature=25.0)
    events = engine.process_record(rec)
    assert len(events) == 0


def test_cpu_warning_confirmation():
    """Verify CPU warning threshold requires 2 consecutive observations."""
    engine = StatefulIncidentEngine()

    # Step 1: 1st warning sample (82%) -> count 1 (no event yet)
    r1 = create_mock_telemetry_record(cpu=82.0, timestamp="2026-08-25T10:00:00Z")
    events1 = engine.process_record(r1)
    assert len(events1) == 0

    # Step 2: 2nd warning sample (85%) -> count 2 (incident opened!)
    r2 = create_mock_telemetry_record(cpu=85.0, timestamp="2026-08-25T10:00:05Z")
    events2 = engine.process_record(r2)
    assert len(events2) == 1

    evt = events2[0]
    assert evt["incident_type"] == "CPU_OVERLOAD"
    assert evt["severity"] == "WARNING"
    assert evt["status"] == "OPEN"
    assert evt["event_type"] == "INCIDENT_OPENED"
    assert evt["notification_required"] is False  # Warning is not SMS eligible


def test_cpu_critical_escalation_and_notification_eligibility():
    """Verify CPU critical threshold requires 2 samples and sets notification_required=True."""
    engine = StatefulIncidentEngine()

    # Confirm Warning
    engine.process_record(create_mock_telemetry_record(cpu=82.0, timestamp="2026-08-25T10:00:00Z"))
    engine.process_record(create_mock_telemetry_record(cpu=85.0, timestamp="2026-08-25T10:00:05Z"))

    # Critical sample 1 (92%) -> count 1
    events_c1 = engine.process_record(create_mock_telemetry_record(cpu=92.0, timestamp="2026-08-25T10:00:10Z"))
    assert len(events_c1) == 0

    # Critical sample 2 (95%) -> count 2 (escalated to CRITICAL)
    events_c2 = engine.process_record(create_mock_telemetry_record(cpu=95.0, timestamp="2026-08-25T10:00:15Z"))
    assert len(events_c2) == 1

    evt = events_c2[0]
    assert evt["incident_type"] == "CPU_OVERLOAD"
    assert evt["severity"] == "CRITICAL"
    assert evt["status"] == "OPEN"
    assert evt["event_type"] == "INCIDENT_ESCALATED"
    assert evt["notification_required"] is True  # New Critical incident is SMS eligible!


def test_memory_warning_and_critical_detection():
    """Verify Memory pressure warning (>=80%) and critical (>=90%) detection."""
    engine = StatefulIncidentEngine()
    engine.process_record(create_mock_telemetry_record(memory=82.0, timestamp="2026-08-25T10:00:00Z"))
    evts_w = engine.process_record(create_mock_telemetry_record(memory=85.0, timestamp="2026-08-25T10:00:05Z"))
    assert len(evts_w) == 1
    assert evts_w[0]["incident_type"] == "MEMORY_PRESSURE"
    assert evts_w[0]["severity"] == "WARNING"

    engine.process_record(create_mock_telemetry_record(memory=91.0, timestamp="2026-08-25T10:00:10Z"))
    evts_c = engine.process_record(create_mock_telemetry_record(memory=93.0, timestamp="2026-08-25T10:00:15Z"))
    assert len(evts_c) == 1
    assert evts_c[0]["incident_type"] == "MEMORY_PRESSURE"
    assert evts_c[0]["severity"] == "CRITICAL"


def test_disk_saturation_detection():
    """Verify Disk saturation warning (>=85%) and critical (>=95%) detection."""
    engine = StatefulIncidentEngine()
    engine.process_record(create_mock_telemetry_record(disk=87.0, timestamp="2026-08-25T10:00:00Z"))
    evts_w = engine.process_record(create_mock_telemetry_record(disk=88.0, timestamp="2026-08-25T10:00:05Z"))
    assert len(evts_w) == 1
    assert evts_w[0]["incident_type"] == "DISK_SATURATION"
    assert evts_w[0]["severity"] == "WARNING"

    engine.process_record(create_mock_telemetry_record(disk=96.0, timestamp="2026-08-25T10:00:10Z"))
    evts_c = engine.process_record(create_mock_telemetry_record(disk=97.0, timestamp="2026-08-25T10:00:15Z"))
    assert len(evts_c) == 1
    assert evts_c[0]["incident_type"] == "DISK_SATURATION"
    assert evts_c[0]["severity"] == "CRITICAL"


def test_overheating_detection():
    """Verify Overheating warning (>=38°C) and critical (>=42°C) detection."""
    engine = StatefulIncidentEngine()
    engine.process_record(create_mock_telemetry_record(temperature=39.0, timestamp="2026-08-25T10:00:00Z"))
    evts_w = engine.process_record(create_mock_telemetry_record(temperature=40.0, timestamp="2026-08-25T10:00:05Z"))
    assert len(evts_w) == 1
    assert evts_w[0]["incident_type"] == "OVERHEATING"
    assert evts_w[0]["severity"] == "WARNING"

    engine.process_record(create_mock_telemetry_record(temperature=43.0, timestamp="2026-08-25T10:00:10Z"))
    evts_c = engine.process_record(create_mock_telemetry_record(temperature=44.0, timestamp="2026-08-25T10:00:15Z"))
    assert len(evts_c) == 1
    assert evts_c[0]["incident_type"] == "OVERHEATING"
    assert evts_c[0]["severity"] == "CRITICAL"


def test_network_congestion_detection():
    """Verify Network congestion warning (>=750 Mbps) and critical (>=900 Mbps) detection."""
    engine = StatefulIncidentEngine()
    engine.process_record(create_mock_telemetry_record(net_in=780.0, timestamp="2026-08-25T10:00:00Z"))
    evts_w = engine.process_record(create_mock_telemetry_record(net_in=800.0, timestamp="2026-08-25T10:00:05Z"))
    assert len(evts_w) == 1
    assert evts_w[0]["incident_type"] == "NETWORK_CONGESTION"
    assert evts_w[0]["severity"] == "WARNING"

    engine.process_record(create_mock_telemetry_record(net_in=920.0, timestamp="2026-08-25T10:00:10Z"))
    evts_c = engine.process_record(create_mock_telemetry_record(net_in=950.0, timestamp="2026-08-25T10:00:15Z"))
    assert len(evts_c) == 1
    assert evts_c[0]["incident_type"] == "NETWORK_CONGESTION"
    assert evts_c[0]["severity"] == "CRITICAL"


def test_alert_deduplication_and_no_repeated_sms_requests():
    """
    Verify continuous critical samples (92%, 94%, 96%, 95%) maintain ONE OPEN incident ID
    and DO NOT repeatedly set notification_required=True.
    """
    engine = StatefulIncidentEngine()

    # Open critical incident
    engine.process_record(create_mock_telemetry_record(cpu=92.0, timestamp="2026-08-25T10:00:00Z"))
    evts_init = engine.process_record(create_mock_telemetry_record(cpu=94.0, timestamp="2026-08-25T10:00:05Z"))
    assert len(evts_init) == 1
    inc_id = evts_init[0]["incident_id"]
    assert evts_init[0]["notification_required"] is True

    # Continuous critical samples (96%, 95%, 93%)
    for i, cpu_val in enumerate([96.0, 95.0, 93.0]):
        ts = f"2026-08-25T10:00:{10 + i * 5:02d}Z"
        evts = engine.process_record(create_mock_telemetry_record(cpu=cpu_val, timestamp=ts))
        # Zero state transition events emitted for ongoing deduplicated state
        assert len(evts) == 0

    state = engine._get_state("SRV001", IncidentType.CPU_OVERLOAD)
    assert state.incident_id == inc_id  # Incident ID unchanged!
    assert state.current_value == 93.0  # Current value updated
    assert state.has_notified_critical is True  # Notification flag set


def test_hysteresis_and_recovery_confirmation():
    """
    Verify recovery requires values below recovery_threshold for 2 consecutive samples
    before closing incident.
    """
    engine = StatefulIncidentEngine()

    # Open and confirm warning
    engine.process_record(create_mock_telemetry_record(cpu=82.0, timestamp="2026-08-25T10:00:00Z"))
    engine.process_record(create_mock_telemetry_record(cpu=85.0, timestamp="2026-08-25T10:00:05Z"))

    # Value drops to 78% (below warning 80%, but above recovery 75%) -> Hysteresis: stays OPEN!
    evts_h1 = engine.process_record(create_mock_telemetry_record(cpu=78.0, timestamp="2026-08-25T10:00:10Z"))
    assert len(evts_h1) == 0
    state = engine._get_state("SRV001", IncidentType.CPU_OVERLOAD)
    assert state.phase == "WARNING"
    assert state.status == IncidentStatus.OPEN

    # Value drops to 72% (below recovery 75%) -> 1st recovery sample -> RECOVERY_STARTED!
    evts_r1 = engine.process_record(create_mock_telemetry_record(cpu=72.0, timestamp="2026-08-25T10:00:15Z"))
    assert len(evts_r1) == 1
    assert evts_r1[0]["event_type"] == "INCIDENT_RECOVERY_STARTED"
    assert evts_r1[0]["status"] == "RECOVERING"

    # Value stays 70% -> 2nd recovery sample -> INCIDENT_CLOSED!
    evts_r2 = engine.process_record(create_mock_telemetry_record(cpu=70.0, timestamp="2026-08-25T10:00:20Z"))
    assert len(evts_r2) == 1
    assert evts_r2[0]["event_type"] == "INCIDENT_CLOSED"
    assert evts_r2[0]["status"] == "CLOSED"

    # State reset
    assert state.phase == "NORMAL"
    assert state.incident_id is None


def test_multi_incident_independence():
    """Verify SRV004 experiencing CPU 95% and Temp 43°C creates 2 separate independent incidents."""
    engine = StatefulIncidentEngine()

    # Step 1
    r1 = create_mock_telemetry_record(server_id="SRV004", cpu=95.0, temperature=43.0, timestamp="2026-08-25T10:00:00Z")
    engine.process_record(r1)

    # Step 2
    r2 = create_mock_telemetry_record(server_id="SRV004", cpu=96.0, temperature=44.0, timestamp="2026-08-25T10:00:05Z")
    evts = engine.process_record(r2)

    assert len(evts) == 2
    inc_types = {e["incident_type"] for e in evts}
    assert inc_types == {"CPU_OVERLOAD", "OVERHEATING"}

    inc_id_cpu = next(e["incident_id"] for e in evts if e["incident_type"] == "CPU_OVERLOAD")
    inc_id_temp = next(e["incident_id"] for e in evts if e["incident_type"] == "OVERHEATING")
    assert inc_id_cpu != inc_id_temp


def test_server_state_isolation():
    """Verify SRV001 CPU overload does not affect SRV002 state."""
    engine = StatefulIncidentEngine()
    engine.process_record(create_mock_telemetry_record(server_id="SRV001", cpu=92.0, timestamp="2026-08-25T10:00:00Z"))
    engine.process_record(create_mock_telemetry_record(server_id="SRV001", cpu=95.0, timestamp="2026-08-25T10:00:05Z"))

    state1 = engine._get_state("SRV001", IncidentType.CPU_OVERLOAD)
    state2 = engine._get_state("SRV002", IncidentType.CPU_OVERLOAD)

    assert state1.phase == "CRITICAL"
    assert state2.phase == "NORMAL"


def test_unique_incident_ids():
    """Verify unique traceable incident IDs are generated across multiple incidents."""
    engine = StatefulIncidentEngine()

    engine.process_record(create_mock_telemetry_record(server_id="SRV001", cpu=85.0, timestamp="2026-08-25T10:00:00Z"))
    evts1 = engine.process_record(create_mock_telemetry_record(server_id="SRV001", cpu=85.0, timestamp="2026-08-25T10:00:05Z"))

    engine.process_record(create_mock_telemetry_record(server_id="SRV002", cpu=85.0, timestamp="2026-08-25T10:00:00Z"))
    evts2 = engine.process_record(create_mock_telemetry_record(server_id="SRV002", cpu=85.0, timestamp="2026-08-25T10:00:05Z"))

    id1 = evts1[0]["incident_id"]
    id2 = evts2[0]["incident_id"]

    assert id1 != id2
    assert id1.startswith("INC-")
    assert id2.startswith("INC-")


def test_state_timeout():
    """Verify open incident times out and closes if telemetry stops for > 30 minutes."""
    engine = StatefulIncidentEngine()
    engine.process_record(create_mock_telemetry_record(cpu=85.0, timestamp="2026-08-25T10:00:00Z"))
    engine.process_record(create_mock_telemetry_record(cpu=85.0, timestamp="2026-08-25T10:00:05Z"))

    # Advance time by 35 minutes
    timeout_ts = "2026-08-25T10:35:10Z"
    closed_events = engine.check_state_timeouts(timeout_ts)

    assert len(closed_events) == 1
    assert closed_events[0]["status"] == "CLOSED"
    assert "State Timeout" in closed_events[0]["message"]

    state = engine._get_state("SRV001", IncidentType.CPU_OVERLOAD)
    assert state.phase == "NORMAL"


def test_cpu_scenario_sequence():
    """
    Scenario Test: SRV001 CPU sequence [70, 78, 82, 85, 92, 95, 94, 91, 88, 72, 70]
    Verifies full lifecycle transitions.
    """
    engine = StatefulIncidentEngine()
    cpu_values = [70, 78, 82, 85, 92, 95, 94, 91, 88, 72, 70]
    all_events = []

    for i, val in enumerate(cpu_values):
        ts = f"2026-08-25T10:00:{i * 5:02d}Z"
        evts = engine.process_record(create_mock_telemetry_record(cpu=val, timestamp=ts))
        all_events.extend(evts)

    # Event sequence expected:
    # 1. INCIDENT_OPENED (WARNING at 85)
    # 2. INCIDENT_ESCALATED (CRITICAL at 95)
    # 3. INCIDENT_RECOVERY_STARTED (at 76)
    # 4. INCIDENT_CLOSED (at 72)
    assert len(all_events) == 4
    assert all_events[0]["event_type"] == "INCIDENT_OPENED"
    assert all_events[0]["severity"] == "WARNING"

    assert all_events[1]["event_type"] == "INCIDENT_ESCALATED"
    assert all_events[1]["severity"] == "CRITICAL"
    assert all_events[1]["notification_required"] is True

    assert all_events[2]["event_type"] == "INCIDENT_RECOVERY_STARTED"
    assert all_events[2]["status"] == "RECOVERING"

    assert all_events[3]["event_type"] == "INCIDENT_CLOSED"
    assert all_events[3]["status"] == "CLOSED"


def test_overheating_scenario_sequence():
    """
    Overheating Scenario: SRV002 Temp sequence [30, 37, 39, 40, 43, 44, 43, 38, 34, 33]
    Verifies warning, critical, recovery, closed transitions.
    """
    engine = StatefulIncidentEngine()
    temp_values = [30, 37, 39, 40, 43, 44, 43, 38, 34, 33]
    all_events = []

    for i, val in enumerate(temp_values):
        ts = f"2026-08-25T10:00:{i * 5:02d}Z"
        evts = engine.process_record(create_mock_telemetry_record(server_id="SRV002", temperature=val, timestamp=ts))
        all_events.extend(evts)

    assert len(all_events) == 4
    assert all_events[0]["event_type"] == "INCIDENT_OPENED"
    assert all_events[0]["incident_type"] == "OVERHEATING"
    assert all_events[1]["event_type"] == "INCIDENT_ESCALATED"
    assert all_events[2]["event_type"] == "INCIDENT_RECOVERY_STARTED"
    assert all_events[3]["event_type"] == "INCIDENT_CLOSED"


def test_raw_telemetry_schema_integrity_and_no_field_leakage():
    """
    Verify raw simulator TelemetryRecord output remains 100% untouched
    and contains ZERO alert/severity/health_status fields.
    """
    sim = DataCenterSimulator(seed=99)
    record = sim.generate_step()[0]
    rec_dict = record.to_dict()

    expected_keys = {
        "timestamp", "server_id", "rack_id", "cpu_utilization", "memory_utilization",
        "disk_utilization", "network_in", "network_out", "temperature",
        "power_consumption", "fan_speed", "disk_read", "disk_write"
    }
    forbidden_keys = {"health_status", "alert", "severity", "sms_sent", "notification_required"}

    assert set(rec_dict.keys()) == expected_keys
    for forbidden in forbidden_keys:
        assert forbidden not in rec_dict, f"Forbidden field '{forbidden}' found in raw telemetry"
