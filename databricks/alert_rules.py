"""
Alert Rules & Threshold Configuration for Data Center Telemetry.

Defines Warning, Critical, and Recovery (Hysteresis) thresholds,
confirmation counts, and human-readable message templates for Step 6.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Any


class IncidentType(Enum):
    CPU_OVERLOAD = "CPU_OVERLOAD"
    OVERHEATING = "OVERHEATING"
    DISK_SATURATION = "DISK_SATURATION"
    NETWORK_CONGESTION = "NETWORK_CONGESTION"
    MEMORY_PRESSURE = "MEMORY_PRESSURE"


class Severity(Enum):
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class IncidentStatus(Enum):
    OPEN = "OPEN"
    RECOVERING = "RECOVERING"
    CLOSED = "CLOSED"


class EventType(Enum):
    INCIDENT_OPENED = "INCIDENT_OPENED"
    INCIDENT_ESCALATED = "INCIDENT_ESCALATED"
    INCIDENT_RECOVERY_STARTED = "INCIDENT_RECOVERY_STARTED"
    INCIDENT_CLOSED = "INCIDENT_CLOSED"


@dataclass
class AlertThreshold:
    """Threshold settings for a specific incident category."""
    incident_type: IncidentType
    metric_field: str
    warning_threshold: float
    critical_threshold: float
    recovery_threshold: float
    unit: str


# Centralized alert threshold configuration
DEFAULT_THRESHOLDS: Dict[IncidentType, AlertThreshold] = {
    IncidentType.CPU_OVERLOAD: AlertThreshold(
        incident_type=IncidentType.CPU_OVERLOAD,
        metric_field="cpu_utilization",
        warning_threshold=80.0,
        critical_threshold=90.0,
        recovery_threshold=75.0,
        unit="%",
    ),
    IncidentType.MEMORY_PRESSURE: AlertThreshold(
        incident_type=IncidentType.MEMORY_PRESSURE,
        metric_field="memory_utilization",
        warning_threshold=80.0,
        critical_threshold=90.0,
        recovery_threshold=75.0,
        unit="%",
    ),
    IncidentType.DISK_SATURATION: AlertThreshold(
        incident_type=IncidentType.DISK_SATURATION,
        metric_field="disk_utilization",
        warning_threshold=85.0,
        critical_threshold=95.0,
        recovery_threshold=80.0,
        unit="%",
    ),
    IncidentType.OVERHEATING: AlertThreshold(
        incident_type=IncidentType.OVERHEATING,
        metric_field="temperature",
        warning_threshold=38.0,
        critical_threshold=42.0,
        recovery_threshold=35.0,
        unit="°C",
    ),
    IncidentType.NETWORK_CONGESTION: AlertThreshold(
        incident_type=IncidentType.NETWORK_CONGESTION,
        metric_field="network_max",  # max(network_in, network_out)
        warning_threshold=750.0,
        critical_threshold=900.0,
        recovery_threshold=700.0,
        unit="Mbps",
    ),
}


@dataclass
class AlertEngineConfig:
    """Centralized engine configuration settings."""
    warning_confirmation_count: int = 2
    critical_confirmation_count: int = 2
    recovery_confirmation_count: int = 2
    state_timeout_minutes: int = 30
    thresholds: Dict[IncidentType, AlertThreshold] = field(default_factory=lambda: DEFAULT_THRESHOLDS)


def format_alert_message(
    severity: Severity,
    incident_type: IncidentType,
    server_id: str,
    rack_id: str,
    current_val: float,
    threshold_val: float,
    unit: str,
    status: IncidentStatus = IncidentStatus.OPEN,
) -> str:
    """Generates human-readable alert message suitable for SMS formatting in Step 7."""
    title_type = incident_type.value.replace("_", " ")
    if status == IncidentStatus.CLOSED:
        return (
            f"RECOVERY RESOLVED: {title_type} on server {server_id} ({rack_id}). "
            f"Current value {current_val:.1f}{unit} is back within normal operational limits."
        )
    return (
        f"{severity.value} {title_type}\n"
        f"Server: {server_id}\n"
        f"Rack: {rack_id}\n"
        f"Current: {current_val:.1f}{unit}\n"
        f"Threshold: {threshold_val:.1f}{unit}\n"
        f"Status: {status.value}"
    )
