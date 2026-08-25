"""
Data models and Enums for the Data Center Telemetry Simulator.

Includes Workload Profiles, Incident Types, Incident Lifecycle Phases,
Server Baselines, internal Incident State tracking, and the raw TelemetryRecord.
"""

from dataclasses import dataclass, asdict
from enum import Enum, auto
import json
from typing import Dict, Any


class WorkloadProfile(Enum):
    """Workload profile affecting server telemetry baselines."""
    NORMAL = "NORMAL"
    COMPUTE_HEAVY = "COMPUTE_HEAVY"
    MEMORY_HEAVY = "MEMORY_HEAVY"
    NETWORK_HEAVY = "NETWORK_HEAVY"
    STORAGE_HEAVY = "STORAGE_HEAVY"


class IncidentType(Enum):
    """Types of controlled incidents injected into simulation."""
    NONE = "NONE"
    CPU_OVERLOAD = "CPU_OVERLOAD"
    OVERHEATING = "OVERHEATING"
    DISK_SATURATION = "DISK_SATURATION"
    NETWORK_CONGESTION = "NETWORK_CONGESTION"
    MEMORY_PRESSURE = "MEMORY_PRESSURE"


class IncidentLifecyclePhase(Enum):
    """
    Internal simulator lifecycle phases for evolving incidents.
    Note: 'START' is an internal simulation state transition, not an operational alert.
    """
    NORMAL = "NORMAL"
    START = "START"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    RECOVERY = "RECOVERY"


@dataclass
class ServerBaseline:
    """Baseline operational parameters for a specific server."""
    server_id: str
    rack_id: str
    workload_profile: WorkloadProfile
    base_cpu: float
    base_memory: float
    base_disk: float
    base_network_in: float
    base_network_out: float
    base_disk_read: float
    base_disk_write: float
    thermal_efficiency: float = 1.0  # multiplier for heat dissipation baseline


@dataclass
class IncidentState:
    """Internal state for tracking active incident lifecycle on a server."""
    incident_type: IncidentType = IncidentType.NONE
    phase: IncidentLifecyclePhase = IncidentLifecyclePhase.NORMAL
    current_step: int = 0
    total_duration: int = 0  # in telemetry ticks/steps


@dataclass
class TelemetryRecord:
    """
    Raw telemetry event reported by infrastructure.
    
    IMPORTANT ARCHITECTURAL REQUIREMENT:
    This model contains raw infrastructure metrics ONLY.
    Authoritative decision fields like health_status, alert, severity, or sms_sent
    are intentionally excluded to maintain architectural separation for downstream
    stream analytics (Databricks).
    """
    timestamp: str
    server_id: str
    rack_id: str
    cpu_utilization: float
    memory_utilization: float
    disk_utilization: float
    network_in: float
    network_out: float
    temperature: float
    power_consumption: float
    fan_speed: float
    disk_read: float
    disk_write: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert telemetry record to rounded dictionary."""
        return {
            "timestamp": self.timestamp,
            "server_id": self.server_id,
            "rack_id": self.rack_id,
            "cpu_utilization": round(self.cpu_utilization, 2),
            "memory_utilization": round(self.memory_utilization, 2),
            "disk_utilization": round(self.disk_utilization, 2),
            "network_in": round(self.network_in, 2),
            "network_out": round(self.network_out, 2),
            "temperature": round(self.temperature, 2),
            "power_consumption": round(self.power_consumption, 2),
            "fan_speed": int(round(self.fan_speed)),
            "disk_read": round(self.disk_read, 2),
            "disk_write": round(self.disk_write, 2),
        }

    def to_json(self) -> str:
        """Serialize record to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TelemetryRecord":
        """Construct record from dictionary."""
        return cls(
            timestamp=str(data["timestamp"]),
            server_id=str(data["server_id"]),
            rack_id=str(data["rack_id"]),
            cpu_utilization=float(data["cpu_utilization"]),
            memory_utilization=float(data["memory_utilization"]),
            disk_utilization=float(data["disk_utilization"]),
            network_in=float(data["network_in"]),
            network_out=float(data["network_out"]),
            temperature=float(data["temperature"]),
            power_consumption=float(data["power_consumption"]),
            fan_speed=float(data["fan_speed"]),
            disk_read=float(data["disk_read"]),
            disk_write=float(data["disk_write"]),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "TelemetryRecord":
        """Deserialize record from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)
