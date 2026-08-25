"""
Configuration module for Data Center Telemetry Simulator.

Contains configurable simulation parameters, topology scale defaults,
inclusive hard metric bounds, and simulation threshold assumptions.
"""

from dataclasses import dataclass, field
from typing import Tuple, Dict, Any


@dataclass(frozen=True)
class MetricBounds:
    """Inclusive hard bounds for simulation metrics."""
    min_val: float
    max_val: float


# Inclusive hard simulation bounds for generic data-center servers.
# Note: These are simulation assumptions, not universal hardware specifications.
HARD_BOUNDS: Dict[str, MetricBounds] = {
    "cpu_utilization": MetricBounds(0.0, 100.0),
    "memory_utilization": MetricBounds(0.0, 100.0),
    "disk_utilization": MetricBounds(0.0, 100.0),
    "temperature": MetricBounds(18.0, 50.0),
    "network_in": MetricBounds(0.0, 1000.0),
    "network_out": MetricBounds(0.0, 1000.0),
    "power_consumption": MetricBounds(250.0, 850.0),
    "fan_speed": MetricBounds(2000.0, 7000.0),
    "disk_read": MetricBounds(0.0, 800.0),
    "disk_write": MetricBounds(0.0, 600.0),
}


# Operational classification threshold ranges (Simulation Assumptions)
SIMULATION_THRESHOLDS: Dict[str, Dict[str, Tuple[float, float]]] = {
    "cpu_utilization": {"normal": (20.0, 75.0), "warning": (75.0, 90.0), "critical": (90.0, 100.0)},
    "memory_utilization": {"normal": (30.0, 75.0), "warning": (75.0, 90.0), "critical": (90.0, 100.0)},
    "disk_utilization": {"normal": (30.0, 80.0), "warning": (80.0, 90.0), "critical": (90.0, 100.0)},
    "temperature": {"normal": (20.0, 35.0), "warning": (35.0, 40.0), "critical": (40.0, 50.0)},
    "network_in": {"normal": (50.0, 700.0), "warning": (700.0, 900.0), "critical": (900.0, 1000.0)},
    "network_out": {"normal": (50.0, 700.0), "warning": (700.0, 900.0), "critical": (900.0, 1000.0)},
    "power_consumption": {"normal": (300.0, 600.0), "warning": (600.0, 750.0), "critical": (750.0, 850.0)},
    "fan_speed": {"normal": (2500.0, 5000.0), "warning": (5000.0, 6500.0), "critical": (6500.0, 7000.0)},
    "disk_read": {"normal": (20.0, 500.0), "warning": (500.0, 700.0), "critical": (700.0, 800.0)},
    "disk_write": {"normal": (10.0, 300.0), "warning": (300.0, 500.0), "critical": (500.0, 600.0)},
}


@dataclass
class SimulationConfig:
    """Configurable simulation parameters."""

    num_racks: int = 5
    servers_per_rack: int = 4
    telemetry_interval: float = 5.0  # seconds

    enable_incidents: bool = True
    incident_check_interval: int = 50  # steps between global incident scheduling attempts
    incident_chance: float = 0.05      # low frequency global incident probability
    incident_min_duration: int = 6     # minimum duration in steps/ticks
    incident_max_duration: int = 15    # maximum duration in steps/ticks

    @property
    def total_servers(self) -> int:
        return self.num_racks * self.servers_per_rack
