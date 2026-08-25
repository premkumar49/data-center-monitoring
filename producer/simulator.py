"""
Core Telemetry Simulator for Data Center Infrastructure.

Implements server topology generation, physical metric correlation rules,
thermal inertia, autoregressive temporal continuity, controlled incident lifecycle
management, scenario overrides, and inclusive hard bound clamping.
"""

from datetime import datetime, timezone
import math
import random
from typing import List, Dict, Optional, Any

from producer.server_config import SimulationConfig, HARD_BOUNDS
from producer.models import (
    WorkloadProfile,
    IncidentType,
    IncidentLifecyclePhase,
    ServerBaseline,
    IncidentState,
    TelemetryRecord,
)


class DataCenterSimulator:
    """
    Simulates telemetry data for multiple servers across multiple racks in a data center.
    """

    def __init__(
        self,
        config: Optional[SimulationConfig] = None,
        seed: Optional[int] = None,
        scenario: Optional[str] = None,
    ):
        self.config = config or SimulationConfig()
        self.rng = random.Random(seed) if seed is not None else random.Random()

        self.scenario = scenario.lower() if scenario else None
        self.global_step = 0

        self.servers: List[ServerBaseline] = []
        self.server_states: Dict[str, Dict[str, float]] = {}
        self.incident_states: Dict[str, IncidentState] = {}

        self._initialize_topology()

        # If an explicit scenario is requested (other than normal), queue it on the first server
        if self.scenario and self.scenario != "normal":
            self.inject_scenario(self.scenario)

    def _initialize_topology(self) -> None:
        """Construct rack and server topology with diverse baselines and server sharding support."""
        server_counter = 1
        profiles = list(WorkloadProfile)

        start_idx = self.config.server_start_index or 1
        end_idx = self.config.server_end_index or self.config.total_servers

        for r in range(1, self.config.num_racks + 1):
            rack_id = f"RACK{r:02d}"
            for s in range(1, self.config.servers_per_rack + 1):
                srv_num = server_counter
                server_id = f"SRV{srv_num:03d}"
                server_counter += 1

                if start_idx <= srv_num <= end_idx:
                    # Select workload profile deterministically or round-robin based on srv_num
                    profile = profiles[(srv_num - 1) % len(profiles)]

                    # Build server-specific baselines
                    baseline = self._create_server_baseline(server_id, rack_id, profile)
                    self.servers.append(baseline)
                    self.incident_states[server_id] = IncidentState()

                    # Initialize state values with baseline
                    self.server_states[server_id] = {
                        "cpu": baseline.base_cpu,
                        "memory": baseline.base_memory,
                        "disk": baseline.base_disk,
                        "network_in": baseline.base_network_in,
                        "network_out": baseline.base_network_out,
                        "temperature": 24.0,  # Ambient thermal baseline °C
                        "power": 320.0,
                        "fan_speed": 3000.0,
                        "disk_read": baseline.base_disk_read,
                        "disk_write": baseline.base_disk_write,
                    }

    def _create_server_baseline(
        self, server_id: str, rack_id: str, profile: WorkloadProfile
    ) -> ServerBaseline:
        """Generate baseline values tailored to workload profile with minor server variance."""
        var = self.rng.uniform(-3.0, 3.0)

        if profile == WorkloadProfile.COMPUTE_HEAVY:
            base_cpu = 62.0 + var
            base_mem = 55.0 + var
            base_disk = 50.0 + var
            net_in = 250.0 + var * 5
            net_out = 200.0 + var * 5
            disk_r = 120.0 + var * 2
            disk_w = 60.0 + var * 2
        elif profile == WorkloadProfile.MEMORY_HEAVY:
            base_cpu = 45.0 + var
            base_mem = 72.0 + var
            base_disk = 50.0 + var
            net_in = 200.0 + var * 5
            net_out = 180.0 + var * 5
            disk_r = 100.0 + var * 2
            disk_w = 50.0 + var * 2
        elif profile == WorkloadProfile.NETWORK_HEAVY:
            base_cpu = 48.0 + var
            base_mem = 55.0 + var
            base_disk = 50.0 + var
            net_in = 550.0 + var * 10
            net_out = 480.0 + var * 10
            disk_r = 150.0 + var * 2
            disk_w = 80.0 + var * 2
        elif profile == WorkloadProfile.STORAGE_HEAVY:
            base_cpu = 45.0 + var
            base_mem = 55.0 + var
            base_disk = 68.0 + var
            net_in = 200.0 + var * 5
            net_out = 150.0 + var * 5
            disk_r = 380.0 + var * 5
            disk_w = 220.0 + var * 5
        else:  # NORMAL
            base_cpu = 40.0 + var
            base_mem = 48.0 + var
            base_disk = 45.0 + var
            net_in = 180.0 + var * 5
            net_out = 140.0 + var * 5
            disk_r = 80.0 + var * 2
            disk_w = 40.0 + var * 2

        return ServerBaseline(
            server_id=server_id,
            rack_id=rack_id,
            workload_profile=profile,
            base_cpu=base_cpu,
            base_memory=base_mem,
            base_disk=base_disk,
            base_network_in=net_in,
            base_network_out=net_out,
            base_disk_read=disk_r,
            base_disk_write=disk_w,
            thermal_efficiency=self.rng.uniform(0.9, 1.1),
        )

    def inject_scenario(self, scenario_name: str, target_server_id: Optional[str] = None) -> None:
        """Deterministically trigger a specific incident scenario for testing."""
        scenario_map = {
            "cpu_overload": IncidentType.CPU_OVERLOAD,
            "overheating": IncidentType.OVERHEATING,
            "disk_saturation": IncidentType.DISK_SATURATION,
            "network_congestion": IncidentType.NETWORK_CONGESTION,
            "memory_pressure": IncidentType.MEMORY_PRESSURE,
        }

        inc_type = scenario_map.get(scenario_name.lower())
        if not inc_type:
            return

        server_id = target_server_id or self.servers[0].server_id
        inc_state = self.incident_states[server_id]

        inc_state.incident_type = inc_type
        inc_state.phase = IncidentLifecyclePhase.START
        inc_state.current_step = 0
        inc_state.total_duration = 10  # 10 telemetry steps for scenario run

    def _update_incident_lifecycle(self, server_id: str) -> None:
        """Advance internal incident lifecycle phase for a server."""
        inc_state = self.incident_states[server_id]
        if inc_state.incident_type == IncidentType.NONE:
            return

        inc_state.current_step += 1
        dur = max(inc_state.total_duration, 4)
        progress = inc_state.current_step / float(dur)

        # Internal simulation phase progression:
        # START (0-20%) -> WARNING (20-50%) -> CRITICAL (50-80%) -> RECOVERY (80-100%) -> NORMAL
        if progress < 0.2:
            inc_state.phase = IncidentLifecyclePhase.START
        elif progress < 0.5:
            inc_state.phase = IncidentLifecyclePhase.WARNING
        elif progress < 0.8:
            inc_state.phase = IncidentLifecyclePhase.CRITICAL
        elif progress < 1.0:
            inc_state.phase = IncidentLifecyclePhase.RECOVERY
        else:
            inc_state.phase = IncidentLifecyclePhase.NORMAL
            inc_state.incident_type = IncidentType.NONE
            inc_state.current_step = 0

    def _maybe_schedule_global_incident(self) -> None:
        """
        Controlled global scheduler to inject low-frequency incidents.
        Prevents incident chaos when scaling to 200+ servers.
        """
        if not self.config.enable_incidents:
            return

        if self.global_step % self.config.incident_check_interval == 0:
            if self.rng.random() < self.config.incident_chance:
                # Pick a server currently without an incident
                idle_servers = [
                    s.server_id
                    for s in self.servers
                    if self.incident_states[s.server_id].incident_type == IncidentType.NONE
                ]
                if idle_servers:
                    chosen_server = self.rng.choice(idle_servers)
                    incident_types = [
                        IncidentType.CPU_OVERLOAD,
                        IncidentType.OVERHEATING,
                        IncidentType.DISK_SATURATION,
                        IncidentType.NETWORK_CONGESTION,
                        IncidentType.MEMORY_PRESSURE,
                    ]
                    chosen_type = self.rng.choice(incident_types)

                    inc_state = self.incident_states[chosen_server]
                    inc_state.incident_type = chosen_type
                    inc_state.phase = IncidentLifecyclePhase.START
                    inc_state.current_step = 0
                    inc_state.total_duration = self.rng.randint(
                        self.config.incident_min_duration, self.config.incident_max_duration
                    )

    def generate_step(self, custom_timestamp: Optional[str] = None) -> List[TelemetryRecord]:
        """
        Advance simulator state by one step and generate telemetry records for all servers.
        """
        self.global_step += 1
        if not self.scenario:
            self._maybe_schedule_global_incident()

        now_str = custom_timestamp or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        records: List[TelemetryRecord] = []

        for server in self.servers:
            sid = server.server_id
            self._update_incident_lifecycle(sid)

            rec = self._generate_server_telemetry(server, now_str)
            records.append(rec)

        return records

    def _generate_server_telemetry(self, server: ServerBaseline, timestamp: str) -> TelemetryRecord:
        """Generate smooth, correlated, clamped telemetry for a single server."""
        sid = server.server_id
        prev = self.server_states[sid]
        inc_state = self.incident_states[sid]

        # ---------------------------------------------------------
        # 1. Autoregressive random walk for primary workload drivers
        # ---------------------------------------------------------
        alpha = 0.85  # Continuity factor

        # Compute raw target deltas from baseline
        target_cpu = alpha * prev["cpu"] + (1 - alpha) * server.base_cpu + self.rng.uniform(-2.5, 2.5)
        target_mem = alpha * prev["memory"] + (1 - alpha) * server.base_memory + self.rng.uniform(-1.5, 1.5)
        target_disk = alpha * prev["disk"] + (1 - alpha) * server.base_disk + self.rng.uniform(-0.5, 0.5)

        target_net_in = alpha * prev["network_in"] + (1 - alpha) * server.base_network_in + self.rng.uniform(-15.0, 15.0)
        target_net_out = alpha * prev["network_out"] + (1 - alpha) * server.base_network_out + self.rng.uniform(-12.0, 12.0)

        # ---------------------------------------------------------
        # 2. Incident Modifiers based on Lifecycle Phase & Type
        # ---------------------------------------------------------
        if inc_state.incident_type != IncidentType.NONE:
            # Curve factor: 0 at start, ramps to 1 at critical, drops during recovery
            progress = inc_state.current_step / float(max(inc_state.total_duration, 1))
            if progress <= 0.7:
                multiplier = math.sin((progress / 0.7) * (math.pi / 2))
            else:
                multiplier = math.cos(((progress - 0.7) / 0.3) * (math.pi / 2))

            if inc_state.incident_type == IncidentType.CPU_OVERLOAD:
                target_cpu += multiplier * 55.0  # Ramps CPU towards critical (>90%)
            elif inc_state.incident_type == IncidentType.OVERHEATING:
                target_cpu += multiplier * 35.0  # CPU boost drives thermal buildup
            elif inc_state.incident_type == IncidentType.DISK_SATURATION:
                target_disk += multiplier * 45.0  # Ramps Disk Util towards critical (>90%)
            elif inc_state.incident_type == IncidentType.NETWORK_CONGESTION:
                target_net_in += multiplier * 600.0   # Ramps Net In/Out (>900 Mbps)
                target_net_out += multiplier * 550.0
                target_cpu += multiplier * 15.0
            elif inc_state.incident_type == IncidentType.MEMORY_PRESSURE:
                target_mem += multiplier * 48.0   # Ramps Memory towards critical (>90%)
                target_cpu += multiplier * 10.0

        # Update primary metrics
        cpu = target_cpu
        memory = target_mem
        disk = target_disk
        net_in = target_net_in
        net_out = target_net_out

        # ---------------------------------------------------------
        # 3. Physical Correlations
        # ---------------------------------------------------------
        # Correlation 1: CPU -> Power Consumption
        # Base idle ~260W + ~4.6W per 1% CPU + noise
        power = 260.0 + (cpu * 4.6) + self.rng.uniform(-8.0, 8.0)

        # Correlation 2 & Thermal Inertia: CPU -> Target Temperature -> Temperature (Gradual Transition)
        # Target Temp = Ambient (20°C) + CPU contribution + thermal efficiency offset
        heat_buildup = 0.0
        if inc_state.incident_type == IncidentType.OVERHEATING:
            # Overheating incident adds direct thermal load (e.g. cooling failure)
            progress = inc_state.current_step / float(max(inc_state.total_duration, 1))
            mult = math.sin(progress * math.pi)
            heat_buildup = mult * 18.0

        target_temp = 20.0 + (cpu * 0.22) * server.thermal_efficiency + heat_buildup + self.rng.uniform(-0.5, 0.5)

        # Apply Thermal Inertia: Temp_t = (1 - gamma) * Temp_{t-1} + gamma * Target_Temp
        gamma = 0.15  # Thermal inertia coefficient (changes gradually over steps)
        temperature = (1.0 - gamma) * prev["temperature"] + gamma * target_temp

        # Correlation 3: Temperature -> Fan Speed
        # Fan ramps up as temperature increases: ~2200 RPM base + 125 RPM per °C above 18°C
        fan_speed = 2200.0 + max(0.0, temperature - 18.0) * 135.0 + self.rng.uniform(-50.0, 50.0)

        # Correlation 4: Disk Utilization & Workload -> Disk Read / Write
        read_mult = 1.0 + (disk / 100.0) * 1.5
        write_mult = 1.0 + (disk / 100.0) * 1.2
        if inc_state.incident_type == IncidentType.DISK_SATURATION:
            progress = inc_state.current_step / float(max(inc_state.total_duration, 1))
            mult = math.sin(progress * math.pi)
            read_mult += mult * 4.0
            write_mult += mult * 3.5

        disk_read = server.base_disk_read * read_mult + self.rng.uniform(-10.0, 10.0)
        disk_write = server.base_disk_write * write_mult + self.rng.uniform(-8.0, 8.0)

        # ---------------------------------------------------------
        # 4. Strict Inclusive Hard Bounds Clamping
        # ---------------------------------------------------------
        cpu = self._clamp("cpu_utilization", cpu)
        memory = self._clamp("memory_utilization", memory)
        disk = self._clamp("disk_utilization", disk)
        net_in = self._clamp("network_in", net_in)
        net_out = self._clamp("network_out", net_out)
        temperature = self._clamp("temperature", temperature)
        power = self._clamp("power_consumption", power)
        fan_speed = self._clamp("fan_speed", fan_speed)
        disk_read = self._clamp("disk_read", disk_read)
        disk_write = self._clamp("disk_write", disk_write)

        # Update stored state for next iteration step
        self.server_states[sid] = {
            "cpu": cpu,
            "memory": memory,
            "disk": disk,
            "network_in": net_in,
            "network_out": net_out,
            "temperature": temperature,
            "power": power,
            "fan_speed": fan_speed,
            "disk_read": disk_read,
            "disk_write": disk_write,
        }

        return TelemetryRecord(
            timestamp=timestamp,
            server_id=server.server_id,
            rack_id=server.rack_id,
            cpu_utilization=cpu,
            memory_utilization=memory,
            disk_utilization=disk,
            network_in=net_in,
            network_out=net_out,
            temperature=temperature,
            power_consumption=power,
            fan_speed=fan_speed,
            disk_read=disk_read,
            disk_write=disk_write,
        )

    def _clamp(self, metric_name: str, value: float) -> float:
        """Clamp value strictly inside inclusive hard bounds [min_val, max_val]."""
        bounds = HARD_BOUNDS[metric_name]
        return max(bounds.min_val, min(bounds.max_val, value))
