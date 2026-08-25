"""
Scenario and physical correlation tests for Data Center Telemetry Simulator.

Validates causal relationships (CPU->Power, CPU->Temp, Temp->Fan) and
deterministic incident scenario progressions.
"""

import pytest
from producer.server_config import SimulationConfig
from producer.simulator import DataCenterSimulator


def test_causal_cpu_to_power_relationship():
    """Verify CPU increase causes Power consumption increase."""
    sim = DataCenterSimulator(SimulationConfig(enable_incidents=False), seed=10)

    # Collect sample data over 50 steps
    cpu_vals = []
    power_vals = []
    for _ in range(50):
        records = sim.generate_step()
        for r in records:
            cpu_vals.append(r.cpu_utilization)
            power_vals.append(r.power_consumption)

    # Compute correlation direction (covariance)
    mean_cpu = sum(cpu_vals) / len(cpu_vals)
    mean_power = sum(power_vals) / len(power_vals)

    covariance = sum((c - mean_cpu) * (p - mean_power) for c, p in zip(cpu_vals, power_vals))
    assert covariance > 0, "CPU and Power should have a strong positive correlation"


def test_causal_cpu_to_temperature_relationship():
    """Verify CPU increase causes Target Temperature increase."""
    sim_normal = DataCenterSimulator(SimulationConfig(enable_incidents=False), seed=20)
    sim_heavy = DataCenterSimulator(SimulationConfig(enable_incidents=False), scenario="cpu_overload", seed=20)

    # Step through 8 iterations
    normal_temps = []
    heavy_temps = []
    for _ in range(8):
        rec_n = sim_normal.generate_step()[0]
        rec_h = sim_heavy.generate_step()[0]
        normal_temps.append(rec_n.temperature)
        heavy_temps.append(rec_h.temperature)

    # Overloading CPU should result in higher average temperature due to thermal buildup
    assert sum(heavy_temps) > sum(normal_temps), "High CPU workload should produce higher temperature"


def test_causal_temperature_to_fan_speed_relationship():
    """Verify Temperature increase causes Fan Speed increase."""
    sim = DataCenterSimulator(SimulationConfig(enable_incidents=False), scenario="overheating", seed=30)

    temp_vals = []
    fan_vals = []
    for _ in range(10):
        rec = sim.generate_step()[0]
        temp_vals.append(rec.temperature)
        fan_vals.append(rec.fan_speed)

    # Covariance between temperature and fan speed
    mean_temp = sum(temp_vals) / len(temp_vals)
    mean_fan = sum(fan_vals) / len(fan_vals)

    cov = sum((t - mean_temp) * (f - mean_fan) for t, f in zip(temp_vals, fan_vals))
    assert cov > 0, "Temperature and Fan speed should have positive correlation"


def test_scenario_cpu_overload():
    """Verify CPU_OVERLOAD scenario ramps CPU above 90% critical threshold."""
    sim = DataCenterSimulator(SimulationConfig(enable_incidents=False), scenario="cpu_overload", seed=42)
    max_cpu = 0.0

    for _ in range(12):
        recs = sim.generate_step()
        target_rec = recs[0]  # SRV001 receives the injected scenario
        if target_rec.cpu_utilization > max_cpu:
            max_cpu = target_rec.cpu_utilization

    assert max_cpu > 90.0, f"CPU_OVERLOAD scenario failed to reach critical >90%, max was {max_cpu}%"


def test_scenario_overheating():
    """Verify OVERHEATING scenario ramps Temperature above 40°C critical threshold."""
    sim = DataCenterSimulator(SimulationConfig(enable_incidents=False), scenario="overheating", seed=42)
    max_temp = 0.0

    for _ in range(12):
        recs = sim.generate_step()
        target_rec = recs[0]
        if target_rec.temperature > max_temp:
            max_temp = target_rec.temperature

    assert max_temp > 40.0, f"OVERHEATING scenario failed to reach critical >40°C, max was {max_temp}°C"


def test_scenario_disk_saturation():
    """Verify DISK_SATURATION scenario ramps Disk Utilization above 90% critical threshold."""
    sim = DataCenterSimulator(SimulationConfig(enable_incidents=False), scenario="disk_saturation", seed=42)
    max_disk = 0.0

    for _ in range(12):
        recs = sim.generate_step()
        target_rec = recs[0]
        if target_rec.disk_utilization > max_disk:
            max_disk = target_rec.disk_utilization

    assert max_disk > 90.0, f"DISK_SATURATION scenario failed to reach critical >90%, max was {max_disk}%"


def test_scenario_network_congestion():
    """Verify NETWORK_CONGESTION scenario ramps Network In above 900 Mbps critical threshold."""
    sim = DataCenterSimulator(SimulationConfig(enable_incidents=False), scenario="network_congestion", seed=42)
    max_net = 0.0

    for _ in range(12):
        recs = sim.generate_step()
        target_rec = recs[0]
        if target_rec.network_in > max_net:
            max_net = target_rec.network_in

    assert max_net > 900.0, f"NETWORK_CONGESTION scenario failed to reach critical >900 Mbps, max was {max_net} Mbps"


def test_scenario_memory_pressure():
    """Verify MEMORY_PRESSURE scenario ramps Memory Utilization above 90% critical threshold."""
    sim = DataCenterSimulator(SimulationConfig(enable_incidents=False), scenario="memory_pressure", seed=42)
    max_mem = 0.0

    for _ in range(12):
        recs = sim.generate_step()
        target_rec = recs[0]
        if target_rec.memory_utilization > max_mem:
            max_mem = target_rec.memory_utilization

    assert max_mem > 90.0, f"MEMORY_PRESSURE scenario failed to reach critical >90%, max was {max_mem}%"
