# Cloud-Native Data Center Infrastructure Monitoring Platform
## Step 1: Realistic Data-Center Telemetry Simulator

This repository contains **Step 1** of the MSc project: a Python-based data-center telemetry simulator that generates realistic, correlated, continuous server telemetry metrics across multi-rack infrastructure.

---

## 1. Project Purpose & Architectural Position

The telemetry simulator acts as the data generator representing physical data center infrastructure reporting raw hardware metrics.

### End-to-End Platform Architecture:
```
┌──────────────────────────────┐
│  Python Telemetry Generator  │  (Step 1 - Current Component)
└──────────────┬───────────────┘
               │ (Produces JSON telemetry events)
               ▼
┌──────────────────────────────┐
│        Apache Kafka          │  (Step 2 - Stream Ingestion)
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│          Databricks          │  (Step 3 - Real-Time Analytics & Alert Engine)
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│     AWS SNS / SMS Alerts     │  (Step 4 - Notification Layer)
└──────────────────────────────┘
```

> **Architectural Separation Principle**: The simulator produces **pure raw telemetry**. Fields like `health_status`, `alert`, `severity`, or `sms_sent` are **intentionally omitted** from raw events. Stream processing engines (Databricks) evaluate operational state downstream.

---

## 2. Telemetry Fields & Units

Each telemetry event is output as a single JSON object containing 13 raw metric fields:

| Metric Field | Unit | Simulation Bounds (Inclusive) | Description |
| :--- | :--- | :--- | :--- |
| `timestamp` | ISO 8601 UTC | - | UTC timestamp string (`YYYY-MM-DDTHH:MM:SSZ`) |
| `server_id` | String | - | Server identifier (`SRV001`, `SRV002`, ...) |
| `rack_id` | String | - | Rack identifier (`RACK01`, `RACK02`, ...) |
| `cpu_utilization` | % | 0.0 – 100.0 | Processing unit workload utilization |
| `memory_utilization` | % | 0.0 – 100.0 | RAM utilization percentage |
| `disk_utilization` | % | 0.0 – 100.0 | Storage volume occupancy percentage |
| `network_in` | Mbps | 0.0 – 1000.0 | Inbound network throughput |
| `network_out` | Mbps | 0.0 – 1000.0 | Outbound network throughput |
| `temperature` | °C | 18.0 – 50.0 | Thermal sensor reading with thermal inertia |
| `power_consumption` | Watts | 250.0 – 850.0 | Server chassis power draw |
| `fan_speed` | RPM | 2000 – 7000 | Cooling fan rotational speed |
| `disk_read` | MB/s | 0.0 – 800.0 | Storage disk read throughput |
| `disk_write` | MB/s | 0.0 – 600.0 | Storage disk write throughput |

---

## 3. Simulation Threshold Assumptions

> **Important Note**: The threshold boundaries below represent **simulation assumptions** for a generic data-center server model. They are not universal hardware specifications across all physical vendors.

| Metric | Normal Range | Warning Range | Critical Range | Hard Bounds (Inclusive) |
| :--- | :--- | :--- | :--- | :--- |
| **CPU Utilization** | 20% – 75% | 75% – 90% | > 90% | 0.0% – 100.0% |
| **Memory Utilization** | 30% – 75% | 75% – 90% | > 90% | 0.0% – 100.0% |
| **Disk Utilization** | 30% – 80% | 80% – 90% | > 90% | 0.0% – 100.0% |
| **Temperature** | 20°C – 35°C | 35°C – 40°C | > 40°C | 18.0°C – 50.0°C |
| **Network In / Out** | 50 – 700 Mbps | 700 – 900 Mbps | > 900 Mbps | 0.0 – 1000.0 Mbps |
| **Power Consumption** | 300 – 600 W | 600 – 750 W | > 750 W | 250.0 – 850.0 W |
| **Fan Speed** | 2500 – 5000 RPM | 5000 – 6500 RPM | > 6500 RPM | 2000 – 7000 RPM |
| **Disk Read** | 20 – 500 MB/s | 500 – 700 MB/s | > 700 MB/s | 0.0 – 800.0 MB/s |
| **Disk Write** | 10 – 300 MB/s | 300 – 500 MB/s | > 500 MB/s | 0.0 – 600.0 MB/s |

---

## 4. Key Metric Relationships & Thermal Inertia

Metrics are not generated independently; the physics engine enforces realistic physical dependencies:

1. **CPU Utilization $\rightarrow$ Power Consumption**: Power draw scales positively with CPU load ($\approx 260\text{W} + 4.6 \times \text{CPU}\%$).
2. **CPU Utilization $\rightarrow$ Temperature (Thermal Inertia)**: Higher CPU workloads generate heat. Temperature transitions smoothly over steps using physical thermal inertia:
   $$\text{Temp}_t = (1 - \gamma) \cdot \text{Temp}_{t-1} + \gamma \cdot \text{TargetTemp}(\text{CPU}_t)$$
3. **Temperature $\rightarrow$ Fan Speed**: Cooling fans ramp up as server chassis temperature increases.
4. **Disk Utilization $\rightarrow$ Disk Read / Write**: High disk activity scales I/O throughput.
5. **Independent Network Fluctuation**: Network traffic fluctuates independently to model varied network workloads.

---

## 5. Workload Profiles

Each server in the topology is assigned a baseline profile at initialization:
- `NORMAL`: Standard balanced datacenter server baseline.
- `COMPUTE_HEAVY`: High baseline CPU utilization, higher power and temperature.
- `MEMORY_HEAVY`: Elevated RAM occupancy and moderate CPU increase.
- `NETWORK_HEAVY`: Higher network throughput (In/Out) with moderate CPU usage.
- `STORAGE_HEAVY`: High disk read/write throughput and elevated disk utilization.

---

## 6. Incident Simulation & Internal Lifecycle

The simulator supports controlled multi-phase incidents for testing downstream alerting:

### Incident Types:
- `CPU_OVERLOAD` (CPU spikes $> 90\%$)
- `OVERHEATING` (Chassis Temperature exceeds $> 40^\circ\text{C}$)
- `DISK_SATURATION` (Disk occupancy exceeds $> 90\%$)
- `NETWORK_CONGESTION` (Network throughput exceeds $> 900\text{ Mbps}$)
- `MEMORY_PRESSURE` (RAM utilization exceeds $> 90\%$)

### Internal Lifecycle Progression:
```
  NORMAL ──► START (Internal transition) ──► WARNING ──► CRITICAL ──► RECOVERY ──► NORMAL
```
> Note: `START` is strictly an internal simulator lifecycle state transition, not an operational alert severity.

Incidents evolve continuously over multiple telemetry records, enabling stream processors to track event duration and escalation.

---

## 7. How to Run the Simulator

### Prerequisites
- Python 3.9+
- Standard Python Library (`argparse`, `dataclasses`, `json`, `math`, `random`, `time`)
- `pytest` for running test suites

Install test requirements:
```bash
pip install -r requirements.txt
```

### Command-Line Usage

#### 1. Continuous Mode (Streaming every 5 seconds)
```bash
python producer/telemetry_generator.py
```

#### 2. Test Mode (Generate finite records immediately without sleep)
```bash
python producer/telemetry_generator.py --test --records 20 --seed 42
```

#### 3. Deterministic Scenario Testing Mode
Inject a specific incident type via `--scenario`:
```bash
python producer/telemetry_generator.py --test --records 15 --scenario cpu_overload
```

Available scenarios: `normal`, `cpu_overload`, `overheating`, `disk_saturation`, `network_congestion`, `memory_pressure`.

#### 4. Scalability Options
Scale topology up to 200+ servers easily:
```bash
python producer/telemetry_generator.py --test --records 200 --racks 20 --servers-per-rack 10
```

---

## 8. Running Unit & Scenario Tests

Execute the full pytest test suite covering all 18 metric bounds, schema integrity, JSON serialization, causal relationship, and incident lifecycle checks:

```bash
python -m pytest -v
```
