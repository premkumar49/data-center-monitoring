# Step 6: Stateful Real-Time Alert and Incident Detection Engine

This document provides a technical specification of the **Stateful Alert and Incident Detection Engine** developed for Step 6 of the MSc project **"Cloud-Native Data Center Infrastructure Monitoring and Real-Time Analytics Platform"**.

---

## 1. Architectural Purpose

The Alert Engine consumes **validated infrastructure telemetry** produced by Step 3 and independently evaluates whether telemetry readings represent operational problems.

```mermaid
flowchart TD
    subgraph Data Input ["Step 3 Ingestion"]
        A[Kafka: server_telemetry] --> B[Databricks Validation Layer]
        B -->|Valid Telemetry| C[04_alert_engine.py]
    end

    subgraph Alert Engine ["Step 6: Stateful Incident Engine"]
        C --> D{Per-(Server, Metric) State Machine}
        D -->|Normal Telemetry| E[No Event Emitted]
        D -->|Warning Threshold Confirmed| F[INCIDENT_OPENED (WARNING)]
        D -->|Critical Threshold Confirmed| G[INCIDENT_ESCALATED (CRITICAL)]
        D -->|Recovery Threshold Confirmed| H[INCIDENT_RECOVERY_STARTED]
        D -->|Confirmed Normal| I[INCIDENT_CLOSED]
    end

    subgraph Output Stream ["Step 6 Derived Stream"]
        F --> J[Derived Incident Stream]
        G -->|notification_required = true| J
        H --> J
        I --> J
    end

    subgraph Step 7 ["Step 7: Notification Service"]
        J -->|Filter: notification_required == true| K[AWS SNS / SMS Service]
    end

    style Data Input fill:#f9f,stroke:#333,stroke-width:1px
    style Alert Engine fill:#bbf,stroke:#333,stroke-width:1px
    style Output Stream fill:#dfd,stroke:#333,stroke-width:1px
    style Step 7 fill:#ffd,stroke:#333,stroke-width:1px
```

- **Independent Decision Making**: The Alert Engine does not trust any simulator incident fields.
- **Raw Telemetry Preservation**: The raw Kafka topic `server_telemetry` remains 100% untouched. No `alert`, `severity`, or `health_status` fields pollute raw infrastructure data.

---

## 2. Five Alert Categories & Threshold Specification

Thresholds are split into **Warning**, **Critical**, and **Recovery (Hysteresis)** levels:

| Category | Metric Field | Warning Threshold | Critical Threshold | Recovery Threshold | Unit |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `CPU_OVERLOAD` | `cpu_utilization` | $\ge 80.0\%$ | $\ge 90.0\%$ | $< 75.0\%$ | $\%$ |
| `MEMORY_PRESSURE` | `memory_utilization` | $\ge 80.0\%$ | $\ge 90.0\%$ | $< 75.0\%$ | $\%$ |
| `DISK_SATURATION` | `disk_utilization` | $\ge 85.0\%$ | $\ge 95.0\%$ | $< 80.0\%$ | $\%$ |
| `OVERHEATING` | `temperature` | $\ge 38.0^\circ\text{C}$ | $\ge 42.0^\circ\text{C}$ | $< 35.0^\circ\text{C}$ | $^\circ\text{C}$ |
| `NETWORK_CONGESTION` | $\max(\text{net\_in}, \text{net\_out})$ | $\ge 750.0\text{ Mbps}$ | $\ge 900.0\text{ Mbps}$ | $< 700.0\text{ Mbps}$ | $\text{Mbps}$ |

### Hysteresis Rationale
Recovery thresholds are set strictly lower than warning thresholds. For example, CPU warning is triggered at 80%, but recovery requires CPU to drop below 75%. This prevents rapid oscillation between `OPEN` and `CLOSED` states when metrics hover near boundary values.

---

## 3. Incident State Machine

Each `(server_id, incident_type)` tuple maintains an independent state machine:

```
  +--------+   2x Warning   +---------+   2x Critical  +----------+
  | NORMAL | -------------> | WARNING | -------------> | CRITICAL |
  +--------+                +---------+                +----------+
      ^                          |                          |
      | 2x Recovery              | 2x Recovery              | 2x Recovery
      |                          v                          v
      +-------------------- +----------+ <------------------+
                            | RECOVERY |
                            +----------+
```

### Confirmation Rule Requirements
- **Warning Confirmation**: Requires `2` consecutive abnormal observations before transitioning to `WARNING`.
- **Critical Confirmation**: Requires `2` consecutive critical observations before transitioning to `CRITICAL`.
- **Recovery Confirmation**: Requires `2` consecutive recovery observations before transitioning to `RECOVERY` and closing.

---

## 4. Alert Deduplication Architecture

Sustained metric spikes (e.g. CPU at 92%, 94%, 96%, 95%, 93%) emit **ONE OPEN INCIDENT** (`INC-20260825-SRV001-CPU-0001`).
- The existing incident record's `last_seen` and `current_value` fields are updated.
- Duplicate incident records are **never** created for ongoing samples.
- `notification_required` is set to `True` **only once** upon initial transition/escalation to `CRITICAL`, preventing SMS spamming.

---

## 5. Multi-Incident Independence Per Server

A single server (e.g. `SRV007`) can experience multiple simultaneous issues (e.g. CPU 96% and Temperature 43°C).
- `(SRV007, CPU_OVERLOAD)` and `(SRV007, OVERHEATING)` maintain separate state instances.
- Independent `incident_id` values are assigned to each category.

---

## 6. State Timeout Strategy

If telemetry from a server stops arriving while an incident is open (e.g. server crash or network failure), the state engine evaluates `state_timeout_minutes = 30`. If `last_seen` exceeds 30 minutes, the incident automatically closes with an `INCIDENT_CLOSED (State Timeout)` event to prevent stale state accumulation.

---

## 7. Derived Incident Stream Output Schema

| Field Name | Data Type | Description |
| :--- | :--- | :--- |
| `incident_id` | `String` | Unique traceable identifier (e.g., `INC-20260825-SRV001-CPU-0001`) |
| `timestamp` | `String` | Event timestamp ISO-8601 |
| `server_id` | `String` | Target server ID (e.g., `SRV001`) |
| `rack_id` | `String` | Target rack ID (e.g., `RACK01`) |
| `incident_type` | `String` | Category (`CPU_OVERLOAD`, `OVERHEATING`, etc.) |
| `severity` | `String` | `WARNING` or `CRITICAL` |
| `status` | `String` | `OPEN`, `RECOVERING`, or `CLOSED` |
| `event_type` | `String` | `INCIDENT_OPENED`, `INCIDENT_ESCALATED`, `INCIDENT_RECOVERY_STARTED`, `INCIDENT_CLOSED` |
| `first_seen` | `String` | First timestamp when problem was detected |
| `last_seen` | `String` | Latest observation timestamp |
| `current_value` | `Double` | Current metric reading |
| `threshold` | `Double` | Evaluation threshold value |
| `notification_required` | `Boolean` | `true` if eligible for Step 7 SMS notification |
| `message` | `String` | Human-readable alert summary formatted for SMS |

---

## 8. Preparation for Step 7 (AWS SNS / SMS)

- **Step 6 Scope Limit**: Step 6 produces the derived incident stream and tags `notification_required`. **No SMS or AWS SNS calls are executed in Step 6.**
- **Step 7 Handoff**: Step 7 will filter the derived incident stream for records where `notification_required == true` and publish the pre-formatted `message` to AWS SNS topics for SMS delivery to system administrators.
