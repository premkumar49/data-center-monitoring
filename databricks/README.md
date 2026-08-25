# Step 3: Databricks Structured Streaming Pipeline

This directory contains the PySpark Structured Streaming pipeline for ingesting, validating, and analyzing real-time data center infrastructure telemetry from Apache Kafka topic `server_telemetry`.

---

## 1. Architecture Overview

```
Python Telemetry Simulator (Step 1)
       │
       ▼
Kafka Producer (Step 2)
       │
       ▼
Kafka Broker on AWS EC2 (Topic: server_telemetry)
       │
       ▼  (External Listener: <EC2_PUBLIC_IP>:9092 / :9094)
Databricks Structured Streaming (Step 3)
       │
       ├──> 01_kafka_stream.py (JSON Parsing & Explicit Schema Enforcement)
       ├──> 02_validate_telemetry.py (Hard Bounds Validation & Quarantine Stream Split)
       └──> 03_streaming_analytics.py (10-min Windowed Aggregations & Checkpointing)
```

---

## 2. Kafka Connectivity & Networking Design

### The Connectivity Requirement
Databricks runs in a separate cloud environment outside the EC2 local container network. Connecting Databricks to `localhost:9092` will fail.

### Recommended Kafka Listener Configuration on AWS EC2
In Docker Compose / Kafka `server.properties` on EC2, configure dual listeners:

```properties
KAFKA_LISTENERS=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094
KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:9092,EXTERNAL://<EC2_PUBLIC_IP_OR_DNS>:9094
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL
```

### AWS EC2 Security Group Rule
Add an Inbound Custom TCP Rule to the EC2 Security Group:
- **Protocol**: TCP
- **Port**: `9094` (or `9092` depending on configuration)
- **Source**: Databricks VPC CIDR / Cluster Public NAT IP (or restricted IP range).

---

## 3. Explicit Spark Schema

Schema enforcement ensures strict type safety across streaming micro-batches without automatic inference overhead:

| Field Name | Databricks / Spark Data Type | Description |
| :--- | :--- | :--- |
| `timestamp` | `TimestampType` | Event timestamp parsed from ISO-8601 string |
| `server_id` | `StringType` | Logical server identifier (e.g., `SRV001`) |
| `rack_id` | `StringType` | Rack identifier (e.g., `RACK01`) |
| `cpu_utilization` | `DoubleType` | CPU usage percentage (0.0 to 100.0%) |
| `memory_utilization` | `DoubleType` | RAM usage percentage (0.0 to 100.0%) |
| `disk_utilization` | `DoubleType` | Storage usage percentage (0.0 to 100.0%) |
| `network_in` | `DoubleType` | Inbound traffic (0.0 to 1000.0 Mbps) |
| `network_out` | `DoubleType` | Outbound traffic (0.0 to 1000.0 Mbps) |
| `temperature` | `DoubleType` | Server thermal temperature (18.0 to 50.0 °C) |
| `power_consumption` | `DoubleType` | Server power usage (250.0 to 850.0 W) |
| `fan_speed` | `DoubleType` | Cooling fan speed (2000.0 to 7000.0 RPM) |
| `disk_read` | `DoubleType` | Storage read throughput (0.0 to 800.0 MB/s) |
| `disk_write` | `DoubleType` | Storage write throughput (0.0 to 600.0 MB/s) |

*Raw Telemetry Integrity*: No alert decision fields (`health_status`, `alert`, `severity`, `sms_sent`) exist in this layer.

---

## 4. Telemetry Validation & Quarantine Rules

Records are evaluated against physical inclusive hard bounds defined in Step 1:
- `cpu_utilization`: `[0.0, 100.0]`
- `memory_utilization`: `[0.0, 100.0]`
- `disk_utilization`: `[0.0, 100.0]`
- `temperature`: `[18.0, 50.0]`
- `network_in`: `[0.0, 1000.0]`
- `network_out`: `[0.0, 1000.0]`
- `power_consumption`: `[250.0, 850.0]`
- `fan_speed`: `[2000.0, 7000.0]`
- `disk_read`: `[0.0, 800.0]`
- `disk_write`: `[0.0, 600.0]`
- Non-null constraints on `server_id`, `rack_id`, and `timestamp`.

Records violating any rule are routed to `quarantined_telemetry_df` for investigation without stopping the main stream.

---

## 5. Streaming Aggregations

Computes real-time windowed metrics:
1. **Average CPU utilization by server** (`avg_cpu_by_server`)
2. **Average memory utilization by server** (`avg_memory_by_server`)
3. **Average temperature by rack** (`avg_temp_by_rack`)
4. **Maximum CPU utilization by server** (`max_cpu_by_server`)
5. **Maximum temperature by rack** (`max_temp_by_rack`)
6. **Average power consumption by server** (`avg_power_by_server`)

### Event-Time Windowing & Watermarking
- **Watermark**: `.withWatermark("timestamp", "10 minutes")` handles out-of-order and late data up to 10 minutes.
- **Window**: `window(col("timestamp"), "10 minutes", "1 minute")`

---

## 6. Checkpointing & Offset Strategy

### Checkpointing Rationale
Structured Streaming uses checkpointing (`option("checkpointLocation", "/tmp/delta/checkpoints/...")`) to store query progress metadata, offset positions, and stateful aggregation state in durable cloud storage. This enables:
- Seamless recovery from driver/cluster restarts.
- Exactly-once processing semantics.
- Prevention of duplicate event reprocessing.

### Starting Offset Configuration
- `startingOffsets = "earliest"`: Used during development, testing, and historical reprocessing to process all events present in the Kafka topic from offset 0.
- `startingOffsets = "latest"`: Used in continuous production deployments to process only newly published events starting from current time.

---

## 7. How to Execute in Databricks Workspace

### Step 1: Upload Scripts to Databricks Workspace
Upload `01_kafka_stream.py`, `02_validate_telemetry.py`, and `03_streaming_analytics.py` as PySpark Notebooks or Python files.

### Step 2: Configure Workspace Widgets / Parameters
Create widgets in notebook interface or job parameters:
- `bootstrap_servers`: `<EC2_PUBLIC_IP>:9094`
- `topic`: `server_telemetry`
- `starting_offsets`: `earliest`

### Step 3: Required Spark Package Dependency
If running standard Spark outside Databricks Runtime, include Maven coordinate:
`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`
*(Pre-installed on Databricks Runtime 13.3+ LTS).*
