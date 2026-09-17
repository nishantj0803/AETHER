# Aether Chaos Engineering Suite

This directory contains **dynamic chaos experiments** that inject real container, network, and storage faults into a running Aether deployment.

Unlike unit-level resilience tests (`tests/test_resilience.py` and `tests/test_ingestion.py`), which simulate faults using mocks, the scripts here execute against **live running containers** (Docker Compose or Kubernetes).

---

## Chaos Experiments Overview

| Script | Target Component | Injected Fault | Expected System Behavior |
|---|---|---|---|
| `experiment_kafka_kill.sh` | Redpanda / Kafka | Container kill / pause during active traffic | Ingestion worker enters reconnect backoff; Go collector buffers in memory; zero offsets committed during downtime; zero message loss upon recovery. |
| `experiment_postgres_pause.sh` | PostgreSQL 16 | Container pause (`docker pause`) | Services gracefully degrade to in-memory fallback; connection pool retries; transactions cleanly abort without partial writes. |
| `experiment_network_latency.sh` | Target Payment Service | Injected 800ms network delay via `tc` / synthetic proxy | Timeout guards trigger; Prometheus p95 latency breaches SLO; Anomaly Detector flags incident; RCA agent identifies latency degradation. |
| `chaos_runner.py` | Full Platform | Orchestrates end-to-end chaos matrix and asserts recovery invariants | Automated verification of platform recovery time (MTTR < 20s). |

---

## Running Chaos Experiments Locally

### Prerequisites
1. Docker Compose running:
   ```bash
   docker compose up -d
   ```
2. Traffic generator active:
   ```bash
   python scripts/traffic_generator.py --rps 25
   ```

### 1. Kafka Broker Outage Experiment
```bash
chmod +x chaos/experiment_kafka_kill.sh
./chaos/experiment_kafka_kill.sh
```

### 2. Database Partition Experiment
```bash
chmod +x chaos/experiment_postgres_pause.sh
./chaos/experiment_postgres_pause.sh
```

### 3. Automated Chaos Suite Runner
```bash
PYTHONPATH=. python chaos/chaos_runner.py --experiment all
```
