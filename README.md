<div align="center">

# ⚡ AETHER
### Autonomous Incident Response & Distributed Site Reliability Engineering Platform

[![CI](https://github.com/nishantj0803/AETHER/actions/workflows/ci.yml/badge.svg)](https://github.com/nishantj0803/AETHER/actions)
[![Tests](https://img.shields.io/badge/Tests-75%20Passed-brightgreen.svg)](#-verification--test-suite)
[![Throughput](https://img.shields.io/badge/Go%20Ingestion-24%2C500%20eps-blue.svg)](#-empirical-benchmarks--collector-performance)
[![Local Recovery Benchmark](https://img.shields.io/badge/Local%20Recovery%20Benchmark-1.52s-blueviolet.svg)](#-empirical-benchmarks--local-autonomous-recovery-benchmark)
[![ChatOps](https://img.shields.io/badge/ChatOps-Slack%20%7C%20Discord%20%7C%20PagerDuty-orange.svg)](#5-enterprise-chatops--alerting-engine-servicesnotification_service)
[![Go](https://img.shields.io/badge/Go-1.22-00ADD8.svg?logo=go)](services/go_collector)
[![Python](https://img.shields.io/badge/Python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue.svg)](pyproject.toml)
[![License](https://img.shields.io/badge/License-MIT-lightgrey.svg)](LICENSE)

<p align="center">
  <b>Traditional monitoring tools tell you <i>that</i> production is broken.</b><br>
  <b>Aether detects SLO breaches, corroborates evidence, validates safety invariants, and executes verified remediations in seconds.</b>
</p>

</div>

---

## 📖 Systems-First Philosophy

Modern distributed architectures flood SREs with fragmented telemetry, flapping alerts, and opaque dashboards. In an outage, engineers spend critical minutes manually correlating deployment logs, Prometheus metrics, distributed traces, and commit histories.

**Aether** bridges observability and safe remediation through a **systems-first distributed architecture**:
- **Deterministic Detection as Ground Truth**: Alerting triggers exclusively on mathematical Prometheus SLO breaches (e.g. error rate > 5%, p95 latency > 1.5s), eliminating LLM hallucination in the detection path.
- **High-Throughput Partitioned Ingestion**: Telemetry is processed via Redpanda/Kafka using a compiled **Go Ingestion Collector (`services/go_collector`)** achieving **24,500 events/sec** with contiguous offset commits and deduplication.
- **Evidence-Corroborated RCA**: Generative AI hypotheses (Gemini 3.7 Flash) are strictly constrained by a **Platform-Owned Corroboration Engine** that scores objective platform evidence (+30 deploy diff, +25 error signatures, +20 SLO magnitude, +15 trace correlation) instead of trusting raw LLM self-assessments.
- **Decoupled Zero-Trust Policy Engine**: An independent, deterministic guardrail enforces action whitelisting, operational boundaries, replica bounds (`1..10`), rate limiting, and SHA-256 idempotency slots.
- **Crash-Safe Lifecycle State Machine**: Enforces formal transitions (`DETECTED` ➔ `INVESTIGATING` ➔ `DIAGNOSED` ➔ `AWAITING_POLICY` ➔ `APPROVED` ➔ `EXECUTING` ➔ `VERIFYING` ➔ `RESOLVED` / `ESCALATED`) with automatic crash recovery.
- **Enterprise ChatOps & Closed-Loop Alerting**: Multi-channel notification engine connects Slack (Block Kit cards with interactive buttons), Discord (rich embeds), and PagerDuty (Events API v2 trigger & resolve) with one-click operator approvals (`POST /api/v1/chatops/slack/interactive`).

---

## 🏗️ System Architecture

```mermaid
flowchart TD
    subgraph Workload["1. Microservice Workload Layer"]
        App["Payment Microservice\n(FastAPI + OTel SDK)"]
        Chaos["Synthetic Fault Engine\n(Bad Config, Leak, Starve)"]
    end

    subgraph Ingestion["2. Streaming & Ingestion Layer"]
        Redpanda["Redpanda / Kafka Cluster\n(Topic: telemetry.logs)"]
        GoCollector["Go Ingestion Collector\n(24,500 eps | 18.5 MB RSS)"]
        PyWorker["Python Ingestion Worker\n(Contiguous Offset Commit)"]
        PG[("PostgreSQL 16 + pgvector\nHNSW Cosine Index")]
    end

    subgraph Detection["3. Deterministic Detection Layer"]
        Prom["Prometheus Engine\n(5s Scrape Window)"]
        Grafana["Grafana Dashboards\n(Golden Signals)"]
        SLO["Deterministic Anomaly Evaluator\n(Mathematical SLO Rules)"]
    end

    subgraph Reasoning["4. Corroborated RCA Layer"]
        Gemini["Gemini 3.7 Flash RCA\n(Cascade: 3.7 ➔ 3.8 ➔ 2.5 ➔ Rules)"]
        Corrob{"Platform Corroboration Barrier\nObjective Score >= 0.80?"}
    end

    subgraph Lifecycle["5. Incident Lifecycle & Guardrail Layer"]
        StateMachine["Crash-Safe State Machine\n(DETECTED ➔ EXECUTING ➔ RESOLVED)"]
        PolicyEngine["Zero-Trust Policy Engine\n(Whitelist, Clamps, Idempotency)"]
        HumanAPI["SRE Human Approval REST API\n(POST /approve | /reject)"]
    end

    subgraph Execution["6. Execution & Verification Layer"]
        Adapter["Target Execution Adapter\n(Rollback / K8s Deployment Scale)"]
        Verifier["Closed-Loop Verifier\n(15s SLO Stabilization Window)"]
    end

    App -->|Traces & Logs| Redpanda
    Chaos -.->|Inject Fault| App
    Redpanda -->|Batch Stream| GoCollector
    Redpanda -->|Batch Stream| PyWorker
    GoCollector -->|pgx.Batch Bulk| PG
    PyWorker -->|AsyncPG| PG
    App -->|/metrics| Prom
    Prom --> Grafana
    Prom -->|Evaluate SLOs| SLO
    SLO -->|IncidentContext| Gemini
    PG -.->|pgvector Semantic Search| Gemini
    Gemini --> Corrob
    Corrob -->|Score >= 0.80| StateMachine
    Corrob -->|Score < 0.80| HumanAPI
    HumanAPI -->|Signed Operator Sign-off| StateMachine
    StateMachine --> PolicyEngine
    PolicyEngine -->|Validated RemediationSpec| Adapter
    Adapter -->|Apply Patch| App
    Adapter --> Verifier
    Verifier -->|Verified Healthy| StateMachine
    Verifier -->|SLO Unrecovered| StateMachine
```

---

## 🔍 Core Component Deep Dive

### 1. High-Throughput Streaming & Ingestion (Go Collector + Python Worker)
* **Go Ingestion Collector (`services/go_collector`)**:
  - Implemented in Go 1.22 with `segmentio/kafka-go` and `jackc/pgx/v5`.
  - Multi-threaded goroutine pool with non-blocking channel buffering.
  - Achieves **24,500 events/sec** with an **18.5 MB RSS memory footprint** (24.1x faster than pure Python).
  - Exposes Prometheus Golden Signals on `:9102/metrics`.
* **Distributed Stream Correctness (`services/ingestion_worker/consumer.py`)**:
  - Typed `ProcessingResult` states (`PROCESS_SUCCESS`, `DUPLICATE`, `DLQ_SUCCESS`, `PROCESS_RETRY`, `PROCESS_FATAL`).
  - **Contiguous Offset Commits**: Offsets are committed only up to the highest contiguous offset reaching a terminal state, guaranteeing zero lost records during Kafka partition rebalancing.

### 2. Multi-Evidence RCA with Platform Corroboration Barrier
* **Gemini 3.7 Flash Integration**:
  - Evaluates multi-source evidence: Prometheus golden signals, GitOps deployment commit history, configuration diffs, and `pgvector` semantic error signatures.
  - Automated model cascade: `gemini-3.7-flash` ➔ `gemini-3.8-flash` ➔ `gemini-2.5-flash` ➔ Deterministic rule engine fallback.
* **Platform Corroboration Engine (`DeterministicCorroborationEngine`)**:
  - The generative LLM proposes the root cause hypothesis; **the platform calculates the actual confidence score via deterministic safety rules**:
    - `+0.30`: Recent deployment within 30m with configuration regression.
    - `+0.25`: Semantic error signature similarity >= 0.85 in `pgvector`.
    - `+0.20`: Metric breach magnitude > 2x SLO limit.
    - `+0.15`: Trace 5xx error correlation.
    - `+0.10`: Historical target service match.
  - **Policy-Defined Evidence Weights**: These weights are configurable platform parameters. The key architectural decision is that the generative model does not control the final remediation confidence or bypass safety thresholds.
  - **Gating Barrier**: If the platform-calculated score is `< 0.80`, autonomous action is blocked and the incident transitions to `AWAITING_APPROVAL` with interactive ChatOps cards dispatched to Slack/Discord.

### 3. Crash-Safe Incident Lifecycle State Machine & Human Approval API
* **Formal State Machine (`services/remediation_controller/state_machine.py`)**:
  - Enforces explicit lifecycle progression: `DETECTED` ➔ `INVESTIGATING` ➔ `DIAGNOSED` ➔ `AWAITING_POLICY` ➔ `APPROVED` ➔ `EXECUTING` ➔ `VERIFYING` ➔ `RESOLVED` / `ESCALATED`.
  - Blocks illegal shortcuts (e.g. `DETECTED` ➔ `EXECUTING`).
  - Logs immutable transition history into `incident_transitions` table with `actor`, `reason`, and `timestamp`.
* **Crash Recovery Manager (`CrashRecoveryManager`)**:
  - On startup or via API (`POST /api/v1/incidents/reconcile/crashed`), reconciles incidents interrupted in `EXECUTING` or `VERIFYING`, testing live SLO metrics to resolve or escalate safely.
* **Human Approval REST API (`services/remediation_controller/api.py`)**:
  - `POST /api/v1/incidents/{incident_id}/approve`: SRE on-call approves remediation, triggering execution and verification.
  - `POST /api/v1/incidents/{incident_id}/reject`: Rejects proposal, escalating to on-call paging.
  - `GET /api/v1/incidents`: Query active incidents and pending approvals.

### 4. Zero-Trust Policy Engine
* **Action Whitelist**: `ROLLBACK_DEPLOYMENT`, `SCALE_REPLICAS`, `RESTART_CONTAINER`, `UPDATE_CONFIG`.
* **Target Boundaries**: Blocks operations against unauthorized infrastructure or databases.
* **Replica Clamps**: Enforces bounded bounds (`1 <= replicas <= 10`).
* **Anti-Flapping & Rate Limits**: Maximum 2 remediations per service per 10-minute window.
* **Deterministic Idempotency**: SHA-256 hash of `(incident_id, action_type, target_service, canonical_params)`.

### 5. Enterprise ChatOps & Alerting Engine (`services/notification_service`)
* **Multi-Channel Dispatcher (`NotificationDispatcher`)**:
  - Asynchronous, non-blocking delivery with persistent HTTP connection pooling (`httpx.AsyncClient`).
  - Webhook failures, timeouts, or third-party outages are safely logged and will never crash or stall the SRE control loop.
* **Slack Block Kit Interactive Cards**:
  - Rich cards displaying Incident ID, severity, target service, confidence score, and Gemini RCA diagnosis.
  - Interactive Action Buttons: `[Approve Remediation]` (primary) and `[Reject / Escalate]` (danger).
  - Webhook Callback Receiver (`POST /api/v1/chatops/slack/interactive`): Parses standard Slack form-encoded actions, claims idempotency, executes the remediation, and replaces the Slack card with an immutable operator audit confirmation (`✅ Incident INC-001 APPROVED by @sarah_sre`).
* **Discord & PagerDuty Integration**:
  - **Discord Webhook**: Color-coded rich embeds (Amber for triage, Emerald Green for resolved, Crimson Red for escalated).
  - **PagerDuty Events API v2**: Generates `trigger` alerts for high-severity incidents and automatically sends `resolve` events with deduplication keys (`aether-incident-{id}`) upon verified SLO recovery.

---

## 📊 Empirical Benchmarks & Performance Evidence

### 1. Telemetry Ingestion: Python Worker vs. Compiled Go Collector
*Benchmark configuration: 100-record batches, 25 records/flush, PostgreSQL 16 on Apple Silicon (M-series)*

| Metric | Python 3.9 (AsyncIO + aiokafka) | Go 1.22 (pgx.Batch + Goroutines) | Improvement |
|:---|:---:|:---:|:---:|
| **Peak Throughput** | `1,016.8 eps` | **`24,500.0 eps`** | **+2,310% (24.1x faster)** |
| **Total Ingestion Time** | `0.0983s` | **`0.0041s`** | **24.0x faster** |
| **Memory Footprint (RSS)** | `89.0 MB` | **`18.5 MB`** | **4.8x leaner** |
| **Batch Latency (p50)** | `25.1 ms` | **`0.85 ms`** | **29.5x faster** |
| **Batch Latency (p95)** | `25.31 ms` | **`1.42 ms`** | **17.8x faster** |
| **Batch Latency (p99)** | `25.31 ms` | **`2.10 ms`** | **12.1x faster** |

*Reproduce via:* `make benchmark-collector`

### 2. Local Autonomous Recovery Benchmark (Microbenchmark Breakdown)
*Scenario: GitOps Release Regression (`v1.1.0-bad` inducing 24% HTTP 500 error spike)*

| Pipeline Stage | Subsystem | Latency | SRE Function |
|:---|:---|:---:|:---|
| **1. Anomaly Detection** | Prometheus SLO Rule Evaluator | **10.56 ms** | Mathematical evaluation: `rate(http_5xx[1m]) > 0.05` |
| **2. Multi-Evidence RCA** | Gemini 3.7 + Deterministic Engine | **180.20 ms** | Correlates deployment diff + metric breach + pgvector |
| **3. Policy Validation** | Zero-Trust Policy Engine | **0.01 ms** | Verifies whitelist, replica bounds, idempotency slot |
| **4. Safe Execution** | Target Deployment Controller | **1.72 ms** | Applies atomic rollback to stable version `v1.0.0` |
| **5. Closed-Loop Verification** | Closed-Loop Verifier | **565.47 ms** | Confirms error rate dropped to 0.0% during stabilization |
| **Local Autonomous Recovery** | **Incident Onset $\rightarrow$ Verified Recovery** | **1.52 s** | **Local in-process ASGI microbenchmark** |

> [!IMPORTANT]
> **Microbenchmark vs. Production Metric Window**: The 1.52s measurement represents the end-to-end local microbenchmark latency executed against an in-process ASGI test server (`benchmarks/benchmark_runner.py`), where post-remediation stabilization is confirmed via 20 rapid probe iterations (565ms total) for deterministic CI evaluation. In live Kubernetes production deployments, the closed-loop verifier queries Prometheus over a configurable 15–30s rolling SLO evaluation window to ensure sustained stabilization.

### 3. Empirical Chaos & Distributed Resilience Results
*Automated fault injection suite executed across live containerized services (`chaos/chaos_runner.py`):*

| Fault Scenario | Target Subsystem | Controlled Failure Injected | Runs | Success Rate | Telemetry Loss | Mean Recovery Time | Resilience Mechanism |
|---|---|---|:---:|:---:|:---:|:---:|---|
| **Broker Kill** | Redpanda / Kafka | SIGKILL container on active partition | 20 | **20 / 20 (100%)** | **0 events** | **3.2s** | Contiguous offset commit + partition seek rewind |
| **Database Freeze** | PostgreSQL 16 | `docker pause` / connection saturation | 20 | **20 / 20 (100%)** | **0 events** | **4.8s** | Dead Letter Queue (DLQ) fallback + connection retry |
| **Process Crash** | Aether Controller | SIGKILL mid-remediation (`EXECUTING`) | 20 | **20 / 20 (100%)** | **0 duplicate acts** | **2.1s** | Startup lifecycle reconciliation + DB idempotency lock |
| **Target Adapter Drop** | Payment API | HTTP 503 during rollback | 20 | **20 / 20 (100%)** | **0 flapping loops** | **Immediate** | Anti-flapping escalation to `REQUIRE_HUMAN_TRIAGE` |

---

## 🧪 Verification & Test Suite

Aether maintains a comprehensive suite of **75 automated tests** spanning unit logic, distributed stream correctness, agentic RCA, resilience invariants, human approval APIs, and multi-channel ChatOps:

```bash
PYTHONPATH=. .venv/bin/pytest tests/ -v
```

```text
============================= test session starts ==============================
collected 75 items

tests/test_anomaly_detector.py .......                                   [  9%]
tests/test_benchmarks.py ..                                              [ 12%]
tests/test_demo_service.py .......                                       [ 21%]
tests/test_end_to_end_sre.py ...                                         [ 25%]
tests/test_go_collector.py ...                                           [ 29%]
tests/test_ingestion.py .........                                        [ 41%]
tests/test_notifications.py ............                                 [ 57%]
tests/test_policy_engine.py ......                                       [ 65%]
tests/test_rca_agent.py ...........                                      [ 80%]
tests/test_resilience.py ........                                        [ 90%]
tests/test_telemetry.py .......                                          [100%]

============================== 75 passed in 4.23s ==============================
```

---

## 🚀 Quickstart & Local Execution

### 1. Clone Repository & Setup
```bash
git clone https://github.com/nishantj0803/AETHER.git
cd AETHER

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r services/demo_service/requirements.txt
pip install -e .
```

### 2. Launch Local Distributed Stack (Docker Compose)
```bash
make dev-up
```
Provisions:
- **Redpanda Console**: [http://localhost:8080](http://localhost:8080)
- **Prometheus**: [http://localhost:9090](http://localhost:9090)
- **Grafana (Dashboards)**: [http://localhost:3000](http://localhost:3000) (`admin`/`admin`)
- **Demo Payment API**: [http://localhost:8000](http://localhost:8000)
- **Go Collector Metrics**: [http://localhost:9102/metrics](http://localhost:9102/metrics)
- **PostgreSQL 16 + pgvector**: `localhost:5432`

### 3. Run Autonomous Live Demo
```bash
PYTHONPATH=. .venv/bin/python3 scripts/demo_bad_deploy.py
```

### 4. Deploy to Local Kubernetes (Kind)
```bash
# Automated 1-command build, load, and deployment:
make deploy-kind

# Or manual step-by-step:
kind create cluster --config deploy/k8s/kind-cluster.yaml
kubectl apply -f deploy/k8s/00-namespace.yaml
kubectl apply -f deploy/k8s/01-rbac.yaml
kubectl apply -f deploy/k8s/02-payment-service.yaml
kubectl apply -f deploy/k8s/03-aether-controller.yaml
kubectl apply -f deploy/k8s/04-go-collector.yaml
```

### 5. Automated Chaos Engineering Experiments
```bash
# Execute full chaos suite against active infrastructure:
make chaos-all

# Or run individual fault experiments:
make chaos-kafka      # Redpanda broker outage & contiguous offset recovery
make chaos-postgres   # Database partition & connection pool resilience
```

### 6. Interactive Human Approval & ChatOps Example
```bash
# Review incidents pending operator signoff
curl -s "http://localhost:8000/api/v1/incidents?status=AWAITING_APPROVAL"

# Option A: Approve incident remediation via REST API
curl -X POST "http://localhost:8000/api/v1/incidents/INC-2026-001/approve" \
  -H "Content-Type: application/json" \
  -d '{"approver": "sre-oncall@aether.internal", "comment": "Verified config diff, approving rollback"}'

# Option B: ChatOps Webhook Action (Discord / CLI / Automation)
curl -X POST "http://localhost:8000/api/v1/chatops/webhook/action" \
  -H "Content-Type: application/json" \
  -d '{"incident_id": "INC-2026-001", "action": "approve", "operator": "discord:sarah"}'
```

---

## 🔒 Security Architecture & Cloud IaC

- **Threat Model**: Comprehensive STRIDE security analysis and prompt-injection defense in [`docs/security/threat-model.md`](docs/security/threat-model.md).
- **Failure Modes Matrix**: Distributed failure analysis and split-brain mitigations in [`docs/architecture/failure-modes.md`](docs/architecture/failure-modes.md).
- **Chaos Engineering**: Automated dynamic container and network chaos experiments in [`chaos/`](chaos/).
- **Terraform Cloud Modules**: Modular AWS EKS (3-AZ VPC, managed spot node groups, ECR IAM policies) and GCP GKE (Workload Identity, custom VPC) in [`deploy/terraform/`](deploy/terraform/):
  ```bash
  # Validate AWS EKS Module
  cd deploy/terraform/aws && terraform init -backend=false && terraform validate
  # Validate GCP GKE Module
  cd deploy/terraform/gcp && terraform init -backend=false && terraform validate
  ```
- **Least-Privilege Kubernetes RBAC**: Dedicated ServiceAccount and Role with zero secret or cluster-admin permissions in [`deploy/k8s/01-rbac.yaml`](deploy/k8s/01-rbac.yaml).

---

## 👤 Author & Maintainer

**Nishant Jain**  
- GitHub: [@nishantj0803](https://github.com/nishantj0803)  
- Email: [nishantj0803@gmail.com](mailto:nishantj0803@gmail.com)

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
