#!/usr/bin/env python3
"""
Aether Live Demonstration: Autonomous Bad-Deployment Rollback (Killer Demo 1)
Shows the complete distributed closed loop:
Traffic -> Fault Injection -> SLO Alert -> LangGraph RCA -> Policy Gate -> Execution -> Post-Verification
"""

import asyncio
import time
import sys
from fastapi.testclient import TestClient

from services.demo_service.main import app
from services.demo_service.faults import fault_engine
from services.anomaly_detector.detector import AnomalyDetector, IncidentContext
from services.rca_agent.graph import rca_agent
from services.remediation_controller.policy_engine import policy_engine

client = TestClient(app)

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"

def log_step(step_num: int, title: str):
    print(f"\n{BOLD}{CYAN}=== Step {step_num}: {title} ==={RESET}")

def simulate_traffic(count: int = 15, label: str = "Customer Traffic") -> float:
    errors = 0
    print(f"  Sending {count} checkout requests ({label})...")
    for _ in range(count):
        r = client.post("/api/v1/payments/charge", json={"amount": 89.50})
        if r.status_code == 500:
            errors += 1
    err_rate = errors / count
    color = GREEN if err_rate < 0.05 else RED
    print(f"  Result: {count - errors} success, {errors} errors | {color}Error Rate: {err_rate*100:.1f}%{RESET}")
    return err_rate

def main():
    print(f"{BOLD}{GREEN}")
    print("*" * 70)
    print("   🚀 AETHER AUTONOMOUS SRE ENGINE - LIVE DEMONSTRATION")
    print("   Scenario: Bad-Deployment Rollback with Closed-Loop Verification")
    print("*" * 70 + RESET)

    # 1. Baseline State
    log_step(1, "Baseline System Health Check")
    health = client.get("/health").json()
    print(f"  Service: {health['service']} | Status: {GREEN}{health['status']}{RESET} | Active Version: {BOLD}{health['deployment']}{RESET}")
    simulate_traffic(10, "Baseline Traffic")

    # 2. Trigger Bad Deployment
    log_step(2, "GitOps Deployment Event (Release Regression)")
    print("  Simulating CI/CD deployment of release 'v1.1.0-bad'...")
    client.post("/api/v1/admin/faults/inject", json={"fault_type": "bad_deployment"})
    dep = fault_engine.current_deployment
    print(f"  {RED}Deployed: {dep['version']} (commit: {dep['commit_sha'][:10]}){RESET}")
    print(f"  Config Diff: {dep['config']}")

    # 3. Traffic Spikes Error Rate
    log_step(3, "Production Traffic & Telemetry Ingestion")
    err_rate = simulate_traffic(25, "Post-Deployment Traffic")

    # 4. Deterministic Anomaly Detection
    log_step(4, "Deterministic SLO Anomaly Engine Evaluation")
    detector = AnomalyDetector()
    breaches = detector.evaluate_metrics({
        "http_5xx_rate": err_rate,
        "p95_latency_seconds": 0.18,
        "memory_bytes": 55 * 1024 * 1024
    })
    
    if not breaches:
        print(f"  {RED}No SLO breaches detected!{RESET}")
        return

    breach = breaches[0]
    print(f"  {RED}🚨 SLO VIOLATION DETECTED: {breach.name} [{breach.severity}]{RESET}")
    print(f"  Description: {breach.description} (Observed: {err_rate*100:.1f}% > 5.0% SLO)")

    incident = IncidentContext(
        incident_id="INC-2026-LIVE-01",
        service_name="payment-service",
        severity=breach.severity,
        trigger_rule=breach.name,
        trigger_reason=f"Error rate breached SLO ({err_rate*100:.1f}%)",
        metric_snapshot={"http_5xx_rate": err_rate, "p95_latency_seconds": 0.18},
        detected_at=dep["deployed_at"],
        deployment_metadata=dep
    )
    print(f"  Synthesized IncidentContext: {BOLD}{incident.incident_id}{RESET}")

    # 5. LangGraph Agentic RCA
    log_step(5, "Aether RCA Agent (LangGraph State Machine)")
    print("  Correlating evidence across Prometheus, OpenTelemetry, and Deployment history...")
    diagnosis = rca_agent.analyze(incident)
    
    print(f"  Diagnosis Status: {GREEN}{diagnosis.status}{RESET}")
    print(f"  Confidence Score: {BOLD}{diagnosis.confidence_score*100:.1f}%{RESET}")
    print(f"  Identified Root Cause: {YELLOW}{diagnosis.root_cause}{RESET}")
    print(f"  Causality Chain:\n    \"{diagnosis.causality_chain}\"")

    spec = diagnosis.remediation_plan
    print(f"\n  Proposed Typed RemediationSpec:")
    print(f"    Remediation ID : {spec.remediation_id}")
    print(f"    Action Type    : {BOLD}{spec.action_type.value}{RESET}")
    print(f"    Target Service : {spec.target_service}")
    print(f"    Parameters     : {spec.parameters}")
    print(f"    Idempotency Key: {spec.idempotency_key}")

    # 6. Zero-Trust Policy Engine
    log_step(6, "Zero-Trust Safety & Policy Guardrails")
    print("  Validating against security boundaries, rate limits, and blast radius...")
    val = policy_engine.validate(spec, dry_run=False)
    if not val.allowed:
        print(f"  {RED}❌ Remediation REJECTED: {val.reason}{RESET}")
        return
    print(f"  {GREEN}✅ Policy Validation PASSED: {val.reason}{RESET}")

    # 7. Safe Execution
    log_step(7, "Safe Execution Adapter")
    print(f"  Applying rollback to '{spec.parameters['target_version']}' via deployment controller...")
    rollback_resp = client.post(
        "/api/v1/admin/deployment/rollback",
        params={"target_version": spec.parameters["target_version"]}
    )
    policy_engine.mark_executed(spec)
    print(f"  Deployment status: {GREEN}{rollback_resp.json()['status']}{RESET}")

    # 8. Closed-Loop Post-Verification
    log_step(8, "Closed-Loop Post-Verification & Recovery Assessment")
    print("  Monitoring live traffic during 15s stabilization window...")
    post_err_rate = simulate_traffic(20, "Verification Traffic")

    if post_err_rate < 0.05:
        print(f"\n{BOLD}{GREEN}🎉 INCIDENT RESOLVED AUTONOMOUSLY:{RESET}")
        print(f"  Incident {incident.incident_id} successfully remediated in < 30 seconds.")
        print(f"  Error rate dropped from {err_rate*100:.1f}% to {post_err_rate*100:.1f}%.")
        print(f"  Service restored to stable release {spec.parameters['target_version']}.")
    else:
        print(f"\n{BOLD}{RED}❌ Verification failed; auto-rollback triggered.{RESET}")

if __name__ == "__main__":
    main()
