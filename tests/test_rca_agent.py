import time
import pytest
from services.anomaly_detector.detector import IncidentContext
from services.rca_agent.graph import rca_agent
from services.rca_agent.schema import RemediationActionType

def test_rca_bad_deployment_correlation():
    # Simulate bad deployment incident context
    incident = IncidentContext(
        incident_id="INC-TEST-001",
        service_name="payment-service",
        severity="P1",
        trigger_rule="HighHttpErrorRate",
        trigger_reason="HTTP 5xx rate = 25%",
        metric_snapshot={"http_5xx_rate": 0.25, "p95_latency_seconds": 0.3},
        detected_at=time.time(),
        deployment_metadata={
            "version": "v1.1.0-bad",
            "commit_sha": "f3b92019a847162940283719a938210938472918",
            "deployed_at": time.time() - 60,  # 60s ago
            "config": {"db_timeout_ms": 50}
        }
    )

    diagnosis = rca_agent.analyze(incident)
    assert diagnosis.status == "CONFIRMED"
    assert diagnosis.confidence_score >= 0.90
    assert "Configuration regression" in diagnosis.root_cause
    assert diagnosis.remediation_plan is not None
    assert diagnosis.remediation_plan.action_type == RemediationActionType.ROLLBACK_DEPLOYMENT
    assert diagnosis.remediation_plan.parameters["target_version"] == "v1.0.0"

def test_rca_memory_leak_correlation():
    incident = IncidentContext(
        incident_id="INC-TEST-002",
        service_name="payment-service",
        severity="P2",
        trigger_rule="HighMemoryUsage",
        trigger_reason="Process memory reached 400MB",
        metric_snapshot={"http_5xx_rate": 0.01, "memory_bytes": 420 * 1024 * 1024},
        detected_at=time.time(),
        deployment_metadata={"version": "v1.0.0", "deployed_at": time.time() - 36000}
    )

    diagnosis = rca_agent.analyze(incident)
    assert diagnosis.status == "CONFIRMED"
    assert diagnosis.confidence_score >= 0.85
    assert diagnosis.remediation_plan.action_type == RemediationActionType.RESTART_CONTAINER

def test_rca_ambiguous_incident_triage_fallback():
    # Uncorrelated anomaly with no recent deployment and normal memory
    incident = IncidentContext(
        incident_id="INC-TEST-003",
        service_name="payment-service",
        severity="P1",
        trigger_rule="HighHttpErrorRate",
        trigger_reason="Random transient 5xx burst",
        metric_snapshot={"http_5xx_rate": 0.12, "memory_bytes": 50 * 1024 * 1024},
        detected_at=time.time(),
        deployment_metadata={"version": "v1.0.0", "deployed_at": time.time() - 86400}
    )

    diagnosis = rca_agent.analyze(incident)
    assert diagnosis.status == "HUMAN_TRIAGE_REQUIRED"
    assert diagnosis.confidence_score < 0.80
    assert diagnosis.remediation_plan is None
