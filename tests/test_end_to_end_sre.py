import asyncio
import pytest
from fastapi.testclient import TestClient

from services.demo_service.main import app
from services.demo_service.faults import fault_engine
from services.anomaly_detector.detector import AnomalyDetector, IncidentContext
from services.rca_agent.graph import rca_agent
from services.rca_agent.schema import RemediationActionType
from services.remediation_controller.policy_engine import ZeroTrustPolicyEngine

client = TestClient(app)

@pytest.fixture(autouse=True)
def clean_state():
    fault_engine.reset_all()
    yield
    fault_engine.reset_all()

def test_full_autonomous_sre_loop_bad_deployment():
    # -------------------------------------------------------------
    # Step 1: Baseline Healthy State
    # -------------------------------------------------------------
    health_resp = client.get("/health")
    assert health_resp.status_code == 200
    assert health_resp.json()["deployment"] == "v1.0.0"

    # -------------------------------------------------------------
    # Step 2: Inject Bad Deployment Regression (v1.1.0-bad)
    # -------------------------------------------------------------
    inject_resp = client.post("/api/v1/admin/faults/inject", json={"fault_type": "bad_deployment"})
    assert inject_resp.status_code == 200
    assert fault_engine.current_deployment["version"] == "v1.1.0-bad"

    # Simulate payment traffic under the bad deployment
    errors = 0
    total = 30
    for _ in range(total):
        r = client.post("/api/v1/payments/charge", json={"amount": 49.99})
        if r.status_code == 500:
            errors += 1

    error_rate = errors / total
    assert error_rate > 0.05  # Breaches 5% SLO threshold

    # -------------------------------------------------------------
    # Step 3: Deterministic Anomaly Detection
    # -------------------------------------------------------------
    detector = AnomalyDetector()
    breaches = detector.evaluate_metrics({
        "http_5xx_rate": error_rate,
        "p95_latency_seconds": 0.12,
        "memory_bytes": 60 * 1024 * 1024
    })
    assert len(breaches) > 0
    primary_breach = breaches[0]
    assert primary_breach.name == "HighHttpErrorRate"

    incident = IncidentContext(
        incident_id="INC-E2E-TEST-001",
        service_name="payment-service",
        severity=primary_breach.severity,
        trigger_rule=primary_breach.name,
        trigger_reason=f"Error rate breached SLO ({error_rate*100:.1f}%)",
        metric_snapshot={"http_5xx_rate": error_rate},
        detected_at=fault_engine.current_deployment["deployed_at"],
        deployment_metadata=fault_engine.current_deployment
    )

    # -------------------------------------------------------------
    # Step 4: Multi-Evidence Agentic RCA
    # -------------------------------------------------------------
    diagnosis = rca_agent.analyze(incident)
    assert diagnosis.status == "CONFIRMED"
    assert diagnosis.confidence_score >= 0.90
    assert diagnosis.remediation_plan is not None
    assert diagnosis.remediation_plan.action_type == RemediationActionType.ROLLBACK_DEPLOYMENT
    assert diagnosis.remediation_plan.parameters["target_version"] == "v1.0.0"

    # -------------------------------------------------------------
    # Step 5: Zero-Trust Policy Engine Validation
    # -------------------------------------------------------------
    policy = ZeroTrustPolicyEngine()
    validation = policy.validate(diagnosis.remediation_plan, dry_run=False)
    assert validation.allowed is True
    assert validation.dry_run_passed is True

    # -------------------------------------------------------------
    # Step 6: Safe Remediation Execution (Rollback to v1.0.0)
    # -------------------------------------------------------------
    rollback_resp = client.post(
        "/api/v1/admin/deployment/rollback",
        params={"target_version": diagnosis.remediation_plan.parameters["target_version"]}
    )
    assert rollback_resp.status_code == 200
    assert fault_engine.current_deployment["version"] == "v1.0.0"

    # -------------------------------------------------------------
    # Step 7: Closed-Loop Post-Verification
    # -------------------------------------------------------------
    recovered_errors = 0
    for _ in range(15):
        r = client.post("/api/v1/payments/charge", json={"amount": 49.99})
        if r.status_code == 500:
            recovered_errors += 1

    post_error_rate = recovered_errors / 15
    assert post_error_rate == 0.0  # Successfully returned within healthy SLO!

def test_controller_low_confidence_routes_to_human_triage():
    from services.remediation_controller.controller import RemediationController
    from services.rca_agent.schema import RemediationSpec

    controller = RemediationController()
    spec = RemediationSpec.create(
        incident_id="INC-HUMAN-001",
        action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
        target_service="payment-service",
        parameters={"target_version": "v1.0.0"},
        rationale="Low confidence scenario",
        confidence_score=0.70  # < 0.80 threshold
    )
    result = asyncio.run(controller.process_remediation(spec))
    assert result["status"] == "REQUIRES_HUMAN_APPROVAL"

def test_controller_verification_failure_escalates_without_flapping():
    from unittest.mock import AsyncMock, patch
    from services.anomaly_detector.detector import detector
    from services.remediation_controller.controller import RemediationController
    from services.rca_agent.schema import RemediationSpec

    controller = RemediationController()
    # Populate detector active incident
    detector.active_incidents["payment-service"] = "placeholder_incident"

    spec = RemediationSpec.create(
        incident_id="INC-FAIL-001",
        action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
        target_service="payment-service",
        parameters={"target_version": "v1.0.0"},
        rationale="Rollback that fails verification",
        confidence_score=0.95
    )

    # Mock execute_action (succeeds) but verify_recovery (fails, service still unhealthy)
    with patch.object(controller, "execute_action", new=AsyncMock(return_value=True)), \
         patch.object(controller, "verify_recovery", new=AsyncMock(return_value=False)):
        result = asyncio.run(controller.process_remediation(spec))
        assert result["status"] == "REQUIRE_HUMAN_TRIAGE"
        assert result["verified"] is False
        # Flapping prevention check: incident is NOT cleared from detector
        assert "payment-service" in detector.active_incidents

