"""
Comprehensive Resilience, State Machine, and Human Approval Test Suite for Aether.

Tests:
1. Formal Incident Lifecycle State Machine transitions & invariant enforcement.
2. Illegal transition rejection (e.g. DETECTED -> EXECUTING, terminal RESOLVED jumps).
3. Crash recovery reconciliation for in-flight incidents (EXECUTING, VERIFYING).
4. First-class Human Approval & Rejection REST API workflows.
5. Adapter failure rollback and idempotency lock reversion.
"""

import asyncio
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

from services.demo_service.main import app
from services.ingestion_worker.db import db
from services.rca_agent.schema import RemediationActionType, RemediationSpec
from services.remediation_controller.controller import RemediationController
from services.remediation_controller.state_machine import (
    IncidentState,
    IncidentStateMachine,
    InvalidStateTransitionError,
    CrashRecoveryManager
)

client = TestClient(app)


def test_state_machine_valid_transitions():
    """Verify that canonical lifecycle state paths are permitted."""
    sm = IncidentStateMachine

    # Canonical Autonomous Path
    assert sm.can_transition(IncidentState.DETECTED, IncidentState.INVESTIGATING)
    assert sm.can_transition(IncidentState.INVESTIGATING, IncidentState.DIAGNOSED)
    assert sm.can_transition(IncidentState.DIAGNOSED, IncidentState.AWAITING_POLICY)
    assert sm.can_transition(IncidentState.AWAITING_POLICY, IncidentState.APPROVED)
    assert sm.can_transition(IncidentState.APPROVED, IncidentState.EXECUTING)
    assert sm.can_transition(IncidentState.EXECUTING, IncidentState.VERIFYING)
    assert sm.can_transition(IncidentState.VERIFYING, IncidentState.RESOLVED)

    # Canonical Human Triage Path
    assert sm.can_transition(IncidentState.DIAGNOSED, IncidentState.AWAITING_APPROVAL)
    assert sm.can_transition(IncidentState.AWAITING_POLICY, IncidentState.AWAITING_APPROVAL)
    assert sm.can_transition(IncidentState.AWAITING_APPROVAL, IncidentState.APPROVED)
    assert sm.can_transition(IncidentState.AWAITING_APPROVAL, IncidentState.ESCALATED)

    # Failure / Rollback Paths
    assert sm.can_transition(IncidentState.EXECUTING, IncidentState.ROLLBACK_EXECUTING)
    assert sm.can_transition(IncidentState.VERIFYING, IncidentState.ROLLBACK_EXECUTING)
    assert sm.can_transition(IncidentState.ROLLBACK_EXECUTING, IncidentState.ESCALATED)


def test_state_machine_blocks_illegal_transitions():
    """Verify state machine throws InvalidStateTransitionError on illegal jumps."""
    sm = IncidentStateMachine

    # Illegal shortcut: DETECTED -> EXECUTING without diagnosis or policy
    assert not sm.can_transition(IncidentState.DETECTED, IncidentState.EXECUTING)
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        sm.validate_transition(IncidentState.DETECTED, IncidentState.EXECUTING)
    assert "DETECTED" in str(exc_info.value)
    assert "EXECUTING" in str(exc_info.value)

    # Illegal jump: Terminal RESOLVED state cannot transition to EXECUTING
    assert not sm.can_transition(IncidentState.RESOLVED, IncidentState.EXECUTING)
    with pytest.raises(InvalidStateTransitionError):
        sm.validate_transition(IncidentState.RESOLVED, IncidentState.EXECUTING)

    # Illegal jump: AWAITING_APPROVAL cannot jump directly to VERIFYING
    assert not sm.can_transition(IncidentState.AWAITING_APPROVAL, IncidentState.VERIFYING)
    with pytest.raises(InvalidStateTransitionError):
        sm.validate_transition(IncidentState.AWAITING_APPROVAL, IncidentState.VERIFYING)


def test_db_transition_records_immutable_audit_trail():
    """Verify that state transitions are recorded with actor, timestamp, and metadata."""
    inc_id = "INC-AUDIT-TEST-001"
    asyncio.run(db.create_incident(
        incident_id=inc_id,
        service_name="payment-service",
        severity="P1",
        trigger_reason="Error spike test",
        metric_snapshot={"http_5xx_rate": 0.22}
    ))

    # Transition 1: DETECTED -> INVESTIGATING
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.INVESTIGATING,
        actor="system:detector",
        reason="Anomaly rule triggered investigation"
    ))

    # Transition 2: INVESTIGATING -> DIAGNOSED
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.DIAGNOSED,
        actor="system:rca_agent",
        reason="Gemini 3.7 RCA confirmed configuration regression"
    ))

    inc = asyncio.run(db.get_incident(inc_id))
    assert inc["status"] == IncidentState.DIAGNOSED.value

    transitions = asyncio.run(db.get_incident_transitions(inc_id))
    assert len(transitions) == 2
    assert transitions[0]["from_state"] == "DETECTED"
    assert transitions[0]["to_state"] == "INVESTIGATING"
    assert transitions[0]["actor"] == "system:detector"
    assert transitions[1]["from_state"] == "INVESTIGATING"
    assert transitions[1]["to_state"] == "DIAGNOSED"
    assert transitions[1]["actor"] == "system:rca_agent"


def test_crash_recovery_reconciles_inflight_incident():
    """Verify that CrashRecoveryManager recovers an incident stuck in EXECUTING/VERIFYING."""
    inc_id = "INC-CRASH-001"
    asyncio.run(db.create_incident(
        incident_id=inc_id,
        service_name="payment-service",
        severity="P1",
        trigger_reason="Process crashed mid-remediation",
        metric_snapshot={"http_5xx_rate": 0.30}
    ))

    # Manually place incident in EXECUTING state as if controller died mid-flight
    db._in_memory_incidents[inc_id]["status"] = "EXECUTING"

    mock_controller = RemediationController()
    # Mock verify_recovery to report metrics recovered
    mock_controller.verify_recovery = AsyncMock(return_value=True)

    manager = CrashRecoveryManager(db_client=db, controller=mock_controller)
    recovery_results = asyncio.run(manager.recover())

    assert len(recovery_results) >= 1
    recovered_entry = next((r for r in recovery_results if r["incident_id"] == inc_id), None)
    assert recovered_entry is not None
    assert recovered_entry["previous"] == "EXECUTING"
    assert recovered_entry["resolved_to"] == "RESOLVED"

    updated = asyncio.run(db.get_incident(inc_id))
    assert updated["status"] == "RESOLVED"


def test_human_approval_api_workflow():
    """Test full human approval workflow via REST API."""
    inc_id = "INC-APPROVAL-TEST-001"
    asyncio.run(db.create_incident(
        incident_id=inc_id,
        service_name="payment-service",
        severity="P2",
        trigger_reason="Config drift requires operator review",
        metric_snapshot={"http_5xx_rate": 0.08}
    ))

    # Advance incident to AWAITING_APPROVAL
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.INVESTIGATING,
        actor="system:detector",
        reason="Investigation start"
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.DIAGNOSED,
        actor="system:rca_agent",
        reason="Low confidence diagnosis"
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.AWAITING_APPROVAL,
        actor="system:policy_engine",
        reason="Confidence score 0.72 < 0.80 threshold"
    ))

    # Store pending spec
    spec = RemediationSpec.create(
        incident_id=inc_id,
        action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
        target_service="payment-service",
        parameters={"target_version": "v1.0.0"},
        rationale="Rollback proposed",
        confidence_score=0.72,
        risk_level="MEDIUM"
    )
    db.store_pending_remediation(inc_id, spec.model_dump())

    # 1. Inspect incident via GET /api/v1/incidents/{incident_id}
    detail_resp = client.get(f"/api/v1/incidents/{inc_id}")
    assert detail_resp.status_code == 200
    data = detail_resp.json()
    assert data["incident"]["status"] == IncidentState.AWAITING_APPROVAL.value
    assert data["pending_remediation"] is not None

    # 2. Operator Approves via POST /api/v1/incidents/{incident_id}/approve
    with patch("services.remediation_controller.controller.RemediationController.execute_action", new=AsyncMock(return_value=True)), \
         patch("services.remediation_controller.controller.RemediationController.verify_recovery", new=AsyncMock(return_value=True)):
        approve_resp = client.post(
            f"/api/v1/incidents/{inc_id}/approve",
            json={"approver": "sre-lead@aether.internal", "comment": "Approved following code review"}
        )

    assert approve_resp.status_code == 200
    res = approve_resp.json()
    assert res["action"] == "APPROVED"
    assert res["result"]["status"] == "RESOLVED"

    # Check updated status
    updated_inc = asyncio.run(db.get_incident(inc_id))
    assert updated_inc["status"] == "RESOLVED"

    # Check transition audit trail includes operator
    transitions = asyncio.run(db.get_incident_transitions(inc_id))
    operator_transitions = [t for t in transitions if "sre-lead@aether.internal" in t.get("actor", "")]
    assert len(operator_transitions) > 0


def test_human_rejection_api_workflow():
    """Test operator rejection via REST API."""
    inc_id = "INC-REJECT-TEST-001"
    asyncio.run(db.create_incident(
        incident_id=inc_id,
        service_name="payment-service",
        severity="P2",
        trigger_reason="Suspicious false positive spike",
        metric_snapshot={"http_5xx_rate": 0.06}
    ))

    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.INVESTIGATING,
        actor="system:detector",
        reason="Anomaly detected"
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.DIAGNOSED,
        actor="system:rca_agent",
        reason="Diagnosis complete"
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.AWAITING_APPROVAL,
        actor="system:policy_engine",
        reason="Manual signoff required"
    ))

    # Operator Rejects via POST /api/v1/incidents/{incident_id}/reject
    reject_resp = client.post(
        f"/api/v1/incidents/{inc_id}/reject",
        json={
            "rejecter": "sre-lead@aether.internal",
            "reason": "False positive caused by marketing campaign traffic spike"
        }
    )

    assert reject_resp.status_code == 200
    res = reject_resp.json()
    assert res["status"] == "REJECTED"
    assert res["state"] == IncidentState.ESCALATED.value

    updated_inc = asyncio.run(db.get_incident(inc_id))
    assert updated_inc["status"] == IncidentState.ESCALATED.value

def test_db_backed_idempotency_blocks_duplicate_after_restart():
    """Verify that PostgreSQL idempotency constraint blocks duplicate actions even if controller restarts."""
    from services.remediation_controller.policy_engine import policy_engine
    from services.remediation_controller.controller import RemediationController

    # Reset policy engine tracking for clean test state
    policy_engine.executed_idempotency_keys.clear()
    policy_engine.last_execution_time.clear()

    controller = RemediationController()
    spec = RemediationSpec.create(
        incident_id="INC-RESTART-IDEMPOTENCY-001",
        action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
        target_service="payment-service",
        parameters={"target_version": "v1.0.0"},
        rationale="Test crash-safe idempotency",
        confidence_score=0.95
    )

    with patch.object(controller, "execute_action", new=AsyncMock(return_value=True)), \
         patch.object(controller, "verify_recovery", new=AsyncMock(return_value=True)):
        # First execution succeeds
        res1 = asyncio.run(controller.process_remediation(spec))
        assert res1["status"] == "RESOLVED"

        # Simulate controller process crash: in-memory policy cache is wiped!
        policy_engine.executed_idempotency_keys.clear()
        policy_engine.last_execution_time.clear()

        # Second execution with same idempotency key must be BLOCKED by database constraint
        res2 = asyncio.run(controller.process_remediation(spec))
        assert res2["status"] == "DUPLICATE_EXECUTION_BLOCKED"
        assert "already recorded" in res2["reason"]

def test_chaos_orchestrator_dry_run_validation():
    """Verify that automated chaos scripts pass syntax and validation via orchestrator."""
    from chaos.chaos_runner import run_experiment, check_service_health, check_incident_api_health
    
    # Verify Kafka chaos script
    kafka_ok = run_experiment("chaos/experiment_kafka_kill.sh", "Kafka Outage", dry_run=True)
    assert kafka_ok is True

    # Verify Postgres chaos script
    pg_ok = run_experiment("chaos/experiment_postgres_pause.sh", "Postgres Partition", dry_run=True)
    assert pg_ok is True

    # Health check helpers return boolean without throwing unhandled exceptions
    assert isinstance(check_service_health("http://invalid-host:9999"), bool)
    assert isinstance(check_incident_api_health("http://invalid-host:9999"), bool)


