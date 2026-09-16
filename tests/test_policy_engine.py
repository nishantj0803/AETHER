import pytest
from services.rca_agent.schema import RemediationActionType, RemediationSpec
from services.remediation_controller.policy_engine import ZeroTrustPolicyEngine

@pytest.fixture
def fresh_engine():
    return ZeroTrustPolicyEngine()

def test_policy_valid_rollback(fresh_engine):
    spec = RemediationSpec.create(
        incident_id="INC-001",
        action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
        target_service="payment-service",
        parameters={"target_version": "v1.0.0"},
        rationale="Bad deployment detected",
        confidence_score=0.95
    )

    res = fresh_engine.validate(spec, dry_run=False)
    assert res.allowed is True
    assert res.dry_run_passed is True

def test_policy_unauthorized_target_service(fresh_engine):
    spec = RemediationSpec.create(
        incident_id="INC-002",
        action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
        target_service="production-billing-database",  # Not in allowed list
        parameters={"target_version": "v1.0.0"},
        rationale="Attempted unauthorized target modification",
        confidence_score=0.95
    )

    res = fresh_engine.validate(spec, dry_run=False)
    assert res.allowed is False
    assert "permitted operational boundary" in res.reason

def test_policy_replica_bounds_clamp(fresh_engine):
    spec = RemediationSpec.create(
        incident_id="INC-003",
        action_type=RemediationActionType.SCALE_REPLICAS,
        target_service="payment-service",
        parameters={"to_replicas": 50},  # Exceeds max 10
        rationale="Scale up under load",
        confidence_score=0.92
    )

    res = fresh_engine.validate(spec, dry_run=False)
    assert res.allowed is False
    assert "violates bounds" in res.reason

def test_policy_idempotency_duplicate_blocked(fresh_engine):
    spec = RemediationSpec.create(
        incident_id="INC-004",
        action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
        target_service="payment-service",
        parameters={"target_version": "v1.0.0"},
        rationale="Rollback test",
        confidence_score=0.95
    )

    # First validation & execution
    res1 = fresh_engine.validate(spec, dry_run=False)
    assert res1.allowed is True
    fresh_engine.mark_executed(spec)

    # Duplicate execution attempt with same idempotency key
    res2 = fresh_engine.validate(spec, dry_run=False)
    assert res2.allowed is False
    assert "Duplicate execution blocked" in res2.reason

def test_policy_low_confidence_rejected(fresh_engine):
    spec = RemediationSpec.create(
        incident_id="INC-005",
        action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
        target_service="payment-service",
        parameters={"target_version": "v1.0.0"},
        rationale="Uncertain rollback",
        confidence_score=0.65  # Below 0.80
    )

    res = fresh_engine.validate(spec, dry_run=False)
    assert res.allowed is False
    assert "below autonomous safety threshold" in res.reason
