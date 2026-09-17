"""
Aether Incident Management & Human Approval REST API.

Provides operators and SRE dashboards with first-class controls over:
1. Reviewing pending incidents in AWAITING_APPROVAL state
2. One-click operator approval / rejection
3. Real-time audit trails of state machine transitions
4. Triggering on-demand crash recovery reconciliation
"""

import logging
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from services.ingestion_worker.db import db
from services.rca_agent.schema import RemediationActionType, RemediationSpec
from services.remediation_controller.controller import remediation_controller
from services.remediation_controller.state_machine import IncidentState

logger = logging.getLogger("aether.approval_api")

router = APIRouter(prefix="/api/v1/incidents", tags=["incidents"])


class IncidentApprovalRequest(BaseModel):
    approver: str = Field(default="sre-oncall@aether.internal", description="Operator / SRE identity")
    comment: Optional[str] = Field(default=None, description="Audit log rationale for manual approval")


class IncidentRejectionRequest(BaseModel):
    rejecter: str = Field(default="sre-oncall@aether.internal", description="Operator / SRE identity")
    reason: str = Field(default="Manual rejection by SRE on-call", description="Rejection rationale")


@router.get("", response_model=List[Dict[str, Any]])
async def list_incidents(
    status: Optional[str] = Query(None, description="Filter by state (e.g., AWAITING_APPROVAL, DETECTED, RESOLVED)"),
    limit: int = Query(50, ge=1, le=200)
):
    """List incidents with optional status filter for SRE dashboards."""
    return await db.list_incidents(status=status, limit=limit)


@router.get("/{incident_id}")
async def get_incident(incident_id: str):
    """Fetch incident details along with complete lifecycle transition history."""
    inc = await db.get_incident(incident_id)
    if not inc:
        raise HTTPException(status_code=404, detail=f"Incident '{incident_id}' not found")

    transitions = await db.get_incident_transitions(incident_id)
    pending_remediation = db.get_pending_remediation(incident_id)

    return {
        "incident": inc,
        "pending_remediation": pending_remediation,
        "transitions": transitions
    }


@router.post("/{incident_id}/approve")
async def approve_incident(incident_id: str, req: IncidentApprovalRequest):
    """
    Operator Human Approval Endpoint.
    Approves a pending incident in AWAITING_APPROVAL / REQUIRE_HUMAN_TRIAGE state.
    Executes the validated remediation spec and initiates post-verification.
    """
    inc = await db.get_incident(incident_id)
    if not inc:
        raise HTTPException(status_code=404, detail=f"Incident '{incident_id}' not found")

    curr_state_raw = inc.get("status", "")
    try:
        curr_state = IncidentState.normalize(curr_state_raw)
    except ValueError:
        curr_state = IncidentState.AWAITING_APPROVAL

    if curr_state not in (IncidentState.AWAITING_APPROVAL, IncidentState.REQUIRE_HUMAN_TRIAGE, IncidentState.ESCALATED):
        raise HTTPException(
            status_code=400,
            detail=f"Incident '{incident_id}' is in state '{curr_state.value}', not awaiting approval."
        )

    # Fetch stored remediation spec or reconstruct from audit log
    spec_dict = db.get_pending_remediation(incident_id)
    if not spec_dict:
        # Fallback to default safe rollback spec if not cached
        spec = RemediationSpec.create(
            incident_id=incident_id,
            action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
            target_service=inc.get("service_name", "payment-service"),
            parameters={"target_version": "v1.0.0"},
            rationale=f"Operator approved remediation: {req.comment or 'Manual override'}",
            confidence_score=1.0,
            risk_level="LOW"
        )
    else:
        action_type = RemediationActionType(spec_dict.get("action_type", "ROLLBACK_DEPLOYMENT"))
        spec = RemediationSpec(
            remediation_id=spec_dict.get("remediation_id", f"REM-{incident_id}"),
            incident_id=incident_id,
            action_type=action_type,
            target_service=spec_dict.get("target_service", "payment-service"),
            parameters=spec_dict.get("parameters", {}),
            rationale=spec_dict.get("rationale", req.comment or "Approved"),
            confidence_score=1.0,
            risk_level=spec_dict.get("risk_level", "LOW"),
            requires_human_approval=False,
            idempotency_key=spec_dict.get("idempotency_key", f"{incident_id}:approved")
        )

    result = await remediation_controller.execute_approved_remediation(spec, approver=req.approver)
    return {
        "incident_id": incident_id,
        "action": "APPROVED",
        "result": result
    }


@router.post("/{incident_id}/reject")
async def reject_incident(incident_id: str, req: IncidentRejectionRequest):
    """
    Operator Rejection Endpoint.
    Rejects the proposed remediation and transitions incident to ESCALATED.
    """
    inc = await db.get_incident(incident_id)
    if not inc:
        raise HTTPException(status_code=404, detail=f"Incident '{incident_id}' not found")

    result = await remediation_controller.reject_pending_remediation(
        incident_id=incident_id,
        rejecter=req.rejecter,
        reason=req.reason
    )
    return result


@router.post("/reconcile/crashed")
async def reconcile_crashed_incidents():
    """Manual trigger to reconcile in-flight incidents after a process crash."""
    recovered = await remediation_controller.recover_in_flight_incidents()
    return {
        "status": "reconciliation_complete",
        "recovered_count": len(recovered),
        "details": recovered
    }
