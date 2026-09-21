"""
Aether Incident Management & Human Approval REST API.

Provides operators and SRE dashboards with first-class controls over:
1. Reviewing pending incidents in AWAITING_APPROVAL state
2. One-click operator approval / rejection
3. Real-time audit trails of state machine transitions
4. Triggering on-demand crash recovery reconciliation
"""

import json
import logging
import urllib.parse
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from services.ingestion_worker.db import db
from services.notification_service.formatters import SlackFormatter
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


def get_or_build_approval_spec(
    incident_id: str,
    inc: Dict[str, Any],
    comment: Optional[str] = None
) -> RemediationSpec:
    """Retrieve cached pending spec or construct a safe rollback fallback spec."""
    spec_dict = db.get_pending_remediation(incident_id)
    if not spec_dict:
        return RemediationSpec.create(
            incident_id=incident_id,
            action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
            target_service=inc.get("service_name", "payment-service"),
            parameters={"target_version": "v1.0.0"},
            rationale=f"Operator approved remediation: {comment or 'Manual override'}",
            confidence_score=1.0,
            risk_level="LOW"
        )
    action_type = RemediationActionType(spec_dict.get("action_type", "ROLLBACK_DEPLOYMENT"))
    return RemediationSpec(
        remediation_id=spec_dict.get("remediation_id", f"REM-{incident_id}"),
        incident_id=incident_id,
        action_type=action_type,
        target_service=spec_dict.get("target_service", "payment-service"),
        parameters=spec_dict.get("parameters", {}),
        rationale=spec_dict.get("rationale", comment or "Approved"),
        confidence_score=1.0,
        risk_level=spec_dict.get("risk_level", "LOW"),
        requires_human_approval=False,
        idempotency_key=spec_dict.get("idempotency_key", f"{incident_id}:approved")
    )


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

    spec = get_or_build_approval_spec(incident_id, inc, req.comment)
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


# ----------------- ChatOps Interactive Router -----------------
chatops_router = APIRouter(prefix="/api/v1/chatops", tags=["chatops"])


class ChatOpsActionRequest(BaseModel):
    incident_id: str = Field(description="Target incident ID")
    action: str = Field(default="approve", description="Action: 'approve' or 'reject'")
    operator: str = Field(default="chatops-operator", description="Operator identity or Slack user")
    reason: Optional[str] = Field(default=None, description="Optional rationale")


@chatops_router.post("/slack/interactive")
async def handle_slack_interactive(request: Request):
    """
    Receives Slack Block Kit button clicks (Approve / Reject).
    Supports both application/x-www-form-urlencoded (standard Slack) and application/json.
    """
    content_type = request.headers.get("content-type", "")
    if "application/x-www-form-urlencoded" in content_type:
        raw_body = await request.body()
        decoded_str = raw_body.decode("utf-8")
        parsed_form = urllib.parse.parse_qs(decoded_str)
        payload_list = parsed_form.get("payload")
        if not payload_list or not payload_list[0]:
            raise HTTPException(status_code=400, detail="Missing 'payload' form field in Slack request")
        try:
            payload = json.loads(payload_list[0])
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON in Slack form payload: {e}")
    else:
        try:
            payload = await request.json()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON request body: {e}")

    user_info = payload.get("user", {})
    operator = user_info.get("username") or user_info.get("name") or user_info.get("id") or "slack-operator"
    actions = payload.get("actions", [])

    if not actions:
        raise HTTPException(status_code=400, detail="No action found in Slack interactive payload")

    action = actions[0]
    action_id = action.get("action_id", "")
    raw_val = action.get("value", "{}")
    if isinstance(raw_val, str):
        try:
            val_dict = json.loads(raw_val)
        except Exception:
            val_dict = {"incident_id": raw_val}
    else:
        val_dict = raw_val

    incident_id = val_dict.get("incident_id")
    action_intent = val_dict.get("action")
    if not action_intent:
        if "approve" in action_id:
            action_intent = "approve"
        elif "reject" in action_id:
            action_intent = "reject"
        else:
            action_intent = "approve"

    if not incident_id:
        raise HTTPException(status_code=400, detail="Could not determine incident_id from action value")

    inc = await db.get_incident(incident_id)
    if not inc:
        raise HTTPException(status_code=404, detail=f"Incident '{incident_id}' not found")

    if action_intent == "approve":
        spec = get_or_build_approval_spec(incident_id, inc, comment="Approved via Slack interactive card")
        result = await remediation_controller.execute_approved_remediation(spec, approver=f"slack:@{operator}")
        status_msg = f"Remediation '{spec.action_type.value}' executed (Status: {result.get('status', 'OK')})"
        return SlackFormatter.format_interactive_ack(incident_id, operator, "approve", status_msg)
    else:
        await remediation_controller.reject_pending_remediation(
            incident_id=incident_id,
            rejecter=f"slack:@{operator}",
            reason="Rejected via Slack interactive card"
        )
        return SlackFormatter.format_interactive_ack(incident_id, operator, "reject", "Incident escalated to on-call engineer")


@chatops_router.post("/webhook/action")
async def handle_generic_webhook_action(req: ChatOpsActionRequest):
    """
    Generic ChatOps action endpoint for Discord bots, custom webhooks, or PagerDuty incident actions.
    """
    inc = await db.get_incident(req.incident_id)
    if not inc:
        raise HTTPException(status_code=404, detail=f"Incident '{req.incident_id}' not found")

    action_lower = req.action.lower().strip()
    if action_lower == "approve":
        spec = get_or_build_approval_spec(req.incident_id, inc, comment=req.reason or "Approved via ChatOps webhook")
        result = await remediation_controller.execute_approved_remediation(spec, approver=req.operator)
        return {
            "incident_id": req.incident_id,
            "action": "APPROVED",
            "operator": req.operator,
            "result": result
        }
    elif action_lower == "reject":
        result = await remediation_controller.reject_pending_remediation(
            incident_id=req.incident_id,
            rejecter=req.operator,
            reason=req.reason or "Rejected via ChatOps webhook"
        )
        return {
            "incident_id": req.incident_id,
            "action": "REJECTED",
            "operator": req.operator,
            "result": result
        }
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported ChatOps action '{req.action}'. Must be 'approve' or 'reject'."
        )
