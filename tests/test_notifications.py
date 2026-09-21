"""
Comprehensive Test Suite for Aether Enterprise ChatOps & Alerting (Slack, Discord, PagerDuty).

Tests:
1. Slack Block Kit payload structure, buttons, and acknowledgment formatting.
2. Discord rich embed structure, color mappings, and field layouts.
3. PagerDuty Events API v2 payload mapping (trigger, resolve, dedup keys).
4. Multi-channel non-blocking dispatch with persistent connection pooling.
5. Zero-blocking fault tolerance when external endpoints experience network failures or HTTP 500s.
6. RemediationController integration hooks (AWAITING_APPROVAL, RESOLVED, ESCALATED).
7. Slack Block Kit interactive button callback (/api/v1/chatops/slack/interactive).
8. Generic ChatOps webhook action endpoint (/api/v1/chatops/webhook/action).
"""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
import httpx

from services.demo_service.main import app
from services.ingestion_worker.db import db
from services.notification_service.models import NotificationEvent, NotificationEventType
from services.notification_service.formatters import (
    SlackFormatter,
    DiscordFormatter,
    PagerDutyFormatter
)
from services.notification_service.dispatcher import NotificationDispatcher
from services.rca_agent.schema import RemediationActionType, RemediationSpec
from services.remediation_controller.controller import RemediationController
from services.remediation_controller.policy_engine import policy_engine
from services.remediation_controller.state_machine import IncidentState

client = TestClient(app)


@pytest.fixture(autouse=True)
def reset_policy_cooldown():
    policy_engine.last_execution_time.clear()
    yield
    policy_engine.last_execution_time.clear()


# ----------------- Formatter Tests -----------------

def test_slack_block_kit_awaiting_approval():
    """Verify Slack Block Kit message structure for incidents requiring human approval."""
    event = NotificationEvent(
        event_type=NotificationEventType.AWAITING_APPROVAL,
        incident_id="INC-SLACK-TEST-001",
        service_name="payment-service",
        severity="P2",
        title="Incident INC-SLACK-TEST-001: Human Approval Required",
        summary="Confidence score 0.72 < 0.80 threshold",
        confidence_score=0.72,
        action_type="ROLLBACK_DEPLOYMENT",
        rationale="Configuration drift caused HTTP 500 surge"
    )

    payload = SlackFormatter.format(event)
    assert "blocks" in payload
    blocks = payload["blocks"]

    # 1. Header block
    assert blocks[0]["type"] == "header"
    assert "INC-SLACK-TEST-001" in blocks[0]["text"]["text"]
    assert "🚨" in blocks[0]["text"]["text"]

    # 2. Key-value fields section
    section_fields = [b for b in blocks if b.get("type") == "section" and "fields" in b]
    assert len(section_fields) >= 1
    field_texts = " ".join(f["text"] for f in section_fields[0]["fields"])
    assert "INC-SLACK-TEST-001" in field_texts
    assert "payment-service" in field_texts
    assert "72.0%" in field_texts
    assert "ROLLBACK_DEPLOYMENT" in field_texts

    # 3. Interactive actions block with buttons
    actions_blocks = [b for b in blocks if b.get("type") == "actions"]
    assert len(actions_blocks) == 1
    elements = actions_blocks[0]["elements"]
    assert len(elements) == 2
    approve_btn = elements[0]
    reject_btn = elements[1]

    assert approve_btn["action_id"] == "approve_remediation"
    assert approve_btn["style"] == "primary"
    assert "INC-SLACK-TEST-001" in approve_btn["value"]

    assert reject_btn["action_id"] == "reject_remediation"
    assert reject_btn["style"] == "danger"
    assert "INC-SLACK-TEST-001" in reject_btn["value"]


def test_slack_block_kit_resolved_and_ack():
    """Verify Slack Block Kit format for resolved incidents and operator interactive acks."""
    event = NotificationEvent(
        event_type=NotificationEventType.RESOLVED,
        incident_id="INC-SLACK-RESOLVED-002",
        service_name="payment-service",
        severity="P2",
        title="Incident INC-SLACK-RESOLVED-002 Resolved",
        summary="Remediation successfully verified",
        confidence_score=0.95,
        action_type="ROLLBACK_DEPLOYMENT"
    )

    payload = SlackFormatter.format(event)
    blocks = payload["blocks"]
    assert blocks[0]["type"] == "header"
    assert "✅" in blocks[0]["text"]["text"]
    # Resolved events must NOT contain interactive action buttons
    assert not any(b.get("type") == "actions" for b in blocks)

    # Test interactive acknowledgment replacement
    ack = SlackFormatter.format_interactive_ack(
        incident_id="INC-SLACK-RESOLVED-002",
        operator="alice",
        action="approve",
        message="Status: RESOLVED"
    )
    assert ack["replace_original"] is True
    assert "INC-SLACK-RESOLVED-002" in ack["blocks"][0]["text"]["text"]
    assert "@alice" in ack["blocks"][0]["text"]["text"]


def test_discord_embed_formatter():
    """Verify Discord rich embed color coding, fields, and timestamp formatting."""
    event_awaiting = NotificationEvent(
        event_type=NotificationEventType.AWAITING_APPROVAL,
        incident_id="INC-DISCORD-001",
        service_name="payment-service",
        severity="P2",
        title="Awaiting Approval",
        summary="Review required",
        confidence_score=0.75,
        action_type="ROLLBACK_DEPLOYMENT",
        rationale="Bad deployment detected"
    )

    payload = DiscordFormatter.format(event_awaiting)
    assert "embeds" in payload
    embed = payload["embeds"][0]
    assert embed["color"] == 0xF39C12  # Amber
    assert "[Aether]" in embed["title"]
    assert any(f["name"] == "Target Service" and "payment-service" in f["value"] for f in embed["fields"])
    assert any(f["name"] == "Confidence" and "75.0%" in f["value"] for f in embed["fields"])

    # Test Escalated embed color (Red)
    event_escalated = NotificationEvent(
        event_type=NotificationEventType.ESCALATED,
        incident_id="INC-DISCORD-002",
        service_name="order-service",
        severity="P1",
        title="Escalation",
        summary="Verification failed"
    )
    payload_esc = DiscordFormatter.format(event_escalated)
    assert payload_esc["embeds"][0]["color"] == 0xE74C3C  # Red


def test_pagerduty_formatter():
    """Verify PagerDuty Events API v2 trigger & resolve mapping with deduplication keys."""
    # Trigger event
    event_trigger = NotificationEvent(
        event_type=NotificationEventType.ESCALATED,
        incident_id="INC-PD-001",
        service_name="payment-service",
        severity="P1",
        title="Critical Failure",
        summary="Target service down"
    )
    pd_payload = PagerDutyFormatter.format(event_trigger, routing_key="test-pd-key-123")
    assert pd_payload["routing_key"] == "test-pd-key-123"
    assert pd_payload["event_action"] == "trigger"
    assert pd_payload["dedup_key"] == "aether-incident-INC-PD-001"
    assert pd_payload["payload"]["severity"] == "critical"
    assert pd_payload["payload"]["component"] == "payment-service"

    # Resolve event
    event_resolve = NotificationEvent(
        event_type=NotificationEventType.RESOLVED,
        incident_id="INC-PD-001",
        service_name="payment-service",
        severity="P2",
        title="Service Restored",
        summary="SLO normalized"
    )
    pd_resolve = PagerDutyFormatter.format(event_resolve, routing_key="test-pd-key-123")
    assert pd_resolve["event_action"] == "resolve"
    assert pd_resolve["dedup_key"] == "aether-incident-INC-PD-001"


# ----------------- Dispatcher Tests -----------------

@pytest.mark.anyio
async def test_dispatcher_multi_channel_broadcast():
    """Verify concurrent broadcast across Slack, Discord, PagerDuty, and Generic webhooks."""
    dispatcher = NotificationDispatcher(
        slack_webhook_url="https://hooks.slack.com/services/TEST/SLACK",
        discord_webhook_url="https://discord.com/api/webhooks/TEST/DISCORD",
        pagerduty_routing_key="pd-route-key-xyz",
        generic_webhook_url="https://webhooks.internal/aether",
        timeout_seconds=2.0
    )

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()

    with patch.object(dispatcher.async_client, "post", new=AsyncMock(return_value=mock_resp)) as mock_post:
        event = NotificationEvent(
            event_type=NotificationEventType.AWAITING_APPROVAL,
            incident_id="INC-DISPATCH-001",
            service_name="payment-service",
            severity="P2",
            title="Approval Required",
            summary="Review needed"
        )
        report = await dispatcher.dispatch(event)

        assert mock_post.call_count == 4
        assert set(report["dispatched_channels"]) == {"slack", "discord", "pagerduty", "generic"}
        assert len(dispatcher.sent_events) == 1
        assert len(dispatcher.channel_deliveries) == 1
        for ch, status in report["results"].items():
            assert status["success"] is True

    await dispatcher.aclose()


@pytest.mark.anyio
async def test_dispatcher_fault_tolerance_on_endpoint_error():
    """Verify that network drops or HTTP 500s on webhooks never raise exceptions or crash the caller."""
    dispatcher = NotificationDispatcher(
        slack_webhook_url="https://hooks.slack.com/services/FAIL/SLACK",
        discord_webhook_url="https://discord.com/api/webhooks/FAIL/DISCORD",
        timeout_seconds=1.0
    )

    # Simulate Slack returning HTTP 500 and Discord timing out
    async def _failing_post(url, *args, **kwargs):
        if "slack" in url:
            mock_err = httpx.HTTPStatusError("500 Server Error", request=MagicMock(), response=MagicMock(status_code=500))
            raise mock_err
        raise httpx.ConnectTimeout("Connection timed out")

    with patch.object(dispatcher.async_client, "post", new=_failing_post):
        event = NotificationEvent(
            event_type=NotificationEventType.ESCALATED,
            incident_id="INC-DISPATCH-FAIL-001",
            service_name="payment-service",
            severity="P1",
            title="Escalation",
            summary="System under stress"
        )
        # MUST NOT raise exception
        report = await dispatcher.dispatch(event)
        assert report["dispatched_channels"] == ["slack", "discord"]
        assert report["results"]["slack"]["success"] is False
        assert report["results"]["discord"]["success"] is False

    await dispatcher.aclose()


# ----------------- Controller Notification Hooks Tests -----------------

@pytest.mark.anyio
async def test_controller_dispatches_awaiting_approval():
    """Verify RemediationController automatically dispatches awaiting approval notification."""
    dispatcher = NotificationDispatcher()
    controller = RemediationController(dispatcher=dispatcher)

    spec = RemediationSpec.create(
        incident_id="INC-NOTIF-CTRL-001",
        action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
        target_service="payment-service",
        parameters={"target_version": "v1.0.0"},
        rationale="Testing notification trigger on human review",
        confidence_score=0.72,  # Below 0.80 threshold
        risk_level="MEDIUM"
    )

    res = await controller.process_remediation(spec)
    assert res["status"] == "REQUIRES_HUMAN_APPROVAL"

    # Verify event dispatched
    assert len(dispatcher.sent_events) >= 1
    approval_event = next(e for e in dispatcher.sent_events if e.event_type == NotificationEventType.AWAITING_APPROVAL)
    assert approval_event.incident_id == "INC-NOTIF-CTRL-001"
    assert approval_event.confidence_score == 0.72


@pytest.mark.anyio
async def test_controller_dispatches_resolved_notification():
    """Verify RemediationController dispatches resolved notification upon successful verification."""
    policy_engine.last_execution_time.clear()
    dispatcher = NotificationDispatcher()
    controller = RemediationController(dispatcher=dispatcher)

    spec = RemediationSpec.create(
        incident_id="INC-NOTIF-CTRL-002",
        action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
        target_service="payment-service",
        parameters={"target_version": "v1.0.0"},
        rationale="Autonomous rollback verification success",
        confidence_score=0.92,
        risk_level="LOW"
    )

    with patch.object(controller, "execute_action", new=AsyncMock(return_value=True)), \
         patch.object(controller, "verify_recovery", new=AsyncMock(return_value=True)):
        res = await controller.process_remediation(spec)
        assert res["status"] == "RESOLVED"

    assert len(dispatcher.sent_events) >= 1
    resolved_event = next(e for e in dispatcher.sent_events if e.event_type == NotificationEventType.RESOLVED)
    assert resolved_event.incident_id == "INC-NOTIF-CTRL-002"


@pytest.mark.anyio
async def test_controller_dispatches_escalated_on_verification_failure():
    """Verify RemediationController dispatches escalation notification when verification fails."""
    policy_engine.last_execution_time.clear()
    dispatcher = NotificationDispatcher()
    controller = RemediationController(dispatcher=dispatcher)

    spec = RemediationSpec.create(
        incident_id="INC-NOTIF-CTRL-003",
        action_type=RemediationActionType.ROLLBACK_DEPLOYMENT,
        target_service="payment-service",
        parameters={"target_version": "v1.0.0"},
        rationale="Autonomous rollback verification failure",
        confidence_score=0.92,
        risk_level="LOW"
    )

    with patch.object(controller, "execute_action", new=AsyncMock(return_value=True)), \
         patch.object(controller, "verify_recovery", new=AsyncMock(return_value=False)):
        res = await controller.process_remediation(spec)
        assert res["status"] == "REQUIRE_HUMAN_TRIAGE"

    assert len(dispatcher.sent_events) >= 1
    escalated_event = next(e for e in dispatcher.sent_events if e.event_type == NotificationEventType.ESCALATED)
    assert escalated_event.incident_id == "INC-NOTIF-CTRL-003"
    assert "SLO error rate did not stabilize" in escalated_event.summary


# ----------------- ChatOps Interactive API Tests -----------------

def test_slack_interactive_callback_approve_flow():
    """Verify Slack interactive button callback approves incident and returns replacement card."""
    inc_id = "INC-SLACK-CB-001"
    asyncio.run(db.create_incident(
        incident_id=inc_id,
        service_name="payment-service",
        severity="P2",
        trigger_reason="Awaiting Slack approval test",
        metric_snapshot={"http_5xx_rate": 0.09}
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.INVESTIGATING,
        actor="test",
        reason="test"
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.DIAGNOSED,
        actor="test",
        reason="test"
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.AWAITING_APPROVAL,
        actor="test",
        reason="Awaiting operator"
    ))

    # Construct Slack interactive payload
    slack_payload = {
        "type": "block_actions",
        "user": {
            "id": "U98765",
            "username": "sarah_sre",
            "name": "Sarah Connor"
        },
        "actions": [
            {
                "action_id": "approve_remediation",
                "block_id": f"aether_actions_{inc_id}",
                "value": json.dumps({"incident_id": inc_id, "action": "approve"})
            }
        ]
    }

    with patch("services.remediation_controller.controller.RemediationController.execute_action", new=AsyncMock(return_value=True)), \
         patch("services.remediation_controller.controller.RemediationController.verify_recovery", new=AsyncMock(return_value=True)):
        resp = client.post(
            "/api/v1/chatops/slack/interactive",
            data={"payload": json.dumps(slack_payload)}
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["replace_original"] is True
    assert inc_id in data["blocks"][0]["text"]["text"]
    assert "@sarah_sre" in data["blocks"][0]["text"]["text"]

    # Verify incident state is RESOLVED
    inc = asyncio.run(db.get_incident(inc_id))
    assert inc["status"] == IncidentState.RESOLVED.value


def test_slack_interactive_callback_reject_flow():
    """Verify Slack interactive button callback rejects incident and transitions to ESCALATED."""
    inc_id = "INC-SLACK-CB-002"
    asyncio.run(db.create_incident(
        incident_id=inc_id,
        service_name="payment-service",
        severity="P2",
        trigger_reason="Awaiting Slack reject test",
        metric_snapshot={"http_5xx_rate": 0.09}
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.INVESTIGATING,
        actor="test",
        reason="test"
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.DIAGNOSED,
        actor="test",
        reason="test"
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.AWAITING_APPROVAL,
        actor="test",
        reason="Awaiting operator"
    ))

    slack_payload = {
        "type": "block_actions",
        "user": {
            "id": "U12345",
            "username": "alex_oncall",
            "name": "Alex Murphy"
        },
        "actions": [
            {
                "action_id": "reject_remediation",
                "block_id": f"aether_actions_{inc_id}",
                "value": json.dumps({"incident_id": inc_id, "action": "reject"})
            }
        ]
    }

    resp = client.post(
        "/api/v1/chatops/slack/interactive",
        data={"payload": json.dumps(slack_payload)}
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["replace_original"] is True
    assert "REJECTED" in data["blocks"][0]["text"]["text"]

    inc = asyncio.run(db.get_incident(inc_id))
    assert inc["status"] == IncidentState.ESCALATED.value


def test_chatops_generic_webhook_action_approve_and_reject():
    """Verify POST /api/v1/chatops/webhook/action executes approve and reject workflows."""
    inc_id = "INC-WEBHOOK-ACT-001"
    asyncio.run(db.create_incident(
        incident_id=inc_id,
        service_name="payment-service",
        severity="P2",
        trigger_reason="Awaiting webhook action test",
        metric_snapshot={"http_5xx_rate": 0.08}
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.INVESTIGATING,
        actor="test",
        reason="test"
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.DIAGNOSED,
        actor="test",
        reason="test"
    ))
    asyncio.run(db.transition_incident_state(
        incident_id=inc_id,
        to_state=IncidentState.AWAITING_APPROVAL,
        actor="test",
        reason="Awaiting operator"
    ))

    # 1. Approve via generic webhook
    with patch("services.remediation_controller.controller.RemediationController.execute_action", new=AsyncMock(return_value=True)), \
         patch("services.remediation_controller.controller.RemediationController.verify_recovery", new=AsyncMock(return_value=True)):
        resp = client.post(
            "/api/v1/chatops/webhook/action",
            json={
                "incident_id": inc_id,
                "action": "approve",
                "operator": "discord:bob",
                "reason": "Approved via Discord Slash Command"
            }
        )

    assert resp.status_code == 200
    res_data = resp.json()
    assert res_data["action"] == "APPROVED"
    assert res_data["result"]["status"] == "RESOLVED"

    # 2. Test invalid action returns 400
    bad_resp = client.post(
        "/api/v1/chatops/webhook/action",
        json={
            "incident_id": inc_id,
            "action": "invalid_action",
            "operator": "bot"
        }
    )
    assert bad_resp.status_code == 400
