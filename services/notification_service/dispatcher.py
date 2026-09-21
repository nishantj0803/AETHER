"""
Multi-channel Non-blocking Notification Dispatcher for Aether ChatOps & Alerting.

Dispatches incident lifecycle events to:
- Slack (Block Kit interactive cards)
- Discord (Rich embeds)
- PagerDuty (Events API v2)
- Generic Webhook (Raw JSON)

Guarantees zero-blocking: delivery errors or external endpoint degradation
will never disrupt the autonomous SRE control loop.
"""

import asyncio
import logging
import os
from typing import Any, Dict, List, Optional
import httpx

from services.notification_service.models import NotificationEvent, NotificationEventType
from services.notification_service.formatters import (
    SlackFormatter,
    DiscordFormatter,
    PagerDutyFormatter
)
from services.rca_agent.schema import RemediationSpec

logger = logging.getLogger("aether.notification_dispatcher")


class NotificationDispatcher:
    """Enterprise multi-channel notification dispatcher with connection pooling and non-blocking delivery."""

    def __init__(
        self,
        slack_webhook_url: Optional[str] = None,
        discord_webhook_url: Optional[str] = None,
        pagerduty_routing_key: Optional[str] = None,
        pagerduty_events_url: Optional[str] = None,
        generic_webhook_url: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout_seconds: float = 5.0
    ):
        self.slack_webhook_url = slack_webhook_url or os.getenv("SLACK_WEBHOOK_URL")
        self.discord_webhook_url = discord_webhook_url or os.getenv("DISCORD_WEBHOOK_URL")
        self.pagerduty_routing_key = pagerduty_routing_key or os.getenv("PAGERDUTY_ROUTING_KEY")
        self.pagerduty_events_url = (
            pagerduty_events_url or
            os.getenv("PAGERDUTY_EVENTS_URL", "https://events.pagerduty.com/v2/enqueue")
        )
        self.generic_webhook_url = generic_webhook_url or os.getenv("GENERIC_WEBHOOK_URL")
        self.base_url = base_url or os.getenv("AETHER_BASE_URL", "http://localhost:8000")
        self.timeout = timeout_seconds

        self._async_client: Optional[httpx.AsyncClient] = None

        # In-memory history for local development, auditability, and testing
        self.sent_events: List[NotificationEvent] = []
        self.channel_deliveries: List[Dict[str, Any]] = []

    @property
    def async_client(self) -> httpx.AsyncClient:
        """Persistent pooled HTTP client with keep-alive and request timeout."""
        if self._async_client is None or self._async_client.is_closed:
            self._async_client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout, connect=2.0),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
            )
        return self._async_client

    async def aclose(self):
        """Cleanly close persistent HTTP client connection pool."""
        if self._async_client is not None and not self._async_client.is_closed:
            await self._async_client.aclose()
            self._async_client = None

    async def dispatch(self, event: NotificationEvent) -> Dict[str, Any]:
        """
        Broadcast notification event to all configured channels concurrently.
        Guaranteed non-blocking: catches and absorbs all network/http exceptions.
        """
        event.base_url = self.base_url
        self.sent_events.append(event)

        tasks = []
        channel_names = []

        if self.slack_webhook_url:
            tasks.append(self._send_slack(event))
            channel_names.append("slack")

        if self.discord_webhook_url:
            tasks.append(self._send_discord(event))
            channel_names.append("discord")

        if self.pagerduty_routing_key:
            tasks.append(self._send_pagerduty(event))
            channel_names.append("pagerduty")

        if self.generic_webhook_url:
            tasks.append(self._send_generic(event))
            channel_names.append("generic")

        if not tasks:
            logger.debug(f"No external notification channels configured for event {event.event_id}")
            return {
                "event_id": event.event_id,
                "dispatched_channels": [],
                "results": {}
            }

        results = await asyncio.gather(*tasks, return_exceptions=True)

        delivery_report: Dict[str, Any] = {}
        for name, res in zip(channel_names, results):
            if isinstance(res, Exception):
                logger.warning(f"Failed to dispatch notification to {name}: {res}")
                delivery_report[name] = {"success": False, "error": str(res)}
            else:
                delivery_report[name] = {"success": True, "detail": res}

        self.channel_deliveries.append({
            "event_id": event.event_id,
            "report": delivery_report
        })

        return {
            "event_id": event.event_id,
            "dispatched_channels": channel_names,
            "results": delivery_report
        }

    async def _send_slack(self, event: NotificationEvent) -> Dict[str, Any]:
        payload = SlackFormatter.format(event)
        resp = await self.async_client.post(self.slack_webhook_url, json=payload)
        resp.raise_for_status()
        return {"status_code": resp.status_code}

    async def _send_discord(self, event: NotificationEvent) -> Dict[str, Any]:
        payload = DiscordFormatter.format(event)
        resp = await self.async_client.post(self.discord_webhook_url, json=payload)
        resp.raise_for_status()
        return {"status_code": resp.status_code}

    async def _send_pagerduty(self, event: NotificationEvent) -> Dict[str, Any]:
        payload = PagerDutyFormatter.format(event, routing_key=self.pagerduty_routing_key)
        resp = await self.async_client.post(self.pagerduty_events_url, json=payload)
        resp.raise_for_status()
        return {"status_code": resp.status_code}

    async def _send_generic(self, event: NotificationEvent) -> Dict[str, Any]:
        payload = event.model_dump()
        resp = await self.async_client.post(self.generic_webhook_url, json=payload)
        resp.raise_for_status()
        return {"status_code": resp.status_code}

    # ----------------- High-Level Lifecycle Dispatch Helpers -----------------

    async def notify_awaiting_approval(
        self,
        spec: RemediationSpec,
        reason: str,
        service_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Dispatch notification when an incident requires manual operator review."""
        event = NotificationEvent(
            event_type=NotificationEventType.AWAITING_APPROVAL,
            incident_id=spec.incident_id,
            service_name=service_name or spec.target_service,
            severity="P2",
            title=f"Incident {spec.incident_id}: Human Approval Required",
            summary=f"Automated remediation blocked by zero-trust policy: {reason}",
            confidence_score=spec.confidence_score,
            action_type=spec.action_type.value if hasattr(spec.action_type, "value") else str(spec.action_type),
            parameters=spec.parameters,
            rationale=spec.rationale
        )
        return await self.dispatch(event)

    async def notify_resolved(
        self,
        spec: RemediationSpec,
        recovery_duration: Optional[float] = None,
        metrics: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Dispatch notification when post-remediation SLO verification succeeds."""
        duration_str = f" in {recovery_duration:.1f}s" if recovery_duration else ""
        event = NotificationEvent(
            event_type=NotificationEventType.RESOLVED,
            incident_id=spec.incident_id,
            service_name=spec.target_service,
            severity="P2",
            title=f"Incident {spec.incident_id} Resolved",
            summary=f"Remediation '{spec.action_type.value}' successfully executed and verified{duration_str}. SLO normalized.",
            confidence_score=spec.confidence_score,
            action_type=spec.action_type.value if hasattr(spec.action_type, "value") else str(spec.action_type),
            parameters=spec.parameters,
            rationale=spec.rationale,
            metadata={"metrics": metrics, "recovery_duration_seconds": recovery_duration}
        )
        return await self.dispatch(event)

    async def notify_escalated(
        self,
        spec: RemediationSpec,
        reason: str,
        service_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Dispatch notification when remediation verification or execution fails (escalation)."""
        event = NotificationEvent(
            event_type=NotificationEventType.ESCALATED,
            incident_id=spec.incident_id,
            service_name=service_name or spec.target_service,
            severity="P1",
            title=f"🚨 CRITICAL: Incident {spec.incident_id} Escalated",
            summary=f"Automated recovery failed verification: {reason}. Manual SRE triage required.",
            confidence_score=spec.confidence_score,
            action_type=spec.action_type.value if hasattr(spec.action_type, "value") else str(spec.action_type),
            parameters=spec.parameters,
            rationale=spec.rationale
        )
        return await self.dispatch(event)

    async def notify_rejected(
        self,
        incident_id: str,
        rejecter: str,
        reason: str,
        service_name: str = "payment-service"
    ) -> Dict[str, Any]:
        """Dispatch notification when an operator rejects a proposed remediation."""
        event = NotificationEvent(
            event_type=NotificationEventType.REJECTED,
            incident_id=incident_id,
            service_name=service_name,
            severity="P2",
            title=f"Incident {incident_id} Rejected by Operator",
            summary=f"Remediation was rejected by <@{rejecter}>: {reason}",
            rationale=reason
        )
        return await self.dispatch(event)


notification_dispatcher = NotificationDispatcher()
