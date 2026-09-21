"""
Payload formatters for Slack Block Kit, Discord Embeds, and PagerDuty Events API v2.
"""

import datetime
import json
from typing import Any, Dict, List, Optional
from services.notification_service.models import NotificationEvent, NotificationEventType


class SlackFormatter:
    """Formats incident lifecycle events into Slack Block Kit messages."""

    @staticmethod
    def format(event: NotificationEvent) -> Dict[str, Any]:
        emoji_map = {
            NotificationEventType.AWAITING_APPROVAL: "🚨",
            NotificationEventType.APPROVED: "⏳",
            NotificationEventType.EXECUTING: "⚙️",
            NotificationEventType.RESOLVED: "✅",
            NotificationEventType.ESCALATED: "🔥",
            NotificationEventType.REJECTED: "🛑"
        }
        emoji = emoji_map.get(event.event_type, "ℹ️")
        confidence_str = f"{event.confidence_score * 100:.1f}%" if event.confidence_score is not None else "N/A"

        blocks: List[Dict[str, Any]] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} [Aether] {event.title}"[:150],
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{event.summary}*"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Incident ID:*\n`{event.incident_id}`"},
                    {"type": "mrkdwn", "text": f"*Target Service:*\n`{event.service_name}`"},
                    {"type": "mrkdwn", "text": f"*Severity:*\n`{event.severity}`"},
                    {"type": "mrkdwn", "text": f"*Confidence:*\n`{confidence_str}`"},
                    {"type": "mrkdwn", "text": f"*Action Type:*\n`{event.action_type or 'None'}`"},
                    {"type": "mrkdwn", "text": f"*Status:*\n`{event.event_type.value}`"}
                ]
            }
        ]

        if event.rationale:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Root Cause & Rationale:*\n> {event.rationale}"
                }
            })

        blocks.append({"type": "divider"})

        # Interactive action buttons for AWAITING_APPROVAL
        if event.event_type == NotificationEventType.AWAITING_APPROVAL:
            blocks.append({
                "type": "actions",
                "block_id": f"aether_actions_{event.incident_id}",
                "elements": [
                    {
                        "type": "button",
                        "action_id": "approve_remediation",
                        "text": {
                            "type": "plain_text",
                            "text": "Approve Remediation",
                            "emoji": True
                        },
                        "style": "primary",
                        "value": json.dumps({"incident_id": event.incident_id, "action": "approve"})
                    },
                    {
                        "type": "button",
                        "action_id": "reject_remediation",
                        "text": {
                            "type": "plain_text",
                            "text": "Reject / Escalate",
                            "emoji": True
                        },
                        "style": "danger",
                        "value": json.dumps({"incident_id": event.incident_id, "action": "reject"})
                    }
                ]
            })

        # Footer context
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Aether Autonomous SRE Engine • <{event.base_url}/api/v1/incidents/{event.incident_id}|View Incident Details>"
                }
            ]
        })

        return {
            "text": f"[{event.event_type.value}] {event.title} - {event.incident_id}",
            "blocks": blocks
        }

    @staticmethod
    def format_interactive_ack(
        incident_id: str,
        operator: str,
        action: str,
        message: str
    ) -> Dict[str, Any]:
        """Replacement Slack message body after button click (removes buttons to prevent double-click)."""
        icon = "✅" if action.lower() == "approve" else "🛑"
        verb = "APPROVED" if action.lower() == "approve" else "REJECTED"
        return {
            "replace_original": True,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{icon} *Incident `{incident_id}` {verb}*\n\n"
                                f"*Operator:* <@{operator}>\n"
                                f"*Result:* {message}\n"
                                f"*Timestamp:* `{datetime.datetime.now(datetime.timezone.utc).isoformat()}`"
                    }
                }
            ]
        }


class DiscordFormatter:
    """Formats incident lifecycle events into Discord rich embed messages."""

    @staticmethod
    def format(event: NotificationEvent) -> Dict[str, Any]:
        color_map = {
            NotificationEventType.AWAITING_APPROVAL: 0xF39C12,  # Amber
            NotificationEventType.APPROVED: 0x3498DB,           # Blue
            NotificationEventType.EXECUTING: 0x3498DB,          # Blue
            NotificationEventType.RESOLVED: 0x2ECC71,           # Emerald Green
            NotificationEventType.ESCALATED: 0xE74C3C,          # Crimson Red
            NotificationEventType.REJECTED: 0x95A5A6            # Slate Gray
        }
        color = color_map.get(event.event_type, 0x34495E)
        confidence_str = f"{event.confidence_score * 100:.1f}%" if event.confidence_score is not None else "N/A"

        fields = [
            {"name": "Incident ID", "value": f"`{event.incident_id}`", "inline": True},
            {"name": "Target Service", "value": f"`{event.service_name}`", "inline": True},
            {"name": "Severity", "value": f"`{event.severity}`", "inline": True},
            {"name": "Action Type", "value": f"`{event.action_type or 'None'}`", "inline": True},
            {"name": "Confidence", "value": f"`{confidence_str}`", "inline": True},
            {"name": "Status", "value": f"`{event.event_type.value}`", "inline": True}
        ]

        if event.rationale:
            fields.append({
                "name": "RCA Rationale",
                "value": event.rationale[:1024],
                "inline": False
            })

        embed = {
            "title": f"[Aether] {event.title}"[:256],
            "description": event.summary[:2048],
            "url": f"{event.base_url}/api/v1/incidents/{event.incident_id}",
            "color": color,
            "fields": fields,
            "footer": {
                "text": f"Aether Autonomous SRE • Event ID: {event.event_id[:8]}"
            },
            "timestamp": datetime.datetime.fromtimestamp(event.timestamp, tz=datetime.timezone.utc).isoformat()
        }

        return {
            "embeds": [embed]
        }


class PagerDutyFormatter:
    """Formats incident lifecycle events for PagerDuty Events API v2."""

    @staticmethod
    def format(event: NotificationEvent, routing_key: str) -> Dict[str, Any]:
        # Map event_action
        if event.event_type == NotificationEventType.RESOLVED:
            event_action = "resolve"
        else:
            event_action = "trigger"

        # Map severity
        if event.severity == "P1" or event.event_type == NotificationEventType.ESCALATED:
            pd_severity = "critical"
        elif event.severity == "P2":
            pd_severity = "error"
        elif event.severity == "P3":
            pd_severity = "warning"
        else:
            pd_severity = "info"

        dedup_key = f"aether-incident-{event.incident_id}"

        payload: Dict[str, Any] = {
            "summary": f"[{event.severity}] {event.title} on {event.service_name}: {event.summary}"[:1024],
            "source": "aether-sre-engine",
            "severity": pd_severity,
            "component": event.service_name,
            "group": "production",
            "class": event.action_type or "sre-incident",
            "custom_details": {
                "incident_id": event.incident_id,
                "event_type": event.event_type.value,
                "action_type": event.action_type,
                "confidence_score": event.confidence_score,
                "rationale": event.rationale,
                "parameters": event.parameters or {},
                "metadata": event.metadata or {}
            }
        }

        return {
            "routing_key": routing_key,
            "event_action": event_action,
            "dedup_key": dedup_key,
            "client": "Aether Autonomous SRE",
            "client_url": f"{event.base_url}/api/v1/incidents/{event.incident_id}",
            "payload": payload
        }
