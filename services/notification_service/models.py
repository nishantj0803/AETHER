"""
Pydantic data models for Aether ChatOps and Alerting Notifications.
"""

from enum import Enum
import time
import uuid
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class NotificationEventType(str, Enum):
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    APPROVED = "APPROVED"
    EXECUTING = "EXECUTING"
    RESOLVED = "RESOLVED"
    ESCALATED = "ESCALATED"
    REJECTED = "REJECTED"


class NotificationEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: NotificationEventType
    incident_id: str
    service_name: str
    severity: str = "P2"
    title: str
    summary: str
    confidence_score: Optional[float] = None
    action_type: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    rationale: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    timestamp: float = Field(default_factory=time.time)
    base_url: str = "http://localhost:8000"
