import hashlib
import json
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class RemediationActionType(str, Enum):
    ROLLBACK_DEPLOYMENT = "ROLLBACK_DEPLOYMENT"
    SCALE_REPLICAS = "SCALE_REPLICAS"
    RESTART_CONTAINER = "RESTART_CONTAINER"
    UPDATE_CONFIG = "UPDATE_CONFIG"

class RemediationSpec(BaseModel):
    remediation_id: str
    incident_id: str
    action_type: RemediationActionType
    target_service: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    rationale: str
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    risk_level: str = Field(default="LOW", description="LOW, MEDIUM, HIGH")
    requires_human_approval: bool = False
    idempotency_key: str

    @classmethod
    def create(
        cls,
        incident_id: str,
        action_type: RemediationActionType,
        target_service: str,
        parameters: Dict[str, Any],
        rationale: str,
        confidence_score: float,
        risk_level: str = "LOW"
    ) -> "RemediationSpec":
        # Generate deterministic idempotency key using canonical JSON serialization
        canon_params = json.dumps(parameters, sort_keys=True, default=str)
        key_raw = f"{incident_id}:{action_type.value}:{target_service}:{canon_params}"
        idempotency_key = hashlib.sha256(key_raw.encode("utf-8")).hexdigest()[:32]
        
        remediation_id = f"REM-{idempotency_key[:8]}"
        requires_approval = (risk_level == "HIGH" or confidence_score < 0.80)

        return cls(
            remediation_id=remediation_id,
            incident_id=incident_id,
            action_type=action_type,
            target_service=target_service,
            parameters=parameters,
            rationale=rationale,
            confidence_score=confidence_score,
            risk_level=risk_level,
            requires_human_approval=requires_approval,
            idempotency_key=idempotency_key
        )

class RCADiagnosis(BaseModel):
    incident_id: str
    service_name: str
    root_cause: str
    evidence_sources: List[str]
    causality_chain: str
    confidence_score: float
    remediation_plan: Optional[RemediationSpec] = None
    status: str = Field(default="CONFIRMED", description="CONFIRMED, INCONCLUSIVE, HUMAN_TRIAGE_REQUIRED")
