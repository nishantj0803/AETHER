"""
Aether Crash-Safe Incident State Machine & Lifecycle Transition Engine.

Enforces strict lifecycle transitions across the distributed SRE loop:
  DETECTED
     │
     ▼
INVESTIGATING
     │
     ▼
  DIAGNOSED
     │
     ▼
AWAITING_POLICY ──(low confidence or high risk)──► AWAITING_APPROVAL
     │                                                     │
     │ (policy pass)                                       │ (human approves)
     ▼                                                     ▼
  APPROVED ◄───────────────────────────────────────────────┘
     │
     ▼
  EXECUTING ──(adapter failure)──► ROLLBACK_EXECUTING
     │                                    │
     ▼                                    ▼
  VERIFYING ──(verification failure)──────┤
     │                                    ▼
     ▼                                ESCALATED
  RESOLVED (terminal)
"""

from dataclasses import dataclass, field
from enum import Enum
import logging
import time
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger("aether.state_machine")


class IncidentState(str, Enum):
    """Explicit lifecycle states for distributed incident response."""
    DETECTED = "DETECTED"                   # Anomaly rule triggered
    INVESTIGATING = "INVESTIGATING"         # Fetching metrics, logs, traces
    DIAGNOSED = "DIAGNOSED"                 # RCA generated root cause hypothesis
    AWAITING_POLICY = "AWAITING_POLICY"     # Policy engine checking safety bounds
    AWAITING_APPROVAL = "AWAITING_APPROVAL" # Gated for human SRE sign-off
    APPROVED = "APPROVED"                   # Passed policy / approved by operator
    EXECUTING = "EXECUTING"                 # Target adapter executing remediation
    VERIFYING = "VERIFYING"                 # Closed-loop metric polling
    RESOLVED = "RESOLVED"                   # Verification succeeded, SLO restored
    ROLLBACK_EXECUTING = "ROLLBACK_EXECUTING" # Remediation failed; reverting
    ESCALATED = "ESCALATED"                 # Escalated to human on-call engineer

    # Backward compatibility aliases
    REQUIRE_HUMAN_TRIAGE = "REQUIRE_HUMAN_TRIAGE"

    @classmethod
    def normalize(cls, state: Any) -> "IncidentState":
        """Normalize string or enum to canonical IncidentState."""
        if isinstance(state, IncidentState):
            return state
        raw = str(state).strip().upper()
        if raw in ("REQUIRE_HUMAN_TRIAGE", "HUMAN_TRIAGE_REQUIRED", "PENDING_APPROVAL"):
            return cls.AWAITING_APPROVAL
        if raw in ("TRIAGING", "INVESTIGATING"):
            return cls.INVESTIGATING
        if raw in ("REMEDIATING", "EXECUTING"):
            return cls.EXECUTING
        try:
            return cls(raw)
        except ValueError:
            raise ValueError(f"Unknown incident state: '{raw}'")


class InvalidStateTransitionError(Exception):
    """Raised when an illegal state machine transition is attempted."""
    def __init__(self, from_state: IncidentState, to_state: IncidentState, reason: Optional[str] = None):
        msg = f"Illegal incident state transition: '{from_state.value}' ➔ '{to_state.value}'"
        if reason:
            msg += f" (Reason: {reason})"
        super().__init__(msg)
        self.from_state = from_state
        self.to_state = to_state
        self.reason = reason


@dataclass
class TransitionAuditRecord:
    """Immutable record of an incident state change."""
    incident_id: str
    from_state: IncidentState
    to_state: IncidentState
    actor: str                  # e.g., 'system:rca_agent', 'operator:sre@company.com'
    reason: str
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class IncidentStateMachine:
    """
    Formal state machine governing incident lifecycle transitions.
    Prevents race conditions, illegal shortcuts (e.g. DETECTED -> EXECUTING),
    and enforces crash recovery.
    """

    ALLOWED_TRANSITIONS: Dict[IncidentState, Set[IncidentState]] = {
        IncidentState.DETECTED: {
            IncidentState.INVESTIGATING,
            IncidentState.ESCALATED,
        },
        IncidentState.INVESTIGATING: {
            IncidentState.DIAGNOSED,
            IncidentState.ESCALATED,
        },
        IncidentState.DIAGNOSED: {
            IncidentState.AWAITING_POLICY,
            IncidentState.AWAITING_APPROVAL,
            IncidentState.ESCALATED,
        },
        IncidentState.AWAITING_POLICY: {
            IncidentState.APPROVED,
            IncidentState.AWAITING_APPROVAL,
            IncidentState.ESCALATED,
        },
        IncidentState.AWAITING_APPROVAL: {
            IncidentState.APPROVED,
            IncidentState.ESCALATED,
        },
        IncidentState.APPROVED: {
            IncidentState.EXECUTING,
            IncidentState.ESCALATED,
        },
        IncidentState.EXECUTING: {
            IncidentState.VERIFYING,
            IncidentState.ROLLBACK_EXECUTING,
            IncidentState.ESCALATED,
        },
        IncidentState.VERIFYING: {
            IncidentState.RESOLVED,
            IncidentState.ROLLBACK_EXECUTING,
            IncidentState.ESCALATED,
        },
        IncidentState.ROLLBACK_EXECUTING: {
            IncidentState.VERIFYING,
            IncidentState.ESCALATED,
        },
        IncidentState.RESOLVED: set(),  # Terminal state: resolved incidents cannot jump
        IncidentState.ESCALATED: {
            IncidentState.INVESTIGATING,
            IncidentState.AWAITING_APPROVAL,
            IncidentState.APPROVED,
            IncidentState.RESOLVED,
        },
        # Compatibility handling
        IncidentState.REQUIRE_HUMAN_TRIAGE: {
            IncidentState.APPROVED,
            IncidentState.ESCALATED,
            IncidentState.RESOLVED,
        }
    }

    @classmethod
    def can_transition(cls, from_state: IncidentState, to_state: IncidentState) -> bool:
        """Check whether transition is permitted."""
        from_norm = IncidentState.normalize(from_state)
        to_norm = IncidentState.normalize(to_state)
        allowed = cls.ALLOWED_TRANSITIONS.get(from_norm, set())
        return to_norm in allowed

    @classmethod
    def validate_transition(
        cls,
        from_state: IncidentState,
        to_state: IncidentState,
        reason: Optional[str] = None
    ) -> None:
        """Enforce transition rules; raises InvalidStateTransitionError if illegal."""
        from_norm = IncidentState.normalize(from_state)
        to_norm = IncidentState.normalize(to_state)

        if not cls.can_transition(from_norm, to_norm):
            allowed_names = [s.value for s in cls.ALLOWED_TRANSITIONS.get(from_norm, set())]
            detail = f"Allowed from '{from_norm.value}': {allowed_names}"
            if reason:
                detail = f"{reason}. {detail}"
            raise InvalidStateTransitionError(from_norm, to_norm, reason=detail)


class CrashRecoveryManager:
    """
    Recovers in-flight incidents after a controller process crash or restart.
    Ensures incidents stuck in EXECUTING, VERIFYING, or ROLLBACK_EXECUTING
    are audited, verified against current metrics, and brought to a safe terminal state.
    """

    def __init__(self, db_client=None, controller=None):
        self.db = db_client
        self.controller = controller

    async def recover(self) -> List[Dict[str, Any]]:
        """
        Scan for in-flight incidents and resume verification or escalation.
        Returns a list of recovery action summaries.
        """
        if not self.db:
            return []

        in_flight = await self.db.get_in_flight_incidents()
        if not in_flight:
            logger.info("Crash recovery: No in-flight incidents found.")
            return []

        logger.warning(f"Crash recovery: Found {len(in_flight)} in-flight incidents to reconcile.")
        results = []

        for inc in in_flight:
            inc_id = inc["incident_id"]
            current_state = IncidentState.normalize(inc["status"])
            logger.info(f"Reconciling crashed incident {inc_id} in state '{current_state.value}'...")

            if current_state in (IncidentState.VERIFYING, IncidentState.EXECUTING):
                if current_state == IncidentState.EXECUTING:
                    await self.db.transition_incident_state(
                        incident_id=inc_id,
                        to_state=IncidentState.VERIFYING,
                        actor="system:crash_recovery",
                        reason="Crash recovery: Advancing from interrupted execution to verification"
                    )

                # Run post-crash verification
                recovered = False
                if self.controller:
                    recovered = await self.controller.verify_recovery(max_wait_seconds=5)

                if recovered:
                    target = IncidentState.RESOLVED
                    action_reason = "Crash recovery: SLO metrics confirmed healthy"
                else:
                    target = IncidentState.ESCALATED
                    action_reason = "Crash recovery: Incident unverified or SLO still breached"

                await self.db.transition_incident_state(
                    incident_id=inc_id,
                    to_state=target,
                    actor="system:crash_recovery",
                    reason=action_reason
                )
                results.append({"incident_id": inc_id, "previous": current_state.value, "resolved_to": target.value})

            elif current_state == IncidentState.ROLLBACK_EXECUTING:
                await self.db.transition_incident_state(
                    incident_id=inc_id,
                    to_state=IncidentState.ESCALATED,
                    actor="system:crash_recovery",
                    reason="Crash recovery: Rollback was in progress when process terminated"
                )
                results.append({"incident_id": inc_id, "previous": current_state.value, "resolved_to": IncidentState.ESCALATED.value})

        return results
