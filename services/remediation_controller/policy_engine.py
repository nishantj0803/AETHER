import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from services.rca_agent.schema import RemediationActionType, RemediationSpec

logger = logging.getLogger("aether.policy_engine")

@dataclass
class PolicyValidationResult:
    allowed: bool
    reason: str
    risk_level: str
    dry_run_passed: bool

class ZeroTrustPolicyEngine:
    """
    Independent Zero-Trust Policy Engine.
    Enforces deterministic safety boundaries, blast radius clamps, rate limits,
    and idempotency checks on all proposed remediation specs.
    The LLM proposes; this engine decides.
    """
    ALLOWED_ACTIONS = {
        RemediationActionType.ROLLBACK_DEPLOYMENT,
        RemediationActionType.SCALE_REPLICAS,
        RemediationActionType.RESTART_CONTAINER,
        RemediationActionType.UPDATE_CONFIG
    }

    ALLOWED_TARGETS = {"payment-service", "checkout-service", "order-service"}
    MAX_REPLICAS = 10
    MIN_REPLICAS = 1
    RATE_LIMIT_WINDOW_SECONDS = 300  # Max 1 action per service every 5 minutes

    def __init__(self):
        # Service -> last executed action timestamp
        self.last_execution_time: Dict[str, float] = {}
        # Set of active or completed idempotency keys
        self.executed_idempotency_keys: set = set()

    def validate(self, spec: RemediationSpec, dry_run: bool = True) -> PolicyValidationResult:
        logger.info(f"Evaluating policy for remediation {spec.remediation_id} on {spec.target_service}...")

        # 1. Action Whitelist Check
        if spec.action_type not in self.ALLOWED_ACTIONS:
            return PolicyValidationResult(
                allowed=False,
                reason=f"Action '{spec.action_type}' is not whitelisted by safety policy",
                risk_level="HIGH",
                dry_run_passed=False
            )

        # 2. Target Service Whitelist Check
        if spec.target_service not in self.ALLOWED_TARGETS:
            return PolicyValidationResult(
                allowed=False,
                reason=f"Target service '{spec.target_service}' is outside permitted operational boundary",
                risk_level="HIGH",
                dry_run_passed=False
            )

        # 3. Idempotency Check (Prevent duplicate execution)
        if spec.idempotency_key in self.executed_idempotency_keys:
            return PolicyValidationResult(
                allowed=False,
                reason=f"Duplicate execution blocked: idempotency_key '{spec.idempotency_key}' already processed",
                risk_level=spec.risk_level,
                dry_run_passed=False
            )

        # 4. Rate Limiting / Blast Radius Flapping Guard
        last_time = self.last_execution_time.get(spec.target_service, 0)
        elapsed = time.time() - last_time
        if elapsed < self.RATE_LIMIT_WINDOW_SECONDS and not dry_run:
            return PolicyValidationResult(
                allowed=False,
                reason=f"Rate limit exceeded: target '{spec.target_service}' was remediated {elapsed:.0f}s ago (< {self.RATE_LIMIT_WINDOW_SECONDS}s cooldown)",
                risk_level="HIGH",
                dry_run_passed=False
            )

        # 5. Parameter Boundary Checks
        if spec.action_type == RemediationActionType.SCALE_REPLICAS:
            to_replicas = spec.parameters.get("to_replicas", 1)
            if to_replicas > self.MAX_REPLICAS or to_replicas < self.MIN_REPLICAS:
                return PolicyValidationResult(
                    allowed=False,
                    reason=f"Scale parameter {to_replicas} violates bounds [{self.MIN_REPLICAS}, {self.MAX_REPLICAS}]",
                    risk_level="HIGH",
                    dry_run_passed=False
                )

        if spec.action_type == RemediationActionType.ROLLBACK_DEPLOYMENT:
            target_version = spec.parameters.get("target_version")
            if not target_version:
                return PolicyValidationResult(
                    allowed=False,
                    reason="Rollback parameter 'target_version' is required",
                    risk_level="HIGH",
                    dry_run_passed=False
                )

        # 6. Confidence Check
        if spec.confidence_score < 0.80:
            return PolicyValidationResult(
                allowed=False,
                reason=f"Confidence score ({spec.confidence_score:.2f}) below autonomous safety threshold (0.80)",
                risk_level="HIGH",
                dry_run_passed=True
            )

        logger.info(f"✅ Policy validation PASSED for remediation {spec.remediation_id}")
        return PolicyValidationResult(
            allowed=True,
            reason="All policy guardrails, parameters, and blast-radius checks passed",
            risk_level=spec.risk_level,
            dry_run_passed=True
        )

    def mark_executed(self, spec: RemediationSpec):
        """Record execution to update rate-limit and idempotency gates."""
        self.executed_idempotency_keys.add(spec.idempotency_key)
        self.last_execution_time[spec.target_service] = time.time()

policy_engine = ZeroTrustPolicyEngine()
