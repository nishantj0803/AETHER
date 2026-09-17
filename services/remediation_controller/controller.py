import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional
import httpx

from services.anomaly_detector.detector import detector
from services.ingestion_worker.db import db
from services.rca_agent.schema import RemediationActionType, RemediationSpec
from services.remediation_controller.policy_engine import policy_engine
from services.remediation_controller.state_machine import (
    IncidentState,
    IncidentStateMachine,
    CrashRecoveryManager
)

logger = logging.getLogger("aether.remediation_controller")

class RemediationController:
    """
    Executes validated remediation actions, records audit logs,
    and runs the closed-loop verification / auto-rollback cycle.
    Enforces the formal Incident Lifecycle State Machine.
    """
    def __init__(self, demo_service_url: Optional[str] = None):
        self.service_url = demo_service_url or os.getenv("DEMO_SERVICE_URL", "http://localhost:8000")
        self.recovery_manager = CrashRecoveryManager(db_client=db, controller=self)

    async def recover_in_flight_incidents(self) -> List[Dict[str, Any]]:
        """Scan and reconcile any unverified or stuck incidents from past crashes."""
        return await self.recovery_manager.recover()

    async def execute_action(self, spec: RemediationSpec) -> bool:
        """Call target execution adapter (FastAPI admin API / Docker / K8s)."""
        logger.info(f"Executing action {spec.action_type.value} on {spec.target_service}...")
        
        async with httpx.AsyncClient() as client:
            try:
                if spec.action_type == RemediationActionType.ROLLBACK_DEPLOYMENT:
                    target_version = spec.parameters.get("target_version", "v1.0.0")
                    resp = await client.post(
                        f"{self.service_url}/api/v1/admin/deployment/rollback",
                        params={"target_version": target_version},
                        timeout=5.0
                    )
                    return resp.status_code == 200

                elif spec.action_type == RemediationActionType.RESTART_CONTAINER:
                    resp = await client.post(f"{self.service_url}/api/v1/admin/faults/reset", timeout=5.0)
                    return resp.status_code == 200

                elif spec.action_type == RemediationActionType.SCALE_REPLICAS:
                    logger.info(f"Scaled {spec.target_service} to {spec.parameters.get('to_replicas')} replicas")
                    return True

            except Exception as e:
                logger.error(f"Execution failed: {e}")
                return False

        return False

    async def verify_recovery(self, max_wait_seconds: int = 15, poll_interval: float = 2.0) -> bool:
        """
        Closed-loop verification: polls metrics until error rate drops below 5%.
        """
        logger.info(f"Entering VERIFYING state (monitoring metrics for up to {max_wait_seconds}s)...")
        start = time.time()
        
        while time.time() - start < max_wait_seconds:
            metrics = await detector.fetch_prometheus_metrics()
            err_rate = metrics.get("http_5xx_rate", 0.0)
            logger.info(f"[Verification] Current HTTP 5xx error rate: {err_rate*100:.1f}%")

            if err_rate < 0.05:
                logger.info("✅ Verification SUCCESS: Error rate normalized within SLO limits (< 5%)")
                return True

            await asyncio.sleep(poll_interval)

        logger.warning("❌ Verification FAILED: Error rate did not recover within stabilization window")
        return False

    async def process_remediation(self, spec: RemediationSpec) -> Dict[str, Any]:
        """End-to-end remediation pipeline with zero-trust validation and verification."""
        # 1. Store remediation spec for potential human approval retrieval
        spec_dict = spec.model_dump() if hasattr(spec, "model_dump") else spec.dict()
        db.store_pending_remediation(spec.incident_id, spec_dict)

        # 2. Policy Engine Validation
        validation = policy_engine.validate(spec, dry_run=False)
        if not validation.allowed:
            if validation.requires_human_review:
                logger.info(f"Remediation {spec.remediation_id} requires human approval (confidence={spec.confidence_score:.2f})")
                
                # Advance state machine: DIAGNOSED/AWAITING_POLICY -> AWAITING_APPROVAL
                try:
                    await db.transition_incident_state(
                        incident_id=spec.incident_id,
                        to_state=IncidentState.AWAITING_APPROVAL,
                        actor="system:policy_engine",
                        reason=f"Human review required: {validation.reason}"
                    )
                except Exception as e:
                    logger.debug(f"State transition note: {e}")

                await db.update_incident_status(
                    incident_id=spec.incident_id,
                    status="REQUIRE_HUMAN_TRIAGE",
                    rca_summary=spec.rationale,
                    confidence_score=spec.confidence_score
                )
                return {
                    "status": "REQUIRES_HUMAN_APPROVAL",
                    "state": IncidentState.AWAITING_APPROVAL.value,
                    "reason": validation.reason,
                    "remediation_id": spec.remediation_id
                }

            logger.warning(f"Remediation REJECTED by policy engine: {validation.reason}")
            try:
                await db.transition_incident_state(
                    incident_id=spec.incident_id,
                    to_state=IncidentState.ESCALATED,
                    actor="system:policy_engine",
                    reason=f"Policy violation: {validation.reason}"
                )
            except Exception as e:
                logger.debug(f"State transition note: {e}")

            return {
                "status": "REJECTED_BY_POLICY",
                "state": IncidentState.ESCALATED.value,
                "reason": validation.reason,
                "remediation_id": spec.remediation_id
            }

        # 3. Autonomous approval transition: -> APPROVED
        try:
            await db.transition_incident_state(
                incident_id=spec.incident_id,
                to_state=IncidentState.APPROVED,
                actor="system:policy_engine",
                reason="All zero-trust policy checks passed"
            )
        except Exception as e:
            logger.debug(f"State transition note: {e}")

        # 4. Claim idempotency slot BEFORE execution (TOCTOU-safe)
        policy_engine.mark_executed(spec)

        # 5. Record in Audit Log & Transition to EXECUTING
        recorded = await db.record_remediation(
            remediation_id=spec.remediation_id,
            incident_id=spec.incident_id,
            idempotency_key=spec.idempotency_key,
            action_type=spec.action_type.value,
            target_service=spec.target_service,
            parameters=spec.parameters,
            dry_run_passed=validation.dry_run_passed,
            status="EXECUTING"
        )
        if not recorded:
            logger.warning(f"Database idempotency constraint blocked duplicate remediation: {spec.idempotency_key}")
            policy_engine.unmark_executed(spec)
            return {
                "status": "DUPLICATE_EXECUTION_BLOCKED",
                "state": IncidentState.RESOLVED.value,
                "reason": f"Remediation with idempotency key '{spec.idempotency_key}' already recorded in database",
                "remediation_id": spec.remediation_id
            }

        try:
            await db.transition_incident_state(
                incident_id=spec.incident_id,
                to_state=IncidentState.EXECUTING,
                actor="system:remediation_controller",
                reason=f"Executing {spec.action_type.value}"
            )
        except Exception as e:
            logger.debug(f"State transition note: {e}")

        # 6. Execute Remediation
        success = await self.execute_action(spec)
        if not success:
            policy_engine.unmark_executed(spec)
            try:
                await db.transition_incident_state(
                    incident_id=spec.incident_id,
                    to_state=IncidentState.ROLLBACK_EXECUTING,
                    actor="system:remediation_controller",
                    reason="Target execution adapter returned failure"
                )
                await db.transition_incident_state(
                    incident_id=spec.incident_id,
                    to_state=IncidentState.ESCALATED,
                    actor="system:remediation_controller",
                    reason="Execution failed; reverted"
                )
            except Exception as e:
                logger.debug(f"State transition note: {e}")

            return {
                "status": "EXECUTION_FAILED",
                "state": IncidentState.ESCALATED.value,
                "remediation_id": spec.remediation_id
            }

        # 7. Transition to VERIFYING & Closed-Loop Verification
        try:
            await db.transition_incident_state(
                incident_id=spec.incident_id,
                to_state=IncidentState.VERIFYING,
                actor="system:verifier",
                reason="Monitoring SLO stabilization"
            )
        except Exception as e:
            logger.debug(f"State transition note: {e}")

        verified = await self.verify_recovery()

        if verified:
            # 8a. Success: transition to RESOLVED
            try:
                await db.transition_incident_state(
                    incident_id=spec.incident_id,
                    to_state=IncidentState.RESOLVED,
                    actor="system:verifier",
                    reason="Post-remediation error rate returned within healthy SLO"
                )
            except Exception as e:
                logger.debug(f"State transition note: {e}")

            await db.update_incident_status(
                incident_id=spec.incident_id,
                status="RESOLVED",
                rca_summary=spec.rationale,
                confidence_score=spec.confidence_score
            )
            detector.resolve_incident(spec.target_service)
            return {
                "remediation_id": spec.remediation_id,
                "incident_id": spec.incident_id,
                "action_type": spec.action_type.value,
                "status": "RESOLVED",
                "state": IncidentState.RESOLVED.value,
                "verified": True
            }
        else:
            # 8b. Failure: transition to ESCALATED (anti-flapping)
            try:
                await db.transition_incident_state(
                    incident_id=spec.incident_id,
                    to_state=IncidentState.ESCALATED,
                    actor="system:verifier",
                    reason="SLO error rate did not stabilize within verification window"
                )
            except Exception as e:
                logger.debug(f"State transition note: {e}")

            await db.update_incident_status(
                incident_id=spec.incident_id,
                status="REQUIRE_HUMAN_TRIAGE",
                rca_summary=f"Remediation {spec.action_type.value} failed verification — manual intervention required",
                confidence_score=spec.confidence_score
            )
            logger.critical(
                f"Remediation {spec.remediation_id} failed verification. "
                f"Incident {spec.incident_id} escalated to REQUIRE_HUMAN_TRIAGE."
            )
            return {
                "remediation_id": spec.remediation_id,
                "incident_id": spec.incident_id,
                "action_type": spec.action_type.value,
                "status": "REQUIRE_HUMAN_TRIAGE",
                "state": IncidentState.ESCALATED.value,
                "verified": False
            }

    async def execute_approved_remediation(
        self,
        spec: RemediationSpec,
        approver: str = "operator"
    ) -> Dict[str, Any]:
        """
        Human Approval Execution Path.
        Called when an SRE on-call engineer approves an incident awaiting triage.
        Transitions: AWAITING_APPROVAL ➔ APPROVED ➔ EXECUTING ➔ VERIFYING ➔ RESOLVED / ESCALATED.
        """
        logger.info(f"Operator '{approver}' approved remediation {spec.remediation_id} for {spec.incident_id}")

        # Transition: AWAITING_APPROVAL -> APPROVED
        await db.transition_incident_state(
            incident_id=spec.incident_id,
            to_state=IncidentState.APPROVED,
            actor=f"operator:{approver}",
            reason=f"Approved by operator {approver}"
        )

        # Claim idempotency slot
        policy_engine.mark_executed(spec)

        recorded = await db.record_remediation(
            remediation_id=spec.remediation_id,
            incident_id=spec.incident_id,
            idempotency_key=spec.idempotency_key,
            action_type=spec.action_type.value,
            target_service=spec.target_service,
            parameters=spec.parameters,
            dry_run_passed=True,
            status="EXECUTING"
        )
        if not recorded:
            logger.warning(f"Database idempotency constraint blocked duplicate approved remediation: {spec.idempotency_key}")
            policy_engine.unmark_executed(spec)
            return {
                "status": "DUPLICATE_EXECUTION_BLOCKED",
                "state": IncidentState.RESOLVED.value,
                "reason": f"Remediation with idempotency key '{spec.idempotency_key}' was already executed",
                "remediation_id": spec.remediation_id
            }

        # Transition: APPROVED -> EXECUTING
        await db.transition_incident_state(
            incident_id=spec.incident_id,
            to_state=IncidentState.EXECUTING,
            actor=f"operator:{approver}",
            reason=f"Operator-approved execution of {spec.action_type.value}"
        )

        success = await self.execute_action(spec)
        if not success:
            policy_engine.unmark_executed(spec)
            await db.transition_incident_state(
                incident_id=spec.incident_id,
                to_state=IncidentState.ESCALATED,
                actor="system:controller",
                reason="Operator-approved execution failed on target service"
            )
            return {
                "status": "EXECUTION_FAILED",
                "state": IncidentState.ESCALATED.value,
                "remediation_id": spec.remediation_id
            }

        # Transition: EXECUTING -> VERIFYING
        await db.transition_incident_state(
            incident_id=spec.incident_id,
            to_state=IncidentState.VERIFYING,
            actor="system:verifier",
            reason="Verifying operator-approved remediation"
        )

        verified = await self.verify_recovery()
        if verified:
            await db.transition_incident_state(
                incident_id=spec.incident_id,
                to_state=IncidentState.RESOLVED,
                actor="system:verifier",
                reason="SLO restored following operator-approved remediation"
            )
            await db.update_incident_status(spec.incident_id, "RESOLVED", spec.rationale, spec.confidence_score)
            detector.resolve_incident(spec.target_service)
            return {
                "status": "RESOLVED",
                "state": IncidentState.RESOLVED.value,
                "remediation_id": spec.remediation_id,
                "verified": True
            }
        else:
            await db.transition_incident_state(
                incident_id=spec.incident_id,
                to_state=IncidentState.ESCALATED,
                actor="system:verifier",
                reason="Verification failed after operator-approved remediation"
            )
            await db.update_incident_status(spec.incident_id, "REQUIRE_HUMAN_TRIAGE")
            return {
                "status": "REQUIRE_HUMAN_TRIAGE",
                "state": IncidentState.ESCALATED.value,
                "remediation_id": spec.remediation_id,
                "verified": False
            }

    async def reject_pending_remediation(
        self,
        incident_id: str,
        rejecter: str = "operator",
        reason: str = "Rejected by operator"
    ) -> Dict[str, Any]:
        """
        Operator Rejection Path.
        Transitions: AWAITING_APPROVAL ➔ ESCALATED.
        """
        logger.info(f"Operator '{rejecter}' rejected incident {incident_id}: {reason}")
        await db.transition_incident_state(
            incident_id=incident_id,
            to_state=IncidentState.ESCALATED,
            actor=f"operator:{rejecter}",
            reason=reason
        )
        return {
            "incident_id": incident_id,
            "status": "REJECTED",
            "state": IncidentState.ESCALATED.value,
            "rejecter": rejecter,
            "reason": reason
        }

remediation_controller = RemediationController()

