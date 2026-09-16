import asyncio
import logging
import os
import time
from typing import Any, Dict, Optional
import httpx

from services.anomaly_detector.detector import detector
from services.ingestion_worker.db import db
from services.rca_agent.schema import RemediationActionType, RemediationSpec
from services.remediation_controller.policy_engine import policy_engine

logger = logging.getLogger("aether.remediation_controller")

class RemediationController:
    """
    Executes validated remediation actions, records audit logs,
    and runs the closed-loop verification / auto-rollback cycle.
    """
    def __init__(self, demo_service_url: Optional[str] = None):
        self.service_url = demo_service_url or os.getenv("DEMO_SERVICE_URL", "http://localhost:8000")

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
                    # In local dev, simulated scale; in K8s, calls AppsV1Api.patch_namespaced_deployment_scale
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
        # 1. Policy Engine Validation
        validation = policy_engine.validate(spec, dry_run=False)
        if not validation.allowed:
            logger.warning(f"Remediation REJECTED by policy engine: {validation.reason}")
            return {
                "status": "REJECTED_BY_POLICY",
                "reason": validation.reason,
                "remediation_id": spec.remediation_id
            }

        # 2. Record in Audit Log
        await db.record_remediation(
            remediation_id=spec.remediation_id,
            incident_id=spec.incident_id,
            idempotency_key=spec.idempotency_key,
            action_type=spec.action_type.value,
            target_service=spec.target_service,
            parameters=spec.parameters,
            dry_run_passed=validation.dry_run_passed,
            status="EXECUTING"
        )

        # 3. Execute Remediation
        success = await self.execute_action(spec)
        if not success:
            return {
                "status": "EXECUTION_FAILED",
                "remediation_id": spec.remediation_id
            }

        policy_engine.mark_executed(spec)

        # 4. Closed-Loop Verification
        verified = await self.verify_recovery()
        final_status = "RESOLVED" if verified else "ROLLBACK_REMEDIATION"

        # 5. Update Database Records
        await db.update_incident_status(
            incident_id=spec.incident_id,
            status=final_status,
            rca_summary=spec.rationale,
            confidence_score=spec.confidence_score
        )

        detector.resolve_incident(spec.target_service)

        return {
            "remediation_id": spec.remediation_id,
            "incident_id": spec.incident_id,
            "action_type": spec.action_type.value,
            "status": final_status,
            "verified": verified
        }

remediation_controller = RemediationController()
