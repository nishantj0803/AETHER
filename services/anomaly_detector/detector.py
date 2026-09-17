import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional
import httpx

from services.anomaly_detector.rules import DEFAULT_SLO_RULES, SLORule
from services.ingestion_worker.db import db

logger = logging.getLogger("aether.anomaly_detector")

@dataclass
class IncidentContext:
    incident_id: str
    service_name: str
    severity: str
    trigger_rule: str
    trigger_reason: str
    metric_snapshot: Dict[str, Any]
    detected_at: float
    deployment_metadata: Optional[Dict[str, Any]] = None
    recent_errors: Optional[List[Dict[str, Any]]] = None

class AnomalyDetector:
    def __init__(
        self,
        prometheus_url: Optional[str] = None,
        service_url: Optional[str] = None,
        service_name: Optional[str] = None
    ):
        self.prometheus_url = prometheus_url or os.getenv("PROMETHEUS_URL", "http://localhost:9090")
        self.service_url = service_url or os.getenv("DEMO_SERVICE_URL", "http://localhost:8000")
        self.service_name = service_name or os.getenv("SERVICE_NAME", "payment-service")
        self.active_incidents: Dict[str, IncidentContext] = {}

    async def fetch_prometheus_metrics(self, window: str = "1m") -> Dict[str, float]:
        """Query Prometheus API or scrape service /metrics directly."""
        metrics = {
            "http_5xx_rate": 0.0,
            "p95_latency_seconds": 0.04,
            "memory_bytes": 45 * 1024 * 1024
        }
        
        async with httpx.AsyncClient() as client:
            # 1. Try Prometheus query API
            try:
                # HTTP 5xx error rate query with configurable evaluation window
                query = f'(sum(rate(http_requests_total{{status=~"5.."}}[{window}])) or vector(0)) / (sum(rate(http_requests_total[{window}])) > 0)'
                resp = await client.get(f"{self.prometheus_url}/api/v1/query", params={"query": query}, timeout=2.0)
                if resp.status_code == 200:
                    data = resp.json()
                    results = data.get("data", {}).get("result", [])
                    if results:
                        metrics["http_5xx_rate"] = float(results[0]["value"][1])
            except Exception as e:
                logger.debug(f"Prometheus API query failed ({e}); falling back to direct service scrape.")
                # Fallback: scrape demo-service /metrics directly
                try:
                    resp = await client.get(f"{self.service_url}/metrics", timeout=2.0)
                    if resp.status_code == 200:
                        text = resp.text
                        # Simple parsing of prometheus output for standalone mode
                        total_reqs = 0.0
                        error_reqs = 0.0
                        for line in text.splitlines():
                            if line.startswith("http_requests_total{"):
                                val = float(line.split()[-1])
                                total_reqs += val
                                if 'status="500"' in line or 'status="503"' in line:
                                    error_reqs += val
                            elif line.startswith("service_memory_usage_bytes"):
                                metrics["memory_bytes"] = float(line.split()[-1])
                        
                        if total_reqs > 0:
                            metrics["http_5xx_rate"] = error_reqs / total_reqs
                except Exception as e:
                    logger.debug(f"Metrics scrape failed: {e}")

        return metrics

    async def fetch_deployment_metadata(self) -> Optional[Dict[str, Any]]:
        """Fetch current deployment version and config diff."""
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{self.service_url}/api/v1/admin/deployment", timeout=2.0)
                if resp.status_code == 200:
                    return resp.json()
        except Exception as e:
            logger.debug(f"Could not fetch deployment info: {e}")
        return None

    def evaluate_metrics(self, metrics: Dict[str, float]) -> List[SLORule]:
        """Deterministically check if metrics breach any SLO rules."""
        breached_rules = []
        for rule in DEFAULT_SLO_RULES:
            val = metrics.get(rule.metric_name, 0.0)
            if rule.condition == "gt" and val > rule.threshold:
                breached_rules.append(rule)
            elif rule.condition == "lt" and val < rule.threshold:
                breached_rules.append(rule)
        return breached_rules

    async def check_for_anomalies(self) -> Optional[IncidentContext]:
        """Main detection cycle: evaluate SLOs and synthesize IncidentContext on breach."""
        metrics = await self.fetch_prometheus_metrics()
        breaches = self.evaluate_metrics(metrics)

        if not breaches:
            return None

        # Take highest severity breach
        primary_breach = breaches[0]
        service_name = self.service_name

        # Prevent flapping / duplicate incident spam if an incident is already ongoing
        if service_name in self.active_incidents:
            return self.active_incidents[service_name]

        incident_id = f"INC-{time.strftime('%Y%m%d')}-{uuid.uuid4().hex[:8]}"
        deployment = await self.fetch_deployment_metadata()

        # Query recent similar error traces from pgvector
        recent_errors = []
        try:
            recent_errors = await db.search_similar_logs("Connection timeout database error", limit=3)
        except Exception as e:
            logger.debug(f"Vector search for similar error logs failed: {e}")

        metric_val = metrics.get(primary_breach.metric_name)
        val_str = f"{metric_val:.3f}" if isinstance(metric_val, (int, float)) else str(metric_val)

        context = IncidentContext(
            incident_id=incident_id,
            service_name=service_name,
            severity=primary_breach.severity,
            trigger_rule=primary_breach.name,
            trigger_reason=f"{primary_breach.description} (value: {val_str})",
            metric_snapshot=metrics,
            detected_at=time.time(),
            deployment_metadata=deployment,
            recent_errors=recent_errors
        )

        self.active_incidents[service_name] = context
        logger.warning(f"🚨 Anomaly Detected: {incident_id} [{primary_breach.severity}] {context.trigger_reason}")

        # Record in PostgreSQL
        try:
            await db.create_incident(
                incident_id=incident_id,
                service_name=service_name,
                severity=primary_breach.severity,
                trigger_reason=context.trigger_reason,
                metric_snapshot=metrics
            )
        except Exception as e:
            logger.error(f"Failed to record incident in DB: {e}")

        return context

    def resolve_incident(self, service_name: Optional[str] = None):
        target = service_name or self.service_name
        if target in self.active_incidents:
            del self.active_incidents[target]
            logger.info(f"Incident on {target} marked as cleared in detector")

detector = AnomalyDetector()
