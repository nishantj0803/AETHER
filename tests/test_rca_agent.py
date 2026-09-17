import time
import pytest
from services.anomaly_detector.detector import IncidentContext
from services.rca_agent.graph import rca_agent
from services.rca_agent.schema import RemediationActionType

def test_rca_bad_deployment_correlation():
    # Simulate bad deployment incident context
    incident = IncidentContext(
        incident_id="INC-TEST-001",
        service_name="payment-service",
        severity="P1",
        trigger_rule="HighHttpErrorRate",
        trigger_reason="HTTP 5xx rate = 25%",
        metric_snapshot={"http_5xx_rate": 0.25, "p95_latency_seconds": 0.3},
        detected_at=time.time(),
        deployment_metadata={
            "version": "v1.1.0-bad",
            "commit_sha": "f3b92019a847162940283719a938210938472918",
            "deployed_at": time.time() - 60,  # 60s ago
            "config": {"db_timeout_ms": 50}
        }
    )

    diagnosis = rca_agent.analyze(incident)
    assert diagnosis.status == "CONFIRMED"
    assert diagnosis.confidence_score >= 0.90
    assert "Configuration regression" in diagnosis.root_cause
    assert diagnosis.remediation_plan is not None
    assert diagnosis.remediation_plan.action_type == RemediationActionType.ROLLBACK_DEPLOYMENT
    assert diagnosis.remediation_plan.parameters["target_version"] == "v1.0.0"

def test_rca_memory_leak_correlation():
    incident = IncidentContext(
        incident_id="INC-TEST-002",
        service_name="payment-service",
        severity="P2",
        trigger_rule="HighMemoryUsage",
        trigger_reason="Process memory reached 400MB",
        metric_snapshot={"http_5xx_rate": 0.01, "memory_bytes": 420 * 1024 * 1024},
        detected_at=time.time(),
        deployment_metadata={"version": "v1.0.0", "deployed_at": time.time() - 36000}
    )

    diagnosis = rca_agent.analyze(incident)
    assert diagnosis.status == "CONFIRMED"
    assert diagnosis.confidence_score >= 0.85
    assert diagnosis.remediation_plan.action_type == RemediationActionType.RESTART_CONTAINER

def test_rca_ambiguous_incident_triage_fallback():
    # Uncorrelated anomaly with no recent deployment and normal memory
    incident = IncidentContext(
        incident_id="INC-TEST-003",
        service_name="payment-service",
        severity="P1",
        trigger_rule="HighHttpErrorRate",
        trigger_reason="Random transient 5xx burst",
        metric_snapshot={"http_5xx_rate": 0.12, "memory_bytes": 50 * 1024 * 1024},
        detected_at=time.time(),
        deployment_metadata={"version": "v1.0.0", "deployed_at": time.time() - 86400}
    )

    diagnosis = rca_agent.analyze(incident)
    assert diagnosis.status == "HUMAN_TRIAGE_REQUIRED"
    assert diagnosis.confidence_score < 0.80
    assert diagnosis.remediation_plan is None

def test_gemini_rca_prompt_generation():
    from services.rca_agent.graph import GeminiRCAClient

    client = GeminiRCAClient(api_key="test-key", primary_model="gemini-3.7-flash")
    incident = IncidentContext(
        incident_id="INC-PROMPT-001",
        service_name="payment-service",
        severity="P1",
        trigger_rule="HighHttpErrorRate",
        trigger_reason="HTTP 5xx rate = 28%",
        metric_snapshot={"http_5xx_rate": 0.28, "p95_latency_seconds": 1.2},
        detected_at=time.time(),
        deployment_metadata={"version": "v1.1.0-bad", "config": {"db_timeout_ms": 50}}
    )
    prompt = client._build_prompt(incident)
    assert "INC-PROMPT-001" in prompt
    assert "payment-service" in prompt
    assert "28.00%" in prompt
    assert "db_timeout_ms" in prompt
    assert "ROLLBACK_DEPLOYMENT" in prompt

def test_gemini_rca_mocked_llm_response():
    import json
    from unittest.mock import patch, MagicMock
    from services.rca_agent.graph import GeminiRCAClient, AetherRCAAgent

    mock_llm_json = {
        "root_cause": "Database connection timeout regression in v1.1.0-bad deployment",
        "evidence_sources": ["Prometheus Metrics", "Deployment Change Log", "Semantic Log Store (pgvector)"],
        "causality_chain": "Deployment v1.1.0-bad set db_timeout_ms=50ms, causing 28% of queries to abort.",
        "confidence_score": 0.98,
        "status": "CONFIRMED",
        "remediation_plan": {
            "action_type": "ROLLBACK_DEPLOYMENT",
            "target_service": "payment-service",
            "parameters": {"target_version": "v1.0.0", "reason": "Config rollback"},
            "rationale": "Restore stable database connection timeout limit",
            "confidence_score": 0.98,
            "risk_level": "LOW"
        }
    }

    gemini_response_payload = {
        "candidates": [
            {
                "content": {
                    "parts": [{"text": json.dumps(mock_llm_json)}]
                }
            }
        ]
    }

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = gemini_response_payload

    gemini_client = GeminiRCAClient(api_key="mock-api-key", primary_model="gemini-3.7-flash")
    agent = AetherRCAAgent(gemini_client=gemini_client)

    incident = IncidentContext(
        incident_id="INC-GEMINI-001",
        service_name="payment-service",
        severity="P1",
        trigger_rule="HighHttpErrorRate",
        trigger_reason="Error spike",
        metric_snapshot={"http_5xx_rate": 0.28},
        detected_at=time.time(),
        deployment_metadata={"version": "v1.1.0-bad"}
    )

    with patch("httpx.Client.post", return_value=mock_response):
        diagnosis = agent.analyze(incident)

    assert diagnosis.status == "CONFIRMED"
    assert diagnosis.confidence_score == 0.98
    assert "Database connection timeout regression" in diagnosis.root_cause
    assert diagnosis.remediation_plan is not None
    assert diagnosis.remediation_plan.action_type == RemediationActionType.ROLLBACK_DEPLOYMENT
    assert diagnosis.remediation_plan.idempotency_key is not None

def test_gemini_rca_model_cascade_fallback():
    import json
    from unittest.mock import patch, MagicMock
    from services.rca_agent.graph import GeminiRCAClient, AetherRCAAgent

    mock_llm_json = {
        "root_cause": "Cascade success on fallback model",
        "evidence_sources": ["Prometheus Metrics"],
        "causality_chain": "Model cascade diagnosed issue successfully",
        "confidence_score": 0.92,
        "status": "CONFIRMED",
        "remediation_plan": {
            "action_type": "SCALE_REPLICAS",
            "target_service": "payment-service",
            "parameters": {"to_replicas": 3},
            "rationale": "Scale up replicas",
            "confidence_score": 0.92,
            "risk_level": "LOW"
        }
    }

    resp_404 = MagicMock()
    resp_404.status_code = 404

    resp_200 = MagicMock()
    resp_200.status_code = 200
    resp_200.json.return_value = {
        "candidates": [{"content": {"parts": [{"text": json.dumps(mock_llm_json)}]}}]
    }

    # First call (gemini-3.7-flash) returns 404, second call (gemini-3.8-flash) returns 200
    gemini_client = GeminiRCAClient(
        api_key="mock-api-key",
        primary_model="gemini-3.7-flash",
        fallback_models=["gemini-3.8-flash"]
    )
    agent = AetherRCAAgent(gemini_client=gemini_client)

    incident = IncidentContext(
        incident_id="INC-CASCADE-001",
        service_name="payment-service",
        severity="P2",
        trigger_rule="HighP95Latency",
        trigger_reason="Latency spike",
        metric_snapshot={"p95_latency_seconds": 1.6},
        detected_at=time.time()
    )

    with patch("httpx.Client.post", side_effect=[resp_404, resp_200]):
        diagnosis = agent.analyze(incident)

    assert diagnosis.status == "CONFIRMED"
    assert "Cascade success" in diagnosis.root_cause
    assert diagnosis.remediation_plan.action_type == RemediationActionType.SCALE_REPLICAS

def test_gemini_rca_network_failure_falls_back_to_deterministic():
    from unittest.mock import patch
    from services.rca_agent.graph import GeminiRCAClient, AetherRCAAgent

    gemini_client = GeminiRCAClient(api_key="mock-key", primary_model="gemini-3.7-flash")
    agent = AetherRCAAgent(gemini_client=gemini_client)

    incident = IncidentContext(
        incident_id="INC-FALLBACK-001",
        service_name="payment-service",
        severity="P1",
        trigger_rule="HighHttpErrorRate",
        trigger_reason="HTTP 5xx rate = 25%",
        metric_snapshot={"http_5xx_rate": 0.25},
        detected_at=time.time(),
        deployment_metadata={
            "version": "v1.1.0-bad",
            "deployed_at": time.time() - 30,
            "config": {"db_timeout_ms": 50}
        }
    )

    # Network timeout exception during Gemini API call
    with patch("httpx.Client.post", side_effect=Exception("Connection timed out")):
        diagnosis = agent.analyze(incident)

    # Must fall back gracefully to deterministic rule engine
    assert diagnosis.status == "CONFIRMED"
    assert diagnosis.confidence_score >= 0.90
    assert "Configuration regression" in diagnosis.root_cause

def test_gemini_rca_async_interface():
    import asyncio
    incident = IncidentContext(
        incident_id="INC-ASYNC-001",
        service_name="payment-service",
        severity="P1",
        trigger_rule="HighHttpErrorRate",
        trigger_reason="HTTP 5xx rate = 25%",
        metric_snapshot={"http_5xx_rate": 0.25},
        detected_at=time.time(),
        deployment_metadata={
            "version": "v1.1.0-bad",
            "deployed_at": time.time() - 30,
            "config": {"db_timeout_ms": 50}
        }
    )

    diagnosis = asyncio.run(rca_agent.analyze_async(incident))
    assert diagnosis.status == "CONFIRMED"
    assert diagnosis.remediation_plan.action_type == RemediationActionType.ROLLBACK_DEPLOYMENT

