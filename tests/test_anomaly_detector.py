import pytest
from services.anomaly_detector.rules import DEFAULT_SLO_RULES
from services.anomaly_detector.detector import AnomalyDetector

def test_slo_evaluation_healthy():
    detector = AnomalyDetector()
    healthy_metrics = {
        "http_5xx_rate": 0.005,  # 0.5% error rate (well below 5%)
        "p95_latency_seconds": 0.12,
        "memory_bytes": 100 * 1024 * 1024
    }
    breaches = detector.evaluate_metrics(healthy_metrics)
    assert len(breaches) == 0

def test_slo_evaluation_error_rate_breach():
    detector = AnomalyDetector()
    faulty_metrics = {
        "http_5xx_rate": 0.245,  # 24.5% error rate (> 5%)
        "p95_latency_seconds": 0.15,
        "memory_bytes": 100 * 1024 * 1024
    }
    breaches = detector.evaluate_metrics(faulty_metrics)
    assert len(breaches) == 1
    assert breaches[0].name == "HighHttpErrorRate"
    assert breaches[0].severity == "P1"

def test_slo_evaluation_memory_saturation():
    detector = AnomalyDetector()
    sat_metrics = {
        "http_5xx_rate": 0.01,
        "p95_latency_seconds": 0.20,
        "memory_bytes": 450 * 1024 * 1024  # 450MB (> 350MB limit)
    }
    breaches = detector.evaluate_metrics(sat_metrics)
    assert len(breaches) == 1
    assert breaches[0].name == "HighMemoryUsage"

def test_anomaly_detector_parameterized_service_name():
    detector = AnomalyDetector(service_name="checkout-service")
    assert detector.service_name == "checkout-service"
    assert len(detector.active_incidents) == 0

def test_anomaly_detector_resolve_clears_active_incident():
    detector = AnomalyDetector(service_name="order-service")
    # Simulate an active incident
    detector.active_incidents["order-service"] = "placeholder"
    assert "order-service" in detector.active_incidents

    # Clear via resolve_incident
    detector.resolve_incident("order-service")
    assert "order-service" not in detector.active_incidents

