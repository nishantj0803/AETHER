import pytest
from fastapi.testclient import TestClient

from services.demo_service.main import app
from services.demo_service.faults import fault_engine

client = TestClient(app)

@pytest.fixture(autouse=True)
def reset_faults_fixture():
    fault_engine.reset_all()
    yield
    fault_engine.reset_all()

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "payment-service"
    assert data["deployment"] == "v1.0.0"

def test_metrics_endpoint():
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "http_requests_total" in response.text
    assert "payment_transactions_total" in response.text

def test_successful_payment():
    payload = {
        "account_id": "acc_12345",
        "amount": 99.50,
        "currency": "USD"
    }
    response = client.post("/api/v1/payments/charge", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SUCCESS"
    assert data["amount"] == 99.50
    assert data["transaction_id"].startswith("txn_")

def test_bad_deployment_fault_and_rollback():
    # 1. Inject bad deployment fault
    resp = client.post("/api/v1/admin/faults/inject", json={"fault_type": "bad_deployment"})
    assert resp.status_code == 200
    assert fault_engine.current_deployment["version"] == "v1.1.0-bad"

    # 2. In bad deployment, errors occur (we simulate multiple requests)
    errors = 0
    total = 20
    for _ in range(total):
        r = client.post("/api/v1/payments/charge", json={"amount": 50.0})
        if r.status_code == 500:
            errors += 1
    
    # Check that error rate is elevated
    assert errors > 0

    # 3. Trigger Rollback
    rollback_resp = client.post("/api/v1/admin/deployment/rollback", params={"target_version": "v1.0.0"})
    assert rollback_resp.status_code == 200
    assert fault_engine.current_deployment["version"] == "v1.0.0"

    # 4. Verify system is restored to 100% success
    recovered_errors = 0
    for _ in range(10):
        r = client.post("/api/v1/payments/charge", json={"amount": 50.0})
        if r.status_code == 500:
            recovered_errors += 1
    assert recovered_errors == 0
