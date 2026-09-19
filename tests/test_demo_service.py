import pytest
from fastapi.testclient import TestClient

from services.demo_service.main import app
from services.demo_service.faults import FaultEngine, fault_engine

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

def test_fault_engine_lifecycle_and_fault_types():
    engine = FaultEngine()
    try:
        # 1. Memory leak fault
        res = engine.activate_fault("memory_leak", {"chunk_mb": 1})
        assert res["status"] == "injected"
        assert "memory_leak" in engine.active_faults
        assert len(engine.leaked_memory_chunks) == 1
        assert len(engine.leaked_memory_chunks[0]) == 1024 * 1024

        # 2. DB starvation fault
        res = engine.activate_fault("db_starve", {"starve_count": 12})
        assert "db_starve" in engine.active_faults
        assert engine.held_db_connections == 12

        # 3. Error burst fault
        res = engine.activate_fault("error_burst")
        assert "error_burst" in engine.active_faults

        # 4. Deactivate specific faults
        res_clr = engine.deactivate_fault("memory_leak")
        assert res_clr["status"] == "cleared"
        assert "memory_leak" not in engine.active_faults
        assert len(engine.leaked_memory_chunks) == 0

        res_clr = engine.deactivate_fault("db_starve")
        assert engine.held_db_connections == 0

        # 5. Reset all
        engine.reset_all()
        assert len(engine.active_faults) == 0
        assert engine.held_db_connections == 0
        assert len(engine.leaked_memory_chunks) == 0
    finally:
        engine.reset_all()

def test_fault_engine_bad_deployment_and_rollback():
    engine = FaultEngine()
    try:
        assert engine.current_deployment["version"] == "v1.0.0"
        assert engine.current_deployment["config"]["db_timeout_ms"] == 2000

        # Activate bad deployment
        engine.activate_fault("bad_deployment")
        assert engine.current_deployment["version"] == "v1.1.0-bad"
        assert engine.current_deployment["config"]["db_timeout_ms"] == 50
        assert "bad_deployment" in engine.active_faults

        # Rollback
        res = engine.rollback_deployment("v1.0.0")
        assert res["status"] == "rolled_back"
        assert engine.current_deployment["version"] == "v1.0.0"
        assert engine.current_deployment["config"]["db_timeout_ms"] == 2000
        assert "bad_deployment" not in engine.active_faults
    finally:
        engine.reset_all()

def test_fault_engine_simulate_request_execution():
    import asyncio
    from unittest.mock import patch

    async def _test():
        engine = FaultEngine()
        try:
            # 1. Error burst simulation
            engine.activate_fault("error_burst")
            with pytest.raises(RuntimeError, match="InternalPaymentGatewayError"):
                await engine.simulate_request_execution()
            engine.deactivate_fault("error_burst")

            # 2. Memory leak simulation
            engine.activate_fault("memory_leak", {"chunk_mb": 1})
            assert len(engine.leaked_memory_chunks) == 1
            await engine.simulate_request_execution()
            # Simulation appends an additional 5MB chunk
            assert len(engine.leaked_memory_chunks) == 2
            engine.deactivate_fault("memory_leak")

            # 3. DB starvation simulation
            engine.activate_fault("db_starve")
            with patch("random.random", return_value=0.1):
                with pytest.raises(ConnectionResetError, match="ConnectionPoolExhausted"):
                    await engine.simulate_request_execution()
            with patch("random.random", return_value=0.9):
                await engine.simulate_request_execution()
            engine.deactivate_fault("db_starve")

            # 4. Bad deployment timeout simulation
            engine.activate_fault("bad_deployment")
            with patch("random.random", return_value=0.1):
                with pytest.raises(TimeoutError, match="limit"):
                    await engine.simulate_request_execution()
            with patch("random.random", return_value=0.9):
                await engine.simulate_request_execution()
        finally:
            engine.reset_all()

    asyncio.run(_test())

