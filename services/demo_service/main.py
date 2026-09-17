import asyncio
import time
import uuid
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from services.demo_service.telemetry import (
    telemetry,
    REQUESTS_TOTAL,
    REQUEST_DURATION,
    PAYMENTS_TOTAL,
    tracer
)
from services.demo_service.faults import fault_engine
from services.remediation_controller.api import router as incident_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Kafka connection if broker is available
    await telemetry.init_kafka()
    telemetry.log("INFO", "Demo Payment Service initialized and ready to serve traffic")

    # Automatically reconcile any in-flight incidents interrupted by prior crash/restart
    try:
        from services.remediation_controller.controller import remediation_controller
        recovered = await remediation_controller.recover_in_flight_incidents()
        if recovered:
            telemetry.log("WARN", f"Startup crash recovery: reconciled {len(recovered)} in-flight incidents", metadata={"recovered": recovered})
    except Exception as e:
        telemetry.log("ERROR", f"Failed to execute startup crash reconciliation: {e}")

    yield
    await telemetry.close_kafka()

app = FastAPI(
    title="Aether Demo Payment Service",
    version="1.0.0",
    lifespan=lifespan
)
app.include_router(incident_router)

# ----------------- Data Models -----------------
class PaymentRequest(BaseModel):
    account_id: str = Field(default="acc_982341", description="Customer Account ID")
    amount: float = Field(default=129.99, gt=0, description="Transaction amount")
    currency: str = Field(default="USD", description="Currency ISO code")
    idempotency_key: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Client-side transaction idempotency key"
    )

class PaymentResponse(BaseModel):
    transaction_id: str
    account_id: str
    amount: float
    currency: str
    status: str
    timestamp: float

class FaultInjectionRequest(BaseModel):
    fault_type: str = Field(..., description="bad_deployment, memory_leak, db_starve, cpu_burn, error_burst")
    parameters: Optional[dict] = Field(default=None)

# ----------------- Middleware for Telemetry -----------------
@app.middleware("http")
async def telemetry_middleware(request: Request, call_next):
    start_time = time.perf_counter()
    endpoint = request.url.path
    method = request.method
    
    with tracer.start_as_current_span(f"{method} {endpoint}"):
        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as exc:
            status_code = 500
            elapsed = time.perf_counter() - start_time
            duration_ms = elapsed * 1000
            REQUESTS_TOTAL.labels(status=500, method=method, endpoint=endpoint).inc()
            REQUEST_DURATION.labels(endpoint=endpoint).observe(elapsed)
            
            telemetry.log(
                "ERROR",
                f"Unhandled exception in {method} {endpoint}: {exc}",
                http_status=500,
                duration_ms=duration_ms,
                metadata={"exception": str(exc), "endpoint": endpoint}
            )
            raise exc

        elapsed = time.perf_counter() - start_time
        duration_ms = elapsed * 1000
        REQUESTS_TOTAL.labels(status=status_code, method=method, endpoint=endpoint).inc()
        REQUEST_DURATION.labels(endpoint=endpoint).observe(elapsed)
        return response

# ----------------- Core Endpoints -----------------
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "payment-service",
        "deployment": fault_engine.current_deployment["version"]
    }

@app.get("/metrics")
async def metrics_endpoint():
    """Prometheus metrics scrape target."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/api/v1/payments/charge", response_model=PaymentResponse)
async def process_payment(payment: PaymentRequest):
    """Process a customer checkout transaction with realistic simulation and fault testing."""
    start_time = time.perf_counter()
    
    # 1. Run through synthetic fault engine checks
    try:
        await fault_engine.simulate_request_execution()
    except Exception as e:
        PAYMENTS_TOTAL.labels(status="failed", currency=payment.currency).inc()
        duration_ms = (time.perf_counter() - start_time) * 1000
        log_entry = telemetry.log(
            "ERROR",
            f"Payment processing failed for {payment.account_id}: {str(e)}",
            http_status=500,
            duration_ms=duration_ms,
            metadata={
                "account_id": payment.account_id,
                "amount": payment.amount,
                "currency": payment.currency,
                "error_type": type(e).__name__,
                "stack_trace": str(e)
            }
        )
        # Asynchronously push to Kafka topic if available
        await telemetry.emit_to_kafka("telemetry.logs", log_entry)
        raise HTTPException(status_code=500, detail=str(e))

    # 2. Simulate standard healthy database read/write latency (20ms - 40ms)
    await asyncio.sleep(0.025)

    # 3. Successful payment record
    PAYMENTS_TOTAL.labels(status="success", currency=payment.currency).inc()
    duration_ms = (time.perf_counter() - start_time) * 1000

    log_entry = telemetry.log(
        "INFO",
        f"Payment charge successful for {payment.account_id} amount=${payment.amount}",
        http_status=200,
        duration_ms=duration_ms,
        metadata={
            "account_id": payment.account_id,
            "amount": payment.amount,
            "currency": payment.currency
        }
    )
    await telemetry.emit_to_kafka("telemetry.logs", log_entry)

    return PaymentResponse(
        transaction_id=f"txn_{uuid.uuid4().hex[:12]}",
        account_id=payment.account_id,
        amount=payment.amount,
        currency=payment.currency,
        status="SUCCESS",
        timestamp=time.time()
    )

# ----------------- Fault & Deployment Admin Endpoints -----------------
@app.post("/api/v1/admin/faults/inject")
async def inject_fault(request: FaultInjectionRequest):
    result = fault_engine.activate_fault(request.fault_type, request.parameters)
    return result

@app.post("/api/v1/admin/faults/reset")
async def reset_faults():
    fault_engine.reset_all()
    return {"status": "all_faults_cleared"}

@app.get("/api/v1/admin/deployment")
async def get_deployment():
    return fault_engine.current_deployment

@app.post("/api/v1/admin/deployment/rollback")
async def rollback_deployment(target_version: str = "v1.0.0"):
    result = fault_engine.rollback_deployment(target_version)
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
