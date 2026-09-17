import asyncio
import random
import time
from typing import Any, Dict, List, Optional
from services.demo_service.telemetry import telemetry, ACTIVE_FAULTS_GAUGE, ACTIVE_DB_CONNECTIONS

class FaultEngine:
    def __init__(self):
        self.active_faults: Dict[str, Dict[str, Any]] = {}
        self.leaked_memory_chunks: List[bytearray] = []
        self.held_db_connections: int = 0
        self.cpu_burn_tasks: List[asyncio.Task] = []
        
        # Deployment state
        self.current_deployment = {
            "version": "v1.0.0",
            "commit_sha": "a7f2910c438bde1940172834b9281a0293817420",
            "deployed_at": time.time() - 7200,
            "config": {
                "db_pool_size": 20,
                "db_timeout_ms": 2000,
                "retry_attempts": 3,
                "rate_limit_rps": 500
            }
        }

    def activate_fault(self, fault_type: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        params = params or {}
        self.active_faults[fault_type] = params
        ACTIVE_FAULTS_GAUGE.labels(fault_type=fault_type).set(1)

        telemetry.log(
            "WARN",
            f"Synthetic fault injected: {fault_type}",
            metadata={"fault_type": fault_type, "params": params}
        )

        if fault_type == "bad_deployment":
            # Upgrade deployment to regression version
            self.current_deployment = {
                "version": "v1.1.0-bad",
                "commit_sha": "f3b92019a847162940283719a938210938472918",
                "deployed_at": time.time(),
                "config": {
                    "db_pool_size": 20,
                    "db_timeout_ms": 50,  # Drastically reduced timeout -> causes failure on slow queries!
                    "retry_attempts": 1,
                    "rate_limit_rps": 500
                }
            }
            telemetry.log(
                "CRITICAL",
                "Deployment updated to v1.1.0-bad: db_timeout_ms reduced to 50ms",
                metadata={"deployment": self.current_deployment}
            )

        elif fault_type == "memory_leak":
            # Immediately allocate a chunk
            chunk_size_mb = params.get("chunk_mb", 50)
            self.leaked_memory_chunks.append(bytearray(chunk_size_mb * 1024 * 1024))

        elif fault_type == "db_starve":
            self.held_db_connections = params.get("starve_count", 18)
            ACTIVE_DB_CONNECTIONS.set(self.held_db_connections)

        return {"status": "injected", "fault_type": fault_type, "active_faults": list(self.active_faults.keys())}

    def deactivate_fault(self, fault_type: str) -> Dict[str, Any]:
        if fault_type in self.active_faults:
            del self.active_faults[fault_type]
            ACTIVE_FAULTS_GAUGE.labels(fault_type=fault_type).set(0)

        if fault_type == "bad_deployment":
            self.rollback_deployment("v1.0.0")

        elif fault_type == "memory_leak":
            self.leaked_memory_chunks.clear()

        elif fault_type == "db_starve":
            self.held_db_connections = 0
            ACTIVE_DB_CONNECTIONS.set(0)

        telemetry.log(
            "INFO",
            f"Synthetic fault cleared: {fault_type}",
            metadata={"fault_type": fault_type}
        )
        return {"status": "cleared", "fault_type": fault_type}

    def rollback_deployment(self, target_version: str = "v1.0.0") -> Dict[str, Any]:
        """Rollback to a known stable deployment."""
        if target_version == "v1.0.0":
            self.current_deployment = {
                "version": "v1.0.0",
                "commit_sha": "a7f2910c438bde1940172834b9281a0293817420",
                "deployed_at": time.time(),
                "config": {
                    "db_pool_size": 20,
                    "db_timeout_ms": 2000,
                    "retry_attempts": 3,
                    "rate_limit_rps": 500
                }
            }
        if "bad_deployment" in self.active_faults:
            del self.active_faults["bad_deployment"]
            ACTIVE_FAULTS_GAUGE.labels(fault_type="bad_deployment").set(0)

        telemetry.log(
            "INFO",
            f"Deployment successfully rolled back to {target_version}",
            metadata={"deployment": self.current_deployment}
        )
        return {"status": "rolled_back", "deployment": self.current_deployment}

    def reset_all(self):
        for f in list(self.active_faults.keys()):
            ACTIVE_FAULTS_GAUGE.labels(fault_type=f).set(0)
        self.active_faults.clear()
        self.leaked_memory_chunks.clear()
        self.held_db_connections = 0
        ACTIVE_DB_CONNECTIONS.set(0)
        self.rollback_deployment("v1.0.0")

    @staticmethod
    def _cpu_burn_sync() -> None:
        """CPU-intensive work offloaded to thread pool to avoid blocking the event loop."""
        end = time.time() + 0.15
        while time.time() < end:
            _ = [i * i for i in range(1000)]

    async def simulate_request_execution(self) -> None:
        """Called inside payment request handler to simulate effects of active faults.
        
        All blocking operations use asyncio-compatible primitives to avoid
        starving the ASGI event loop under concurrent load.
        """
        # 1. Check Bad Deployment / DB Timeout
        if "bad_deployment" in self.active_faults:
            # 25% of queries exceed the 50ms timeout
            if random.random() < 0.25:
                await asyncio.sleep(0.06)  # Non-blocking sleep
                raise TimeoutError("Database connection timed out: query exceeded db_timeout_ms=50ms limit")

        # 2. Check Memory Leak
        if "memory_leak" in self.active_faults:
            # Continually leak 5MB per request
            self.leaked_memory_chunks.append(bytearray(5 * 1024 * 1024))

        # 3. Check DB Starvation
        if "db_starve" in self.active_faults:
            if random.random() < 0.35:
                raise ConnectionResetError("ConnectionPoolExhausted: all 20 connections in pool are currently busy")

        # 4. Check Error Burst
        if "error_burst" in self.active_faults:
            raise RuntimeError("InternalPaymentGatewayError: unexpected upstream 500 returned")

        # 5. Check CPU Burn — offloaded to thread pool
        if "cpu_burn" in self.active_faults:
            await asyncio.to_thread(self._cpu_burn_sync)

fault_engine = FaultEngine()
