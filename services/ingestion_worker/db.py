import json
import logging
import os
from typing import Any, Dict, List, Optional
import asyncpg

from services.ingestion_worker.embeddings import embedder

logger = logging.getLogger("aether.db")

class DatabaseClient:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.getenv(
            "DATABASE_URL",
            "postgresql://aether_user:aether_password@localhost:5432/aether_db"
        )
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Initialize PostgreSQL connection pool."""
        if not self.pool:
            try:
                self.pool = await asyncpg.create_pool(
                    self.dsn,
                    min_size=2,
                    max_size=10,
                    command_timeout=10.0
                )
                logger.info(f"Connected to PostgreSQL at {self.dsn}")
            except Exception as e:
                logger.warning(f"Failed to connect to PostgreSQL ({e}). Operating in memory/dry-run mode.")
                self.pool = None

    async def disconnect(self):
        if self.pool:
            await self.pool.close()

    async def insert_log(self, record: Dict[str, Any]) -> bool:
        """
        Idempotently insert log record using event_id deduplication.
        Returns True if inserted, False if duplicate.
        """
        if not self.pool:
            return True

        message = record.get("message", "")
        # Embed message if error or warn level
        embedding = embedder.embed(message) if record.get("level") in ("ERROR", "CRITICAL", "WARN") else None
        embedding_str = f"[{','.join(str(x) for x in embedding)}]" if embedding else None

        query = """
        INSERT INTO telemetry_logs (
            event_id, timestamp, service_name, level, message,
            trace_id, span_id, http_status, duration_ms, metadata, embedding
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
        ) ON CONFLICT (event_id) DO NOTHING
        RETURNING id;
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                record["event_id"],
                record.get("timestamp"),
                record["service_name"],
                record["level"],
                message,
                record.get("trace_id"),
                record.get("span_id"),
                record.get("http_status"),
                record.get("duration_ms"),
                json.dumps(record.get("metadata", {})),
                embedding_str
            )
            return row is not None

    async def search_similar_logs(
        self,
        query_text: str,
        limit: int = 5,
        service_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Semantic search over historical error logs using pgvector cosine distance (<=>).
        """
        if not self.pool:
            return []

        query_vec = embedder.embed(query_text)
        query_vec_str = f"[{','.join(str(x) for x in query_vec)}]"

        sql = """
        SELECT 
            event_id, timestamp, service_name, level, message, trace_id, http_status,
            1 - (embedding <=> $1::vector) AS similarity
        FROM telemetry_logs
        WHERE embedding IS NOT NULL
        """
        params = [query_vec_str]

        if service_name:
            sql += " AND service_name = $2"
            params.append(service_name)

        sql += f" ORDER BY embedding <=> $1::vector LIMIT {limit};"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
            return [dict(row) for row in rows]

    async def create_incident(
        self,
        incident_id: str,
        service_name: str,
        severity: str,
        trigger_reason: str,
        metric_snapshot: Dict[str, Any]
    ) -> bool:
        """Record newly detected incident."""
        if not self.pool:
            return True

        query = """
        INSERT INTO incidents (
            incident_id, service_name, severity, status, trigger_reason, metric_snapshot
        ) VALUES ($1, $2, $3, 'DETECTED', $4, $5)
        ON CONFLICT (incident_id) DO UPDATE SET
            status = EXCLUDED.status,
            updated_at = NOW();
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                incident_id,
                service_name,
                severity,
                trigger_reason,
                json.dumps(metric_snapshot)
            )
            return True

    async def update_incident_status(
        self,
        incident_id: str,
        status: str,
        rca_summary: Optional[str] = None,
        confidence_score: Optional[float] = None
    ) -> bool:
        """Update incident lifecycle state."""
        if not self.pool:
            return True

        query = """
        UPDATE incidents
        SET status = $2,
            rca_summary = COALESCE($3, rca_summary),
            confidence_score = COALESCE($4, confidence_score),
            updated_at = NOW(),
            resolved_at = CASE WHEN $2 = 'RESOLVED' THEN NOW() ELSE resolved_at END
        WHERE incident_id = $1;
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, incident_id, status, rca_summary, confidence_score)
            return True

    async def record_remediation(
        self,
        remediation_id: str,
        incident_id: str,
        idempotency_key: str,
        action_type: str,
        target_service: str,
        parameters: Dict[str, Any],
        dry_run_passed: bool,
        status: str = "PENDING"
    ) -> bool:
        """
        Idempotent write of remediation audit log.
        Prevents double remediation using UNIQUE(idempotency_key).
        """
        if not self.pool:
            return True

        query = """
        INSERT INTO remediation_audit_log (
            remediation_id, incident_id, idempotency_key, action_type,
            target_service, parameters, dry_run_passed, status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING remediation_id;
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                remediation_id,
                incident_id,
                idempotency_key,
                action_type,
                target_service,
                json.dumps(parameters),
                dry_run_passed,
                status
            )
            return row is not None

    async def get_active_deployment(self, service_name: str = "payment-service") -> Optional[Dict[str, Any]]:
        """Fetch active deployment metadata."""
        if not self.pool:
            return None

        query = """
        SELECT version, commit_sha, config_diff, deployed_at
        FROM deployment_history
        WHERE service_name = $1 AND is_active = TRUE
        ORDER BY deployed_at DESC LIMIT 1;
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, service_name)
            return dict(row) if row else None

db = DatabaseClient()
