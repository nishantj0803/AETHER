import asyncio
import json
import logging
import os
import re
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
        self._in_memory_incidents: Dict[str, Dict[str, Any]] = {}
        self._in_memory_transitions: List[Dict[str, Any]] = []
        self._in_memory_remediations: Dict[str, Dict[str, Any]] = {}

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
                safe_dsn = re.sub(r':([^@]+)@', ':****@', self.dsn)
                logger.info(f"Connected to PostgreSQL at {safe_dsn}")
            except Exception as e:
                logger.warning(
                    f"Failed to connect to PostgreSQL ({e}). Operating in memory/dry-run mode.")
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
        embedding = embedder.embed(message) if record.get(
            "level") in ("ERROR", "CRITICAL", "WARN") else None
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

        limit_param_idx = len(params) + 1
        sql += f" ORDER BY embedding <=> $1::vector LIMIT ${limit_param_idx};"
        params.append(limit)

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
        now_ts = os.environ.get("MOCK_TIMESTAMP") or str(
            asyncio.get_event_loop().time()) if False else None
        inc_data = {
            "incident_id": incident_id,
            "service_name": service_name,
            "severity": severity,
            "status": "DETECTED",
            "trigger_reason": trigger_reason,
            "metric_snapshot": metric_snapshot,
            "rca_summary": None,
            "confidence_score": None,
            "created_at": None,
            "updated_at": None,
            "resolved_at": None,
        }
        self._in_memory_incidents[incident_id] = inc_data

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

    async def get_incident(self, incident_id: str) -> Optional[Dict[str, Any]]:
        """Fetch incident by ID."""
        if not self.pool:
            return self._in_memory_incidents.get(incident_id)

        query = "SELECT * FROM incidents WHERE incident_id = $1;"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, incident_id)
            if row:
                d = dict(row)
                if isinstance(d.get("metric_snapshot"), str):
                    d["metric_snapshot"] = json.loads(d["metric_snapshot"])
                return d
            return self._in_memory_incidents.get(incident_id)

    async def list_incidents(self, status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """List incidents with optional status filter."""
        if not self.pool:
            results = list(self._in_memory_incidents.values())
            if status:
                norm = status.strip().upper()
                results = [i for i in results if i.get(
                    "status", "").upper() == norm]
            return results[:limit]

        query = "SELECT * FROM incidents"
        params: List[Any] = []
        if status:
            query += " WHERE status = $1"
            params.append(status.strip().upper())
        query += " ORDER BY created_at DESC LIMIT $" + \
            str(len(params) + 1) + ";"
        params.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            res = []
            for r in rows:
                d = dict(r)
                if isinstance(d.get("metric_snapshot"), str):
                    d["metric_snapshot"] = json.loads(d["metric_snapshot"])
                res.append(d)
            return res

    async def get_in_flight_incidents(self) -> List[Dict[str, Any]]:
        """Fetch incidents currently executing, verifying, or rolling back."""
        in_flight_states = ("EXECUTING", "VERIFYING", "ROLLBACK_EXECUTING")
        if not self.pool:
            return [
                inc for inc in self._in_memory_incidents.values()
                if inc.get("status") in in_flight_states
            ]

        query = """
        SELECT * FROM incidents 
        WHERE status IN ('EXECUTING', 'VERIFYING', 'ROLLBACK_EXECUTING')
        ORDER BY updated_at ASC;
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [dict(r) for r in rows]

    async def transition_incident_state(
        self,
        incident_id: str,
        to_state: Any,
        actor: str = "system",
        reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Validate and execute atomic state machine transition.
        Enforces state transition rules and records an immutable audit log row.
        """
        from services.remediation_controller.state_machine import (
            IncidentState,
            IncidentStateMachine
        )

        target_norm = IncidentState.normalize(to_state)
        current_data = await self.get_incident(incident_id)
        current_raw = current_data.get(
            "status", "DETECTED") if current_data else "DETECTED"
        current_norm = IncidentState.normalize(current_raw)

        # Enforce formal state transition rules
        IncidentStateMachine.validate_transition(
            current_norm, target_norm, reason=reason)

        # Update in-memory record
        if incident_id in self._in_memory_incidents:
            self._in_memory_incidents[incident_id]["status"] = target_norm.value
        else:
            self._in_memory_incidents[incident_id] = {
                "incident_id": incident_id,
                "service_name": "unknown",
                "severity": "P1",
                "status": target_norm.value,
                "trigger_reason": reason or "",
                "metric_snapshot": {}
            }

        transition_record = {
            "incident_id": incident_id,
            "from_state": current_norm.value,
            "to_state": target_norm.value,
            "actor": actor,
            "reason": reason,
            "metadata": metadata or {}
        }
        self._in_memory_transitions.append(transition_record)

        if self.pool:
            update_query = """
            UPDATE incidents
            SET status = $2,
                updated_at = NOW(),
                resolved_at = CASE WHEN $2 = 'RESOLVED' THEN NOW() ELSE resolved_at END
            WHERE incident_id = $1;
            """
            insert_trans_query = """
            INSERT INTO incident_transitions (
                incident_id, from_state, to_state, actor, reason, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6);
            """
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(update_query, incident_id, target_norm.value)
                    await conn.execute(
                        insert_trans_query,
                        incident_id,
                        current_norm.value,
                        target_norm.value,
                        actor,
                        reason,
                        json.dumps(metadata or {})
                    )

        logger.info(
            f"Incident {incident_id} transitioned: {current_norm.value} ➔ {target_norm.value} "
            f"by {actor} (reason: {reason})"
        )
        return target_norm.value

    async def get_incident_transitions(self, incident_id: str) -> List[Dict[str, Any]]:
        """Fetch audit log of transitions for this incident."""
        if not self.pool:
            return [t for t in self._in_memory_transitions if t["incident_id"] == incident_id]

        query = """
        SELECT * FROM incident_transitions
        WHERE incident_id = $1
        ORDER BY created_at ASC;
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, incident_id)
            return [dict(r) for r in rows]

    async def update_incident_status(
        self,
        incident_id: str,
        status: str,
        rca_summary: Optional[str] = None,
        confidence_score: Optional[float] = None
    ) -> bool:
        """Update incident lifecycle state and RCA metadata."""
        if incident_id in self._in_memory_incidents:
            self._in_memory_incidents[incident_id]["status"] = status
            if rca_summary:
                self._in_memory_incidents[incident_id]["rca_summary"] = rca_summary
            if confidence_score is not None:
                self._in_memory_incidents[incident_id]["confidence_score"] = confidence_score

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

    def store_pending_remediation(self, incident_id: str, spec_dict: Dict[str, Any]) -> None:
        """Store pending remediation spec for human approval retrieval."""
        self._in_memory_remediations[incident_id] = spec_dict

    def get_pending_remediation(self, incident_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve pending remediation spec."""
        return self._in_memory_remediations.get(incident_id)

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
        if not hasattr(self, "_seen_idempotency_keys"):
            self._seen_idempotency_keys = set()

        if idempotency_key in self._seen_idempotency_keys:
            logger.warning(
                f"Database idempotency constraint: key '{idempotency_key}' already recorded.")
            return False

        rec = {
            "remediation_id": remediation_id,
            "incident_id": incident_id,
            "idempotency_key": idempotency_key,
            "action_type": action_type,
            "target_service": target_service,
            "parameters": parameters,
            "dry_run_passed": dry_run_passed,
            "status": status
        }
        self._in_memory_remediations[incident_id] = rec
        self._seen_idempotency_keys.add(idempotency_key)

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
