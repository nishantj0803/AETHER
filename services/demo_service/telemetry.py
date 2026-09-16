import json
import logging
import os
import sys
import uuid
import psutil
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter

# 1. OpenTelemetry Setup
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer("payment-service")

# 2. Prometheus Golden Signal Metrics
REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total count of HTTP requests processed",
    ["status", "method", "endpoint"]
)

REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency distribution",
    ["endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 5.0]
)

PAYMENTS_TOTAL = Counter(
    "payment_transactions_total",
    "Count of payment charge attempts",
    ["status", "currency"]
)

MEMORY_USAGE_BYTES = Gauge(
    "service_memory_usage_bytes",
    "Current process memory usage (RSS in bytes)"
)

ACTIVE_DB_CONNECTIONS = Gauge(
    "active_db_connections",
    "Active simulated database connections in pool"
)

ACTIVE_FAULTS_GAUGE = Gauge(
    "injected_fault_active",
    "Currently active synthetic faults",
    ["fault_type"]
)

# 3. Structured Logging & Kafka Streaming
class StructuredLogger:
    def __init__(self, service_name: str = "payment-service"):
        self.service_name = service_name
        self.kafka_producer = None
        self.kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
        self._setup_logger()

    def _setup_logger(self):
        self.logger = logging.getLogger("aether.telemetry")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)

    async def init_kafka(self):
        """Optionally connect to Redpanda/Kafka if available."""
        try:
            from aiokafka import AIOKafkaProducer
            self.kafka_producer = AIOKafkaProducer(
                bootstrap_servers=self.kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            await self.kafka_producer.start()
            self.log(
                "INFO",
                f"Connected to Kafka/Redpanda at {self.kafka_bootstrap}",
                metadata={"component": "telemetry"}
            )
        except Exception as e:
            self.log(
                "WARN",
                f"Kafka not reachable at {self.kafka_bootstrap}; logging to stdout only: {e}",
                metadata={"component": "telemetry"}
            )
            self.kafka_producer = None

    async def close_kafka(self):
        if self.kafka_producer:
            await self.kafka_producer.stop()

    def log(
        self,
        level: str,
        message: str,
        http_status: Optional[int] = None,
        duration_ms: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Produce structured JSON log with trace correlation and unique event_id."""
        current_span = trace.get_current_span()
        span_ctx = current_span.get_span_context() if current_span else None

        trace_id = format(span_ctx.trace_id, "032x") if span_ctx and span_ctx.trace_id else None
        span_id = format(span_ctx.span_id, "016x") if span_ctx and span_ctx.span_id else None

        # Idempotency / Correlation ID
        event_id = str(uuid.uuid4())

        payload = {
            "event_id": event_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service_name": self.service_name,
            "level": level.upper(),
            "message": message,
            "trace_id": trace_id,
            "span_id": span_id,
            "http_status": http_status,
            "duration_ms": duration_ms,
            "metadata": metadata or {}
        }

        # Print JSON to stdout for container log collection
        self.logger.info(json.dumps(payload))

        # Update process memory gauge
        try:
            process = psutil.Process()
            MEMORY_USAGE_BYTES.set(process.memory_info().rss)
        except Exception:
            pass

        return payload

    async def emit_to_kafka(self, topic: str, record: Dict[str, Any]):
        """Asynchronously stream log record to Redpanda topic."""
        if self.kafka_producer:
            try:
                await self.kafka_producer.send_and_wait(topic, record)
            except Exception as e:
                self.logger.warning(f"Failed to stream record {record.get('event_id')} to Kafka: {e}")

telemetry = StructuredLogger()
