import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from opentelemetry import trace

from services.demo_service.telemetry import StructuredLogger, tracer

def test_structured_logger_default_initialization():
    logger = StructuredLogger(service_name="test-payment-service")
    assert logger.service_name == "test-payment-service"
    assert logger.kafka_producer is None
    assert logger.logger.name == "aether.telemetry"

def test_structured_logger_log_payload_structure():
    logger = StructuredLogger(service_name="payment-service")
    payload = logger.log(
        level="INFO",
        message="Order processed successfully",
        http_status=200,
        duration_ms=15.42,
        metadata={"order_id": "ord_1001", "amount": 99.99}
    )

    assert isinstance(payload, dict)
    assert payload["service_name"] == "payment-service"
    assert payload["level"] == "INFO"
    assert payload["message"] == "Order processed successfully"
    assert payload["http_status"] == 200
    assert payload["duration_ms"] == 15.42
    assert payload["metadata"]["order_id"] == "ord_1001"
    assert "timestamp" in payload

    # Verify event_id is valid UUID
    parsed_uuid = uuid.UUID(payload["event_id"])
    assert str(parsed_uuid) == payload["event_id"]

def test_structured_logger_opentelemetry_context():
    logger = StructuredLogger(service_name="payment-service")

    with tracer.start_as_current_span("simulated-checkout-span"):
        payload = logger.log(level="ERROR", message="Payment gateway timed out")

    # In active tracing context, trace_id and span_id should be extracted
    assert payload["trace_id"] is not None
    assert len(payload["trace_id"]) == 32
    assert payload["span_id"] is not None
    assert len(payload["span_id"]) == 16

def test_structured_logger_kafka_initialization_success():
    async def _test():
        logger = StructuredLogger()
        mock_producer_instance = AsyncMock()
        mock_producer_cls = MagicMock(return_value=mock_producer_instance)

        with patch("aiokafka.AIOKafkaProducer", new=mock_producer_cls):
            await logger.init_kafka()
            assert logger.kafka_producer is not None
            mock_producer_instance.start.assert_awaited_once()

    asyncio.run(_test())

def test_structured_logger_kafka_initialization_failure_fallback():
    async def _test():
        logger = StructuredLogger()
        mock_producer_cls = MagicMock(side_effect=Exception("Broker connection refused"))

        with patch("aiokafka.AIOKafkaProducer", new=mock_producer_cls):
            await logger.init_kafka()
            # Gracefully falls back to stdout logging
            assert logger.kafka_producer is None

    asyncio.run(_test())

def test_structured_logger_emit_to_kafka():
    async def _test():
        logger = StructuredLogger()
        mock_producer = AsyncMock()
        logger.kafka_producer = mock_producer

        record = {"event_id": "evt_test_123", "message": "Test event"}
        await logger.emit_to_kafka("telemetry.logs", record)
        mock_producer.send_and_wait.assert_awaited_once_with("telemetry.logs", record)

        # Failure during send should be logged gracefully without raising
        mock_producer.send_and_wait.side_effect = Exception("Kafka send timeout")
        await logger.emit_to_kafka("telemetry.logs", record)

    asyncio.run(_test())

def test_structured_logger_close_kafka():
    async def _test():
        logger = StructuredLogger()
        mock_producer = AsyncMock()
        logger.kafka_producer = mock_producer

        await logger.close_kafka()
        mock_producer.stop.assert_awaited_once()

        # No-op when producer is None
        logger.kafka_producer = None
        await logger.close_kafka()

    asyncio.run(_test())
