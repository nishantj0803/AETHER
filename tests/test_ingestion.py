import pytest
from services.ingestion_worker.embeddings import embedder, EMBEDDING_DIMENSION
from services.ingestion_worker.consumer import LRUDeduplicationCache, AetherLogConsumer

def test_semantic_embedding_dimensions():
    text = "Database connection timed out: query exceeded 50ms"
    vec = embedder.embed(text)
    assert len(vec) == EMBEDDING_DIMENSION
    # Check L2 normalization: norm should be ~ 1.0
    import numpy as np
    norm = np.linalg.norm(vec)
    assert pytest.approx(norm, 0.01) == 1.0

def test_semantic_similarity_clustering():
    query = "database connection timeout in query pool"
    similar_log = "DB connection pool exhausted: query timeout"
    unrelated_log = "Order status updated to shipped for customer"

    vec_query = embedder.embed(query)
    vec_similar = embedder.embed(similar_log)
    vec_unrelated = embedder.embed(unrelated_log)

    sim_high = embedder.cosine_similarity(vec_query, vec_similar)
    sim_low = embedder.cosine_similarity(vec_query, vec_unrelated)

    # Similar error phrases should score significantly higher than unrelated activity
    assert sim_high > 0.60
    assert sim_high > sim_low

def test_lru_deduplication_cache():
    cache = LRUDeduplicationCache(capacity=3)
    
    cache.add("event-1")
    cache.add("event-2")
    cache.add("event-3")
    
    # Access event-1 so it becomes most recently used
    assert cache.has("event-1") is True
    # Order in cache is now: [event-2, event-3, event-1]

    # Adding a 4th event should evict event-2 (least recently used)
    cache.add("event-4")
    assert cache.has("event-4") is True
    assert cache.has("event-1") is True
    assert cache.has("event-3") is True
    assert cache.has("event-2") is False

def test_consumer_deduplication():
    import asyncio
    from services.ingestion_worker.consumer import ProcessingResult

    async def _test():
        consumer = AetherLogConsumer(bootstrap_servers="mock:9092")
        
        record = {
            "event_id": "evt_unique_12345",
            "service_name": "payment-service",
            "level": "INFO",
            "message": "Payment processed",
            "timestamp": "2026-09-16T20:00:00Z"
        }

        # First arrival
        res1 = await consumer.process_record(record)
        assert res1 == ProcessingResult.PROCESS_SUCCESS
        assert res1.is_terminal is True

        # Duplicate arrival with same event_id
        res2 = await consumer.process_record(record)
        assert res2 == ProcessingResult.DUPLICATE
        assert res2.is_terminal is True
        assert consumer.deduped_count == 1

    asyncio.run(_test())

def test_consumer_dlq_routing_on_missing_event_id():
    import asyncio
    from services.ingestion_worker.consumer import ProcessingResult

    async def _test():
        consumer = AetherLogConsumer(bootstrap_servers="mock:9092")
        
        # Poison pill record missing event_id
        bad_record = {
            "service_name": "payment-service",
            "level": "ERROR",
            "message": "Corrupted log message"
        }

        res = await consumer.process_record(bad_record)
        assert res == ProcessingResult.DLQ_SUCCESS
        assert res.is_terminal is True
        assert consumer.dlq_count == 1

    asyncio.run(_test())

def test_consumer_contiguous_offset_halts_on_retry():
    import asyncio
    from unittest.mock import AsyncMock, patch
    from services.ingestion_worker.consumer import ProcessingResult

    async def _test():
        consumer = AetherLogConsumer(bootstrap_servers="mock:9092", max_retries=1)

        # Mock DB failure for a transient database connection issue
        with patch("services.ingestion_worker.db.db.insert_log", new=AsyncMock(side_effect=Exception("DB pool timeout"))):
            res = await consumer.process_record({"event_id": "evt_fail_1", "message": "fail"})
            assert res == ProcessingResult.PROCESS_RETRY
            assert res.is_terminal is False

    asyncio.run(_test())


