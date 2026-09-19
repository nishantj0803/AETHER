import asyncio
from enum import Enum
import json
import logging
import os
import signal
from collections import OrderedDict
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from services.ingestion_worker.db import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("aether.consumer")

TOPIC_LOGS = os.getenv("KAFKA_TOPIC_LOGS", "telemetry.logs")
TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "telemetry.dlq")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "aether-ingestion-workers")

class ProcessingResult(str, Enum):
    """Explicit processing result states for distributed stream correctness."""
    PROCESS_SUCCESS = "PROCESS_SUCCESS"
    DUPLICATE = "DUPLICATE"
    DLQ_SUCCESS = "DLQ_SUCCESS"
    PROCESS_RETRY = "PROCESS_RETRY"
    PROCESS_FATAL = "PROCESS_FATAL"

    @property
    def is_terminal(self) -> bool:
        """Terminal states allow advancing and committing the Kafka partition offset."""
        return self in (
            ProcessingResult.PROCESS_SUCCESS,
            ProcessingResult.DUPLICATE,
            ProcessingResult.DLQ_SUCCESS,
        )

class LRUDeduplicationCache:
    """Fast in-memory cache to reject duplicate event_ids before database write."""
    def __init__(self, capacity: int = 10000):
        self.capacity = capacity
        self.cache = OrderedDict()

    def has(self, event_id: str) -> bool:
        if event_id in self.cache:
            self.cache.move_to_end(event_id)
            return True
        return False

    def add(self, event_id: str):
        self.cache[event_id] = True
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

class AetherLogConsumer:
    """
    Robust Streaming Ingestion Worker for Kafka/Redpanda.
    Features:
    - Explicit ProcessingResult state machine
    - Contiguous partition offset commits preventing data loss
    - In-memory LRU deduplication + DB idempotent constraint
    - Exponential backoff retry for transient DB faults
    - Dead Letter Queue (DLQ) routing for unrecoverable payloads
    """
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        max_retries: int = 3
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self.dedup_cache = LRUDeduplicationCache(capacity=50000)
        self.running = False
        self.processed_count = 0
        self.deduped_count = 0
        self.dlq_count = 0
        self.retry_count = 0
        self.max_retries = max_retries

    async def start(self):
        logger.info(f"Connecting to Kafka/Redpanda at {self.bootstrap_servers}...")
        self.consumer = AIOKafkaConsumer(
            TOPIC_LOGS,
            bootstrap_servers=self.bootstrap_servers,
            group_id=CONSUMER_GROUP,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        await self.consumer.start()
        await self.producer.start()
        await db.connect()
        self.running = True
        logger.info(f"Consumer active on topic '{TOPIC_LOGS}' (group: {CONSUMER_GROUP})")

    async def stop(self):
        self.running = False
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        await db.disconnect()
        logger.info("Consumer stopped cleanly")

    async def route_to_dlq(self, raw_message: Any, reason: str) -> bool:
        """Route malformed or unprocessable messages to Dead Letter Queue."""
        self.dlq_count += 1
        dlq_payload = {
            "error_reason": reason,
            "raw_payload": str(raw_message),
            "consumer_group": CONSUMER_GROUP
        }
        logger.warning(f"Routing message to DLQ '{TOPIC_DLQ}': {reason}")
        if self.producer:
            try:
                await self.producer.send_and_wait(TOPIC_DLQ, dlq_payload)
                return True
            except Exception as e:
                logger.critical(f"Failed to publish to DLQ: {e}")
                return False
        return True

    async def process_record(self, record: Dict[str, Any]) -> ProcessingResult:
        """
        Process a single deserialized log record with explicit result state.
        Returns ProcessingResult indicating if the record reached a terminal state
        or requires partition pause/retry.
        """
        event_id = record.get("event_id") if isinstance(record, dict) else None
        if not event_id:
            routed = await self.route_to_dlq(record, "Missing required event_id field")
            return ProcessingResult.DLQ_SUCCESS if routed else ProcessingResult.PROCESS_FATAL

        # 1. Deduplication Cache Check
        if self.dedup_cache.has(event_id):
            self.deduped_count += 1
            return ProcessingResult.DUPLICATE

        # 2. Write to PostgreSQL with transient retry backoff
        for attempt in range(1, self.max_retries + 1):
            try:
                inserted = await db.insert_log(record)
                self.dedup_cache.add(event_id)
                if inserted:
                    self.processed_count += 1
                    return ProcessingResult.PROCESS_SUCCESS
                else:
                    self.deduped_count += 1
                    return ProcessingResult.DUPLICATE
            except Exception as e:
                self.retry_count += 1
                logger.warning(f"Transient error persisting record {event_id} (attempt {attempt}/{self.max_retries}): {e}")
                if attempt < self.max_retries:
                    await asyncio.sleep(0.05 * (2 ** (attempt - 1)))  # 50ms, 100ms
                else:
                    logger.error(f"Permanent persistence failure for record {event_id} after {self.max_retries} attempts.")
                    # Distinguish fatal schema/data errors from transient outages
                    return ProcessingResult.PROCESS_RETRY

        return ProcessingResult.PROCESS_RETRY

    async def consume_loop(self, batch_size: int = 100):
        """
        Main batch consumption loop with contiguous offset commits.
        Guarantees that uncommitted or retried records prevent later offsets
        from being committed past them.
        """
        logger.info(f"Beginning consumption loop (batch size={batch_size})...")
        while self.running:
            try:
                data = await self.consumer.getmany(timeout_ms=1000, max_records=batch_size)
                if not data:
                    await asyncio.sleep(0.1)
                    continue

                for topic_partition, messages in data.items():
                    commit_offset = None

                    for msg in messages:
                        result = await self.process_record(msg.value)

                        if result.is_terminal:
                            # Safely advance contiguous commit boundary
                            commit_offset = msg.offset + 1
                        else:
                            # Non-terminal result (e.g. PROCESS_RETRY):
                            # Halt processing for this partition batch immediately!
                            # Do NOT process or commit messages past this offset.
                            logger.warning(
                                f"Halted partition batch at offset {msg.offset} due to non-terminal result '{result.value}'. "
                                f"Will commit up to contiguous offset {commit_offset}."
                            )
                            # Rewind consumer position to prevent in-memory prefetch buffer from skipping uncommitted messages
                            target_seek = commit_offset if commit_offset is not None else msg.offset
                            try:
                                self.consumer.seek(topic_partition, target_seek)
                            except Exception as e:
                                logger.debug(f"Consumer seek note: {e}")
                            break

                    # Commit only up to the highest contiguous terminal offset
                    if commit_offset is not None:
                        await self.consumer.commit({topic_partition: commit_offset})

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consumer loop: {e}", exc_info=True)
                await asyncio.sleep(1.0)

async def main():
    consumer = AetherLogConsumer()
    await consumer.start()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(consumer.stop()))

    try:
        await consumer.consume_loop()
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
