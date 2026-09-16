import asyncio
import json
import logging
import os
import signal
import sys
from collections import OrderedDict
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from services.ingestion_worker.db import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("aether.consumer")

TOPIC_LOGS = "telemetry.logs"
TOPIC_DLQ = "telemetry.dlq"
CONSUMER_GROUP = "aether-ingestion-workers"

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
    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self.dedup_cache = LRUDeduplicationCache(capacity=50000)
        self.running = False
        self.processed_count = 0
        self.deduped_count = 0
        self.dlq_count = 0

    async def start(self):
        self.running = True
        await db.connect()

        self.consumer = AIOKafkaConsumer(
            TOPIC_LOGS,
            bootstrap_servers=self.bootstrap,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Manual batch commits for at-least-once reliability
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        await self.consumer.start()
        await self.producer.start()
        logger.info(f"Consumer started on topic '{TOPIC_LOGS}' (group: {CONSUMER_GROUP})")

    async def stop(self):
        self.running = False
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        await db.disconnect()
        logger.info("Consumer stopped cleanly")

    async def route_to_dlq(self, raw_message: Any, reason: str):
        """Route malformed or unprocessable messages to Dead Letter Queue."""
        self.dlq_count += 1
        dlq_payload = {
            "error_reason": reason,
            "raw_payload": str(raw_message),
            "consumer_group": CONSUMER_GROUP
        }
        logger.warning(f"Routing message to DLQ '{TOPIC_DLQ}': {reason}")
        if self.producer:
            await self.producer.send_and_wait(TOPIC_DLQ, dlq_payload)

    async def process_record(self, record: Dict[str, Any]) -> bool:
        """Process a single deserialized log record with deduplication and DB write."""
        event_id = record.get("event_id")
        if not event_id:
            await self.route_to_dlq(record, "Missing required event_id field")
            return False

        # 1. Deduplication Cache Check
        if self.dedup_cache.has(event_id):
            self.deduped_count += 1
            return True

        # 2. Write to PostgreSQL + pgvector
        try:
            inserted = await db.insert_log(record)
            self.dedup_cache.add(event_id)
            if inserted:
                self.processed_count += 1
            else:
                self.deduped_count += 1
            return True
        except Exception as e:
            logger.error(f"Error persisting record {event_id}: {e}")
            await self.route_to_dlq(record, f"DB insert error: {str(e)}")
            return False

    async def consume_loop(self, batch_size: int = 100):
        """Main batch consumption loop with manual offset commits."""
        logger.info(f"Beginning consumption loop (batch size={batch_size})...")
        while self.running:
            try:
                # Fetch batch of messages with 1-second poll timeout
                data = await self.consumer.getmany(timeout_ms=1000, max_records=batch_size)
                if not data:
                    await asyncio.sleep(0.1)
                    continue

                for topic_partition, messages in data.items():
                    for msg in messages:
                        await self.process_record(msg.value)

                    # Commit partition offsets only after all messages in batch are processed
                    await self.consumer.commit({topic_partition: messages[-1].offset + 1})

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
