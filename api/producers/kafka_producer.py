"""
KafkaLogProducer — Fire-and-forget Kafka producer for log ingestion.

Fire-and-forget: we create_task instead of awaiting the produce call.
This decouples ingestion latency from Kafka broker round-trip time.
Tradeoff: at-least-once delivery — if the task fails, the event is lost.
Mitigation: producers log failures; consumers handle duplicates via idempotency key.

Design decisions:
  - acks="all" ensures the broker acknowledges the write for durability,
    but the API handler doesn't wait for this acknowledgment.
  - The background task captures and logs produce errors to stderr rather
    than swallowing them, so operational issues surface in logs.
  - Partitioning by service name ensures logs from the same service land
    on the same partition, preserving per-service ordering.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from aiokafka import AIOKafkaProducer

from models import LogEvent

logger = logging.getLogger(__name__)


class KafkaLogProducer:
    """
    Async Kafka producer that sends log events to the ingestion topic.

    Wraps AIOKafkaProducer with:
      - JSON serialization of LogEvent payloads
      - Fire-and-forget sending via asyncio.create_task()
      - Service-based partitioning for per-service ordering
      - Error logging (never swallows failures silently)
    """

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._producer: Optional[AIOKafkaProducer] = None
        self._background_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        """Initialize and start the Kafka producer."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            # acks="all" — wait for all in-sync replicas to acknowledge.
            # With replication_factor=1 this is equivalent to acks=1,
            # but we set it explicitly to show production intent.
            acks="all",
            # Compress payloads to reduce broker storage and network overhead.
            # gzip provides the best compression ratio for JSON log messages.
            compression_type="gzip",
            # Linger slightly to enable micro-batching of concurrent requests
            linger_ms=5,
            # Max request size — 1MB is generous for log events capped at 8KB
            max_request_size=1048576,
        )
        await self._producer.start()
        logger.info(
            "Kafka producer started",
            extra={"bootstrap_servers": self._bootstrap_servers, "topic": self._topic},
        )

    async def stop(self) -> None:
        """Gracefully stop the producer, flushing any pending messages."""
        if self._background_tasks:
            logger.info(f"Waiting for {len(self._background_tasks)} background tasks to finish...")
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped")

    async def send(self, event: LogEvent) -> None:
        """
        Send a log event to Kafka (awaitable version).

        This is the actual produce call. Use send_fire_and_forget() for
        non-blocking ingestion in the API handler.
        """
        if not self._producer:
            raise RuntimeError("Producer not started — call start() first")

        start_time = time.monotonic()

        try:
            # Partition by service name for per-service ordering
            key = event.service.encode("utf-8")
            value = event.to_kafka_bytes()

            await self._producer.send_and_wait(
                self._topic,
                key=key,
                value=value,
            )

            latency_ms = (time.monotonic() - start_time) * 1000
            logger.debug(
                "Kafka produce succeeded",
                extra={
                    "event_id": str(event.id),
                    "service": event.service,
                    "level": event.level,
                    "latency_ms": round(latency_ms, 2),
                },
            )
        except Exception as e:
            latency_ms = (time.monotonic() - start_time) * 1000
            # Log to stderr — never swallow produce errors silently.
            # This surfaces broker connectivity issues in operational logs.
            logger.error(
                "Kafka produce failed",
                extra={
                    "event_id": str(event.id),
                    "service": event.service,
                    "error": str(e),
                    "latency_ms": round(latency_ms, 2),
                },
                exc_info=True,
            )
            raise

    def send_fire_and_forget(self, event: LogEvent) -> asyncio.Task:
        """
        Fire-and-forget Kafka produce — returns immediately.

        Uses asyncio.create_task() to decouple the API response time from
        Kafka broker round-trip. The task runs in the background and logs
        any errors without propagating them to the caller.

        Returns the Task object for testing/monitoring purposes.
        """

        async def _produce_with_error_handling():
            try:
                await self.send(event)
            except Exception:
                # Error already logged in send() — don't propagate to the
                # event loop's exception handler.
                pass

        task = asyncio.create_task(_produce_with_error_handling())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    async def health_check(self) -> bool:
        """
        Check if the producer is connected to the Kafka cluster.

        WARNING: This relies on partitions_for(), which returns data from a
        local metadata cache. It will not immediately detect if the broker
        goes down after startup, until the cache expires or a fetch fails.
        """
        if not self._producer:
            return False
        try:
            # Partitions metadata call verifies broker connectivity
            partitions = self._producer.partitions_for(self._topic)
            return partitions is not None
        except Exception:
            return False
