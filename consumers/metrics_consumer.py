"""
MetricsConsumer — Real-time metrics aggregation via Redis counters.

For each consumed log event, increments four Redis counters:
  - metrics:total — global counter
  - metrics:service:{service} — per-service counter
  - metrics:level:{level} — per-level counter
  - metrics:service:{service}:level:{level} — cross-dimension counter

Uses Redis INCR (not sorted sets) because these are simple monotonic counters.
INCR is O(1) and atomic, making it ideal for high-throughput counter updates.

The /metrics API endpoint reads these counters and formats them as
Prometheus exposition text, enabling integration with Grafana dashboards.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

from redis.asyncio import Redis

from base_consumer import BaseConsumer

logger = logging.getLogger(__name__)


class MetricsConsumer(BaseConsumer):
    """
    Kafka consumer that aggregates log metrics into Redis counters.

    Responsibilities:
      - Consume log events from logs.ingest topic
      - Increment Redis counters for total, per-service, per-level, and cross-dimension
      - Use Redis pipeline for batched counter updates (reduces round trips)

    The consumer uses Redis INCR which is atomic and O(1), making it suitable
    for high-throughput counter updates without race conditions.
    """

    topic = os.getenv("KAFKA_TOPIC_INGEST", "logs.ingest")
    group_id = os.getenv("KAFKA_GROUP_METRICS", "logfoundry-metrics")

    def __init__(self) -> None:
        super().__init__()
        self._redis = None

    async def start(self) -> None:
        """Start the consumer with a Redis connection."""
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self._redis = Redis.from_url(redis_url, decode_responses=True)

        logger.info("MetricsConsumer initialized", extra={"redis_url": redis_url})

        try:
            await super().start()
        finally:
            if self._redis:
                await self._redis.close()

    async def process(self, message) -> None:
        """
        Process a single message by incrementing Redis counters.

        Uses a Redis pipeline to batch all four INCR operations into
        a single network round trip, reducing latency by ~4x compared
        to individual INCR calls.
        """
        try:
            payload = json.loads(message.value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Failed to parse message for metrics: {e}")
            raise

        service = payload.get("service", "unknown")
        level = payload.get("level", "UNKNOWN")

        # Pipeline batches all commands into a single Redis round trip.
        # This is a significant performance optimization for high-throughput counters.
        pipe = self._redis.pipeline(transaction=False)
        pipe.incr("metrics:total")
        pipe.hincrby("metrics:services", service, 1)
        pipe.hincrby("metrics:levels", level, 1)
        pipe.hincrby("metrics:service_levels", f"{service}:{level}", 1)
        await pipe.execute()

        logger.debug(
            "Metrics updated",
            extra={"service": service, "level": level},
        )


if __name__ == "__main__":
    consumer = MetricsConsumer()
    asyncio.run(consumer.start())
