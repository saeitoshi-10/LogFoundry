"""
BaseConsumer — Abstract base class for all Kafka consumers with retry and dead-letter logic.

Design decisions:
  - Subclasses only implement process() — the retry + dead-letter logic lives once in the base.
  - Exponential backoff (2^attempt seconds) prevents thundering herd on transient failures.
  - After max_retries exhausted, the message is sent to a dead-letter topic for manual inspection.
  - Graceful shutdown: on SIGTERM/SIGINT, the consumer finishes processing the current batch,
    commits offsets, and exits cleanly. This is critical for at-least-once delivery semantics —
    an ungraceful shutdown can cause offset commit to be lost, leading to message reprocessing.

Consumer group coordination:
  - Each consumer type has its own group_id, so all three consumers independently process
    every message from the logs.ingest topic. This is a fan-out pattern.
  - Within a consumer group, Kafka handles partition assignment automatically.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from abc import ABC, abstractmethod
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
try:
    from pythonjsonlogger.json import JsonFormatter
except ImportError:
    from pythonjsonlogger.jsonlogger import JsonFormatter

logger = logging.getLogger(__name__)


def setup_consumer_logging() -> None:
    """Configure structured JSON logging for consumer processes."""
    handler = logging.StreamHandler(sys.stdout)
    formatter = JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

    # Suppress noisy third-party loggers
    logging.getLogger("aiokafka").setLevel(logging.WARNING)


class BaseConsumer(ABC):
    """
    Abstract base class for Kafka consumers with retry and dead-letter logic.

    Subclasses must implement:
      - process(message) — handle a single consumed message

    The base class provides:
      - Consumer lifecycle management (start/stop)
      - Exponential backoff retry on processing failures
      - Dead-letter routing for messages that exhaust all retries
      - Graceful shutdown on SIGTERM/SIGINT
      - Structured JSON logging
    """

    topic: str
    group_id: str
    dead_letter_topic: str = "logs.dead-letter"

    def __init__(self) -> None:
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._running = False
        self._bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

        # Read topic/group from env vars with class-level defaults
        self.dead_letter_topic = os.getenv("KAFKA_TOPIC_DLT", self.dead_letter_topic)

    @abstractmethod
    async def process(self, message) -> None:
        """
        Process a single consumed message.

        Subclasses implement this with their specific logic (DB insert, pattern matching, etc.).
        Exceptions raised here trigger the retry mechanism in _run_with_retry().
        """
        raise NotImplementedError

    async def start(self) -> None:
        """
        Initialize Kafka consumer and producer (for dead-letter), then start the consume loop.

        The producer is used solely for dead-letter routing — messages that exhaust
        all retries are forwarded to the dead-letter topic for manual inspection.
        """
        setup_consumer_logging()

        logger.info(
            f"{self.__class__.__name__} starting",
            extra={
                "topic": self.topic,
                "group_id": self.group_id,
                "bootstrap_servers": self._bootstrap_servers,
            },
        )

        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self.group_id,
            # Start from earliest if no committed offset exists.
            # This ensures new consumer groups process all existing messages.
            auto_offset_reset="earliest",
            # Disable auto-commit — we commit manually after successful processing.
            # This is the foundation of at-least-once delivery semantics.
            enable_auto_commit=False,
            # Limit max poll records to prevent memory pressure on M1 8GB
            max_poll_records=100,
        )

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
        )

        # Register signal handlers for graceful shutdown
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_shutdown_signal)

        await self._consumer.start()
        await self._producer.start()

        self._running = True
        logger.info(f"{self.__class__.__name__} started, consuming from '{self.topic}'")

        try:
            await self._consume_loop()
        finally:
            await self._cleanup()

    def _handle_shutdown_signal(self) -> None:
        """
        Handle SIGTERM/SIGINT for graceful shutdown.

        Sets _running to False, which causes the consume loop to exit
        after finishing the current batch. This ensures in-flight messages
        are fully processed and offsets are committed before the consumer stops.
        """
        logger.info(f"{self.__class__.__name__} received shutdown signal, draining...")
        self._running = False

    async def _consume_loop(self) -> None:
        """
        Main consume loop — polls messages and processes them with retry logic.

        The loop continues until _running is set to False by a shutdown signal.
        Each message is processed individually with retry + dead-letter fallback.
        """
        while self._running:
            try:
                # Poll with timeout to check _running flag periodically
                messages = await self._consumer.getmany(timeout_ms=500, max_records=100)

                for tp, batch in messages.items():
                    for message in batch:
                        await self._run_with_retry(message)

                    # Commit offset after successfully processing the entire batch
                    # for this topic-partition. This is at-least-once semantics:
                    # if we crash between processing and commit, messages are re-delivered.
                    await self._consumer.commit()

            except Exception as e:
                logger.error(
                    f"Error in consume loop: {e}",
                    exc_info=True,
                )
                # Brief sleep to prevent tight error loops
                await asyncio.sleep(1)

    async def _run_with_retry(self, message, max_retries: int = 3) -> None:
        """
        Process a message with exponential backoff retry.

        Retry schedule: 1s → 2s → 4s (2^attempt)
        After max_retries exhausted, the message is sent to the dead-letter topic.

        This pattern handles transient failures (network blips, temporary DB unavailability)
        while ensuring permanently failing messages don't block the consumer.
        """
        for attempt in range(max_retries):
            try:
                await self.process(message)
                return  # Success — exit retry loop
            except Exception as e:
                wait = 2 ** attempt
                logger.warning(
                    f"Processing failed (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {wait}s",
                    extra={
                        "error": str(e),
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "attempt": attempt + 1,
                        "max_retries": max_retries,
                    },
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(wait)

        # All retries exhausted — send to dead-letter topic
        await self._send_to_dead_letter(message)

    async def _send_to_dead_letter(self, message) -> None:
        """
        Route a failed message to the dead-letter topic for manual inspection.

        The dead-letter message includes the original payload plus metadata
        about the failure (source topic, partition, offset, consumer class).
        This enables operators to inspect, fix, and replay failed messages.
        """
        try:
            dead_letter_payload = {
                "original_topic": message.topic,
                "original_partition": message.partition,
                "original_offset": message.offset,
                "original_key": message.key.decode("utf-8") if message.key else None,
                "original_value": message.value.decode("utf-8") if message.value else None,
                "consumer_class": self.__class__.__name__,
                "group_id": self.group_id,
            }

            await self._producer.send_and_wait(
                self.dead_letter_topic,
                key=message.key,
                value=json.dumps(dead_letter_payload).encode("utf-8"),
            )

            logger.error(
                "Message sent to dead-letter topic",
                extra={
                    "dead_letter_topic": self.dead_letter_topic,
                    "original_topic": message.topic,
                    "original_partition": message.partition,
                    "original_offset": message.offset,
                },
            )
        except Exception as e:
            # If dead-letter routing itself fails, log and move on.
            # The original message's offset will still be committed,
            # which means data loss — but blocking the consumer is worse.
            logger.critical(
                f"Failed to send to dead-letter topic: {e}",
                exc_info=True,
            )

    async def _cleanup(self) -> None:
        """Stop consumer and producer connections gracefully."""
        logger.info(f"{self.__class__.__name__} cleaning up...")

        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()

        logger.info(f"{self.__class__.__name__} stopped cleanly")
