"""
LogWriterConsumer — Batch inserts log events from Kafka into PostgreSQL.

We commit the offset AFTER the DB insert, not before.
This gives at-least-once semantics: a crash between insert and commit
causes re-processing, not data loss. Downstream must tolerate duplicates
(the UUID primary key makes duplicate inserts a no-op via ON CONFLICT DO NOTHING).

Performance design:
  - Batch poll: up to 500ms or 100 messages, whichever comes first
  - Bulk insert via asyncpg executemany() — never insert one row at a time
  - ON CONFLICT DO NOTHING makes batch inserts idempotent, supporting
    at-least-once delivery without duplicate rows

This consumer is the most write-heavy component. On an M1 Mac with 8GB RAM,
asyncpg's binary protocol and batch execution keep PostgreSQL load manageable.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

import asyncpg

from base_consumer import BaseConsumer

logger = logging.getLogger(__name__)

# SQL for batch insert with ON CONFLICT DO NOTHING for idempotency
INSERT_SQL = """
    INSERT INTO logs (id, service, level, message, timestamp, trace_id, metadata)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (id, timestamp) DO NOTHING
"""


class LogWriterConsumer(BaseConsumer):
    """
    Kafka consumer that batch-inserts log events into PostgreSQL.

    Responsibilities:
      - Deserialize LogEvent JSON from Kafka messages
      - Batch-insert into the partitioned logs table
      - Commit Kafka offsets only after successful DB insert
      - Handle idempotent inserts via ON CONFLICT DO NOTHING

    The at-least-once delivery guarantee is achieved by:
      1. Process batch (insert into PostgreSQL)
      2. Commit Kafka offset
    If step 1 succeeds but step 2 fails (crash), the messages are re-delivered
    and the insert is a no-op due to the primary key constraint.
    """

    topic = os.getenv("KAFKA_TOPIC_INGEST", "logs.ingest")
    group_id = os.getenv("KAFKA_GROUP_LOG_WRITER", "logfoundry-log-writer")

    def __init__(self) -> None:
        super().__init__()
        self._pg_pool: Optional[asyncpg.Pool] = None

    async def start(self) -> None:
        """Start the consumer with a PostgreSQL connection pool."""
        postgres_dsn = os.getenv(
            "POSTGRES_DSN",
            "postgresql://logfoundry:logfoundry@localhost/logfoundry",
        )
        self._pg_pool = await asyncpg.create_pool(
            dsn=postgres_dsn,
            min_size=2,
            max_size=5,
            command_timeout=30,
        )
        logger.info("PostgreSQL pool created for LogWriterConsumer")

        try:
            await super().start()
        finally:
            if self._pg_pool:
                await self._pg_pool.close()

    async def process(self, message) -> None:
        """
        Process a single Kafka message by inserting into PostgreSQL.

        Note: In practice, the base class consume loop batches messages
        from getmany(). This process() handles individual messages within
        that batch, but the actual DB insert is batched via _batch_insert().
        """
        await self._insert_single(message)

    async def _insert_single(self, message) -> None:
        """Parse and insert a single log event."""
        try:
            payload = json.loads(message.value.decode("utf-8"))

            async with self._pg_pool.acquire() as conn:
                await conn.execute(
                    INSERT_SQL,
                    UUID(payload["id"]),
                    payload["service"],
                    payload["level"],
                    payload["message"],
                    datetime.fromisoformat(payload["timestamp"]) if isinstance(payload["timestamp"], str) else payload["timestamp"],
                    payload.get("trace_id"),
                    json.dumps(payload.get("metadata")) if payload.get("metadata") else None,
                )

            logger.debug(
                "Log event inserted",
                extra={
                    "event_id": payload["id"],
                    "service": payload["service"],
                    "level": payload["level"],
                },
            )
        except Exception as e:
            logger.error(
                f"Failed to insert log event: {e}",
                extra={
                    "partition": message.partition,
                    "offset": message.offset,
                },
                exc_info=True,
            )
            raise

    async def _consume_loop(self) -> None:
        """
        Override consume loop for batch insert optimization.

        Instead of processing one message at a time, we accumulate messages
        from getmany() and insert them as a batch using executemany().
        This dramatically reduces PostgreSQL round trips.
        """
        while self._running:
            try:
                messages = await self._consumer.getmany(
                    timeout_ms=500, max_records=100
                )

                for tp, batch in messages.items():
                    if not batch:
                        continue

                    start_time = time.monotonic()

                    # Parse all messages in the batch
                    rows = []
                    valid_messages = []
                    failed_messages = []
                    for message in batch:
                        try:
                            payload = json.loads(message.value.decode("utf-8"))
                            timestamp = payload["timestamp"]
                            if isinstance(timestamp, str):
                                timestamp = datetime.fromisoformat(timestamp)

                            rows.append((
                                UUID(payload["id"]),
                                payload["service"],
                                payload["level"],
                                payload["message"],
                                timestamp,
                                payload.get("trace_id"),
                                json.dumps(payload.get("metadata")) if payload.get("metadata") else None,
                            ))
                            valid_messages.append(message)
                        except Exception as e:
                            logger.warning(
                                f"Failed to parse message: {e}",
                                extra={
                                    "partition": message.partition,
                                    "offset": message.offset,
                                },
                            )
                            failed_messages.append(message)

                    # Bulk insert — executemany is significantly faster than individual inserts
                    if rows:
                        try:
                            async with self._pg_pool.acquire() as conn:
                                async with conn.transaction():
                                    await conn.executemany(INSERT_SQL, rows)
                        except Exception as batch_err:
                            logger.warning(
                                f"Batch insert failed ({batch_err}), falling back to single inserts",
                                extra={
                                    "partition": tp.partition,
                                    "batch_insert_fallback_triggered": 1
                                }
                            )
                            # Fallback: insert one-by-one to isolate the poison pill
                            async with self._pg_pool.acquire() as conn:
                                for row, msg in zip(rows, valid_messages):
                                    try:
                                        await conn.execute(INSERT_SQL, *row)
                                    except Exception as single_err:
                                        logger.error(
                                            f"Poison pill detected: {single_err}",
                                            extra={"partition": msg.partition, "offset": msg.offset}
                                        )
                                        failed_messages.append(msg)

                    # Route unparseable messages to dead-letter
                    for msg in failed_messages:
                        await self._send_to_dead_letter(msg)

                    latency_ms = (time.monotonic() - start_time) * 1000

                    logger.info(
                        "Batch insert completed",
                        extra={
                            "batch_size": len(rows),
                            "failed_count": len(failed_messages),
                            "insert_latency_ms": round(latency_ms, 2),
                            "partition": tp.partition,
                        },
                    )

                    # Commit offset AFTER successful insert — this is the key
                    # to at-least-once delivery. If we crash here, the messages
                    # will be re-delivered and ON CONFLICT DO NOTHING handles dedup.
                    await self._consumer.commit()

            except Exception as e:
                logger.error(f"Error in batch consume loop: {e}", exc_info=True)
                await asyncio.sleep(1)


if __name__ == "__main__":
    consumer = LogWriterConsumer()
    asyncio.run(consumer.start())
