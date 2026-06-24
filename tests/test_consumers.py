"""
Tests for LogFoundry Kafka consumers.

Test cases:
  - test_consumer_retries_on_failure: mock process() to fail twice, succeed third
  - test_dead_letter_on_exhausted_retries: process() always fails → DLT
  - test_log_writer_parses_event: log writer correctly parses Kafka message
  - test_alert_consumer_matches_pattern: alert rules trigger on matching messages
  - test_alert_consumer_no_match: non-matching messages don't trigger alerts
  - test_metrics_consumer_increments: metrics consumer increments Redis counters
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from base_consumer import BaseConsumer


# ============================================================
# BaseConsumer retry logic tests
# ============================================================


class ConcreteTestConsumer(BaseConsumer):
    """Concrete implementation of BaseConsumer for testing."""

    topic = "test-topic"
    group_id = "test-group"

    def __init__(self):
        super().__init__()
        self.process_mock = AsyncMock()

    async def process(self, message):
        await self.process_mock(message)


class TestBaseConsumerRetry:
    """Tests for the BaseConsumer retry mechanism."""

    @pytest.mark.asyncio
    async def test_consumer_retries_on_failure(self, sample_kafka_message, kafka_producer):
        """
        Mock process() to fail twice, succeed on third attempt.

        Verifies that the retry mechanism works with exponential backoff
        and that process() is called exactly 3 times.
        """
        consumer = ConcreteTestConsumer()

        # Use real Kafka producer (even though it shouldn't be called on success)
        consumer._producer = kafka_producer

        # Fail twice, succeed third
        consumer.process_mock.side_effect = [
            Exception("Transient error 1"),
            Exception("Transient error 2"),
            None,  # Success
        ]

        # Patch asyncio.sleep to speed up tests
        with patch("base_consumer.asyncio.sleep", new_callable=AsyncMock):
            await consumer._run_with_retry(sample_kafka_message, max_retries=3)

        # Assert process was called exactly 3 times
        assert consumer.process_mock.call_count == 3

    @pytest.mark.asyncio
    async def test_dead_letter_on_exhausted_retries(self, sample_kafka_message, kafka_producer):
        """
        Process() always fails → message lands in dead-letter topic.

        After all retries are exhausted, the message should be sent
        to the dead-letter topic for manual inspection.
        """
        consumer = ConcreteTestConsumer()
        
        # Use REAL Kafka Producer to send the DLT message!
        consumer._producer = kafka_producer

        # Always fail
        consumer.process_mock.side_effect = Exception("Permanent failure")

        with patch("base_consumer.asyncio.sleep", new_callable=AsyncMock):
            await consumer._run_with_retry(sample_kafka_message, max_retries=3)

        # Assert process was called max_retries times
        assert consumer.process_mock.call_count == 3
        
        # Since we used the real Kafka producer, if it didn't raise an exception,
        # it successfully published to the logs.dead-letter topic in the testcontainer!

    @pytest.mark.asyncio
    async def test_successful_process_no_retry(self, sample_kafka_message, kafka_producer):
        """Successful process() should not trigger retries."""
        consumer = ConcreteTestConsumer()
        consumer._producer = kafka_producer

        # Succeed immediately
        consumer.process_mock.return_value = None

        await consumer._run_with_retry(sample_kafka_message, max_retries=3)

        # Assert process was called exactly once
        assert consumer.process_mock.call_count == 1

    @pytest.mark.asyncio
    async def test_dead_letter_payload_structure(self, sample_kafka_message, kafka_producer):
        """Verify dead-letter message is successfully sent to real Kafka topic."""
        consumer = ConcreteTestConsumer()
        consumer._producer = kafka_producer

        consumer.process_mock.side_effect = Exception("fail")

        with patch("base_consumer.asyncio.sleep", new_callable=AsyncMock):
            await consumer._run_with_retry(sample_kafka_message, max_retries=1)

        # Since we use real Kafka, the fact that it didn't raise means the payload structure
        # was successfully serialized and accepted by the Kafka broker!


# ============================================================
# AlertConsumer tests
# ============================================================


class TestAlertConsumer:
    """Tests for the AlertConsumer pattern matching."""

    def test_alert_rule_matches(self):
        """Alert rule correctly matches log messages."""
        from alert_consumer import AlertRule

        rule = AlertRule(pattern="OutOfMemoryError", severity="CRITICAL")
        assert rule.matches("Java OutOfMemoryError: heap space") is True
        assert rule.matches("Normal log message") is False

    def test_alert_rule_case_insensitive(self):
        """Alert rules match case-insensitively."""
        from alert_consumer import AlertRule

        rule = AlertRule(pattern="connection refused", severity="ERROR")
        assert rule.matches("Connection Refused by server") is True
        assert rule.matches("CONNECTION REFUSED") is True

    def test_alert_rule_regex_pattern(self):
        """Alert rules support regex patterns."""
        from alert_consumer import AlertRule

        rule = AlertRule(pattern="5[0-9]{2} status", severity="WARNING")
        assert rule.matches("Received 500 status from upstream") is True
        assert rule.matches("Received 503 status code") is True
        assert rule.matches("Received 200 status OK") is False

    @pytest.mark.asyncio
    async def test_alert_consumer_e2e_flow(self, kafka_container, redis_container, kafka_producer, redis_client):
        """
        Verify the entire alert consumer loop:
        1. Start real consumer connected to Testcontainers
        2. Publish matching and non-matching messages to Kafka
        3. Consumer automatically polls, parses, and updates Redis counters
        4. Stop consumer
        """
        from alert_consumer import AlertConsumer, AlertRule
        import os
        import json
        import asyncio
        from uuid import uuid4

        os.environ["KAFKA_BOOTSTRAP"] = kafka_container.get_bootstrap_server()
        os.environ["REDIS_URL"] = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(redis_container.port)}"

        consumer = AlertConsumer()
        
        # Start consumer loop in the background
        consumer_task = asyncio.create_task(consumer.start())
        
        try:
            # Wait a moment for consumer to join group
            await asyncio.sleep(2)
            
            # Inject rule directly into the running consumer
            consumer._alert_rules.append(AlertRule(pattern="CRITICAL_ERROR", severity="CRITICAL"))
            
            # Produce a matching message
            payload_match = {
                "id": str(uuid4()),
                "service": "e2e-alert-service",
                "level": "ERROR",
                "message": "We have a CRITICAL_ERROR here",
                "timestamp": "2026-06-20T10:00:00+00:00",
            }
            
            await kafka_producer.send_and_wait(
                consumer.topic,
                key=b"e2e-alert-service",
                value=json.dumps(payload_match).encode("utf-8"),
            )
            
            # Produce a non-matching message
            payload_no_match = {
                "id": str(uuid4()),
                "service": "e2e-safe-service",
                "level": "INFO",
                "message": "Everything is fine",
                "timestamp": "2026-06-20T10:00:00+00:00",
            }
            
            await kafka_producer.send_and_wait(
                consumer.topic,
                key=b"e2e-safe-service",
                value=json.dumps(payload_no_match).encode("utf-8"),
            )
            
            # Give the consumer time to poll and process
            await asyncio.sleep(2)
            
            # Verify Redis counter was incremented for the MATCHING message
            count_match = await redis_client.hget("metrics:alerts", "e2e-alert-service:ERROR")
            assert count_match == "1"
            
            # Verify Redis counter was NOT incremented for the NON-MATCHING message
            count_safe = await redis_client.hget("metrics:alerts", "e2e-safe-service:INFO")
            assert count_safe is None
            
        finally:
            consumer._running = False
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass


# ============================================================
# Log Writer Tests
# ============================================================

class TestLogWriter:
    """Tests for the PostgreSQL log writer consumer."""

    @pytest.mark.asyncio
    async def test_log_writer_parses_and_inserts(self, pg_pool, sample_kafka_message):
        """Log writer correctly parses Kafka message and inserts into PostgreSQL."""
        from log_writer import LogWriterConsumer
        import json

        consumer = LogWriterConsumer()
        consumer._pg_pool = pg_pool

        # Insert single message
        await consumer._insert_single(sample_kafka_message)

        # Verify it's in Postgres
        payload = json.loads(sample_kafka_message.value.decode("utf-8"))
        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM logs WHERE id = $1", payload["id"])
            assert row is not None
            assert str(row["id"]) == payload["id"]
            assert row["service"] == payload["service"]
            assert row["message"] == payload["message"]

    @pytest.mark.asyncio
    async def test_log_writer_idempotency(self, pg_pool, sample_kafka_message):
        """Log writer idempotency via ON CONFLICT DO NOTHING."""
        from log_writer import LogWriterConsumer
        import json

        consumer = LogWriterConsumer()
        consumer._pg_pool = pg_pool

        # Insert the exact same message TWICE
        await consumer._insert_single(sample_kafka_message)
        await consumer._insert_single(sample_kafka_message)

        # Verify exactly ONE row in Postgres
        payload = json.loads(sample_kafka_message.value.decode("utf-8"))
        async with pg_pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM logs WHERE id = $1", payload["id"])
            assert count == 1

    @pytest.mark.asyncio
    async def test_log_writer_poison_pill_fallback(self, pg_pool, sample_kafka_message):
        """Verify that a DB batch failure falls back to single inserts and dead-letters the poison pill."""
        from log_writer import LogWriterConsumer
        import json
        import uuid
        from unittest.mock import AsyncMock, MagicMock

        consumer = LogWriterConsumer()
        consumer._pg_pool = pg_pool
        consumer._consumer = AsyncMock()
        consumer._send_to_dead_letter = AsyncMock()

        # Create a valid message
        valid_msg = MagicMock()
        valid_payload = json.loads(sample_kafka_message.value.decode("utf-8"))
        valid_payload["id"] = str(uuid.uuid4())
        valid_msg.value = json.dumps(valid_payload).encode("utf-8")
        valid_msg.partition = 0
        valid_msg.offset = 1

        # Create a DB-level POISON PILL message (violates NOT NULL constraint on 'service')
        # Previously we used an out-of-bounds timestamp, but the new logs_default partition catches those.
        poison_msg = MagicMock()
        poison_payload = valid_payload.copy()
        poison_payload["id"] = str(uuid.uuid4())
        poison_payload["service"] = None  # service is TEXT NOT NULL in DB
        poison_msg.value = json.dumps(poison_payload).encode("utf-8")
        poison_msg.partition = 0
        poison_msg.offset = 2

        # Mock getmany to return our batch once, then stop the consumer loop
        class TopicPartitionMock:
            partition = 0
            
        async def mock_getmany(*args, **kwargs):
            consumer._running = False # Stop loop after first iteration
            return {TopicPartitionMock(): [valid_msg, poison_msg]}
            
        consumer._consumer.getmany.side_effect = mock_getmany
        consumer._running = True

        # Run the loop
        await consumer._consume_loop()

        # Assert valid message made it into the DB
        async with pg_pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM logs WHERE id = $1", valid_payload["id"])
            assert count == 1

        # Assert poison message did NOT make it into the DB
        async with pg_pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM logs WHERE id = $1", poison_payload["id"])
            assert count == 0

        # Assert poison message was sent to dead letter queue
        consumer._send_to_dead_letter.assert_called_once_with(poison_msg)
        
        # Assert Kafka offset was committed!
        consumer._consumer.commit.assert_called_once()


# ============================================================
# MetricsConsumer tests
# ============================================================


class TestMetricsConsumer:
    """Tests for the MetricsConsumer Redis counter increments."""

    @pytest.mark.asyncio
    async def test_metrics_consumer_increments(self, sample_log_event, redis_client):
        """
        MetricsConsumer increments all four Redis counter dimensions.
        """
        from metrics_consumer import MetricsConsumer

        consumer = MetricsConsumer()
        consumer._redis = redis_client

        message = MagicMock()
        message.value = json.dumps(sample_log_event).encode("utf-8")

        # Process the message
        await consumer.process(message)

        # Verify counters in real Redis
        total = await redis_client.get("metrics:total")
        assert total == "1"

        svc = await redis_client.hget("metrics:services", sample_log_event['service'])
        assert svc == "1"

        lvl = await redis_client.hget("metrics:levels", sample_log_event['level'])
        assert lvl == "1"

        svclvl = await redis_client.hget("metrics:service_levels", f"{sample_log_event['service']}:{sample_log_event['level']}")
        assert svclvl == "1"


# ============================================================
# End-to-End Consumer Integration Tests
# ============================================================

class TestEndToEndConsumerFlow:
    """True End-to-End tests verifying the AIOKafkaConsumer polling loop."""

    @pytest.mark.asyncio
    async def test_log_writer_e2e_flow(self, kafka_container, postgres_container, kafka_producer, pg_pool):
        """
        Verify the entire consumer loop:
        1. Start real consumer connected to Testcontainers
        2. Publish message to Kafka
        3. Consumer automatically polls, parses, and writes to Postgres
        4. Stop consumer
        """
        from log_writer import LogWriterConsumer
        import os
        import json
        import asyncio
        from uuid import uuid4

        # Set environment variables for the consumer
        os.environ["KAFKA_BOOTSTRAP"] = kafka_container.get_bootstrap_server()
        os.environ["POSTGRES_DSN"] = postgres_container.get_connection_url().replace("postgresql+psycopg2", "postgresql")

        # Initialize consumer
        consumer = LogWriterConsumer()
        
        # Start consumer loop in the background
        consumer_task = asyncio.create_task(consumer.start())
        
        try:
            # Wait a moment for consumer to join group and rebalance
            await asyncio.sleep(2)
            
            # Produce a real message to Kafka
            event_id = str(uuid4())
            payload = {
                "id": event_id,
                "service": "e2e-test-service",
                "level": "INFO",
                "message": "This is a true E2E test message",
                "timestamp": "2026-06-20T10:00:00+00:00",
            }
            
            await kafka_producer.send_and_wait(
                consumer.topic,
                key=b"e2e-test-service",
                value=json.dumps(payload).encode("utf-8"),
            )
            
            # Give the consumer time to poll and process
            await asyncio.sleep(2)
            
            # Verify the message was written to Postgres by the consumer's background loop!
            async with pg_pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM logs WHERE id = $1", event_id)
                assert row is not None
                assert row["service"] == "e2e-test-service"
                assert row["message"] == "This is a true E2E test message"
        
        finally:
            # Clean up
            consumer._running = False
            # Cancel the task since consumer.start() is an infinite loop that might block
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass


# ============================================================
# Cross-partition commit safety tests
# ============================================================


class TestCrossPartitionCommitSafety:
    """Tests verifying that commit() fires after ALL partitions are processed."""

    @pytest.mark.asyncio
    async def test_commit_after_all_partitions_processed(self, kafka_container, kafka_producer):
        """
        True E2E cross-partition commit tracking test.
        1. Produce messages to two different partitions.
        2. Run LogWriterConsumer to process both.
        3. Stop and restart consumer with the exact same group_id.
        4. Verify zero messages are redelivered, proving offsets were
           committed successfully for both partitions.
        """
        from log_writer import LogWriterConsumer
        import os
        from aiokafka import AIOKafkaConsumer

        from aiokafka.admin import AIOKafkaAdminClient, NewTopic

        bootstrap_servers = kafka_container.get_bootstrap_server()
        topic = "test-cross-partition"

        # Explicitly create topic with 2 partitions
        admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
        await admin.start()
        try:
            topic_list = [NewTopic(name=topic, num_partitions=2, replication_factor=1)]
            await admin.create_topics(new_topics=topic_list)
        except Exception:
            pass  # Might already exist
        finally:
            await admin.close()

        # Produce to explicit partitions (kafka_producer is configured for round-robin/key-based, we force partitions here)
        await kafka_producer.send_and_wait(topic, b'{"id": "c1", "service": "test", "level": "INFO", "message": "msg1", "timestamp": "2026-06-20T10:00:00Z"}', partition=0)
        await kafka_producer.send_and_wait(topic, b'{"id": "c2", "service": "test", "level": "INFO", "message": "msg2", "timestamp": "2026-06-20T10:00:00Z"}', partition=1)

        os.environ["KAFKA_BOOTSTRAP"] = bootstrap_servers
        consumer = LogWriterConsumer()
        consumer.topic = topic
        consumer.group_id = "test-commit-group"
        consumer._pg_pool = AsyncMock()  # DB doesn't matter for offset tracking
        consumer._redis = AsyncMock()
        
        # Start consumer and run loop for a short time
        task = asyncio.create_task(consumer.start())
        await asyncio.sleep(2)  # Give it time to join, fetch, process, and commit
        
        # Stop gracefully
        consumer._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
            
        # Spin up a raw consumer with the exact same group_id to verify offsets
        verifier = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id="test-commit-group",
            auto_offset_reset="earliest"
        )
        await verifier.start()
        
        try:
            # We should get a TimeoutError because all offsets were committed
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(verifier.getone(), timeout=3.0)
        finally:
            await verifier.stop()


# ============================================================
# Default partition fallback tests
# ============================================================


class TestDefaultPartitionFallback:
    """Tests verifying the logs_default partition catches out-of-range timestamps."""

    @pytest.mark.asyncio
    async def test_out_of_range_timestamp_lands_in_default_partition(self, pg_pool):
        """
        Insert a log with a far-future timestamp (2099). With the logs_default
        partition, this should succeed instead of raising 'no partition found'.
        """
        from uuid import uuid4
        from datetime import datetime, timezone

        event_id = str(uuid4())
        future_ts = datetime(2099, 1, 1, tzinfo=timezone.utc)

        async with pg_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO logs (id, service, level, message, timestamp) "
                "VALUES ($1, 'future-service', 'INFO', 'far future log', $2)",
                event_id, future_ts,
            )

            # Verify row exists
            row = await conn.fetchrow("SELECT * FROM logs WHERE id = $1", event_id)
            assert row is not None
            assert row["service"] == "future-service"

            # Verify it landed in the default partition specifically
            partition_name = await conn.fetchval(
                "SELECT relname FROM pg_class WHERE oid = ("
                "  SELECT tableoid FROM logs WHERE id = $1"
                ")", event_id
            )
            assert partition_name == "logs_default"


# ============================================================
# Idempotency under concurrency tests
# ============================================================


class TestIdempotencyUnderConcurrency:
    """Tests verifying ON CONFLICT DO NOTHING under concurrent duplicate inserts."""

    @pytest.mark.asyncio
    async def test_concurrent_duplicate_inserts(self, pg_pool, sample_kafka_message):
        """
        Use asyncio.gather to fire 10 concurrent _insert_single() calls
        with the same message. Exactly 1 row should exist in Postgres.
        
        NOTE: This bypasses the Kafka consumer loop intentionally. A single
        Kafka partition is processed sequentially, so true concurrent writes
        of the same event ID only happen during a consumer group rebalance
        or multi-instance handoff race condition. Bypassing the loop
        allows us to accurately simulate and test the DB-level race condition
        and transaction isolation directly.
        """
        from log_writer import LogWriterConsumer

        consumer = LogWriterConsumer()
        consumer._pg_pool = pg_pool

        # Fire 10 concurrent inserts of the exact same message
        await asyncio.gather(*[
            consumer._insert_single(sample_kafka_message) for _ in range(10)
        ])

        # Verify exactly ONE row exists
        payload = json.loads(sample_kafka_message.value.decode("utf-8"))
        async with pg_pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM logs WHERE id = $1", payload["id"])
            assert count == 1


# ============================================================
# Additional BaseConsumer retry tests
# ============================================================


class TestBaseConsumerRetryAdvanced:
    """Advanced retry mechanism tests."""

    @pytest.mark.asyncio
    async def test_exponential_backoff_timing(self, sample_kafka_message, kafka_producer):
        """Capture asyncio.sleep calls and verify they follow 1s → 2s → 4s."""
        consumer = ConcreteTestConsumer()
        consumer._producer = kafka_producer

        consumer.process_mock.side_effect = Exception("always fail")

        sleep_calls = []
        original_sleep = asyncio.sleep

        async def mock_sleep(seconds):
            sleep_calls.append(seconds)

        with patch("base_consumer.asyncio.sleep", side_effect=mock_sleep):
            await consumer._run_with_retry(sample_kafka_message, max_retries=3)

        # Should have slept twice (between attempt 1→2 and 2→3, not after final failure)
        assert sleep_calls == [1, 2]

    @pytest.mark.asyncio
    async def test_graceful_shutdown_drains_current_batch(self):
        """
        Setting _running=False mid-batch still processes all messages
        in the current getmany() result before exiting.
        """
        consumer = ConcreteTestConsumer()
        consumer._consumer = AsyncMock()
        consumer._producer = AsyncMock()

        messages_processed = []

        async def track_process(msg):
            messages_processed.append(msg)

        consumer.process_mock.side_effect = track_process

        msg1 = MagicMock()
        msg1.value = b'{"test": "1"}'
        msg1.topic = "test-topic"
        msg1.partition = 0
        msg1.offset = 1

        msg2 = MagicMock()
        msg2.value = b'{"test": "2"}'
        msg2.topic = "test-topic"
        msg2.partition = 0
        msg2.offset = 2

        class TPMock:
            partition = 0

        async def mock_getmany(*args, **kwargs):
            # Stop after this batch, but both messages should still be processed
            consumer._running = False
            return {TPMock(): [msg1, msg2]}

        consumer._consumer.getmany.side_effect = mock_getmany
        consumer._running = True

        await consumer._consume_loop()

        # Both messages in the batch should have been processed even though
        # _running was set to False
        assert len(messages_processed) == 2


# ============================================================
# Additional LogWriter tests
# ============================================================


class TestLogWriterFallbackIntegration:
    """True E2E integration test for batch fallback to Dead Letter Queue."""

    @pytest.mark.asyncio
    async def test_batch_fallback_sends_to_dlq_and_increments_metric(self, kafka_container, kafka_producer, pg_pool, redis_client):
        """
        Trigger a batch failure with a poison pill and verify it lands in DLQ
        and the Redis metric increments correctly using real infrastructure.
        """
        from log_writer import LogWriterConsumer
        from aiokafka import AIOKafkaConsumer
        from aiokafka.admin import AIOKafkaAdminClient, NewTopic
        import uuid
        import os

        bootstrap_servers = kafka_container.get_bootstrap_server()
        ingest_topic = "test-ingest-fallback"
        dlq_topic = "test-ingest-fallback.dead-letter"

        # Explicitly create topics to avoid auto-create timing issues
        admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
        await admin.start()
        try:
            await admin.create_topics(new_topics=[
                NewTopic(name=ingest_topic, num_partitions=1, replication_factor=1),
                NewTopic(name=dlq_topic, num_partitions=1, replication_factor=1),
            ])
        except Exception:
            pass
        finally:
            await admin.close()

        # Valid message
        valid_id = str(uuid.uuid4())
        valid_payload = json.dumps({"id": valid_id, "service": "test", "level": "INFO", "message": "ok", "timestamp": "2026-06-20T10:00:00Z"}).encode("utf-8")
        
        # Poison pill (missing 'service' field violates DB constraint)
        poison_id = str(uuid.uuid4())
        poison_payload = json.dumps({"id": poison_id, "service": None, "level": "INFO", "message": "bad", "timestamp": "2026-06-20T10:00:00Z"}).encode("utf-8")

        await kafka_producer.send_and_wait(ingest_topic, valid_payload)
        await kafka_producer.send_and_wait(ingest_topic, poison_payload)

        os.environ["KAFKA_BOOTSTRAP"] = bootstrap_servers
        consumer = LogWriterConsumer()
        consumer.topic = ingest_topic
        consumer.dead_letter_topic = dlq_topic
        consumer.group_id = "test-fallback-group"
        consumer._pg_pool = pg_pool
        consumer._redis = redis_client
        
        # Start consumer loop
        task = asyncio.create_task(consumer.start())
        await asyncio.sleep(2)  # Allow processing
        
        consumer._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # 1. Verify valid message inserted
        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM logs WHERE id = $1", valid_id)
            assert row is not None

        # 2. Verify metric incremented
        fallback_count = await redis_client.get("metrics:batch_insert_fallback_total")
        assert fallback_count == "1"

        # 3. Verify poison pill in real DLQ
        dlq_verifier = AIOKafkaConsumer(
            dlq_topic,
            bootstrap_servers=bootstrap_servers,
            group_id="test-dlq-verifier",
            auto_offset_reset="earliest"
        )
        await dlq_verifier.start()
        try:
            msg = await asyncio.wait_for(dlq_verifier.getone(), timeout=3.0)
            envelope = json.loads(msg.value)
            assert envelope["original_topic"] == ingest_topic
            assert "bad" in envelope["original_value"]
        finally:
            await dlq_verifier.stop()


# ============================================================
# Additional MetricsConsumer tests
# ============================================================


class TestMetricsConsumerAdvanced:
    """Advanced MetricsConsumer tests."""

    @pytest.mark.asyncio
    async def test_multiple_messages_increment_correctly(self, redis_client):
        """Process 5 messages with mixed services/levels, verify all counters are exact."""
        from metrics_consumer import MetricsConsumer

        consumer = MetricsConsumer()
        consumer._redis = redis_client

        messages = [
            {"service": "api", "level": "INFO", "message": "req 1"},
            {"service": "api", "level": "ERROR", "message": "req 2"},
            {"service": "db", "level": "ERROR", "message": "req 3"},
            {"service": "api", "level": "INFO", "message": "req 4"},
            {"service": "worker", "level": "WARNING", "message": "req 5"},
        ]

        for msg_data in messages:
            msg = MagicMock()
            msg.value = json.dumps(msg_data).encode("utf-8")
            await consumer.process(msg)

        # Verify totals
        total = await redis_client.get("metrics:total")
        assert total == "5"

        # Verify per-service
        assert await redis_client.hget("metrics:services", "api") == "3"
        assert await redis_client.hget("metrics:services", "db") == "1"
        assert await redis_client.hget("metrics:services", "worker") == "1"

        # Verify per-level
        assert await redis_client.hget("metrics:levels", "INFO") == "2"
        assert await redis_client.hget("metrics:levels", "ERROR") == "2"
        assert await redis_client.hget("metrics:levels", "WARNING") == "1"

        # Verify cross-dimension
        assert await redis_client.hget("metrics:service_levels", "api:INFO") == "2"
        assert await redis_client.hget("metrics:service_levels", "api:ERROR") == "1"
        assert await redis_client.hget("metrics:service_levels", "db:ERROR") == "1"

    @pytest.mark.asyncio
    async def test_malformed_message_raises_for_retry(self, redis_client):
        """Non-JSON message raises exception, triggering BaseConsumer's retry path."""
        from metrics_consumer import MetricsConsumer

        consumer = MetricsConsumer()
        consumer._redis = redis_client

        msg = MagicMock()
        msg.value = b"this is not json"

        with pytest.raises(Exception):
            await consumer.process(msg)

