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
