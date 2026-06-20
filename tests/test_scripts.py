import asyncio
import json
from datetime import datetime

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from scripts.manage_partitions import add_months, manage_partitions
from scripts.replay_dlq import replay_dlq


@pytest.mark.asyncio
async def test_manage_partitions_creates_and_drops(pg_pool, postgres_container):
    """Verify manage_partitions creates future partitions and drops old ones."""
    # Insert a dummy partition that is very old to simulate an expired one
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS logs_2020_01 PARTITION OF logs FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');"
        )
        
    dsn = postgres_container.get_connection_url().replace("postgresql+psycopg2", "postgresql")
    
    # Run the script: create 2 future months, retain 2 past months
    await manage_partitions(dsn, create_months=2, retain_months=2)
    
    now = datetime.now()
    next_month = add_months(now, 1)
    
    expected_partition = f"logs_{next_month.strftime('%Y_%m')}"
    
    async with pg_pool.acquire() as conn:
        # Check if the new partition exists
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = $1);",
            expected_partition
        )
        assert exists is True, f"Expected partition {expected_partition} was not created"
        
        # Check if the old partition was dropped
        old_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'logs_2020_01');"
        )
        assert old_exists is False, "Expired partition logs_2020_01 was not dropped"


@pytest.mark.asyncio
async def test_replay_dlq_moves_messages(kafka_container, kafka_producer):
    """Verify replay_dlq consumes from DLQ and publishes to ingest topic."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    dlq_topic = "test_logs.dead-letter"
    ingest_topic = "test_logs.ingest"
    
    # Publish a valid JSON message and an invalid garbage message to DLQ
    valid_payload = b'{"service": "test", "message": "hello"}'
    invalid_payload = b'not_json_garbage'
    
    await kafka_producer.send_and_wait(dlq_topic, valid_payload)
    await kafka_producer.send_and_wait(dlq_topic, invalid_payload)
    
    # Setup a consumer on the ingest topic to verify the replay
    consumer = AIOKafkaConsumer(
        ingest_topic,
        bootstrap_servers=bootstrap_servers,
        group_id="test_replayer_verifier",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    
    # Run the replay script
    # It should process the DLQ, skip the garbage, and publish the valid one
    await replay_dlq(bootstrap_servers, dlq_topic, ingest_topic, limit=10)
    
    try:
        # The consumer should receive exactly 1 message (the valid one)
        msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)
        assert msg.value == valid_payload
        
        # Try to get another, should timeout since the garbage was skipped
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(consumer.getone(), timeout=2.0)
    finally:
        await consumer.stop()
