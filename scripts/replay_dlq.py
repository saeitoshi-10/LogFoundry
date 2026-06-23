#!/usr/bin/env python3
"""
Dead-Letter Queue Replay Script

Consumes messages from `logs.dead-letter`, attempts to parse them,
and re-publishes valid messages to `logs.ingest` for reprocessing.
"""

import argparse
import asyncio
import json
import logging
import sys
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# Ensure api directory is in python path to import models
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'api')))
from models import LogEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("replay_dlq")


async def replay_dlq(bootstrap_servers: str, dlq_topic: str, ingest_topic: str, limit: int):
    consumer = AIOKafkaConsumer(
        dlq_topic,
        bootstrap_servers=bootstrap_servers,
        group_id="logfoundry-dlq-replayer",
        auto_offset_reset="earliest",
    )
    
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
    )
    
    await consumer.start()
    await producer.start()
    
    logger.info(f"Connected to Kafka. Replaying up to {limit} messages from {dlq_topic} to {ingest_topic}...")
    
    replayed = 0
    try:
        # We use a timeout to prevent blocking forever if the DLQ is empty
        while replayed < limit:
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.info("No more messages in DLQ (timeout reached).")
                break
                
            payload = msg.value
            
            try:
                # Extract original value from the dead-letter envelope
                wrapper = json.loads(payload)
                original_value = wrapper.get("original_value")
                
                if not original_value:
                    logger.error(f"Message lacks original_value, cannot replay: {payload[:100]}...")
                    await consumer.commit()
                    continue
                
                # Schema validation: ensure original_value is a valid LogEvent
                try:
                    LogEvent.model_validate_json(original_value)
                except Exception as e:
                    logger.error(f"Message is not a valid LogEvent, cannot replay. Skipping: {e}")
                    await consumer.commit()
                    continue
                
                # Re-publish to ingest
                publish_payload = original_value.encode("utf-8") if isinstance(original_value, str) else original_value
                await producer.send_and_wait(ingest_topic, publish_payload)
                replayed += 1
                
                # Commit offset after successful re-publish
                await consumer.commit()
                if replayed % 100 == 0:
                    logger.info(f"Replayed {replayed} messages...")
            except json.JSONDecodeError:
                logger.error(f"Message is not valid JSON, cannot replay. Skipping: {payload[:100]}...")
                await consumer.commit() # Skip unrecoverable garbage
                
    finally:
        logger.info(f"Finished replaying {replayed} messages.")
        await producer.stop()
        await consumer.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay dead-lettered messages in LogFoundry.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--dlq-topic", default="logs.dead-letter", help="Topic to read from")
    parser.add_argument("--ingest-topic", default="logs.ingest", help="Topic to publish to")
    parser.add_argument("--limit", type=int, default=1000, help="Max number of messages to replay")
    
    args = parser.parse_args()
    asyncio.run(replay_dlq(args.bootstrap_servers, args.dlq_topic, args.ingest_topic, args.limit))
