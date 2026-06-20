"""
Test fixtures for LogFoundry test suite.

Uses testcontainers for real Kafka, Redis, and PostgreSQL instances in tests.
This ensures tests run against actual infrastructure, not mocks,
which catches integration bugs that unit tests miss.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from uuid import uuid4
from unittest.mock import MagicMock

import pytest
import asyncpg
from redis.asyncio import Redis
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer
from testcontainers.kafka import KafkaContainer

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "api"))
sys.path.insert(0, str(Path(__file__).parent.parent / "consumers"))
sys.path.insert(0, str(Path(__file__).parent.parent / "sdk"))


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def sample_log_event():
    """Create a sample log event dict for testing."""
    return {
        "id": str(uuid4()),
        "service": "test-service",
        "level": "INFO",
        "message": "Test log message",
        "timestamp": "2026-06-20T10:00:00+00:00",
        "trace_id": "trace-123",
        "metadata": {"key": "value"},
    }


@pytest.fixture
def sample_error_event():
    """Create a sample error log event that should trigger alerts."""
    return {
        "id": str(uuid4()),
        "service": "payments-api",
        "level": "ERROR",
        "message": "OutOfMemoryError: Java heap space exhausted",
        "timestamp": "2026-06-20T10:00:00+00:00",
        "trace_id": "trace-456",
        "metadata": {"host": "worker-1"},
    }


@pytest.fixture
def sample_kafka_message(sample_log_event):
    """Create a mock Kafka message containing the sample log event."""
    message = MagicMock()
    message.value = json.dumps(sample_log_event).encode("utf-8")
    message.topic = "logs.ingest"
    message.partition = 0
    message.offset = 123
    return message


# ============================================================
# Testcontainers Fixtures
# ============================================================

@pytest.fixture(scope="session")
def postgres_container():
    """Spin up a real PostgreSQL container for tests."""
    with PostgresContainer("postgres:16-alpine", dbname="logfoundry") as postgres:
        # Initialize schema
        import psycopg2
        conn = psycopg2.connect(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(postgres.port),
            user=postgres.username,
            password=postgres.password,
            dbname=postgres.dbname
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            schema_path = Path(__file__).parent.parent / "db" / "schema.sql"
            with open(schema_path, "r") as f:
                cur.execute(f.read())
        conn.close()
        yield postgres


@pytest.fixture(scope="session")
def redis_container():
    """Spin up a real Redis container for tests."""
    with RedisContainer("redis:7-alpine") as redis:
        yield redis


@pytest.fixture(scope="session")
def kafka_container():
    """Spin up a real Kafka container for tests."""
    with KafkaContainer("confluentinc/cp-kafka:7.5.0") as kafka:
        yield kafka


import pytest_asyncio

# ============================================================
# Async Connection Fixtures
# ============================================================

@pytest_asyncio.fixture
async def pg_pool(postgres_container):
    """Provide an asyncpg connection pool."""
    dsn = postgres_container.get_connection_url().replace("postgresql+psycopg2", "postgresql")
    pool = await asyncpg.create_pool(dsn)
    
    # Truncate tables before each test for isolation
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE logs")
    
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def redis_client(redis_container):
    """Provide an async Redis client."""
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=redis_container.get_exposed_port(redis_container.port),
        decode_responses=True
    )
    await client.flushdb()
    yield client
    await client.close()


@pytest_asyncio.fixture
async def kafka_producer(kafka_container):
    """Provide an AIOKafkaProducer."""
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_container.get_bootstrap_server()
    )
    await producer.start()
    yield producer
    await producer.stop()


@pytest_asyncio.fixture
async def async_client(redis_container, postgres_container, kafka_container):
    """Provide an httpx.AsyncClient wired to the FastAPI application."""
    os.environ["REDIS_URL"] = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(redis_container.port)}"
    os.environ["POSTGRES_DSN"] = postgres_container.get_connection_url().replace("postgresql+psycopg2", "postgresql")
    os.environ["KAFKA_BOOTSTRAP"] = kafka_container.get_bootstrap_server()
    os.environ["OTEL_SDK_DISABLED"] = "true"
    
    # Import app AFTER setting environment variables so the lifespan picks them up
    from main import app
    from httpx import AsyncClient, ASGITransport
    
    transport = ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client
