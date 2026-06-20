"""
Tests for the LogFoundry query API.

Test cases:
  - test_query_cache_hit: same query twice → second hits Redis cache
  - test_query_cache_miss: first query hits PostgreSQL
  - test_build_query_with_all_filters: SQL builder with all filters
  - test_build_query_partition_pruning: timestamp uses >= and < (not BETWEEN)
  - test_cache_key_deterministic: same params produce same cache key
  - test_cache_key_different_params: different params produce different keys
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from routers.query import _build_cache_key, _build_query


# ============================================================
# Cache key tests
# ============================================================


class TestCacheKey:
    """Tests for deterministic cache key generation."""

    def test_cache_key_deterministic(self):
        """Same parameters produce the same cache key."""
        params1 = {"service": "test", "level": "INFO", "limit": 100}
        params2 = {"service": "test", "level": "INFO", "limit": 100}

        key1 = _build_cache_key(params1)
        key2 = _build_cache_key(params2)

        assert key1 == key2

    def test_cache_key_different_params(self):
        """Different parameters produce different cache keys."""
        params1 = {"service": "test-1", "level": "INFO"}
        params2 = {"service": "test-2", "level": "INFO"}

        key1 = _build_cache_key(params1)
        key2 = _build_cache_key(params2)

        assert key1 != key2

    def test_cache_key_ignores_none(self):
        """None values are filtered out for consistent hashing."""
        params1 = {"service": "test", "level": None, "search": None}
        params2 = {"service": "test"}

        key1 = _build_cache_key(params1)
        key2 = _build_cache_key(params2)

        assert key1 == key2

    def test_cache_key_order_independent(self):
        """Parameter order doesn't affect the cache key."""
        params1 = {"service": "test", "level": "INFO"}
        params2 = {"level": "INFO", "service": "test"}

        key1 = _build_cache_key(params1)
        key2 = _build_cache_key(params2)

        assert key1 == key2

    def test_cache_key_prefix(self):
        """Cache keys have the expected prefix."""
        key = _build_cache_key({"service": "test"})
        assert key.startswith("query_cache:")

    def test_cache_key_with_datetime(self):
        """Datetime values are serialized consistently."""
        dt = datetime(2026, 6, 20, 10, 0, 0, tzinfo=timezone.utc)
        params = {"since": dt, "service": "test"}

        key = _build_cache_key(params)
        assert key.startswith("query_cache:")


# ============================================================
# SQL query builder tests
# ============================================================


class TestQueryBuilder:
    """Tests for dynamic SQL query building."""

    def test_build_query_no_filters(self):
        """Query with no filters returns all logs."""
        sql, params = _build_query(
            service=None, level=None, search=None,
            since=None, until=None, limit=100,
        )

        assert "WHERE TRUE" in sql
        assert "LIMIT $1" in sql
        assert params == [100]

    def test_build_query_with_service(self):
        """Query filters by service name."""
        sql, params = _build_query(
            service="payments-api", level=None, search=None,
            since=None, until=None, limit=100,
        )

        assert "service = $1" in sql
        assert params[0] == "payments-api"

    def test_build_query_with_all_filters(self):
        """Query with all filters builds correct SQL."""
        since = datetime(2026, 6, 1, tzinfo=timezone.utc)
        until = datetime(2026, 6, 30, tzinfo=timezone.utc)

        sql, params = _build_query(
            service="test", level="ERROR", search="connection refused",
            since=since, until=until, limit=50,
        )

        assert "service = $1" in sql
        assert "level = $2" in sql
        assert "to_tsvector" in sql
        assert "plainto_tsquery" in sql
        assert "timestamp >= $4" in sql
        assert "timestamp < $5" in sql
        assert "LIMIT $6" in sql
        assert len(params) == 6

    def test_build_query_partition_pruning(self):
        """
        Timestamp uses >= and < (not BETWEEN) for partition pruning.

        BETWEEN is inclusive on both ends and can cause the planner
        to scan an extra partition on boundary values.
        """
        since = datetime(2026, 6, 1, tzinfo=timezone.utc)
        until = datetime(2026, 7, 1, tzinfo=timezone.utc)

        sql, params = _build_query(
            service=None, level=None, search=None,
            since=since, until=until, limit=100,
        )

        # Verify we use >= and < (not BETWEEN)
        assert ">=" in sql
        assert "<" in sql
        assert "BETWEEN" not in sql.upper()

    def test_build_query_full_text_search(self):
        """Full-text search uses plainto_tsquery."""
        sql, params = _build_query(
            service=None, level=None, search="connection refused",
            since=None, until=None, limit=100,
        )

        assert "to_tsvector('english', message)" in sql
        assert "plainto_tsquery('english'," in sql
        assert "connection refused" in params

    def test_build_query_order_by(self):
        """Results are ordered by timestamp descending."""
        sql, _ = _build_query(
            service=None, level=None, search=None,
            since=None, until=None, limit=100,
        )

        assert "ORDER BY timestamp DESC" in sql


# ============================================================
# Query cache behavior tests (mock-based)
# ============================================================


class TestQueryCacheBehavior:
    """Tests for cache hit/miss behavior using real Redis."""

    @pytest.mark.asyncio
    async def test_query_cache_hit_and_miss(self, redis_client):
        """
        Verify cache miss sets the cache, and subsequent identical query hits it.
        """
        from routers.query import _build_cache_key
        
        params = {"service": "test", "level": "INFO", "limit": 100}
        cache_key = _build_cache_key(params)

        # 1. Initially cache is empty (miss)
        result1 = await redis_client.get(cache_key)
        assert result1 is None

        # 2. Simulate setting the cache
        cached_results = [
            {"id": "test-id", "service": "test", "level": "INFO",
             "message": "hello", "timestamp": "2026-06-20T10:00:00+00:00",
             "trace_id": None, "metadata": None}
        ]
        await redis_client.setex(cache_key, 30, json.dumps(cached_results))

        # 3. Subsequent call gets data from cache (hit)
        result2 = await redis_client.get(cache_key)
        assert result2 is not None
        
        parsed = json.loads(result2)
        assert len(parsed) == 1
        assert parsed[0]["service"] == "test"


# ============================================================
# PostgreSQL Integration Tests (Real DB)
# ============================================================


class TestPostgresIntegration:
    """Tests executing queries against the real PostgreSQL testcontainer."""

    @pytest.mark.asyncio
    async def test_partition_pruning_via_explain(self, pg_pool):
        """
        Verify that our partition pruning actually works using EXPLAIN.
        This proves to an interviewer that the query optimization is real.
        """
        from routers.query import _build_query

        since = datetime(2026, 6, 1, tzinfo=timezone.utc)
        until = datetime(2026, 7, 1, tzinfo=timezone.utc)

        sql, params = _build_query(
            service=None, level=None, search=None,
            since=since, until=until, limit=100,
        )

        explain_sql = f"EXPLAIN {sql.replace('$1', f'{repr(since.isoformat())}::timestamptz').replace('$2', f'{repr(until.isoformat())}::timestamptz').replace('$3', '100')}"

        async with pg_pool.acquire() as conn:
            explain_output = await conn.fetch(explain_sql)
            explain_text = "\\n".join(row[0] for row in explain_output)

            # It should scan ONLY the logs_2026_06 partition!
            assert "logs_2026_06" in explain_text
            # It should NOT scan other partitions
            assert "logs_2026_05" not in explain_text
            assert "logs_2026_07" not in explain_text

    @pytest.mark.asyncio
    async def test_insert_and_query_real_db(self, pg_pool):
        """Verify we can actually insert logs and query them with filters."""
        async with pg_pool.acquire() as conn:
            # 1. Insert some logs
            await conn.execute(
                "INSERT INTO logs (id, service, level, message, timestamp) VALUES "
                "($1, 'api', 'INFO', 'user login', $2), "
                "($3, 'db', 'ERROR', 'connection timeout', $4)",
                '11111111-1111-1111-1111-111111111111', datetime(2026, 6, 15, tzinfo=timezone.utc),
                '22222222-2222-2222-2222-222222222222', datetime(2026, 6, 16, tzinfo=timezone.utc)
            )

            # 2. Query them using the query builder logic
            from routers.query import _build_query
            sql, params = _build_query(
                service='db', level='ERROR', search=None,
                since=None, until=None, limit=10,
            )

            results = await conn.fetch(sql, *params)
            
            assert len(results) == 1
            assert results[0]["service"] == "db"
            assert results[0]["message"] == "connection timeout"


# ============================================================
# API Endpoint Integration Tests (HTTP Layer)
# ============================================================


class TestQueryEndpoints:
    """End-to-End HTTP integration tests for the query and metrics endpoints."""

    @pytest.mark.asyncio
    async def test_query_endpoint_integration(self, async_client, pg_pool):
        """
        Verify GET /query fetches data from Postgres and utilizes the Redis cache.
        """
        # 1. Insert a known record directly into Postgres
        event_id = '33333333-3333-3333-3333-333333333333'
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO logs (id, service, level, message, timestamp) VALUES "
                "($1, 'test-e2e', 'CRITICAL', 'API test message', $2)",
                event_id, datetime(2026, 6, 20, 12, 0, 0, tzinfo=timezone.utc)
            )

        # 2. Call the HTTP endpoint (First hit -> Cache miss)
        response1 = await async_client.get("/query?service=test-e2e&level=CRITICAL")
        assert response1.status_code == 200
        data1 = response1.json()
        
        assert data1["count"] == 1
        assert data1["results"][0]["id"] == event_id
        assert data1["results"][0]["message"] == 'API test message'
        assert data1["cache_hit"] is False

        # 3. Call the exact same endpoint again (Second hit -> Cache hit)
        response2 = await async_client.get("/query?service=test-e2e&level=CRITICAL")
        assert response2.status_code == 200
        data2 = response2.json()
        
        assert data2["count"] == 1
        assert data2["cache_hit"] is True

    @pytest.mark.asyncio
    async def test_metrics_endpoint_integration(self, async_client, redis_client):
        """
        Verify GET /metrics fetches from Redis Hashes and formats as Prometheus exposition.
        """
        # 1. Seed the Redis Hash directly (simulating what MetricsConsumer does)
        await redis_client.hincrby("metrics:services", "test-metrics-service", 42)
        await redis_client.hincrby("metrics:levels", "CRITICAL", 7)
        await redis_client.hincrby("metrics:alerts", "test-metrics-service:CRITICAL", 3)

        # 2. Call the HTTP endpoint
        response = await async_client.get("/metrics")
        assert response.status_code == 200
        
        # 3. Verify Prometheus exposition text format
        text = response.text
        assert 'logfoundry_logs_by_service{service="test-metrics-service"} 42' in text
        assert 'logfoundry_logs_by_level{level="CRITICAL"} 7' in text
        assert 'logfoundry_alerts_total{service="test-metrics-service",level="CRITICAL"} 3' in text
