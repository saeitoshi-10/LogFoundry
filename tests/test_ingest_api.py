"""
Tests for the LogFoundry ingest API.

Test cases:
  - test_ingest_validates_payload_size: 8KB+ payload returns 422
  - test_rate_limiter_blocks_after_limit: 101st request in window returns 429
  - test_rate_limiter_allows_after_window: requests allowed after window expires
  - test_ingest_returns_202: valid event returns 202 Accepted
  - test_ingest_batch: batch endpoint accepts multiple events
  - test_ingest_invalid_level: invalid log level returns 422
"""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from models import LogEvent
from middleware.rate_limiter import RateLimiter


# ============================================================
# LogEvent model validation tests
# ============================================================


class TestLogEventValidation:
    """Tests for Pydantic model validation."""

    def test_valid_event(self):
        """Test that a valid event is accepted."""
        event = LogEvent(
            service="test-service",
            level="INFO",
            message="Hello, world!",
        )
        assert event.service == "test-service"
        assert event.level == "INFO"
        assert event.id is not None

    def test_ingest_validates_payload_size(self):
        """
        8KB+ payload returns 422.

        The message field has an 8KB byte limit enforced by the field_validator.
        This test verifies that messages exceeding 8192 bytes are rejected.
        """
        # Create a message that exceeds 8KB when encoded to UTF-8
        oversized_message = "x" * 8193

        with pytest.raises(Exception) as exc_info:
            LogEvent(
                service="test-service",
                level="INFO",
                message=oversized_message,
            )
        assert "8KB" in str(exc_info.value) or "8192" in str(exc_info.value) or "max_length" in str(exc_info.value)

    def test_payload_at_limit_accepted(self):
        """Test that a message exactly at 8KB is accepted."""
        max_message = "x" * 8192
        event = LogEvent(
            service="test-service",
            level="INFO",
            message=max_message,
        )
        assert len(event.message) == 8192

    def test_ingest_invalid_level(self):
        """Invalid log level returns validation error."""
        with pytest.raises(Exception):
            LogEvent(
                service="test-service",
                level="INVALID",
                message="test",
            )

    def test_empty_service_rejected(self):
        """Empty service name is rejected."""
        with pytest.raises(Exception):
            LogEvent(
                service="",
                level="INFO",
                message="test",
            )

    def test_metadata_dict(self):
        """Metadata as a dict is accepted."""
        event = LogEvent(
            service="test",
            level="INFO",
            message="test",
            metadata={"key": "value", "count": 42},
        )
        assert event.metadata == {"key": "value", "count": 42}


# ============================================================
# LogEvent serialization tests
# ============================================================


class TestLogEventSerialization:
    """Tests for Kafka serialization/deserialization."""

    def test_to_kafka_bytes(self):
        """Test serialization to JSON bytes."""
        event = LogEvent(
            service="test",
            level="INFO",
            message="hello",
        )
        data = event.to_kafka_bytes()
        assert isinstance(data, bytes)

        # Verify roundtrip
        parsed = json.loads(data)
        assert parsed["service"] == "test"
        assert parsed["level"] == "INFO"

    def test_from_kafka_bytes(self):
        """Test deserialization from JSON bytes."""
        event = LogEvent(
            service="test",
            level="ERROR",
            message="something broke",
        )
        data = event.to_kafka_bytes()
        restored = LogEvent.from_kafka_bytes(data)

        assert restored.service == event.service
        assert restored.level == event.level
        assert restored.message == event.message
        assert restored.id == event.id


# ============================================================
# RateLimiter tests
# ============================================================


class TestRateLimiter:
    """Tests for the sliding window rate limiter."""

    @pytest.mark.asyncio
    async def test_rate_limiter_allows_within_limit(self, redis_client):
        """Requests within the limit are allowed."""
        limiter = RateLimiter(redis=redis_client, limit=100, window_seconds=60)
        result = await limiter.check("client-1")

        assert result is True

    @pytest.mark.asyncio
    async def test_rate_limiter_blocks_after_limit(self, redis_client):
        """
        101st request in window returns rate limited.
        """
        limiter = RateLimiter(redis=redis_client, limit=100, window_seconds=60)
        
        # Fill up the limit
        for _ in range(100):
            await limiter.check("client-1")
            
        # 101st request should be blocked
        result = await limiter.check("client-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_rate_limiter_allows_after_window(self, redis_client):
        """
        Requests are allowed again after the window expires.
        """
        # Create a limiter with a very short window
        limiter = RateLimiter(redis=redis_client, limit=10, window_seconds=1)
        
        # Fill the limit
        for _ in range(10):
            await limiter.check("client-1")
            
        # Blocked
        assert await limiter.check("client-1") is False
        
        # Wait for window to expire
        await asyncio.sleep(1.1)
        
        # Allowed again
        assert await limiter.check("client-1") is True

    @pytest.mark.asyncio
    async def test_rate_limiter_different_clients(self, redis_client):
        """Different clients have independent rate limits."""
        limiter = RateLimiter(redis=redis_client, limit=10, window_seconds=60)
        
        # Client 1 hits limit
        for _ in range(10):
            await limiter.check("client-1")
            
        assert await limiter.check("client-1") is False
        
        # Client 2 is still allowed
        assert await limiter.check("client-2") is True


# ============================================================
# API Endpoint Integration Tests
# ============================================================


class TestIngestEndpoints:
    """End-to-End integration tests for the FastAPI HTTP endpoints."""

    @pytest.mark.asyncio
    async def test_ingest_returns_202(self, async_client):
        """Valid event returns 202 Accepted via the /ingest endpoint."""
        payload = {
            "service": "test-service",
            "level": "INFO",
            "message": "Testing single ingest API",
        }
        response = await async_client.post("/ingest", json=payload)
        
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert "id" in data

    @pytest.mark.asyncio
    async def test_ingest_batch(self, async_client):
        """Batch endpoint accepts multiple events and returns 202."""
        payload = {
            "events": [
                {
                    "service": "test-service",
                    "level": "INFO",
                    "message": "Batch event 1",
                },
                {
                    "service": "test-service",
                    "level": "ERROR",
                    "message": "Batch event 2",
                }
            ]
        }
        response = await async_client.post("/ingest/batch", json=payload)
        
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert data["count"] == 2
        assert len(data["ids"]) == 2

    @pytest.mark.asyncio
    async def test_ingest_invalid_json_returns_422(self, async_client):
        """Garbage body that isn't valid JSON returns 422."""
        response = await async_client.post(
            "/ingest",
            content=b"not json at all {{{",
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_ingest_missing_required_fields_returns_422(self, async_client):
        """Payload missing required fields (service, level, message) returns 422."""
        # Missing 'message'
        response = await async_client.post("/ingest", json={"service": "test", "level": "INFO"})
        assert response.status_code == 422

        # Missing 'service'
        response = await async_client.post("/ingest", json={"level": "INFO", "message": "hello"})
        assert response.status_code == 422

        # Missing 'level'
        response = await async_client.post("/ingest", json={"service": "test", "message": "hello"})
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_ingest_rate_limited_returns_429_with_retry_after(self, async_client):
        """Exceeding rate limit returns 429 with Retry-After header."""
        # The async_client fixture sets RATE_LIMIT_REQUESTS=100 by default
        # Fire 101 requests rapidly
        for _ in range(100):
            await async_client.post("/ingest", json={
                "service": "rate-test", "level": "INFO", "message": "flood"
            })

        # 101st should be rate-limited
        response = await async_client.post("/ingest", json={
            "service": "rate-test", "level": "INFO", "message": "should be blocked"
        })
        assert response.status_code == 429
        assert "Retry-After" in response.headers
        assert response.json()["detail"] == "Rate limit exceeded"


# ============================================================
# Metadata validation tests
# ============================================================


class TestMetadataValidation:
    """Tests for the metadata field size cap."""

    def test_metadata_over_8kb_rejected(self, sample_oversized_metadata):
        """Metadata dict exceeding 8KB when JSON-encoded is rejected."""
        with pytest.raises(Exception) as exc_info:
            LogEvent(
                service="test-service",
                level="INFO",
                message="test",
                metadata=sample_oversized_metadata,
            )
        assert "Metadata" in str(exc_info.value) or "8KB" in str(exc_info.value)

    def test_metadata_at_limit_accepted(self):
        """Metadata dict just under 8KB is accepted."""
        # ~7.5KB of metadata
        small_meta = {f"k{i}": f"v{i}_{'x' * 10}" for i in range(300)}
        event = LogEvent(
            service="test-service",
            level="INFO",
            message="test",
            metadata=small_meta,
        )
        assert event.metadata is not None

    def test_metadata_none_accepted(self):
        """None metadata is accepted (regression guard)."""
        event = LogEvent(
            service="test-service",
            level="INFO",
            message="test",
            metadata=None,
        )
        assert event.metadata is None


# ============================================================
# Multibyte payload tests
# ============================================================


class TestMultibytePayload:
    """Tests for multi-byte UTF-8 character handling."""

    def test_multibyte_message_over_8kb_bytes_rejected(self, sample_multibyte_message):
        """
        Message with CJK chars under 8192 char length but over 8192 bytes
        when UTF-8 encoded is rejected by the byte-level validator.
        """
        # Verify precondition: under char limit but over byte limit
        assert len(sample_multibyte_message) < 8192
        assert len(sample_multibyte_message.encode("utf-8")) > 8192

        with pytest.raises(Exception):
            LogEvent(
                service="test-service",
                level="INFO",
                message=sample_multibyte_message,
            )

    def test_service_name_max_length_rejected(self):
        """Service name exceeding 256 characters is rejected."""
        with pytest.raises(Exception):
            LogEvent(
                service="x" * 257,
                level="INFO",
                message="test",
            )


# ============================================================
# Batch validation tests (HTTP layer)
# ============================================================


class TestBatchValidation:
    """Tests for batch ingestion edge cases."""

    @pytest.mark.asyncio
    async def test_batch_one_bad_event_rejects_entire_batch(self, async_client):
        """
        One invalid event in a batch of good events → HTTP 422, zero events ingested.
        Documents the all-or-nothing tradeoff of Pydantic list validation.
        """
        payload = {
            "events": [
                {"service": "good", "level": "INFO", "message": "fine"},
                {"service": "good", "level": "INVALID_LEVEL", "message": "bad level"},
                {"service": "good", "level": "ERROR", "message": "also fine"},
            ]
        }
        response = await async_client.post("/ingest/batch", json=payload)
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_batch_empty_rejected(self, async_client):
        """Empty events list is rejected."""
        response = await async_client.post("/ingest/batch", json={"events": []})
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_batch_over_1000_rejected(self, async_client):
        """Batch with more than 1000 events is rejected."""
        events = [{"service": "test", "level": "INFO", "message": f"msg {i}"} for i in range(1001)]
        response = await async_client.post("/ingest/batch", json={"events": events})
        assert response.status_code == 422


# ============================================================
# Rate limiter advanced tests
# ============================================================


class TestRateLimiterAdvanced:
    """Advanced rate limiter tests for atomicity and correctness."""

    @pytest.mark.asyncio
    async def test_rate_limiter_concurrent_requests(self, redis_client):
        """
        Fire 110 concurrent .check() calls with limit=100.
        Exactly 100 should pass — tests Lua script atomicity.
        """
        limiter = RateLimiter(redis=redis_client, limit=100, window_seconds=60)

        results = await asyncio.gather(*[limiter.check("concurrent-client") for _ in range(110)])

        allowed_count = sum(1 for r in results if r is True)
        blocked_count = sum(1 for r in results if r is False)

        assert allowed_count == 100
        assert blocked_count == 10

    @pytest.mark.asyncio
    async def test_rate_limiter_get_remaining(self, redis_client):
        """After N checks, get_remaining() returns limit - N."""
        limiter = RateLimiter(redis=redis_client, limit=100, window_seconds=60)

        for _ in range(30):
            await limiter.check("remaining-client")

        remaining = await limiter.get_remaining("remaining-client")
        assert remaining == 70

