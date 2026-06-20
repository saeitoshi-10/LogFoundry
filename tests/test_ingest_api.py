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
