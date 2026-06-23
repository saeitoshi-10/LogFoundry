"""
Pydantic models for LogFoundry API.

These schemas serve triple duty:
  1. FastAPI request/response validation (automatic 422 on invalid input)
  2. Kafka message serialization format (JSON bytes)
  3. API documentation via OpenAPI spec generation

Design note: LogEvent uses a validator to enforce an 8KB payload limit on the
message field. This prevents oversized log events from consuming Kafka partition
space and slowing down consumer batch inserts.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class LogEvent(BaseModel):
    """
    Core log event schema — the unit of data flowing through the entire pipeline.

    Used as:
      - POST /ingest request body
      - Kafka message payload (serialized to JSON)
      - PostgreSQL row (deserialized by LogWriterConsumer)
    """

    id: UUID = Field(default_factory=uuid4, description="Unique event identifier")
    service: str = Field(
        ..., min_length=1, max_length=256, description="Source service name"
    )
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        ..., description="Log severity level"
    )
    message: str = Field(
        ..., max_length=8192, description="Log message content (max 8KB)"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Event timestamp in UTC",
    )
    trace_id: Optional[str] = Field(
        default=None, description="Distributed trace correlation ID"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None, description="Arbitrary key-value metadata"
    )

    @field_validator("message")
    @classmethod
    def enforce_payload_size(cls, v: str) -> str:
        """
        Enforce 8KB byte-level limit on message payload.

        We check encoded byte length (not character count) because multi-byte
        characters in UTF-8 can cause the serialized Kafka message to exceed
        broker limits even when character count is under 8192.
        """
        if len(v.encode("utf-8")) > 8192:
            raise ValueError("Payload exceeds 8KB limit")
        return v

    @field_validator("metadata")
    @classmethod
    def enforce_metadata_size(cls, v: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Enforce 8KB encoded byte-level limit on metadata to prevent unbounded payload bloat."""
        if v is not None:
            if len(json.dumps(v).encode("utf-8")) > 8192:
                raise ValueError("Metadata payload exceeds 8KB limit")
        return v

    def to_kafka_bytes(self) -> bytes:
        """Serialize to JSON bytes for Kafka produce."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_bytes(cls, data: bytes) -> LogEvent:
        """Deserialize from Kafka message value."""
        return cls.model_validate_json(data)


class IngestResponse(BaseModel):
    """Response for successful log ingestion."""

    status: str = "accepted"
    id: UUID


class BatchIngestRequest(BaseModel):
    """Batch ingestion request — accepts up to 1000 events."""

    events: List[LogEvent] = Field(..., min_length=1, max_length=1000)


class BatchIngestResponse(BaseModel):
    """Response for batch ingestion."""

    status: str = "accepted"
    count: int
    ids: List[UUID]


class QueryRequest(BaseModel):
    """
    Query parameters for log search.

    Design: all fields are optional — the query layer dynamically builds SQL
    based on which filters are provided, enabling flexible ad-hoc queries
    without requiring separate endpoints.
    """

    service: Optional[str] = None
    level: Optional[Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]] = None
    search: Optional[str] = Field(
        default=None, description="Full-text search on message (uses GIN index)"
    )
    since: Optional[datetime] = Field(
        default=None, description="Start of time range (inclusive, enables partition pruning)"
    )
    until: Optional[datetime] = Field(
        default=None, description="End of time range (exclusive, enables partition pruning)"
    )
    limit: int = Field(default=100, ge=1, le=1000)


class QueryResponse(BaseModel):
    """Query response with metadata for observability."""

    results: List[Dict[str, Any]]
    count: int
    cache_hit: bool
    query_time_ms: float


class HealthStatus(BaseModel):
    """Health check response showing backend connectivity."""

    status: str
    kafka: bool
    postgres: bool
    redis: bool
    version: str = "1.0.0"
