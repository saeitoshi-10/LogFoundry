"""
Ingest router — POST /ingest and POST /ingest/batch endpoints.

Request flow:
  1. Validate with Pydantic → 422 on failure
  2. Check rate limit → 429 on failure
  3. asyncio.create_task(producer.send(...)) → returns immediately
  4. Return {"status": "accepted", "id": event.id} with HTTP 202

The fire-and-forget pattern (step 3) is what gives sub-5ms response times.
The tradeoff is that if Kafka is down, the event is lost — but the producer
logs the failure, and the API returns 202 ("accepted") not 200 ("processed").
This is an intentional design choice: the caller should treat 202 as
"we received your event" not "your event is durably stored."
"""

from __future__ import annotations

import asyncio
import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from middleware.rate_limiter import RateLimiter
from models import (
    BatchIngestRequest,
    BatchIngestResponse,
    IngestResponse,
    LogEvent,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ingest"])


async def get_rate_limiter(request: Request) -> RateLimiter:
    """FastAPI dependency — retrieves the RateLimiter from app state."""
    return request.app.state.rate_limiter


async def check_rate_limit(request: Request, rate_limiter: RateLimiter = Depends(get_rate_limiter)):
    """
    FastAPI dependency — checks rate limit before processing the request.

    Uses client IP as the rate limit key. Returns HTTP 429 with Retry-After
    header if the client has exceeded the configured limit.
    """
    # Extract client IP securely — do NOT blindly trust X-Forwarded-For
    # unless behind a trusted reverse proxy that strips spoofed headers.
    client_ip = request.client.host if request.client else "unknown"

    allowed = await rate_limiter.check(client_ip)

    if not allowed:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": str(rate_limiter.window_seconds)},
        )


@router.post(
    "/ingest",
    response_model=IngestResponse,
    status_code=202,
    summary="Ingest a single log event",
    description="Validates the event, checks rate limits, and sends to Kafka asynchronously.",
)
async def ingest(
    event: LogEvent,
    request: Request,
    response: Response,
    _rate_limit: None = Depends(check_rate_limit),
):
    """
    Ingest a single log event.

    The event is validated by Pydantic, rate-limited by client IP,
    then sent to Kafka via fire-and-forget. The response returns
    immediately with HTTP 202 Accepted.
    """
    producer = request.app.state.kafka_producer

    if producer.is_queue_full:
        raise HTTPException(
            status_code=503,
            detail="Service Unavailable: Ingestion queue full",
            headers={"Retry-After": "1"},
        )

    # Fire-and-forget — the Kafka produce runs as a background task.
    # This is what gives us sub-5ms response times.
    producer.send_fire_and_forget(event)

    logger.debug(
        "Event accepted",
        extra={
            "event_id": str(event.id),
            "service": event.service,
            "level": event.level,
        },
    )

    return IngestResponse(status="accepted", id=event.id)


@router.post(
    "/ingest/batch",
    response_model=BatchIngestResponse,
    status_code=202,
    summary="Ingest a batch of log events",
    description="Accepts up to 1000 events in a single request. Each event is sent to Kafka individually.",
)
async def ingest_batch(
    batch: BatchIngestRequest,
    request: Request,
    response: Response,
    _rate_limit: None = Depends(check_rate_limit),
):
    """
    Ingest a batch of log events.

    Each event is individually fire-and-forget produced to Kafka.
    The batch endpoint reduces HTTP overhead for high-throughput clients.
    
    Tradeoff: Rate limiting is currently evaluated per HTTP request, meaning
    a single batch request of 1000 events consumes only 1 unit of rate-limit
    quota. This is an intentional design choice to prioritize throughput over
    strict per-event throttling.
    """
    producer = request.app.state.kafka_producer

    if producer.is_queue_full:
        raise HTTPException(
            status_code=503,
            detail="Service Unavailable: Ingestion queue full",
            headers={"Retry-After": "1"},
        )

    ids = []
    tasks = []
    for event in batch.events:
        tasks.append(producer.send(event))
        ids.append(event.id)

    # Await all produces. This enforces strict backpressure: if Kafka is slow
    # or the queue is saturated, the HTTP handler blocks. This prevents infinite
    # memory buffering and OOM crashes under extreme load.
    await asyncio.gather(*tasks)

    logger.debug(
        "Batch accepted",
        extra={"count": len(batch.events)},
    )

    return BatchIngestResponse(
        status="accepted",
        count=len(batch.events),
        ids=ids,
    )
