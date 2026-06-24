"""
Query router — GET /query and GET /metrics endpoints.

Query flow:
  1. Build deterministic cache key from query parameters (SHA256 hash)
  2. Check Redis cache → if hit, return immediately with X-Cache: HIT
  3. Build SQL dynamically based on provided filters
  4. If search is provided: use to_tsquery + GIN index for full-text search
  5. Execute against PostgreSQL
  6. Store result in Redis with TTL of 30 seconds
  7. Return results with X-Cache: MISS and query timing metadata

Partition pruning requires explicit range predicates on the partition key (timestamp).
We use >= and < instead of BETWEEN because BETWEEN is inclusive on both ends
and can cause the planner to scan an extra partition on boundary values.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from models import QueryResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["query"])

# Cache TTL in seconds — 30s sliding window provides a good balance between
# freshness and query performance. For a log platform, slight staleness is
# acceptable because logs are append-only.
CACHE_TTL_SECONDS = 30


def _build_cache_key(params: Dict[str, Any]) -> str:
    """
    Build a deterministic cache key from query parameters.

    Uses SHA256 of JSON-serialized params (sorted keys) to produce
    a fixed-length key regardless of query complexity.
    """
    # Filter out None values for deterministic hashing
    filtered = {k: v for k, v in sorted(params.items()) if v is not None}
    # Convert datetime objects to ISO format strings for JSON serialization
    for k, v in filtered.items():
        if isinstance(v, datetime):
            filtered[k] = v.isoformat()
    raw = json.dumps(filtered, sort_keys=True)
    return f"query_cache:{hashlib.sha256(raw.encode()).hexdigest()}"


def _build_query(
    service: Optional[str],
    level: Optional[str],
    search: Optional[str],
    since: Optional[datetime],
    until: Optional[datetime],
    limit: int,
) -> tuple[str, list]:
    """
    Dynamically build SQL query based on provided filters.

    Returns (sql_string, params_list) for asyncpg's parameterized queries.
    Parameter numbering starts at $1 and increments for each filter.
    """
    conditions = []
    params = []
    param_idx = 1

    if service is not None:
        conditions.append(f"service = ${param_idx}")
        params.append(service)
        param_idx += 1

    if level is not None:
        conditions.append(f"level = ${param_idx}")
        params.append(level)
        param_idx += 1

    if search is not None:
        # Full-text search using to_tsquery and the GIN index.
        # plainto_tsquery handles plain text input without requiring
        # the caller to know tsquery syntax (e.g., & for AND).
        conditions.append(f"to_tsvector('english', message) @@ plainto_tsquery('english', ${param_idx})")
        params.append(search)
        param_idx += 1

    # Partition pruning requires explicit range predicates on timestamp.
    # Using >= and < (not BETWEEN) avoids scanning an extra partition at boundaries.
    if since is not None:
        conditions.append(f"timestamp >= ${param_idx}")
        params.append(since)
        param_idx += 1

    if until is not None:
        conditions.append(f"timestamp < ${param_idx}")
        params.append(until)
        param_idx += 1

    where_clause = " AND ".join(conditions) if conditions else "TRUE"

    sql = f"""
        SELECT id, service, level, message, timestamp, trace_id, metadata
        FROM logs
        WHERE {where_clause}
        ORDER BY timestamp DESC
        LIMIT ${param_idx}
    """
    params.append(limit)

    return sql, params


@router.get(
    "/query",
    response_model=QueryResponse,
    summary="Query logs with filters",
    description="Search logs by service, level, message content, and time range. Results are cached for 30 seconds.",
)
async def query_logs(
    request: Request,
    response: Response,
    service: Optional[str] = Query(default=None, description="Filter by service name"),
    level: Optional[str] = Query(default=None, description="Filter by log level"),
    search: Optional[str] = Query(default=None, max_length=8192, description="Full-text search on message"),
    since: Optional[datetime] = Query(default=None, description="Start of time range (inclusive)"),
    until: Optional[datetime] = Query(default=None, description="End of time range (exclusive)"),
    limit: int = Query(default=100, ge=1, le=1000, description="Max results to return"),
):
    """
    Query logs with optional filters and full-text search.

    Cache strategy: check Redis first, fall through to PostgreSQL on miss.
    Response includes X-Cache header and query_time_ms for observability.
    """
    start_time = time.monotonic()

    redis = request.app.state.redis
    pg_pool = request.app.state.pg_pool

    # Build deterministic cache key
    cache_params = {
        "service": service,
        "level": level,
        "search": search,
        "since": since,
        "until": until,
        "limit": limit,
    }
    cache_key = _build_cache_key(cache_params)

    # Check Redis cache
    cached = await redis.get(cache_key)
    if cached is not None:
        query_time_ms = (time.monotonic() - start_time) * 1000
        results = json.loads(cached)
        response.headers["X-Cache"] = "HIT"

        logger.debug("Cache hit", extra={"cache_key": cache_key})

        return QueryResponse(
            results=results,
            count=len(results),
            cache_hit=True,
            query_time_ms=round(query_time_ms, 2),
        )

    # Cache miss — query PostgreSQL
    sql, params = _build_query(service, level, search, since, until, limit)

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)

    results = []
    for row in rows:
        record = dict(row)
        # Convert UUID and datetime to strings for JSON serialization
        record["id"] = str(record["id"])
        record["timestamp"] = record["timestamp"].isoformat()
        # Convert JSONB metadata to dict if present
        if record.get("metadata") and isinstance(record["metadata"], str):
            record["metadata"] = json.loads(record["metadata"])
        results.append(record)

    # Cache results in Redis with TTL
    await redis.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(results, default=str))

    query_time_ms = (time.monotonic() - start_time) * 1000
    response.headers["X-Cache"] = "MISS"

    logger.info(
        "Query executed",
        extra={
            "cache_key": cache_key,
            "result_count": len(results),
            "query_time_ms": round(query_time_ms, 2),
        },
    )

    return QueryResponse(
        results=results,
        count=len(results),
        cache_hit=False,
        query_time_ms=round(query_time_ms, 2),
    )


@router.get(
    "/metrics",
    summary="Get log ingestion metrics",
    description="Returns Prometheus-compatible text format metrics from Redis counters.",
)
async def get_metrics(request: Request, response: Response):
    """
    Return log ingestion metrics in Prometheus text format.

    Reads Redis counters set by MetricsConsumer and formats them
    as Prometheus exposition format for easy integration with
    monitoring tools.
    """
    redis = request.app.state.redis

    # Collect all metrics keys
    lines = []
    lines.append("# HELP logfoundry_logs_total Total number of log events ingested")
    lines.append("# TYPE logfoundry_logs_total counter")

    total = await redis.get("metrics:total")
    total_val = int(total) if total else 0
    lines.append(f"logfoundry_logs_total {total_val}")

    # Per-service metrics
    lines.append("")
    lines.append("# HELP logfoundry_logs_by_service Log events by service")
    lines.append("# TYPE logfoundry_logs_by_service counter")

    services_map = await redis.hgetall("metrics:services")
    for service_name_bytes, val_bytes in sorted(services_map.items()):
        service_name = service_name_bytes.decode() if isinstance(service_name_bytes, bytes) else service_name_bytes
        val = val_bytes.decode() if isinstance(val_bytes, bytes) else val_bytes
        lines.append(f'logfoundry_logs_by_service{{service="{service_name}"}} {int(val) if val else 0}')

    # Per-level metrics
    lines.append("")
    lines.append("# HELP logfoundry_logs_by_level Log events by level")
    lines.append("# TYPE logfoundry_logs_by_level counter")

    levels_map = await redis.hgetall("metrics:levels")
    for level_name_bytes, val_bytes in sorted(levels_map.items()):
        level_name = level_name_bytes.decode() if isinstance(level_name_bytes, bytes) else level_name_bytes
        val = val_bytes.decode() if isinstance(val_bytes, bytes) else val_bytes
        lines.append(f'logfoundry_logs_by_level{{level="{level_name}"}} {int(val) if val else 0}')

    # Per-service and level metrics
    lines.append("")
    lines.append("# HELP logfoundry_logs_by_service_level Log events by service and level")
    lines.append("# TYPE logfoundry_logs_by_service_level counter")

    service_levels_map = await redis.hgetall("metrics:service_levels")
    for key_bytes, val_bytes in sorted(service_levels_map.items()):
        key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
        val = val_bytes.decode() if isinstance(val_bytes, bytes) else val_bytes
        parts = key_str.split(":")
        if len(parts) == 2:  # {service}:{level}
            sl_service = parts[0]
            sl_level = parts[1]
            lines.append(
                f'logfoundry_logs_by_service_level{{service="{sl_service}",level="{sl_level}"}} {int(val) if val else 0}'
            )

    # Batch Insert Fallback Counter
    lines.append("")
    lines.append("# HELP logfoundry_batch_insert_fallback_total Total times batch insert hit a poison pill and fell back to single inserts")
    lines.append("# TYPE logfoundry_batch_insert_fallback_total counter")
    fallback_total = await redis.get("metrics:batch_insert_fallback_total")
    lines.append(f"logfoundry_batch_insert_fallback_total {int(fallback_total) if fallback_total else 0}")

    # Alert counters
    lines.append("")
    lines.append("# HELP logfoundry_alerts_total Alerts triggered by pattern matching")
    lines.append("# TYPE logfoundry_alerts_total counter")

    alerts_map = await redis.hgetall("metrics:alerts")
    for key_bytes, val_bytes in sorted(alerts_map.items()):
        key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
        val = val_bytes.decode() if isinstance(val_bytes, bytes) else val_bytes
        parts = key_str.split(":")
        if len(parts) == 2:  # {service}:{level}
            alert_service = parts[0]
            alert_level = parts[1]
            lines.append(
                f'logfoundry_alerts_total{{service="{alert_service}",level="{alert_level}"}} {int(val) if val else 0}'
            )

    response.headers["Content-Type"] = "text/plain; charset=utf-8"
    return Response(
        content="\n".join(lines) + "\n",
        media_type="text/plain",
    )
