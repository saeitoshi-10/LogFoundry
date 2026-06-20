"""
LogFoundry API Gateway — FastAPI application entry point.

This is the central ingestion and query gateway for the LogFoundry platform.
It handles:
  - Log event ingestion (POST /ingest, POST /ingest/batch)
  - Log querying with caching (GET /query)
  - Prometheus-format metrics (GET /metrics)
  - Health checks (GET /health)
  - OpenTelemetry distributed tracing via Jaeger

All backend connections (Kafka, PostgreSQL, Redis) are managed via the
FastAPI lifespan context manager for clean startup/shutdown.
"""

from __future__ import annotations

import logging
import os
import sys
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Response
try:
    from pythonjsonlogger.json import JsonFormatter
except ImportError:
    from pythonjsonlogger.jsonlogger import JsonFormatter
from redis.asyncio import Redis

from middleware.rate_limiter import RateLimiter
from models import HealthStatus
from producers.kafka_producer import KafkaLogProducer
from routers import ingest, query

# ============================================================
# Structured JSON logging
# ============================================================


def _setup_logging() -> None:
    """
    Configure structured JSON logging for all application loggers.

    JSON-formatted logs are essential for production observability:
      - Machine-parseable by log aggregation tools (ELK, Datadog, etc.)
      - Structured fields enable filtering and alerting
      - Consistent format across all services in the platform
    """
    handler = logging.StreamHandler(sys.stdout)
    formatter = JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

    # Suppress noisy third-party loggers
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)


_setup_logging()
logger = logging.getLogger(__name__)


# ============================================================
# OpenTelemetry instrumentation
# ============================================================


def _setup_otel(app: FastAPI) -> None:
    """
    Instrument the FastAPI app with OpenTelemetry tracing.

    We instrument the API gateway only (not consumers) to keep the
    tracing scope tight and the Jaeger UI focused on request flows.

    Traces are exported to Jaeger via OTLP/gRPC. The Jaeger UI at
    :16686 shows end-to-end request traces including:
      - HTTP request handling
      - Rate limit checks
      - Kafka produce latency
    """
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        otel_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
        service_name = os.getenv("OTEL_SERVICE_NAME", "logfoundry-api")

        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(endpoint=otel_endpoint, insecure=True)
            )
        )
        trace.set_tracer_provider(provider)

        FastAPIInstrumentor.instrument_app(app)

        logger.info(
            "OpenTelemetry instrumentation enabled",
            extra={"endpoint": otel_endpoint, "service": service_name},
        )
    except ImportError:
        logger.warning("OpenTelemetry packages not available — tracing disabled")
    except Exception as e:
        # Don't crash the API if OTel setup fails — tracing is optional
        logger.warning(f"OpenTelemetry setup failed: {e} — tracing disabled")


# ============================================================
# Application lifespan (startup/shutdown)
# ============================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage backend connections across the application lifecycle.

    Startup:
      1. Connect to Redis
      2. Initialize RateLimiter
      3. Connect to PostgreSQL (connection pool)
      4. Start Kafka producer

    Shutdown:
      1. Stop Kafka producer (flush pending messages)
      2. Close PostgreSQL pool
      3. Close Redis connection

    Using a lifespan context manager instead of on_event decorators
    is the recommended FastAPI pattern as of v0.100+.
    """
    # --- Startup ---
    logger.info("Starting LogFoundry API gateway...")

    # Redis
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    app.state.redis = Redis.from_url(redis_url, decode_responses=True)
    logger.info("Redis connected", extra={"url": redis_url})

    # Rate limiter
    rate_limit = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
    rate_window = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))
    app.state.rate_limiter = RateLimiter(
        redis=app.state.redis,
        limit=rate_limit,
        window_seconds=rate_window,
    )
    logger.info(
        "Rate limiter initialized",
        extra={"limit": rate_limit, "window_seconds": rate_window},
    )

    # PostgreSQL connection pool
    postgres_dsn = os.getenv("POSTGRES_DSN", "postgresql://logfoundry:logfoundry@localhost/logfoundry")
    app.state.pg_pool = await asyncpg.create_pool(
        dsn=postgres_dsn,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )
    logger.info("PostgreSQL pool created", extra={"dsn": postgres_dsn.split("@")[-1]})

    # Kafka producer
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC_INGEST", "logs.ingest")
    app.state.kafka_producer = KafkaLogProducer(
        bootstrap_servers=kafka_bootstrap,
        topic=kafka_topic,
    )
    await app.state.kafka_producer.start()

    logger.info("LogFoundry API gateway started successfully")

    yield

    # --- Shutdown ---
    logger.info("Shutting down LogFoundry API gateway...")

    await app.state.kafka_producer.stop()
    await app.state.pg_pool.close()
    await app.state.redis.close()

    logger.info("LogFoundry API gateway shut down cleanly")


# ============================================================
# FastAPI application
# ============================================================

app = FastAPI(
    title="LogFoundry",
    description="Distributed log ingestion and query platform",
    version="1.0.0",
    lifespan=lifespan,
)

# Setup OpenTelemetry tracing
_setup_otel(app)

# Include routers
app.include_router(ingest.router)
app.include_router(query.router)


# ============================================================
# Health check endpoint
# ============================================================


@app.get(
    "/health/live",
    summary="Liveness check",
    description="Lightweight check to verify the HTTP server is running.",
)
async def liveness_check():
    return {"status": "alive"}


@app.get(
    "/health/ready",
    response_model=HealthStatus,
    summary="Readiness check",
    description="Verifies connectivity to Kafka, PostgreSQL, and Redis.",
)
async def readiness_check(response: Response):
    """
    Readiness check endpoint — verifies connectivity to Kafka, PostgreSQL, and Redis.

    Returns HTTP 200 with individual backend status. A production load balancer
    would use this to determine if the instance is healthy enough to receive traffic.
    """
    kafka_ok = False
    postgres_ok = False
    redis_ok = False

    # Check Kafka
    try:
        kafka_ok = await app.state.kafka_producer.health_check()
    except Exception:
        pass

    # Check PostgreSQL
    try:
        async with app.state.pg_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        postgres_ok = True
    except Exception:
        pass

    # Check Redis
    try:
        await app.state.redis.ping()
        redis_ok = True
    except Exception:
        pass

    all_healthy = kafka_ok and postgres_ok and redis_ok

    if not all_healthy:
        response.status_code = 503

    return HealthStatus(
        status="healthy" if all_healthy else "degraded",
        kafka=kafka_ok,
        postgres=postgres_ok,
        redis=redis_ok,
    )
