# LogFoundry — Agent Context

## What you are building

A **plug-and-play distributed log ingestion and query platform** called **LogFoundry**.  
One-sentence pitch: *Drop in the SDK, your logs stream into Kafka, persist into partitioned PostgreSQL, and are queryable via a sub-5ms REST API.*

This is a portfolio project for a backend/systems engineering interview. It must look and behave like a production system — not a tutorial clone. Every component must be defensible in a technical deep-dive by a senior engineer.

---

## Hard constraints (never violate these)

- **No frontend dashboard.** Do not build one. A README with a demo GIF is the UI.
- **No authentication layer.** Out of scope. Do not add it.
- **Single-broker Kafka.** No multi-broker cluster. Keep it demoable locally.
- **No unnecessary abstractions.** Do not add layers that aren't on the resume or don't serve a clear purpose.
- **Docker Compose is the only runtime.** Everything runs via `docker compose up`. No Kubernetes, no cloud deploys.
- **Python only.** FastAPI, aiokafka, asyncpg, redis-py, pytest. No Go, no Node sidecars.
- **OOP where it matters.** `RateLimiter`, `LogWriterConsumer`, `AlertConsumer`, `MetricsConsumer`, and the SDK `Logger` class must be proper classes with clear responsibilities — not bare functions.

---

## Project structure

```
logfoundry/
├── docker-compose.yml
├── .env.example
├── README.md
├── context.md                  ← this file
│
├── api/                        ← FastAPI ingestion + query gateway
│   ├── main.py
│   ├── models.py               ← Pydantic schemas (LogEvent, IngestResponse, QueryRequest)
│   ├── routers/
│   │   ├── ingest.py           ← POST /ingest, POST /ingest/batch
│   │   └── query.py            ← GET /query, GET /metrics
│   ├── middleware/
│   │   └── rate_limiter.py     ← RateLimiter class using Redis sorted sets
│   └── producers/
│       └── kafka_producer.py   ← fire-and-forget Kafka produce
│
├── consumers/                  ← aiokafka consumer workers (run as separate processes)
│   ├── base_consumer.py        ← BaseConsumer class with retry + dead-letter logic
│   ├── log_writer.py           ← LogWriterConsumer: batch insert to PostgreSQL
│   ├── alert_consumer.py       ← AlertConsumer: pattern matching on log messages
│   └── metrics_consumer.py     ← MetricsConsumer: Redis counter increments
│
├── db/
│   ├── schema.sql              ← CREATE TABLE, partition setup, GIN index
│   └── migrations/             ← any future Alembic migrations
│
├── sdk/                        ← pip-installable Python SDK
│   ├── __init__.py
│   ├── logger.py               ← Logger class (the plug-and-play client)
│   └── cli.py                  ← `logfoundry tail --service X` CLI entry point
│
└── tests/
    ├── conftest.py             ← testcontainers fixtures (real Kafka + Postgres)
    ├── test_ingest_api.py      ← Pydantic validation, rate limiting, payload size
    ├── test_consumers.py       ← retry logic, offset replay, dead-letter routing
    └── test_query_api.py       ← cache hit/miss, partition pruning, contract compliance
```

---

## Data model

### `LogEvent` (Pydantic, also the Kafka message payload)

```python
class LogEvent(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    service: str                        # e.g. "payments-api"
    level: Literal["DEBUG","INFO","WARNING","ERROR","CRITICAL"]
    message: str = Field(max_length=8192)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    trace_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    @validator("message")
    def enforce_payload_size(cls, v):
        if len(v.encode()) > 8192:
            raise ValueError("Payload exceeds 8KB limit")
        return v
```

### PostgreSQL `logs` table

```sql
CREATE TABLE logs (
    id          UUID        NOT NULL,
    service     TEXT        NOT NULL,
    level       TEXT        NOT NULL,
    message     TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    trace_id    TEXT,
    metadata    JSONB,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Monthly partitions (create at least 3 months ahead)
CREATE TABLE logs_2026_04 PARTITION OF logs
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE logs_2026_05 PARTITION OF logs
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE logs_2026_06 PARTITION OF logs
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

-- Full-text search index
CREATE INDEX logs_message_gin ON logs USING GIN (to_tsvector('english', message));

-- Service + level index for filtered queries
CREATE INDEX logs_service_level ON logs (service, level, timestamp DESC);
```

---

## Component specifications

### 1. RateLimiter (Redis sorted sets)

**File:** `api/middleware/rate_limiter.py`

Use a sliding window algorithm. Key: `ratelimit:{client_ip}:{window_seconds}`.

```python
class RateLimiter:
    def __init__(self, redis: Redis, limit: int, window_seconds: int): ...

    async def check(self, client_id: str) -> bool:
        # 1. Current timestamp in milliseconds
        # 2. Remove members older than (now - window_seconds * 1000)
        # 3. Count remaining members
        # 4. If count >= limit: return False (rate limited)
        # 5. Add current timestamp as both score and member
        # 6. Set key TTL to window_seconds
        # 7. Return True (allowed)
        ...
```

Inject as FastAPI dependency. Return HTTP 429 with `Retry-After` header on rejection.

---

### 2. Kafka producer (fire-and-forget)

**File:** `api/producers/kafka_producer.py`

- Use `aiokafka.AIOKafkaProducer`
- Serialize `LogEvent` to JSON bytes
- `acks="all"` for durability, but do NOT await the response in the request handler
- Use `asyncio.create_task()` to fire and forget — this is what gives sub-5ms response
- Log produce errors to stderr (don't swallow them silently)
- Topic: `logs.ingest`

The `/ingest` endpoint flow:
1. Validate with Pydantic → 422 on failure
2. Check rate limit → 429 on failure  
3. `asyncio.create_task(producer.send(...))` → returns immediately
4. Return `{"status": "accepted", "id": event.id}` with HTTP 202

---

### 3. BaseConsumer

**File:** `consumers/base_consumer.py`

```python
class BaseConsumer:
    topic: str
    group_id: str
    dead_letter_topic: str = "logs.dead-letter"

    async def start(self): ...
    async def process(self, message: ConsumerRecord) -> None:
        raise NotImplementedError

    async def _run_with_retry(self, message, max_retries=3):
        for attempt in range(max_retries):
            try:
                await self.process(message)
                return
            except Exception as e:
                wait = 2 ** attempt
                await asyncio.sleep(wait)
        await self._send_to_dead_letter(message)

    async def _send_to_dead_letter(self, message): ...
```

Subclasses only implement `process()`. The retry + dead-letter logic lives once in the base.

---

### 4. LogWriterConsumer

**File:** `consumers/log_writer.py`

- Poll messages for up to 500ms or 100 messages, whichever comes first
- Bulk insert using `asyncpg` `executemany()` — never insert one row at a time
- Commit offset only after successful insert
- This ordering (insert → commit) gives at-least-once delivery semantics
- Log batch sizes and insert latency to stdout

---

### 5. AlertConsumer

**File:** `consumers/alert_consumer.py`

- Load alert rules from `alerts.yml` on startup (list of regex patterns + severity thresholds)
- Match `message` field against each rule
- On match: write to `logs.alerts` topic + increment `alerts:{service}:{level}` Redis counter
- Rules example:

```yaml
rules:
  - pattern: "OutOfMemoryError"
    severity: CRITICAL
  - pattern: "connection refused"
    severity: ERROR
  - pattern: "5[0-9]{2} status"
    severity: WARNING
```

---

### 6. MetricsConsumer

**File:** `consumers/metrics_consumer.py`

- For each consumed message increment:
  - `metrics:total` (global counter)
  - `metrics:service:{service}` (per-service counter)
  - `metrics:level:{level}` (per-level counter)
  - `metrics:service:{service}:level:{level}` (cross-dimension counter)
- Use Redis `INCR` (not sorted sets — these are counters, not time windows)
- The `/metrics` endpoint reads these and returns Prometheus text format

---

### 7. Query layer (`/query`)

**File:** `api/routers/query.py`

Request:
```python
class QueryRequest(BaseModel):
    service: Optional[str] = None
    level: Optional[Literal["DEBUG","INFO","WARNING","ERROR","CRITICAL"]] = None
    search: Optional[str] = None     # full-text search on message
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    limit: int = Field(default=100, le=1000)
```

Cache key: `SHA256(json.dumps(query_params, sort_keys=True))` — deterministic hash.

Cache flow:
1. Check Redis for key → if hit, return immediately
2. Build SQL dynamically based on provided filters
3. If `search` provided: use `to_tsquery` + GIN index
4. Execute against PostgreSQL
5. Store result in Redis with TTL of 30 seconds (sliding window)
6. Return results

The SQL must use `WHERE timestamp >= since AND timestamp < until` to trigger partition pruning. Never use `BETWEEN` on the partition key — it can cause full scans on some Postgres versions.

---

### 8. Python SDK

**File:** `sdk/logger.py`

```python
from logfoundry import Logger

log = Logger(
    service="payments-api",
    endpoint="http://localhost:8000",
    async_mode=True,   # non-blocking, fire-and-forget HTTP
    batch_size=50,     # buffer up to 50 events before flushing
    flush_interval=2,  # flush every 2 seconds regardless
)

log.info("Payment processed", amount=99.99, user_id="u_123")
log.error("DB connection failed", host="pg-primary", retry=3)
```

Implementation notes:
- Background thread flushes the buffer on interval or when `batch_size` reached
- `atexit` handler flushes remaining buffer on process exit
- Silent failure by default (don't crash the host application)
- `Logger(verbose=True)` mode prints SDK errors to stderr

---

### 9. CLI

**File:** `sdk/cli.py`

Entry point: `logfoundry`

```
logfoundry tail --service payments-api --level ERROR
logfoundry tail --search "connection refused" --since 1h
logfoundry stats --service payments-api
```

Implementation: poll `/query` every 2 seconds, print new logs, track last `timestamp` seen to avoid duplicates. Use `click` for argument parsing. Colorize by level (ERROR=red, WARNING=yellow, INFO=green).

---

## Docker Compose topology

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_NUM_PARTITIONS: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1

  postgres:
    image: postgres:16-alpine
    ports: ["5432:5432"]
    environment:
      POSTGRES_DB: logfoundry
      POSTGRES_USER: logfoundry
      POSTGRES_PASSWORD: logfoundry
    volumes:
      - ./db/schema.sql:/docker-entrypoint-initdb.d/schema.sql

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]

  api:
    build: ./api
    ports: ["8000:8000"]
    depends_on: [kafka, postgres, redis]
    environment:
      KAFKA_BOOTSTRAP: kafka:9092
      POSTGRES_DSN: postgresql://logfoundry:logfoundry@postgres/logfoundry
      REDIS_URL: redis://redis:6379

  log-writer:
    build: ./consumers
    command: python -m log_writer
    depends_on: [kafka, postgres]

  alert-consumer:
    build: ./consumers
    command: python -m alert_consumer
    depends_on: [kafka, redis]

  metrics-consumer:
    build: ./consumers
    command: python -m metrics_consumer
    depends_on: [kafka, redis]

  jaeger:
    image: jaegertracing/all-in-one:latest
    ports: ["16686:16686", "4317:4317"]
```

---

## OpenTelemetry instrumentation

**Instrument the FastAPI app only** (not the consumers — keep scope tight).

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

provider = TracerProvider()
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://jaeger:4317"))
)
trace.set_tracer_provider(provider)
FastAPIInstrumentor.instrument_app(app)
```

Add a custom span in `/ingest` that records:
- `log.service`, `log.level`, `kafka.produce_latency_ms`

Jaeger UI at `http://localhost:16686` must show end-to-end traces.

---

## Test suite requirements

**File:** `tests/conftest.py` — testcontainers fixtures

```python
@pytest.fixture(scope="session")
def kafka_container():
    with KafkaContainer() as kafka:
        yield kafka.get_bootstrap_server()

@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16-alpine") as pg:
        conn = pg.get_connection_url()
        # run schema.sql
        yield conn
```

**Critical test cases (must exist):**

| Test | What it proves |
|---|---|
| `test_ingest_validates_payload_size` | 8KB+ payload returns 422 |
| `test_rate_limiter_blocks_after_limit` | 101st request in window returns 429 |
| `test_rate_limiter_allows_after_window` | requests allowed again after window expires |
| `test_consumer_retries_on_failure` | mock `process()` to fail twice, succeed third — assert 3 calls |
| `test_dead_letter_on_exhausted_retries` | mock `process()` to always fail — assert message lands in DLT |
| `test_offset_not_committed_before_insert` | kill consumer after poll, before insert — assert message re-processed |
| `test_query_cache_hit` | same query twice — assert second hits Redis (mock asyncpg) |
| `test_query_uses_partition_pruning` | EXPLAIN output contains "Partitions: " with single partition |
| `test_api_contract_under_load` | 500 concurrent requests — assert p99 < 10ms, zero 5xx |

Run with: `pytest tests/ -v --tb=short`

---

## Benchmark requirement

Run this after Day 2 and save output to `benchmarks/ingest_load_test.txt`:

```bash
# Using 'hey' (https://github.com/rakyll/hey)
hey -n 10000 -c 100 -m POST \
  -H "Content-Type: application/json" \
  -d '{"service":"bench","level":"INFO","message":"load test event"}' \
  http://localhost:8000/ingest
```

The README must include a results table like:

```
Requests/sec:  4823
Latency p50:   2.1ms
Latency p95:   4.3ms
Latency p99:   6.8ms
Latency p99.9: 9.1ms
Success rate:  100%
```

---

## Environment variables

```env
# .env.example
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC_INGEST=logs.ingest
KAFKA_TOPIC_ALERTS=logs.alerts
KAFKA_TOPIC_DLT=logs.dead-letter
KAFKA_GROUP_LOG_WRITER=logfoundry-log-writer
KAFKA_GROUP_ALERT=logfoundry-alert
KAFKA_GROUP_METRICS=logfoundry-metrics

POSTGRES_DSN=postgresql://logfoundry:logfoundry@localhost/logfoundry

REDIS_URL=redis://localhost:6379
RATE_LIMIT_REQUESTS=100
RATE_LIMIT_WINDOW_SECONDS=60

OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_SERVICE_NAME=logfoundry-api
```

---

## Key tradeoffs the agent must encode into comments

These must appear as docstrings or inline comments in the relevant files. An interviewer will read the code.

**In `kafka_producer.py`:**
```python
# Fire-and-forget: we create_task instead of awaiting the produce call.
# This decouples ingestion latency from Kafka broker round-trip time.
# Tradeoff: at-least-once delivery — if the task fails, the event is lost.
# Mitigation: producers log failures; consumers handle duplicates via idempotency key.
```

**In `log_writer.py`:**
```python
# We commit the offset AFTER the DB insert, not before.
# This gives at-least-once semantics: a crash between insert and commit
# causes re-processing, not data loss. Downstream must tolerate duplicates
# (the UUID primary key makes duplicate inserts a no-op via ON CONFLICT DO NOTHING).
```

**In `rate_limiter.py`:**
```python
# Sliding window via sorted set: score = timestamp_ms, member = timestamp_ms + random suffix.
# We remove scores older than (now - window_ms) before counting.
# Compared to fixed window: no burst at window boundary.
# Compared to token bucket: simpler Redis ops, no separate TTL-based replenishment job.
```

**In `query.py`:**
```python
# Partition pruning requires explicit range predicates on the partition key (timestamp).
# We use >= and < instead of BETWEEN because BETWEEN is inclusive on both ends
# and can cause the planner to scan an extra partition on boundary values.
```

---

## What "done" looks like

The project is complete when:

1. `docker compose up` starts all services with no manual steps
2. `curl -X POST localhost:8000/ingest -d '{...}'` returns in < 5ms
3. Logs appear in PostgreSQL within 2 seconds of ingestion
4. `logfoundry tail --service X` streams live logs to terminal
5. Jaeger at `:16686` shows traces for ingest requests
6. `pytest tests/ -v` is fully green
7. README contains: architecture diagram, quick-start, load-test results table, and one `EXPLAIN ANALYZE` screenshot showing partition pruning
8. Every class has a docstring explaining its responsibility and the key tradeoff it embodies

---

## What the agent must NOT do

- Do not add authentication or JWT middleware
- Do not build a React/Vue/HTML dashboard
- Do not use synchronous `requests` library anywhere in the API (async only)
- Do not use `time.sleep()` in consumer retry logic (use `asyncio.sleep()`)
- Do not hardcode connection strings — read from environment variables
- Do not create Alembic migrations (schema.sql is the source of truth)
- Do not add more Kafka topics than specified
- Do not use `print()` for application logging — use Python `logging` module with structured JSON format
- Do not commit secrets or `.env` files — only `.env.example`
