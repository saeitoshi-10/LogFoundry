# LogFoundry

**An enterprise-grade, distributed log ingestion and telemetry platform.**

LogFoundry is designed with a strict focus on **High Availability**, **Data Durability**, and **Graceful Degradation**. It provides a plug-and-play Python SDK that streams structured logs into Kafka, durably persists them into partitioned PostgreSQL clusters, and exposes them via a high-performance REST API.

---

## 🏛 Architecture & Design Philosophy

At the core of LogFoundry is the "Buffering vs. Backpressure" tradeoff. While many systems prioritize unsafe, unbounded memory buffering to achieve high vanity throughput, LogFoundry is engineered to survive catastrophic load spikes by surfacing natural TCP backpressure to the edge.

```text
┌──────────┐     ┌───────────────────┐     ┌──────────┐     ┌────────────────┐
│          │     │   FastAPI Gateway  │     │          │     │  LogWriter     │
│   SDK    │────▶│                   │────▶│  Kafka   │────▶│  Consumer      │──▶ PostgreSQL
│ (Client) │     │  • Rate Limiter   │     │  (topic: │     │  (batch insert │    (partitioned)
│          │     │  • Validation     │     │  logs.   │     │   + ON CONFLICT│
└──────────┘     │  • OTel Tracing   │     │  ingest) │     │   DO NOTHING)  │
                 │  • Health Check   │     │          │     └────────────────┘
                 └──────┬────────────┘     │          │     ┌────────────────┐
                        │                  │          │────▶│  Alert         │──▶ logs.alerts
                        │                  │          │     │  Consumer      │    + Redis
                 ┌──────▼────────────┐     │          │     │  (regex rules) │
                 │  GET /query       │     │          │     └────────────────┘
                 │  • Redis cache    │     │          │     ┌────────────────┐
                 │  • Full-text      │     │          │────▶│  Metrics       │──▶ Redis
                 │    search (GIN)   │     │          │     │  Consumer      │    counters
                 │  • Partition      │     └──────────┘     │  (INCR pipeline│
                 │    pruning        │                      └────────────────┘
                 └───────────────────┘
                                           ┌──────────┐
                                           │  Jaeger  │◀── OTel traces
                                           │  :16686  │
                                           └──────────┘
```

## ✨ Key Engineering Features

- **Strict Ingestion Backpressure:** The API enforces hard bounds on async fire-and-forget queues. Batch ingestion dynamically switches to blocking `asyncio.gather` under load, surfacing Kafka's natural TCP backpressure to the HTTP client. This guarantees the API will shed load (`503 Service Unavailable`) rather than suffering catastrophic `OOMKills`.
- **Zero-Mock End-to-End Testing:** The entire test suite (`pytest`) utilizes `testcontainers-python` to spin up ephemeral Kafka, PostgreSQL, and Redis Docker containers. We test the real integration boundaries, verifying DLQ routing and consumer group rebalancing against actual infrastructure, not Python `unittest.mock` stubs.
- **Idempotent At-Least-Once Delivery:** Consumers utilize PostgreSQL `ON CONFLICT DO NOTHING` patterns alongside unique Event UUIDs, allowing safe redelivery of Kafka messages during broker elections or consumer crashes.
- **Atomic Rate Limiting:** A Redis sliding-window rate limiter evaluated via Lua scripts guarantees thread-safe, sub-millisecond load shedding per client IP.
- **Partition Pruning & GIN Indexing:** Optimized time-range queries bypass full table scans using native PostgreSQL monthly partitions, while GIN indexes provide sub-millisecond full-text search across millions of logs.
- **Distributed Tracing:** Full end-to-end request visibility, seamlessly integrated with OpenTelemetry and Jaeger.
- **Dead-Letter Resilience:** Poison pills and unprocessable JSON structures are safely quarantined to Dead Letter Queues (DLQs) without halting the consumer group.

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (4GB+ allocated to Docker)
- Mac M1/M2/M3 or Linux (ARM64 and AMD64 supported)

### 1. Start the platform

```bash
git clone https://github.com/yourusername/logfoundry.git
cd logfoundry
docker compose up -d
```

All 9 services start automatically with health checks:
- **API Gateway**: http://localhost:8000
- **API Docs (Swagger)**: http://localhost:8000/docs
- **Jaeger UI**: http://localhost:16686
- **Health Check**: http://localhost:8000/health

### 2. Ingest a log event

```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "service": "payments-api",
    "level": "INFO",
    "message": "Payment of $99.99 processed successfully",
    "metadata": {"amount": 99.99, "user_id": "u_123"}
  }'
```

Response (< 5ms):
```json
{"status": "accepted", "id": "a1b2c3d4-..."}
```

### 3. Query logs

```bash
# By service
curl "http://localhost:8000/query?service=payments-api"

# By level
curl "http://localhost:8000/query?level=ERROR"

# Full-text search
curl "http://localhost:8000/query?search=connection%20refused"

# Time range (partition pruning)
curl "http://localhost:8000/query?since=2026-06-01T00:00:00Z&until=2026-07-01T00:00:00Z"
```

### 4. View metrics

```bash
curl http://localhost:8000/metrics
```

Returns Prometheus-compatible text format:
```
logfoundry_logs_total 1523
logfoundry_logs_by_service{service="payments-api"} 890
logfoundry_logs_by_service{service="auth-service"} 633
logfoundry_logs_by_level{level="INFO"} 1200
logfoundry_logs_by_level{level="ERROR"} 45
```

### 5. Use the Python SDK

```bash
pip install -e .
```

```python
import logging
from sdk.logger import LogFoundryHandler

# Set up standard Python logger
logger = logging.getLogger("payments")
logger.setLevel(logging.INFO)

# Attach the LogFoundry handler
handler = LogFoundryHandler(
    service="payments-api",
    endpoint="http://localhost:8000",
    async_mode=True,
    batch_size=50,
    flush_interval=2,
)
logger.addHandler(handler)

# Use standard logging calls (no changes to business logic!)
logger.info("Payment processed", extra={"amount": 99.99, "user_id": "u_123"})
logger.error("DB connection failed", extra={"host": "pg-primary", "retry": 3})
```

### 6. Tail logs in real-time

```bash
logfoundry tail --service payments-api --level ERROR
logfoundry tail --search "connection refused" --since 1h
logfoundry stats --service payments-api
```

### 7. Run the demo

```bash
chmod +x scripts/demo.sh
./scripts/demo.sh
```

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/ingest` | POST | Ingest a single log event (202 Accepted) |
| `/ingest/batch` | POST | Ingest up to 1000 events (202 Accepted) |
| `/query` | GET | Query logs with filters and full-text search |
| `/metrics` | GET | Prometheus-format ingestion metrics |
| `/health` | GET | Backend connectivity status |
| `/docs` | GET | OpenAPI/Swagger documentation |


## 📊 Load Testing & Backpressure Validation

LogFoundry handles high concurrency elegantly. We separate our benchmarks to demonstrate both **sustained throughput** and **protective load shedding**.

### 1. Sustained Distributed Throughput (The Happy Path)
Using a custom Python benchmark (`scripts/benchmark_throughput.py`) that rotates source IPs to simulate a massive distributed load, bypassing the HTTP rate limiter:

```text
Total requests: 10,000 (Single Events)
Concurrency:    100
Requests/sec:   439.86 RPS
Status codes:   [202 Accepted] 10,000
```
*The system comfortably handles ~440 RPS on a single Uvicorn worker instance without the internal `MAX_FIRE_AND_FORGET` queue ever breaching its 5,000-task bound.*

### 2. Extreme Batch Spikes (The Availability Test)
Using `scripts/benchmark_batch.sh` to fire 1,000 requests of 1,000 events each (**1,000,000 events instantly**) to test the API's memory resilience.

- Instead of infinitely buffering the million events in memory and causing the OS to trigger an `OOMKill`, the API's batch route utilizes blocking `asyncio.gather`.
- When the internal Kafka C-buffer saturates, it organically raises timeouts, forcing the API to safely shed the HTTP requests.
- **Result:** The container survives `OOMKilled: false`, proving that the system prioritizes durability and availability over unsafe, unbounded ingestion.

For full telemetry and deeper architectural analysis, see [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

---

## 🧪 High-Fidelity Test Infrastructure

We explicitly forbid the use of `unittest.mock` for critical path infrastructure. The test suite spins up real integration environments using `testcontainers-python`.

```bash
# Requires Docker running locally
pip install pytest pytest-asyncio pydantic redis aiokafka asyncpg pyyaml python-json-logger testcontainers
python -m pytest tests/ -v
```

This guarantees that every commit is validated against actual Kafka leader elections, authentic PostgreSQL schema constraints, and real Redis networking boundaries.

---

## Docker Memory Usage (M1 8GB)

| Service | Memory Limit | Typical Usage |
|---------|-------------|---------------|
| Zookeeper | 256MB | ~120MB |
| Kafka | 768MB | ~500MB |
| PostgreSQL | 512MB | ~200MB |
| Redis | 256MB | ~30MB |
| API Gateway | 256MB | ~80MB |
| Log Writer | 192MB | ~60MB |
| Alert Consumer | 192MB | ~50MB |
| Metrics Consumer | 192MB | ~50MB |
| Jaeger | 256MB | ~100MB |
| **Total** | **~2.9GB** | **~1.2GB** |

Monitor with: `docker stats --no-stream`

---

## Project Structure

```
logfoundry/
├── docker-compose.yml              # 9-service orchestration with healthchecks
├── .env.example                    # Environment variables template
├── pyproject.toml                  # SDK packaging
│
├── api/                            # FastAPI ingestion + query gateway
│   ├── main.py                     # App entry point, lifespan, OTel
│   ├── models.py                   # Pydantic schemas
│   ├── routers/
│   │   ├── ingest.py               # POST /ingest, /ingest/batch
│   │   └── query.py                # GET /query, /metrics
│   ├── middleware/
│   │   └── rate_limiter.py         # Sliding window (Redis sorted sets)
│   └── producers/
│       └── kafka_producer.py       # Fire-and-forget Kafka produce
│
├── consumers/                      # Kafka consumer workers
│   ├── base_consumer.py            # ABC with retry + dead-letter
│   ├── log_writer.py               # Batch insert to PostgreSQL
│   ├── alert_consumer.py           # Regex pattern matching
│   ├── alerts.yml                  # Alert rule definitions
│   └── metrics_consumer.py         # Redis counter increments
│
├── db/
│   └── schema.sql                  # Partitioned table + indexes
│
├── sdk/                            # Python SDK
│   ├── logger.py                   # Logger class (plug-and-play client)
│   └── cli.py                      # CLI (tail, stats)
│
├── tests/                          # Test suite
│   ├── conftest.py                 # Fixtures
│   ├── test_ingest_api.py          # Validation, rate limiting
│   ├── test_consumers.py           # Retry, dead-letter, alerts
│   └── test_query_api.py           # Cache, query builder, pruning
│
└── scripts/
    ├── demo.sh                     # Full platform demo
    └── load_test.sh                # Benchmark with hey
```

---

## License

MIT
