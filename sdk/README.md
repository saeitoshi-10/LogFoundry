# LogFoundry Python SDK

The LogFoundry Python SDK is a plug-and-play, zero-dependency logging client designed for maximum resilience and zero impact on host application performance. 

It implements `logging.Handler` to integrate natively with Python's standard `logging` module, meaning **zero changes to your business logic** are required to adopt LogFoundry.

## Installation

The SDK requires no external libraries (uses standard `urllib` and `threading`). 

```bash
# If installing from the monorepo
pip install -e .
```

---

## 🚀 Quick Start

Initialize the `LogFoundryHandler` and attach it to your standard Python logger.

```python
import logging
from sdk.logger import LogFoundryHandler

# 1. Create a standard Python logger
logger = logging.getLogger("payments")
logger.setLevel(logging.INFO)

# 2. Attach the LogFoundry Handler
handler = LogFoundryHandler(
    service="payments-api",
    endpoint="http://localhost:8000"
)
logger.addHandler(handler)

# 3. Use standard logging calls
logger.info("Payment processed successfully", extra={"amount": 99.99, "currency": "USD"})
logger.error("Failed to connect to database")
```

Any custom attributes passed via the `extra={}` dictionary are automatically serialized and stored in the `metadata` JSONB column in PostgreSQL, making them instantly searchable.

---

## ⚙️ Configuration Parameters

The `LogFoundryHandler` accepts several parameters to tune performance for your specific environment.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `service` | `str` | **Required** | The name of the service producing the logs (e.g., `auth-service`). |
| `endpoint` | `str` | `http://localhost:8000` | The URL of the LogFoundry API gateway. |
| `async_mode` | `bool` | `True` | If `True`, spawns a background daemon thread to buffer and flush logs periodically. If `False`, blocks the main thread to flush immediately. |
| `batch_size` | `int` | `50` | The maximum number of events to buffer before proactively triggering a background flush. |
| `flush_interval` | `float` | `2.0` | The maximum time (in seconds) to wait before flushing the buffer, regardless of batch size. |
| `max_buffer_size` | `int`| `10000` | The absolute maximum number of events to hold in memory. If reached, the SDK proactively drops logs to prevent OOMing the host app. |
| `verbose` | `bool` | `False` | If `True`, prints SDK internal debug information and flush errors to `sys.stderr`. |

### Tuning for Synchronous Apps (Django / Flask)
If you are running in a highly concurrent synchronous environment (like Gunicorn with threads), the default `async_mode=True` works perfectly. It utilizes a `threading.Lock()` to safely accept logs from multiple request threads and flushes them efficiently in the background without penalizing API response times.

### Tuning for Serverless (AWS Lambda)
Serverless functions freeze their background threads between invocations. For Lambda, disable background threading and flush synchronously, or explicitly call `handler.flush()` before the function returns.

```python
handler = LogFoundryHandler(
    service="billing-lambda",
    async_mode=False  # Blocks briefly to flush immediately
)
```

---

## 🛡️ Resilience & Design Philosophy

The SDK is engineered under the principle that **logging should never crash the host application.**

1. **Silent Failures:** If the LogFoundry API is down, network requests fail, or the buffer overflows, the SDK catches the exceptions and drops the logs silently. Your application will not crash. 
2. **OOM Protection:** The `max_buffer_size` guarantees the background queue will never consume unbounded memory if the network connection drops.
3. **Graceful Shutdown:** The SDK registers an `atexit` handler, ensuring any lingering events in the buffer are flushed durably when the Python process shuts down.

---

## 🛠️ CLI Tooling

The SDK also installs the `logfoundry` command-line tool, enabling real-time debugging and observation directly from your terminal.

### Tailing Logs
Watch logs stream into the system in real-time, filtered by service or severity level.
```bash
logfoundry tail --service payments-api --level ERROR
logfoundry tail --search "connection refused" --since 1h
```

### Viewing Metrics
Quickly query the ingestion metrics (powered by Redis counters).
```bash
logfoundry stats --service payments-api
```
