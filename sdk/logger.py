"""
LogFoundry SDK Logger — Plug-and-play Python client for log ingestion.

Usage:
    import logging
    from logfoundry.sdk.logger import LogFoundryHandler

    # Set up the standard Python logger
    logger = logging.getLogger("payments")
    logger.setLevel(logging.INFO)

    # Attach the LogFoundry handler
    handler = LogFoundryHandler(
        service="payments-api",
        endpoint="http://localhost:8000",
        async_mode=True,
    )
    logger.addHandler(handler)

    # Use standard logging calls
    logger.info("Payment processed", extra={"amount": 99.99, "user_id": "u_123"})
    logger.error("DB connection failed", extra={"host": "pg-primary"})

Design decisions:
  - Background thread (not asyncio) for flushing: the SDK may be used in
    synchronous applications (Django, Flask) where an event loop isn't running.
  - atexit handler ensures the buffer is flushed on process exit.
  - Silent failure by default: the SDK should never crash the host application.
    Use verbose=True during development to see SDK errors on stderr.
  - Thread-safe buffer using threading.Lock for concurrent access.
  - Uses httpx (async-capable HTTP client) instead of requests to avoid
    blocking the main thread and to support async_mode.
"""

from __future__ import annotations

import atexit
import json
import logging
import sys
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4


class LogFoundryHandler(logging.Handler):
    """
    LogFoundry SDK client — buffer, batch, and ship log events.

    The Logger class provides a high-level API for sending structured log
    events to the LogFoundry platform. It handles:
      - Event buffering for batch ingestion
      - Background thread for periodic flushing
      - Automatic flush on process exit
      - Silent failure (won't crash the host app)

    Thread Safety:
      All buffer operations are protected by a threading.Lock,
      making the Logger safe to use from multiple threads concurrently.
    """

    def __init__(
        self,
        service: str,
        endpoint: str = "http://localhost:8000",
        async_mode: bool = True,
        batch_size: int = 50,
        flush_interval: float = 2.0,
        max_buffer_size: int = 10000,
        verbose: bool = False,
        level: int = logging.NOTSET,
    ) -> None:
        super().__init__(level=level)
        self._service = service
        self._endpoint = endpoint.rstrip("/")
        self._async_mode = async_mode
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._max_buffer_size = max_buffer_size
        self._verbose = verbose

        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._closed = False
        self._last_warning_time = 0.0

        # Start background flush thread if async_mode is enabled
        if self._async_mode:
            self._flush_event = threading.Event()
            self._flush_thread = threading.Thread(
                target=self._background_flush_loop,
                daemon=True,
                name=f"logfoundry-flush-{service}",
            )
            self._flush_thread.start()

        # Register atexit handler to flush remaining buffer on process exit
        atexit.register(self._atexit_flush)

    def emit(self, record: logging.LogRecord) -> None:
        """
        Process a log record and add it to the buffer.
        """
        try:
            message = self.format(record)
        except Exception:
            message = record.getMessage()

        # Extract standard logging metadata
        metadata = {
            "logger_name": record.name,
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
        }
        
        # Capture custom attributes added via 'extra' dictionary
        standard_attrs = {
            'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename', 
            'funcName', 'levelname', 'levelno', 'lineno', 'module', 'msecs', 
            'message', 'msg', 'name', 'pathname', 'process', 'processName', 
            'relativeCreated', 'stack_info', 'thread', 'threadName', 'taskName'
        }
        for key, value in record.__dict__.items():
            if key not in standard_attrs:
                metadata[key] = value

        event = {
            "id": str(uuid4()),
            "service": self._service,
            "level": record.levelname,
            "message": message,
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "metadata": metadata,
        }

        events_to_send = None
        with self._lock:
            if self._closed:
                # Process is shutting down, reject to avoid hanging the process or losing events
                return

            if len(self._buffer) >= self._max_buffer_size:
                now = time.monotonic()
                if now - self._last_warning_time > 5.0:
                    print(
                        f"[logfoundry] WARNING: SDK buffer full ({self._max_buffer_size} events). "
                        "Dropping logs to prevent OOM. Check backend API.",
                        file=sys.stderr,
                    )
                    self._last_warning_time = now
                return

            self._buffer.append(event)
            if not self._async_mode:
                events_to_send = self._buffer[:]
                self._buffer.clear()
            elif len(self._buffer) >= self._batch_size:
                # Signal the background thread to flush
                self._flush_event.set()

        if events_to_send:
            self._send_batch(events_to_send)

    def flush(self) -> None:
        """
        Flush the current buffer by sending events to the LogFoundry API.

        Thread-safe: acquires the lock, swaps the buffer, then sends
        without holding the lock (to avoid blocking _log() during HTTP).
        """
        events_to_send = None
        with self._lock:
            if self._buffer:
                events_to_send = self._buffer[:]
                self._buffer.clear()

        if events_to_send:
            self._send_batch(events_to_send)

    def _send_batch(self, events: List[Dict[str, Any]]) -> None:
        """
        Send a batch of events to the /ingest/batch endpoint.

        Uses urllib.request (stdlib) to avoid adding httpx/requests as a dependency.
        This keeps the SDK lightweight and dependency-free.
        """
        if not events:
            return

        url = f"{self._endpoint}/ingest/batch"
        payload = json.dumps({"events": events}).encode("utf-8")

        try:
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )

            with urllib.request.urlopen(req, timeout=5) as response:
                if self._verbose:
                    body = response.read().decode("utf-8")
                    print(
                        f"[logfoundry] Flushed {len(events)} events: {body}",
                        file=sys.stderr,
                    )

        except Exception as e:
            # Silent failure by default — don't crash the host application.
            # The SDK is a "best effort" logger — lost events are acceptable
            # compared to crashing the production service.
            if self._verbose:
                print(
                    f"[logfoundry] Failed to flush {len(events)} events: {e}",
                    file=sys.stderr,
                )

    def _background_flush_loop(self) -> None:
        """
        Background thread that periodically flushes the buffer.

        Runs every flush_interval seconds or when explicitly signaled by
        a batch threshold. Exits when _closed is set to True.
        """
        while not self._closed:
            # Wait up to flush_interval seconds, or until signaled
            self._flush_event.wait(self._flush_interval)
            self._flush_event.clear()
            try:
                self.flush()
            except Exception as e:
                if self._verbose:
                    print(
                        f"[logfoundry] Background flush error: {e}",
                        file=sys.stderr,
                    )

    def _atexit_flush(self) -> None:
        """
        atexit handler — flush remaining buffer on process exit.

        This ensures that events buffered but not yet flushed are sent
        before the process terminates. Critical for short-lived scripts.
        """
        self._closed = True
        try:
            self.flush()
        except Exception:
            pass  # Best effort on exit

    def close(self) -> None:
        """Explicitly close the logger and flush remaining events."""
        self._closed = True
        self.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __repr__(self) -> str:
        return (
            f"LogFoundryHandler(service={self._service!r}, endpoint={self._endpoint!r}, "
            f"async_mode={self._async_mode}, batch_size={self._batch_size})"
        )
