"""
LogFoundry SDK Logger — Plug-and-play Python client for log ingestion.

Usage:
    from logfoundry.sdk.logger import Logger

    log = Logger(
        service="payments-api",
        endpoint="http://localhost:8000",
        async_mode=True,   # non-blocking, fire-and-forget HTTP
        batch_size=50,      # buffer up to 50 events before flushing
        flush_interval=2,   # flush every 2 seconds regardless
    )

    log.info("Payment processed", amount=99.99, user_id="u_123")
    log.error("DB connection failed", host="pg-primary", retry=3)

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
import sys
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4


class Logger:
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
        verbose: bool = False,
    ) -> None:
        self._service = service
        self._endpoint = endpoint.rstrip("/")
        self._async_mode = async_mode
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._verbose = verbose

        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._closed = False

        # Start background flush thread if async_mode is enabled
        if self._async_mode:
            self._flush_thread = threading.Thread(
                target=self._background_flush_loop,
                daemon=True,
                name=f"logfoundry-flush-{service}",
            )
            self._flush_thread.start()

        # Register atexit handler to flush remaining buffer on process exit
        atexit.register(self._atexit_flush)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log a DEBUG level event."""
        self._log("DEBUG", message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log an INFO level event."""
        self._log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log a WARNING level event."""
        self._log("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log an ERROR level event."""
        self._log("ERROR", message, **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log a CRITICAL level event."""
        self._log("CRITICAL", message, **kwargs)

    def _log(
        self,
        level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        message: str,
        **kwargs: Any,
    ) -> None:
        """
        Create a log event and add it to the buffer.

        If async_mode is disabled, flushes immediately after adding.
        If async_mode is enabled, the background thread handles flushing.
        """
        event = {
            "id": str(uuid4()),
            "service": self._service,
            "level": level,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": kwargs if kwargs else None,
        }

        events_to_send = None
        with self._lock:
            self._buffer.append(event)
            if not self._async_mode or len(self._buffer) >= self._batch_size:
                events_to_send = self._buffer[:]
                self._buffer.clear()

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

        Runs every flush_interval seconds. Exits when _closed is set to True.
        """
        while not self._closed:
            time.sleep(self._flush_interval)
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
            f"Logger(service={self._service!r}, endpoint={self._endpoint!r}, "
            f"async_mode={self._async_mode}, batch_size={self._batch_size})"
        )
