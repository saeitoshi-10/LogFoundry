"""
Tests for the LogFoundry SDK Logger.
"""

import sys
import time
import logging
from io import StringIO
from typing import Any, Dict, List

import pytest

from sdk.logger import LogFoundryHandler


class TestSDKLogger:
    def test_logger_respects_max_buffer_size(self, capsys):
        """
        Verify that the logger drops events when the buffer exceeds max_buffer_size
        and emits a warning to stderr.
        """
        handler = LogFoundryHandler(
            service="test-service",
            endpoint="http://localhost:8000",
            async_mode=True,
            flush_interval=10.0,
            batch_size=100,
            max_buffer_size=5,
            verbose=False,
        )
        logger = logging.getLogger("test_max_buffer")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        logger.addHandler(handler)

        # Buffer should be empty
        assert len(handler._buffer) == 0

        # Add 5 events (reaches max capacity)
        for i in range(5):
            logger.info(f"Event {i}", extra={"user_id": 123})
            
        assert len(handler._buffer) == 5
        
        # Verify metadata extraction worked
        assert handler._buffer[0]["metadata"]["user_id"] == 123
        assert handler._buffer[0]["metadata"]["logger_name"] == "test_max_buffer"
        assert handler._buffer[0]["level"] == "INFO"

        # Clear stderr capture
        capsys.readouterr()

        # Add 6th event - should be dropped and emit a warning
        logger.info("Event 6 - should drop")
        
        # Buffer should still be 5
        assert len(handler._buffer) == 5
        
        # Check stderr for the warning
        captured = capsys.readouterr()
        assert "WARNING: SDK buffer full (5 events)" in captured.err

        # Add 7th event - should be dropped but NOT emit a warning (rate limited)
        logger.info("Event 7 - should drop quietly")
        
        # Buffer should still be 5
        assert len(handler._buffer) == 5
        
        # Check stderr to ensure no second warning
        captured = capsys.readouterr()
        assert "WARNING" not in captured.err

        # Verify rate limiting resets after 5 seconds
        original_monotonic = time.monotonic
        try:
            time.monotonic = lambda: original_monotonic() + 6.0
            logger.info("Event 8 - should drop and warn again")
            
            assert len(handler._buffer) == 5
            captured = capsys.readouterr()
            assert "WARNING: SDK buffer full (5 events)" in captured.err
        finally:
            time.monotonic = original_monotonic
            
        handler.close()

    def test_logger_flushes_batch_size(self):
        """Verify the logger automatically flushes when batch_size is reached."""
        handler = LogFoundryHandler(
            service="test-service",
            endpoint="http://localhost:8000",
            async_mode=True,
            flush_interval=10.0, # Avoid background thread flush
            batch_size=3,
        )
        logger = logging.getLogger("test_batch")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        logger.addHandler(handler)

        import json
        from unittest.mock import patch, MagicMock

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_response = MagicMock()
            mock_response.read.return_value = b'{"status": "ok"}'
            mock_urlopen.return_value.__enter__.return_value = mock_response

            # Add 2 events, should not flush
            logger.info("Msg 1")
            logger.info("Msg 2")
            assert len(handler._buffer) == 2
            mock_urlopen.assert_not_called()

            # Add 3rd event, should trigger flush of batch_size 3
            logger.info("Msg 3")
            
            # Wait for the background thread to pick up the signal and flush
            # Polling for up to 5 seconds to avoid flakiness in overloaded CI environments
            for _ in range(100):
                if mock_urlopen.called:
                    break
                time.sleep(0.05)
                
            assert len(handler._buffer) == 0
            mock_urlopen.assert_called_once()
            
            # Inspect payload
            request_obj = mock_urlopen.call_args[0][0]
            assert request_obj.method == "POST"
            assert request_obj.full_url == "http://localhost:8000/ingest/batch"
            
            payload = json.loads(request_obj.data.decode("utf-8"))
            assert "events" in payload
            assert len(payload["events"]) == 3
            assert payload["events"][0]["message"] == "Msg 1"
            assert payload["events"][2]["message"] == "Msg 3"

        handler.close()

    def test_logger_manual_flush(self):
        """Verify manual flush empties the buffer and sends data."""
        handler = LogFoundryHandler(
            service="test-service",
            async_mode=False, # synchronous mode
        )
        # Manually manipulate the buffer to test flush in isolation
        handler._buffer.append({"id": "1", "message": "hello"})
        
        from unittest.mock import patch
        with patch("urllib.request.urlopen") as mock_urlopen:
            handler.flush()
            mock_urlopen.assert_called_once()
            assert len(handler._buffer) == 0

        handler.close()

    def test_logger_silent_http_failure(self):
        """Verify the logger does not crash the host app if the API is down."""
        handler = LogFoundryHandler(
            service="test-service",
            async_mode=False,
            verbose=False,
        )
        handler._buffer.append({"id": "1", "message": "failme"})
        
        import urllib.error
        from unittest.mock import patch
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("Connection refused")):
            # Should NOT raise an exception
            handler.flush()
            
        handler.close()


# ============================================================
# SDK shutdown & lifecycle tests
# ============================================================


class TestSDKShutdown:
    """Tests for handler shutdown, close, and lifecycle behavior."""

    def test_emit_after_close_is_noop(self):
        """Calling emit() after close() should be a no-op — no crash, no buffer growth."""
        handler = LogFoundryHandler(
            service="test-service",
            endpoint="http://localhost:8000",
            async_mode=True,
            flush_interval=10.0,
        )
        logger = logging.getLogger("test_emit_after_close")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        logger.addHandler(handler)

        handler.close()

        # Emit after close — should not crash or add to buffer
        logger.info("This should be silently dropped")
        assert len(handler._buffer) == 0

    def test_close_idempotent(self):
        """Calling close() twice does not crash or double-flush."""
        from unittest.mock import patch, MagicMock

        handler = LogFoundryHandler(
            service="test-service",
            async_mode=False,
        )
        handler._buffer.append({"id": "1", "message": "test"})

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_response = MagicMock()
            mock_response.read.return_value = b'{"status": "ok"}'
            mock_urlopen.return_value.__enter__.return_value = mock_response

            handler.close()  # First close — flushes
            handler.close()  # Second close — should not crash

            # urlopen should be called exactly once (first close flushes, second is a no-op)
            assert mock_urlopen.call_count == 1

    def test_context_manager_protocol(self):
        """Using `with LogFoundryHandler(...) as h:` flushes on exit."""
        from unittest.mock import patch, MagicMock

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_response = MagicMock()
            mock_response.read.return_value = b'{"status": "ok"}'
            mock_urlopen.return_value.__enter__.return_value = mock_response

            with LogFoundryHandler(
                service="test-ctx",
                async_mode=False,
            ) as handler:
                handler._buffer.append({"id": "1", "message": "ctx test"})

            # After exiting the `with` block, buffer should be flushed
            assert len(handler._buffer) == 0
            mock_urlopen.assert_called_once()


# ============================================================
# SDK thread safety tests
# ============================================================


class TestSDKThreadSafety:
    """Tests for concurrent emit from multiple threads."""

    def test_concurrent_emit_from_multiple_threads(self):
        """
        10 threads each emit 100 messages concurrently.
        No crash, no data corruption. Total events buffered + flushed == 1000.
        """
        import threading

        handler = LogFoundryHandler(
            service="thread-test",
            endpoint="http://localhost:8000",
            async_mode=True,
            flush_interval=60.0,  # Disable time-based flush
            batch_size=10000,     # Disable batch-size flush
            max_buffer_size=10000,
        )

        logger_instance = logging.getLogger("test_thread_safety")
        logger_instance.setLevel(logging.INFO)
        logger_instance.handlers.clear()
        logger_instance.addHandler(handler)

        barrier = threading.Barrier(10)

        def emit_100():
            barrier.wait()  # Synchronize all threads to start together
            for i in range(100):
                logger_instance.info(f"msg {i}")

        threads = [threading.Thread(target=emit_100) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All 1000 events should be in the buffer (no flush triggered)
        assert len(handler._buffer) == 1000

        handler.close()


# ============================================================
# SDK true integration tests
# ============================================================


class TestSDKIntegration:
    """True E2E integration test against a real local FastAPI server."""

    @pytest.mark.asyncio
    async def test_sdk_emits_to_real_server(self, redis_container, postgres_container, kafka_container):
        import os
        import asyncio
        import json
        import uvicorn
        from aiokafka import AIOKafkaConsumer
        
        # Make sure 'main' is imported from the 'api' directory properly
        import sys
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'api')))
        from main import app

        # Setup env vars for the API
        os.environ["REDIS_URL"] = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(redis_container.port)}"
        os.environ["POSTGRES_DSN"] = postgres_container.get_connection_url().replace("postgresql+psycopg2", "postgresql")
        os.environ["KAFKA_BOOTSTRAP"] = kafka_container.get_bootstrap_server()
        os.environ["KAFKA_TOPIC_INGEST"] = "logs.sdk-ingest"
        os.environ["OTEL_SDK_DISABLED"] = "true"
        
        # Explicitly create the topic to avoid auto-create timing issues
        from aiokafka.admin import AIOKafkaAdminClient, NewTopic
        admin = AIOKafkaAdminClient(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"])
        await admin.start()
        try:
            await admin.create_topics([NewTopic("logs.sdk-ingest", num_partitions=1, replication_factor=1)])
        except Exception:
            pass
        finally:
            await admin.close()

        # Spin up Uvicorn server in the background
        config = uvicorn.Config(app, host="127.0.0.1", port=8099, log_level="warning")
        server = uvicorn.Server(config)
        
        # Override the server's install_signal_handlers to avoid conflicting with pytest
        server.install_signal_handlers = lambda: None
        
        server_task = asyncio.create_task(server.serve())
        await asyncio.sleep(2)  # Wait for server to start

        try:
            # Configure SDK to point to the real local server
            handler = LogFoundryHandler(
                service="sdk-integration",
                endpoint="http://127.0.0.1:8099",
                async_mode=False,
            )
            logger_instance = logging.getLogger("test_integration")
            logger_instance.setLevel(logging.INFO)
            logger_instance.handlers.clear()
            logger_instance.addHandler(handler)

            # Emit a real log! This performs an actual HTTP POST to the running server.
            logger_instance.info("Hello from the true integration test")

            # Force flush
            handler.close()

            # Verify the log landed in Kafka via the API
            verifier = AIOKafkaConsumer(
                "logs.sdk-ingest",
                bootstrap_servers=kafka_container.get_bootstrap_server(),
                group_id="test-sdk-verifier",
                auto_offset_reset="earliest"
            )
            await verifier.start()
            try:
                msg = await asyncio.wait_for(verifier.getone(), timeout=5.0)
                payload = json.loads(msg.value)
                assert payload["service"] == "sdk-integration"
                assert payload["message"] == "Hello from the true integration test"
            finally:
                await verifier.stop()

        finally:
            server.should_exit = True
            try:
                await server_task
            except asyncio.CancelledError:
                pass


