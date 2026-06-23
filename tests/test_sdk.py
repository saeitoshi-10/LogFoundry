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
