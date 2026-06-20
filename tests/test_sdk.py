"""
Tests for the LogFoundry SDK Logger.
"""

import sys
import time
from io import StringIO
from typing import Any, Dict, List

import pytest

from sdk.logger import Logger


class TestSDKLogger:
    def test_logger_respects_max_buffer_size(self, capsys):
        """
        Verify that the logger drops events when the buffer exceeds max_buffer_size
        and emits a warning to stderr.
        """
        # We set async_mode=True so it doesn't flush on every single log.
        # We set flush_interval=10.0 so the background thread doesn't flush while we test.
        # We set batch_size=100 so it doesn't automatically flush when we add events.
        # We set max_buffer_size=5 to test the limit.
        logger = Logger(
            service="test-service",
            endpoint="http://localhost:8000",
            async_mode=True,
            flush_interval=10.0,
            batch_size=100,
            max_buffer_size=5,
            verbose=False,
        )

        # Buffer should be empty
        assert len(logger._buffer) == 0

        # Add 5 events (reaches max capacity)
        for i in range(5):
            logger.info(f"Event {i}")
            
        assert len(logger._buffer) == 5

        # Clear stderr capture
        capsys.readouterr()

        # Add 6th event - should be dropped and emit a warning
        logger.info("Event 6 - should drop")
        
        # Buffer should still be 5
        assert len(logger._buffer) == 5
        
        # Check stderr for the warning
        captured = capsys.readouterr()
        assert "WARNING: SDK buffer full (5 events)" in captured.err

        # Add 7th event - should be dropped but NOT emit a warning (rate limited)
        logger.info("Event 7 - should drop quietly")
        
        # Buffer should still be 5
        assert len(logger._buffer) == 5
        
        # Check stderr to ensure no second warning
        captured = capsys.readouterr()
        assert "WARNING" not in captured.err

        # Verify rate limiting resets after 5 seconds
        # (Mock time.monotonic to simulate 6 seconds passing)
        import time
        original_monotonic = time.monotonic
        try:
            time.monotonic = lambda: original_monotonic() + 6.0
            logger.info("Event 8 - should drop and warn again")
            
            assert len(logger._buffer) == 5
            captured = capsys.readouterr()
            assert "WARNING: SDK buffer full (5 events)" in captured.err
        finally:
            time.monotonic = original_monotonic
