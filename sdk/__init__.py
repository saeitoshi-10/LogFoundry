"""
LogFoundry Python SDK — plug-and-play log client.

Usage:
    from logfoundry import Logger

    log = Logger(service="payments-api", endpoint="http://localhost:8000")
    log.info("Payment processed", amount=99.99, user_id="u_123")
"""

from .logger import Logger

__all__ = ["Logger"]
__version__ = "1.0.0"
