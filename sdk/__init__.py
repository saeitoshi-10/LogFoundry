"""
LogFoundry Python SDK — plug-and-play log client.

Usage:
    import logging
    from logfoundry import LogFoundryHandler

    logger = logging.getLogger("payments")
    logger.addHandler(LogFoundryHandler(service="payments-api"))
    logger.info("Payment processed", extra={"amount": 99.99, "user_id": "u_123"})
"""

from .logger import LogFoundryHandler

__all__ = ["LogFoundryHandler"]
__version__ = "1.0.0"
