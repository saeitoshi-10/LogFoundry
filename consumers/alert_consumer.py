"""
AlertConsumer — Pattern-matching consumer for log alerting.

Watches the logs.ingest topic and matches log messages against configurable
regex patterns defined in alerts.yml. On match:
  1. Produces the alert to the logs.alerts Kafka topic
  2. Increments the alerts:{service}:{level} Redis counter

This enables real-time alerting without adding latency to the ingestion path.
The alert rules are loaded once on startup — no hot-reloading is needed for
a demo, but the YAML-based configuration makes the system easily extensible.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List

import yaml
from redis.asyncio import Redis

from base_consumer import BaseConsumer

logger = logging.getLogger(__name__)


class AlertRule:
    """A compiled alert rule with a regex pattern and severity threshold."""

    def __init__(self, pattern: str, severity: str) -> None:
        self.pattern_str = pattern
        self.severity = severity
        self.regex = re.compile(pattern, re.IGNORECASE)

    def matches(self, message: str) -> bool:
        """Check if a log message matches this alert rule."""
        return self.regex.search(message) is not None

    def __repr__(self) -> str:
        return f"AlertRule(pattern={self.pattern_str!r}, severity={self.severity})"


class AlertConsumer(BaseConsumer):
    """
    Kafka consumer that matches log messages against alert rules.

    Responsibilities:
      - Load alert rules from alerts.yml on startup
      - Match each consumed log message against all rules
      - On match: produce to logs.alerts topic + increment Redis counter
      - Rules are regex-based for flexible pattern matching

    The consumer runs in its own consumer group, so it processes every
    message independently from LogWriterConsumer and MetricsConsumer.
    """

    topic = os.getenv("KAFKA_TOPIC_INGEST", "logs.ingest")
    group_id = os.getenv("KAFKA_GROUP_ALERT", "logfoundry-alert")

    def __init__(self) -> None:
        super().__init__()
        self._redis = None
        self._alert_rules: List[AlertRule] = []
        self._alerts_topic = os.getenv("KAFKA_TOPIC_ALERTS", "logs.alerts")

    async def start(self) -> None:
        """Start the consumer with Redis connection and loaded alert rules."""
        # Load alert rules from YAML
        self._load_rules()

        # Connect to Redis for alert counters
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self._redis = Redis.from_url(redis_url, decode_responses=True)

        logger.info(
            "AlertConsumer initialized",
            extra={
                "rules_count": len(self._alert_rules),
                "alerts_topic": self._alerts_topic,
            },
        )

        try:
            await super().start()
        finally:
            if self._redis:
                await self._redis.close()

    def _load_rules(self) -> None:
        """
        Load alert rules from alerts.yml.

        The rules file is expected to be in the same directory as this module.
        Each rule has a 'pattern' (regex) and 'severity' (log level threshold).
        """
        rules_path = Path(__file__).parent / "alerts.yml"

        if not rules_path.exists():
            logger.warning(f"Alert rules file not found: {rules_path}")
            return

        with open(rules_path) as f:
            config = yaml.safe_load(f)

        if not config or "rules" not in config:
            logger.warning("No rules found in alerts.yml")
            return

        for rule_def in config["rules"]:
            try:
                rule = AlertRule(
                    pattern=rule_def["pattern"],
                    severity=rule_def["severity"],
                )
                self._alert_rules.append(rule)
                logger.info(f"Loaded alert rule: {rule}")
            except (KeyError, re.error) as e:
                logger.error(f"Invalid alert rule: {rule_def} — {e}")

    async def process(self, message) -> None:
        """
        Process a consumed message by checking against all alert rules.

        For each matching rule:
          1. Produce the alert details to the logs.alerts Kafka topic
          2. Increment the alerts:{service}:{level} counter in Redis
        """
        try:
            payload = json.loads(message.value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Failed to parse message: {e}")
            raise

        log_message = payload.get("message", "")
        service = payload.get("service", "unknown")
        level = payload.get("level", "UNKNOWN")

        for rule in self._alert_rules:
            if rule.matches(log_message):
                await self._fire_alert(payload, rule, service, level)

    async def _fire_alert(
        self,
        payload: Dict[str, Any],
        rule: AlertRule,
        service: str,
        level: str,
    ) -> None:
        """
        Fire an alert: produce to alerts topic and increment Redis counter.

        The alert payload includes the original log event plus the matched
        rule details, enabling downstream consumers to understand why the
        alert was triggered.
        """
        alert_payload = {
            "original_event": payload,
            "alert_rule": {
                "pattern": rule.pattern_str,
                "severity": rule.severity,
            },
            "service": service,
            "level": level,
        }

        # Produce to alerts topic
        try:
            await self._producer.send_and_wait(
                self._alerts_topic,
                key=service.encode("utf-8"),
                value=json.dumps(alert_payload).encode("utf-8"),
            )
        except Exception as e:
            logger.error(f"Failed to produce alert: {e}", exc_info=True)

        # Increment Redis counter
        try:
            await self._redis.incr(f"alerts:{service}:{level}")
        except Exception as e:
            logger.error(f"Failed to increment alert counter: {e}")

        logger.warning(
            "Alert fired",
            extra={
                "service": service,
                "level": level,
                "pattern": rule.pattern_str,
                "severity": rule.severity,
                "event_id": payload.get("id"),
            },
        )


if __name__ == "__main__":
    consumer = AlertConsumer()
    asyncio.run(consumer.start())
