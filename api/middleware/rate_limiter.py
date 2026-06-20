"""
RateLimiter — Sliding window rate limiter using Redis sorted sets.

Sliding window via sorted set: score = timestamp_ms, member = timestamp_ms + random suffix.
We remove scores older than (now - window_ms) before counting.
Compared to fixed window: no burst at window boundary.
Compared to token bucket: simpler Redis ops, no separate TTL-based replenishment job.

Algorithm:
  1. Current timestamp in milliseconds
  2. ZREMRANGEBYSCORE to remove members older than (now - window_ms)
  3. ZCARD to count remaining members
  4. If count >= limit → return False (rate limited)
  5. ZADD current timestamp as both score and member (+ random suffix for uniqueness)
  6. EXPIRE key to window_seconds (garbage collection for inactive keys)
  7. Return True (allowed)

The entire check-and-add sequence runs in a Redis pipeline to minimize round trips.
In a distributed deployment, this is not strictly atomic — but for a single-broker
demo setup, pipeline batching is sufficient. For true atomicity, a Lua script
would be the next step.
"""

from __future__ import annotations

import logging
import time
import uuid

from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Sliding window rate limiter backed by Redis sorted sets.

    Each client (identified by IP) gets a sorted set where:
      - Score = request timestamp in milliseconds
      - Member = timestamp + UUID suffix (ensures uniqueness for concurrent requests)

    This gives O(log N) add and O(log N + M) cleanup per check, where N is the
    window size and M is the number of expired entries.
    """

    def __init__(self, redis: Redis, limit: int, window_seconds: int) -> None:
        self._redis = redis
        self._limit = limit
        self._window_seconds = window_seconds
        self._window_ms = window_seconds * 1000

        # Lua script for atomic sliding window rate limiting
        # KEYS[1] = ratelimit key
        # ARGV[1] = window_start (ms)
        # ARGV[2] = now_ms (ms)
        # ARGV[3] = limit
        # ARGV[4] = member (now_ms:uuid)
        # ARGV[5] = window_seconds (for EXPIRE)
        script = """
        redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
        local current_count = redis.call('ZCARD', KEYS[1])
        if current_count >= tonumber(ARGV[3]) then
            return 0
        end
        redis.call('ZADD', KEYS[1], ARGV[2], ARGV[4])
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[5]))
        return 1
        """
        self._lua_script = self._redis.register_script(script)

        # Lua script for atomic remaining-quota read
        script_read = """
        redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
        local current_count = redis.call('ZCARD', KEYS[1])
        return current_count
        """
        self._lua_script_read = self._redis.register_script(script_read)

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def window_seconds(self) -> int:
        return self._window_seconds

    async def check(self, client_id: str) -> bool:
        """
        Check if a request from client_id is allowed under the rate limit.
        Executes atomically via a Redis Lua script to prevent check-and-add race conditions.
        """
        key = f"ratelimit:{client_id}:{self._window_seconds}"
        now_ms = int(time.time() * 1000)
        window_start = now_ms - self._window_ms
        member = f"{now_ms}:{uuid.uuid4().hex[:8]}"

        # Execute the Lua script atomically
        allowed = await self._lua_script(
            keys=[key],
            args=[window_start, now_ms, self._limit, member, self._window_seconds]
        )

        if not allowed:
            logger.warning(
                "Rate limit exceeded",
                extra={"client_id": client_id, "limit": self._limit},
            )
            return False

        return True

    async def get_remaining(self, client_id: str) -> int:
        """
        Get the number of remaining requests in the current window.
        Executes atomically via a Redis Lua script to ensure the read is not a stale snapshot.
        """
        key = f"ratelimit:{client_id}:{self._window_seconds}"
        now_ms = int(time.time() * 1000)
        window_start = now_ms - self._window_ms

        current_count = await self._lua_script_read(
            keys=[key],
            args=[window_start]
        )

        return max(0, self._limit - current_count)
