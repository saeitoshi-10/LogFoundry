"""
Tests for LogFoundry health check endpoints.

Test cases:
  - test_liveness_returns_200: GET /health/live returns 200 with status alive
  - test_readiness_all_healthy: GET /health/ready returns 200 when all backends are up
  - test_readiness_returns_503_when_degraded: Returns 503 when a backend is down
"""

from __future__ import annotations

import pytest


class TestHealthEndpoints:
    """Tests for liveness and readiness probes."""

    @pytest.mark.asyncio
    async def test_liveness_returns_200(self, async_client):
        """GET /health/live returns 200 with status 'alive'."""
        response = await async_client.get("/health/live")
        assert response.status_code == 200
        assert response.json()["status"] == "alive"

    @pytest.mark.asyncio
    async def test_readiness_all_healthy(self, async_client):
        """GET /health/ready returns 200 when all backends (Kafka, PG, Redis) are up."""
        response = await async_client.get("/health/ready")
        assert response.status_code == 200

        data = response.json()
        assert data["status"] == "healthy"
        assert data["kafka"] is True
        assert data["postgres"] is True
        assert data["redis"] is True
        assert "version" in data

    @pytest.mark.asyncio
    async def test_readiness_returns_503_when_degraded(self, postgres_container, kafka_container):
        """
        When one backend is unavailable, /health/ready returns 503 with status 'degraded'.
        We simulate this by starting a throwaway Redis container, pointing the app to it,
        and then stopping the container to trigger a real ConnectionError.
        """
        import os
        from testcontainers.redis import RedisContainer
        from httpx import AsyncClient, ASGITransport
        
        # 1. Start a throwaway Redis container manually so we can stop it safely
        throwaway_redis = RedisContainer("redis:7.2.4-alpine")
        throwaway_redis.start()
        
        try:
            # 2. Configure environment for this specific test
            redis_url = f"redis://{throwaway_redis.get_container_host_ip()}:{throwaway_redis.get_exposed_port(throwaway_redis.port)}"
            os.environ["REDIS_URL"] = redis_url
            os.environ["POSTGRES_DSN"] = postgres_container.get_connection_url().replace("postgresql+psycopg2", "postgresql")
            os.environ["KAFKA_BOOTSTRAP"] = kafka_container.get_bootstrap_server()
            os.environ["OTEL_SDK_DISABLED"] = "true"
            
            # 3. Import app AFTER setting env vars so lifespan uses our throwaway Redis
            from api.main import app
            transport = ASGITransport(app=app)
            
            async with app.router.lifespan_context(app):
                async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                    # Verify it's healthy first
                    response = await client.get("/health/ready")
                    assert response.status_code == 200
                    
                    # 4. STOP the Redis container dynamically
                    throwaway_redis.stop()
                    
                    # 5. Verify degraded state natively
                    response = await client.get("/health/ready")
                    assert response.status_code == 503
                    data = response.json()
                    assert data["status"] == "degraded"
                    assert data["redis"] is False
        finally:
            try:
                # Cleanup if it wasn't already stopped
                throwaway_redis.stop()
            except Exception:
                pass

