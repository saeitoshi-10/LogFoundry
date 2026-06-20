#!/usr/bin/env bash
# LogFoundry Demo Script
# Demonstrates the full platform: ingestion, querying, alerts, metrics, and CLI.
#
# Usage: ./scripts/demo.sh
#
# Prerequisites: docker compose up -d (all services must be running)

set -euo pipefail

API_URL="http://localhost:8000"
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo -e "${CYAN}${BOLD}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                    LogFoundry Demo                          ║"
echo "║          Distributed Log Ingestion & Query Platform         ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Wait for API to be healthy
echo -e "${YELLOW}⏳ Waiting for API to be healthy...${NC}"
for i in $(seq 1 30); do
    if curl -s "${API_URL}/health/ready" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ API is healthy!${NC}"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo -e "${RED}❌ API not ready after 30 seconds. Is docker compose up?${NC}"
        exit 1
    fi
    sleep 1
done

echo ""
curl -s "${API_URL}/health/ready" | python3 -m json.tool
echo ""

# ============================================================
# 1. Single event ingestion
# ============================================================
echo -e "${BLUE}${BOLD}━━━ Step 1: Single Event Ingestion ━━━${NC}"
echo -e "Sending a single log event to POST /ingest..."

RESPONSE=$(curl -s -w "\n%{time_total}" -X POST "${API_URL}/ingest" \
    -H "Content-Type: application/json" \
    -d '{
        "service": "payments-api",
        "level": "INFO",
        "message": "Payment of $99.99 processed successfully",
        "metadata": {"amount": 99.99, "user_id": "u_123", "currency": "USD"}
    }')

BODY=$(echo "$RESPONSE" | head -1)
TIME=$(echo "$RESPONSE" | tail -1)
echo -e "${GREEN}Response: ${BODY}${NC}"
echo -e "${CYAN}Latency: ${TIME}s${NC}"
echo ""

# ============================================================
# 2. Batch ingestion
# ============================================================
echo -e "${BLUE}${BOLD}━━━ Step 2: Batch Ingestion ━━━${NC}"
echo -e "Sending 10 events via POST /ingest/batch..."

curl -s -X POST "${API_URL}/ingest/batch" \
    -H "Content-Type: application/json" \
    -d '{
        "events": [
            {"service": "payments-api", "level": "INFO", "message": "Order created for user u_100"},
            {"service": "payments-api", "level": "INFO", "message": "Payment validated for order ord_200"},
            {"service": "auth-service", "level": "WARNING", "message": "Rate limit approaching for IP 192.168.1.1"},
            {"service": "auth-service", "level": "INFO", "message": "User u_300 logged in successfully"},
            {"service": "notification-svc", "level": "INFO", "message": "Email sent to user u_100"},
            {"service": "payments-api", "level": "ERROR", "message": "connection refused to payment gateway at pg-primary:5432"},
            {"service": "auth-service", "level": "ERROR", "message": "OutOfMemoryError: Java heap space exhausted on auth-worker-3"},
            {"service": "inventory-svc", "level": "WARNING", "message": "Received 503 status from upstream warehouse API"},
            {"service": "notification-svc", "level": "ERROR", "message": "timeout exceeded waiting for SMTP server response"},
            {"service": "payments-api", "level": "CRITICAL", "message": "deadlock detected in transaction processing pipeline"}
        ]
    }' | python3 -m json.tool

echo ""
echo -e "${YELLOW}⏳ Waiting 3 seconds for consumers to process...${NC}"
sleep 3

# ============================================================
# 3. Query logs
# ============================================================
echo -e "${BLUE}${BOLD}━━━ Step 3: Query Logs ━━━${NC}"

echo -e "\n${CYAN}3a. All logs from payments-api:${NC}"
curl -s "${API_URL}/query?service=payments-api&limit=5" | python3 -m json.tool

echo -e "\n${CYAN}3b. Only ERROR level logs:${NC}"
curl -s "${API_URL}/query?level=ERROR&limit=5" | python3 -m json.tool

echo -e "\n${CYAN}3c. Full-text search for 'connection refused':${NC}"
curl -s "${API_URL}/query?search=connection%20refused&limit=5" | python3 -m json.tool

# ============================================================
# 4. Cache demonstration
# ============================================================
echo -e "${BLUE}${BOLD}━━━ Step 4: Cache Hit Demo ━━━${NC}"
echo -e "Running same query twice to show cache behavior..."

echo -e "\n${CYAN}First query (cache MISS):${NC}"
curl -s -D - "${API_URL}/query?service=payments-api&limit=3" 2>&1 | grep -i "x-cache" || echo "  (check response for cache_hit field)"

echo -e "\n${CYAN}Second query (cache HIT):${NC}"
curl -s -D - "${API_URL}/query?service=payments-api&limit=3" 2>&1 | grep -i "x-cache" || echo "  (check response for cache_hit field)"

# ============================================================
# 5. Metrics
# ============================================================
echo -e "\n${BLUE}${BOLD}━━━ Step 5: Metrics (Prometheus Format) ━━━${NC}"
curl -s "${API_URL}/metrics"

# ============================================================
# 6. Health check
# ============================================================
echo -e "\n${BLUE}${BOLD}━━━ Step 6: Health Check ━━━${NC}"
echo "Check health status after load..."
curl -s "${API_URL}/health/ready" | python3 -m json.tool

# ============================================================
# Summary
# ============================================================
echo ""
echo -e "${CYAN}${BOLD}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                    Demo Complete! 🎉                        ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  API Docs:    http://localhost:8000/docs                    ║"
echo "║  Jaeger UI:   http://localhost:16686                        ║"
echo "║  Health:      http://localhost:8000/health                  ║"
echo "║  Metrics:     http://localhost:8000/metrics                 ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
