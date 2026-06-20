#!/usr/bin/env bash
# LogFoundry Load Test Script
# Uses 'hey' to benchmark the /ingest endpoint.
#
# Install hey: brew install hey
# Usage: ./scripts/load_test.sh
#
# Results are saved to benchmarks/ingest_load_test.txt

set -euo pipefail

API_URL="http://localhost:8000"
OUTPUT_DIR="benchmarks"
OUTPUT_FILE="${OUTPUT_DIR}/ingest_load_test.txt"

# Check if hey is installed
if ! command -v hey &> /dev/null; then
    echo "❌ 'hey' is not installed. Install it with: brew install hey"
    exit 1
fi

# Check if API is running
if ! curl -s "${API_URL}/health" > /dev/null 2>&1; then
    echo "❌ API is not running. Start with: docker compose up -d"
    exit 1
fi

mkdir -p "${OUTPUT_DIR}"

echo "🚀 Running LogFoundry load test..."
echo "   Target: POST ${API_URL}/ingest"
echo "   Requests: 10,000"
echo "   Concurrency: 100"
echo ""

hey -n 10000 -c 100 -m POST \
    -H "Content-Type: application/json" \
    -d '{"service":"bench","level":"INFO","message":"load test event"}' \
    "${API_URL}/ingest" | tee "${OUTPUT_FILE}"

echo ""
echo "✅ Results saved to ${OUTPUT_FILE}"
echo ""
echo "📊 Quick summary:"
grep -E "(Requests/sec|Latency|Status code)" "${OUTPUT_FILE}" || true
