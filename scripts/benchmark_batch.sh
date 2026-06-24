#!/usr/bin/env bash
set -euo pipefail

API_URL="http://localhost:8000/ingest/batch"

echo "🚀 Generating payload with 1000 events..."
PAYLOAD=$(python3 -c '
import json, uuid
events = [{"id": str(uuid.uuid4()), "service": "batch-bench", "level": "INFO", "message": "batch load test event"} for _ in range(1000)]
print(json.dumps({"events": events}))
')

echo "$PAYLOAD" > /tmp/batch_payload.json

echo "🚀 Running LogFoundry batch load test..."
echo "   Target: POST ${API_URL}"
echo "   Requests: 1,000 (1000 events per request = 1,000,000 total events)"
echo "   Concurrency: 50"
echo ""

hey -n 1000 -c 50 -m POST \
    -H "Content-Type: application/json" \
    -D /tmp/batch_payload.json \
    "${API_URL}"
