#!/usr/bin/env bash
set -euo pipefail
: "${ACCESS_TOKEN:?}"; : "${RESOURCE_ID:?}"; : "${CHANNEL_ID:?}"
curl -sS -X POST \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"${CHANNEL_ID}\",\"resourceId\":\"${RESOURCE_ID}\"}" \
  "https://www.googleapis.com/drive/v3/channels/stop"
echo "Stopped channel."
