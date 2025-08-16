#!/usr/bin/env bash
set -euo pipefail
: "${ACCESS_TOKEN:?}"; : "${FILE_ID:?}"; : "${WEBHOOK_URL:?}"
CHANNEL_ID=$(uuidgen)
curl -sS -X POST \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"${CHANNEL_ID}\",\"type\":\"web_hook\",\"address\":\"${WEBHOOK_URL}\"}" \
  "https://www.googleapis.com/drive/v3/files/${FILE_ID}/watch" | tee watch_response.json
echo "Channel ID: ${CHANNEL_ID}"
