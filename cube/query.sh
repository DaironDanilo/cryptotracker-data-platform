#!/usr/bin/env bash
# Convenience wrapper for querying Cube Core's REST API in production mode
# (no Developer Playground UI available there -- see README.md for why).
# Generates a short-lived JWT using CUBEJS_API_SECRET and runs the query.
#
# Usage:
#   ./query.sh '{"measures":["candles.count","candles.high"],"dimensions":["candles.coin_id"],"filters":[{"member":"candles.coin_id","operator":"equals","values":["bitcoin"]}],"timeDimensions":[{"dimension":"candles.open_time","dateRange":"last 7 days"}]}'
#
# Run from the Ubuntu server itself, or from anywhere on the LAN with
# CUBE_HOST set to http://YOUR_SERVER_IP:4000 (default: http://localhost:4000).
set -euo pipefail

QUERY="${1:?Usage: ./query.sh '<JSON query>'}"
HOST="${CUBE_HOST:-http://localhost:4000}"

TOKEN=$(docker compose -f "$(dirname "$0")/docker-compose.yml" exec -T cube-api \
  node -e "console.log(require('jsonwebtoken').sign({}, process.env.CUBEJS_API_SECRET, {expiresIn: '1h'}))")

curl -sS -G "${HOST}/cubejs-api/v1/load" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "query=${QUERY}" | python3 -m json.tool
