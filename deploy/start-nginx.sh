#!/bin/sh
set -e

PORT=${PORT:-80}
export PORT

DEFAULT_API="https://dataflow-api-production-722b.up.railway.app/api/v1"

# Railway may set DATAFLOW_API_BASE at runtime and/or VITE_API_BASE at build.
RAW_API_BASE=${DATAFLOW_API_BASE:-${VITE_API_BASE:-$DEFAULT_API}}
RAW_API_BASE=$(printf '%s' "$RAW_API_BASE" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")
if [ -z "$RAW_API_BASE" ]; then
  RAW_API_BASE="$DEFAULT_API"
fi

# Normalize to an absolute https?://host/api/v1 for the upstream proxy.
UPSTREAM_API="$RAW_API_BASE"
case "$UPSTREAM_API" in
  /api/v1|api/v1)
    UPSTREAM_API="$DEFAULT_API"
    ;;
esac
case "$UPSTREAM_API" in
  http://*|https://*) ;;
  *)
    UPSTREAM_API=$(printf '%s' "$UPSTREAM_API" | sed -e 's|/*$||')
    UPSTREAM_API="https://${UPSTREAM_API}"
    ;;
esac
case "$UPSTREAM_API" in
  */api/v1) ;;
  *) UPSTREAM_API="${UPSTREAM_API%/}/api/v1" ;;
esac

ORIGIN=$(printf '%s' "$UPSTREAM_API" | sed -E 's|(https?://[^/]+).*|\1|')
API_PROXY_HOST=$(printf '%s' "$ORIGIN" | sed -E 's|https?://||')
API_PROXY_PASS="${ORIGIN}/api/"

export API_PROXY_PASS
export API_PROXY_HOST

# Browser always uses same-origin /api/v1 — nginx proxies to UPSTREAM (no CORS).
BROWSER_API_BASE="/api/v1"

# Template was copied to default.conf; rewrite listen port + proxy upstream each start.
if [ -f /etc/nginx/conf.d/default.conf.template ]; then
  TEMPLATE=/etc/nginx/conf.d/default.conf.template
elif [ -f /etc/nginx/templates/default.conf.template ]; then
  TEMPLATE=/etc/nginx/templates/default.conf.template
else
  # Dockerfile copies template → default.conf; keep a seed copy on first run.
  if [ ! -f /etc/nginx/conf.d/default.conf.seed ]; then
    cp /etc/nginx/conf.d/default.conf /etc/nginx/conf.d/default.conf.seed
  fi
  TEMPLATE=/etc/nginx/conf.d/default.conf.seed
fi

envsubst '$PORT $API_PROXY_PASS $API_PROXY_HOST' < "$TEMPLATE" > /etc/nginx/conf.d/default.conf

INDEX=/usr/share/nginx/html/index.html
if [ -f "$INDEX" ]; then
  sed -i '/window\.DATAFLOW_API_BASE/d' "$INDEX"
  sed -i "s|</head>|<script>window.DATAFLOW_API_BASE=\"${BROWSER_API_BASE}\"</script></head>|" "$INDEX"
fi

echo "DataFlow web: browser API_BASE=${BROWSER_API_BASE} proxy→${API_PROXY_PASS} (from ${UPSTREAM_API})"

nginx -g 'daemon off;'
