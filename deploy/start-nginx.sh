#!/bin/sh
set -e

PORT=${PORT:-80}
export PORT

# The API this container proxies /api/ to. Runtime configuration only: read
# DATAWRAP_API_BASE first, then the legacy DATAFLOW_API_BASE and VITE_API_BASE
# spellings (services/brand_env does the same on the API). There is no default
# host: proxying to a guessed one fails silently, so an unset value stops the
# container with the reason, and the platform keeps the previous deploy.
RAW_API_BASE=${DATAWRAP_API_BASE:-${DATAFLOW_API_BASE:-${VITE_API_BASE:-}}}
RAW_API_BASE=$(printf '%s' "$RAW_API_BASE" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")
case "$RAW_API_BASE" in
  ""|/*|api/v1)
    echo "[FATAL] Datawrap web: the API address is not configured." >&2
    echo "[FATAL] Set DATAWRAP_API_BASE on this service to the API's URL, e.g." >&2
    echo "[FATAL]   DATAWRAP_API_BASE=https://<your-api-host>/api/v1" >&2
    echo "[FATAL] (got: '${RAW_API_BASE}'; a same-origin path cannot be proxied to itself)." >&2
    exit 1
    ;;
esac

# Normalize to an absolute https?://host/api/v1 for the upstream proxy.
UPSTREAM_API="$RAW_API_BASE"
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
API_ORIGIN="$ORIGIN"

export API_PROXY_PASS
export API_PROXY_HOST
export API_ORIGIN

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

envsubst '$PORT $API_PROXY_PASS $API_PROXY_HOST $API_ORIGIN' < "$TEMPLATE" > /etc/nginx/conf.d/default.conf

INDEX=/usr/share/nginx/html/index.html
if [ -f "$INDEX" ]; then
  sed -i '/window\.DATAWRAP_API_BASE/d' "$INDEX"
  sed -i "s|</head>|<script>window.DATAWRAP_API_BASE=\"${BROWSER_API_BASE}\"</script></head>|" "$INDEX"
fi

echo "Datawrap web: browser API_BASE=${BROWSER_API_BASE} proxy→${API_PROXY_PASS} (from ${UPSTREAM_API})"

nginx -g 'daemon off;'
