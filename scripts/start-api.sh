#!/bin/sh
# Railway / container entrypoint — bind to $PORT assigned by the platform
set -e
PORT="${PORT:-8000}"
# --no-proxy-headers: the app derives the client address and scheme itself
# (src/middleware/proxy_fix_middleware.py) from DATAWRAP_TRUSTED_PROXY_COUNT,
# taking X-Forwarded-For hops[-N] and X-Forwarded-Proto. uvicorn's own proxy
# handling needs --forwarded-allow-ips '*' behind Railway's edge (its address
# is not fixed), and with '*' it takes the LEFT-most X-Forwarded-For entry,
# which the client controls. See docs/DEPLOYMENT.md (client IP behind proxies).
exec python3 -m uvicorn src.main:app --host 0.0.0.0 --port "$PORT" --workers 1 --no-proxy-headers
