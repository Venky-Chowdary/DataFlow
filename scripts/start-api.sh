#!/bin/sh
# Railway / container entrypoint — bind to $PORT assigned by the platform
set -e
PORT="${PORT:-8000}"
exec python3 -m uvicorn src.main:app --host 0.0.0.0 --port "$PORT" --workers 1
