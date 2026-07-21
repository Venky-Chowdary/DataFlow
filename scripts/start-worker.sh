#!/bin/sh
# Railway worker entrypoint — same image as API, claims transfer jobs from Mongo.
set -e
cd /app/apps/api
export DATAFLOW_WORKER_FLEET=1
exec python3 -m src.worker_main
