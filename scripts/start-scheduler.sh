#!/bin/sh
# Cadence entrypoint — same image as the API. Enqueues due pipelines.
# Does not execute transfers.
set -e
cd /app/apps/api
export DATAFLOW_PROCESS_ROLE=scheduler
export DATAFLOW_SCHEDULE_LOOP=1
export DATAFLOW_API_CLAIM_LOOP=0
export DATAFLOW_WORKER_FLEET=1
exec python3 -m src.scheduler_main
