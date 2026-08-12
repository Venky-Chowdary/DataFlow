#!/usr/bin/env bash
# Per-boot reconciliation: bring up the local Postgres cluster that the
# CSV -> database transfer flow writes to. Idempotent and non-blocking.
set -euo pipefail

PG_VER="$(ls /etc/postgresql 2>/dev/null | sort -n | tail -1 || true)"
if [ -n "${PG_VER:-}" ]; then
  if ! sudo -u postgres pg_isready -q 2>/dev/null; then
    sudo pg_ctlcluster "$PG_VER" main start || true
  fi
  for _ in $(seq 1 30); do sudo -u postgres pg_isready -q && break || sleep 1; done
  sudo -u postgres pg_isready || echo "WARN: Postgres not ready" >&2
fi

echo "DataFlow start reconciliation complete"
