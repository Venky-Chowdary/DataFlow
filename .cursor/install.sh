#!/usr/bin/env bash
# Idempotent Cloud Agent bootstrap for DataFlow.
# Prepares system libraries, the Python venv (API + preflight), Node workspace
# deps, and a local Postgres cluster used by the CSV -> database transfer flow.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> [1/5] System packages"
# Native builds for psycopg2 (libpq), pyodbc (unixodbc), xmlsec/python3-saml
# (libxmlsec1), plus a local Postgres for the live transfer demo.
sudo DEBIAN_FRONTEND=noninteractive apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  build-essential \
  libpq-dev \
  unixodbc unixodbc-dev \
  libxml2-dev libxmlsec1-dev libxmlsec1-openssl \
  libxslt1-dev pkg-config \
  python3.12-venv \
  postgresql postgresql-client

echo "==> [2/5] Python venv + API/preflight dependencies"
if [ ! -x "$REPO_ROOT/.venv/bin/python" ]; then
  python3 -m venv "$REPO_ROOT/.venv"
fi
# shellcheck disable=SC1091
source "$REPO_ROOT/.venv/bin/activate"
python -m pip install --upgrade pip
python -m pip install -r apps/api/requirements.txt
python -m pip install -e "packages/preflight[dev]"

echo "==> [3/5] Node workspace dependencies"
npm ci

echo "==> [4/5] Configure local Postgres (wal_level=logical for CDC parity)"
PG_VER="$(ls /etc/postgresql 2>/dev/null | sort -n | tail -1 || true)"
if [ -n "${PG_VER:-}" ]; then
  sudo pg_conftool "$PG_VER" main set wal_level logical
  sudo pg_conftool "$PG_VER" main set max_replication_slots 10
  sudo pg_conftool "$PG_VER" main set max_wal_senders 10
  sudo pg_conftool "$PG_VER" main set listen_addresses localhost
  sudo pg_ctlcluster "$PG_VER" main start || sudo pg_ctlcluster "$PG_VER" main restart || true

  echo "==> [5/5] Ensure dataflow role + database exist"
  for _ in $(seq 1 30); do sudo -u postgres pg_isready -q && break || sleep 1; done
  sudo -u postgres psql -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dataflow') THEN
    CREATE ROLE dataflow LOGIN PASSWORD 'dataflow' SUPERUSER;
  END IF;
END
$$;
SQL
  sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='dataflow'" \
    | grep -q 1 || sudo -u postgres createdb -O dataflow dataflow
fi

echo "==> DataFlow install complete"
