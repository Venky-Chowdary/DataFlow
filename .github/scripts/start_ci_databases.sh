#!/usr/bin/env bash
# Start the CI databases that need a command line (GitHub `services:` cannot
# pass one), mirroring docker-compose.yml so CI proves the same topology the
# product ships against:
#   mysql — ROW binlog + GTID (CDC) and local_infile=1 (LOAD DATA LOCAL COPY path)
#   mongo — single-node replica set rs0 (snapshot read concern, change streams)
#   gcs   — fake-gcs-server (desktop lab postgresql->gcs execute cell)
#   adls  — Azurite blob (desktop lab postgresql->adls execute cell)
#   redis — CDC lease backend. Started here, not as a service container:
#            a Docker Hub rate-limit on `services:` aborts the job before
#            checkout and every later step exits 254 (package.json missing).
#   postgres — plain postgres:16 for jobs that do not need the CDC build.
#   bigquery — goccy/bigquery-emulator (warehouse SKU proof); re-runnable:
#              an existing container is replaced with a fresh catalog
#   sqlserver — mssql/server 2022 Developer with SQL Server Agent ON (native
#              CDC capture jobs need it), then tests/sqlserver_ct_init.sql and
#              tests/sqlserver_cdc_init.sql. The SA password is read from
#              MSSQL_SA_PASSWORD (a per-run value the workflow generates and
#              masks); it is never written on a command line or in this file.
# Emulator greens are labelled emulator_not_customer_tenant by the lab; they
# are not customer-tenant certificates.
# Usage: start_ci_databases.sh [mysql] [mongo] [gcs] [adls] [bigquery] [redis] [postgres] [sqlserver]
set -euo pipefail

POSTGRES_IMAGE="${POSTGRES_IMAGE:-public.ecr.aws/docker/library/postgres:16}"
MYSQL_IMAGE="${MYSQL_IMAGE:-public.ecr.aws/docker/library/mysql:8.0}"
MONGO_IMAGE="${MONGO_IMAGE:-public.ecr.aws/docker/library/mongo:7}"
GCS_IMAGE="${GCS_IMAGE:-docker.io/fsouza/fake-gcs-server:latest}"
AZURITE_IMAGE="${AZURITE_IMAGE:-mcr.microsoft.com/azure-storage/azurite:latest}"
BIGQUERY_IMAGE="${BIGQUERY_IMAGE:-ghcr.io/goccy/bigquery-emulator:latest}"
REDIS_IMAGE="${REDIS_IMAGE:-public.ecr.aws/docker/library/redis:7-alpine}"
MSSQL_IMAGE="${MSSQL_IMAGE:-mcr.microsoft.com/mssql/server:2022-latest}"

wait_for() {  # wait_for <label> <retries> <sleep> <cmd...>
  local label=$1 retries=$2 pause=$3; shift 3
  for _ in $(seq 1 "$retries"); do
    if "$@" >/dev/null 2>&1; then echo "${label} ready"; return 0; fi
    sleep "$pause"
  done
  echo "${label} failed to become ready" >&2
  return 1
}

# pull_image <primary> <fallback>: public.ecr.aws and docker.io both rate-limit
# anonymous pulls; retry with backoff, then try the other registry. Prints the
# ref that was actually pulled.
pull_image() {
  local primary=$1 fallback=$2 ref
  for ref in "$primary" "$fallback"; do
    for attempt in 1 2 3 4; do
      if docker pull --quiet "$ref" >/dev/null 2>&1; then echo "$ref"; return 0; fi
      echo "pull ${ref} failed (attempt ${attempt})" >&2
      sleep $((attempt * 15))
    done
  done
  echo "could not pull ${primary} or ${fallback}" >&2
  return 1
}

start_mysql() {
  MYSQL_IMAGE=$(pull_image "$MYSQL_IMAGE" "docker.io/library/mysql:8.0")
  docker run -d --name dataflow-ci-mysql \
    -e MYSQL_ROOT_PASSWORD=dataflow \
    -e MYSQL_DATABASE=dataflow \
    -e MYSQL_USER=dataflow \
    -e MYSQL_PASSWORD=dataflow \
    -p 3306:3306 \
    "$MYSQL_IMAGE" \
    --server-id=1 \
    --log-bin=mysql-bin \
    --binlog-format=ROW \
    --binlog-row-image=FULL \
    --binlog-row-metadata=FULL \
    --gtid-mode=ON \
    --enforce-gtid-consistency=ON \
    --local-infile=1 \
    --log-bin-trust-function-creators=1
  # mysqld restarts once during first-boot init; require the user grant to
  # succeed (not just a ping) before returning.
  wait_for "MySQL" 60 2 docker exec dataflow-ci-mysql \
    mysql -h 127.0.0.1 -u root -pdataflow -e "SELECT 1" \
    || { docker logs dataflow-ci-mysql || true; exit 1; }
  docker exec dataflow-ci-mysql mysql -h 127.0.0.1 -u root -pdataflow -e "
    GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'dataflow'@'%';
    GRANT ALL PRIVILEGES ON dataflow.* TO 'dataflow'@'%';
    FLUSH PRIVILEGES;"
  docker exec dataflow-ci-mysql mysql -h 127.0.0.1 -u root -pdataflow -Ne \
    "SHOW VARIABLES WHERE Variable_name IN ('local_infile','log_bin','binlog_format')"
}

start_mongo() {
  MONGO_IMAGE=$(pull_image "$MONGO_IMAGE" "docker.io/library/mongo:7")
  docker run -d --name dataflow-ci-mongo -p 27017:27017 \
    "$MONGO_IMAGE" --replSet rs0 --bind_ip_all
  wait_for "mongod" 40 2 docker exec dataflow-ci-mongo \
    mongosh --quiet --eval 'db.adminCommand({ping:1}).ok' \
    || { docker logs dataflow-ci-mongo || true; exit 1; }
  # localhost:27017 keeps the member reachable from the runner (host network
  # mapping) exactly like docker-compose.yml's mongo-init.
  docker exec dataflow-ci-mongo mongosh --quiet --eval '
    try { rs.status(); }
    catch (e) { rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "localhost:27017" }] }); }'
  wait_for "Mongo replica set primary" 40 2 docker exec dataflow-ci-mongo \
    mongosh --quiet --eval 'if (!db.hello().isWritablePrimary) { quit(1) }' \
    || { docker logs dataflow-ci-mongo || true; exit 1; }
  docker exec dataflow-ci-mongo mongosh --quiet --eval 'printjson({setName: db.hello().setName, primary: db.hello().isWritablePrimary})'
}

start_gcs() {
  GCS_IMAGE=$(pull_image "$GCS_IMAGE" "$GCS_IMAGE")
  docker run -d --name dataflow-ci-fake-gcs -p 4443:4443 "$GCS_IMAGE" \
    -scheme http -port 4443 -public-host localhost -external-url http://localhost:4443
  wait_for "fake-gcs" 30 2 curl -fsS http://127.0.0.1:4443/storage/v1/b \
    || { docker logs dataflow-ci-fake-gcs || true; exit 1; }
  # The live matrices write to bucket `dataflow-test`. Create it here so a
  # shard never depends on another test having created it first.
  curl -fsS -X POST -H 'Content-Type: application/json' \
    -d '{"name":"dataflow-test"}' \
    'http://127.0.0.1:4443/storage/v1/b?project=dataflow-test' >/dev/null \
    || curl -fsS http://127.0.0.1:4443/storage/v1/b/dataflow-test >/dev/null \
    || { echo "fake-gcs: could not create bucket dataflow-test"; exit 1; }
  echo "fake-gcs bucket dataflow-test ready"
}

start_adls() {
  AZURITE_IMAGE=$(pull_image "$AZURITE_IMAGE" "$AZURITE_IMAGE")
  docker run -d --name dataflow-ci-azurite -p 10000:10000 "$AZURITE_IMAGE" \
    azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --loose --skipApiVersionCheck
  # Azurite answers any path with a well-formed HTTP error once it is listening.
  wait_for "Azurite" 30 2 curl -s -o /dev/null http://127.0.0.1:10000/ \
    || { docker logs dataflow-ci-azurite || true; exit 1; }
}

start_redis() {
  # Service-container pulls have no registry fallback. public.ecr.aws first,
  # then Docker Hub, with the same backoff as MySQL/Mongo.
  REDIS_IMAGE=$(pull_image "$REDIS_IMAGE" "docker.io/library/redis:7-alpine")
  docker rm -f dataflow-ci-redis >/dev/null 2>&1 || true
  docker run -d --name dataflow-ci-redis -p 6379:6379 "$REDIS_IMAGE"
  wait_for "Redis" 30 2 docker exec dataflow-ci-redis redis-cli ping \
    || { docker logs dataflow-ci-redis || true; exit 1; }
}

start_bigquery() {
  # The emulator is one Go process over a googlesqlite catalog. DROP TABLE on
  # a catalog that has accumulated many tables can panic inside its catalog
  # rebuild and lose unrelated live tables (seen as "Table not found" on the
  # Gate-8 read-back of an already-MERGEd table). Each caller therefore gets a
  # fresh process + fresh --database file; the api slice and the warehouse SKU
  # proof do not share catalog history. --restart keeps a mid-run panic from
  # taking the port down; --database keeps tables across such a restart.
  BIGQUERY_IMAGE=$(pull_image "$BIGQUERY_IMAGE" "$BIGQUERY_IMAGE")
  docker rm -f dataflow-bigquery-emulator >/dev/null 2>&1 || true
  docker run -d --name dataflow-bigquery-emulator --restart unless-stopped \
    -p 9050:9050 -p 9060:9060 \
    "$BIGQUERY_IMAGE" \
    --project=dataflow-test --dataset=dataflow --log-level=error \
    --database=/tmp/dataflow-bq.db
  # The emulator does not serve /; use the BigQuery discovery API.
  wait_for "BigQuery emulator" 30 2 curl -fsS \
    http://127.0.0.1:9050/discovery/v1/apis/bigquery/v2/rest \
    || { docker logs dataflow-bigquery-emulator || true; exit 1; }
  docker inspect dataflow-bigquery-emulator \
    --format 'emulator restarts={{.RestartCount}} state={{.State.Status}} started={{.State.StartedAt}}'
}

start_postgres() {
  # Service-container pulls have no registry fallback — same public.ecr.aws →
  # Docker Hub retry as the other images.
  POSTGRES_IMAGE=$(pull_image "$POSTGRES_IMAGE" "docker.io/library/postgres:16")
  docker rm -f dataflow-ci-postgres >/dev/null 2>&1 || true
  docker run -d --name dataflow-ci-postgres \
    -e POSTGRES_USER=dataflow \
    -e POSTGRES_PASSWORD=dataflow \
    -e POSTGRES_DB=dataflow \
    -p 5432:5432 \
    "$POSTGRES_IMAGE"
  wait_for "Postgres" 60 2 docker exec dataflow-ci-postgres \
    pg_isready -U dataflow -d dataflow \
    || { docker logs dataflow-ci-postgres || true; exit 1; }
}

start_postgres_cdc() {
  # wal_level=logical needs Dockerfile.postgres-cdc, and `docker build` pulls
  # the FROM ref directly with no retry — a shared-quota 429 on
  # public.ecr.aws killed api-and-web/cdc-sqlserver before tests ran.
  # Pull through pull_image first (backoff + docker.io fallback), tag to the
  # FROM ref so the build resolves it from the local daemon.
  # The pinned pgvector image (Dockerfile.postgres-cdc) has no second
  # registry; pull_image still retries it with backoff before the build.
  local base="docker.io/pgvector/pgvector:0.8.1-pg16" pulled wal
  pulled=$(pull_image "$base" "$base")
  [ "$pulled" = "$base" ] || docker tag "$pulled" "$base"
  docker build -t dataflow-postgres-cdc:ci \
    -f apps/api/docker/Dockerfile.postgres-cdc apps/api/docker
  docker rm -f dataflow-postgres-cdc >/dev/null 2>&1 || true
  docker run -d --name dataflow-postgres-cdc \
    -e POSTGRES_USER=dataflow \
    -e POSTGRES_PASSWORD=dataflow \
    -e POSTGRES_DB=dataflow \
    -p 5432:5432 \
    dataflow-postgres-cdc:ci
  wait_for "Postgres CDC" 30 2 docker exec dataflow-postgres-cdc \
    pg_isready -U dataflow \
    || { docker logs dataflow-postgres-cdc || true; exit 1; }
  wal=$(docker exec dataflow-postgres-cdc psql -U dataflow -d dataflow -Atc "SHOW wal_level")
  echo "wal_level=${wal}"
  [ "${wal}" = "logical" ]
}

# sqlcmd inside the container. SQLCMDPASSWORD keeps the password off every
# command line: `docker exec -e NAME` copies the value from the environment.
mssql_sqlcmd() {
  SQLCMDPASSWORD="${MSSQL_SA_PASSWORD}" docker exec -e SQLCMDPASSWORD dataflow-sqlserver \
    /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C -b "$@"
}

mssql_agent_running() {
  mssql_sqlcmd -h -1 -W -Q "SET NOCOUNT ON; SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%'" \
    | grep -qx Running
}

start_sqlserver() {
  if [ -z "${MSSQL_SA_PASSWORD:-}" ]; then
    echo "MSSQL_SA_PASSWORD is empty: the workflow must generate it first" >&2
    exit 1
  fi
  local image
  image=$(pull_image "$MSSQL_IMAGE" "$MSSQL_IMAGE")
  docker rm -f dataflow-sqlserver >/dev/null 2>&1 || true
  # -e NAME (no value) copies the runner's variable, so it never appears in
  # the process list or the log.
  docker run -d --name dataflow-sqlserver \
    -e ACCEPT_EULA=Y \
    -e MSSQL_SA_PASSWORD \
    -e MSSQL_PID=Developer \
    -e MSSQL_AGENT_ENABLED=true \
    -p 1433:1433 \
    "$image"
  wait_for "SQL Server" 60 5 mssql_sqlcmd -Q "SELECT 1" \
    || { docker logs dataflow-sqlserver || true; exit 1; }
  docker cp apps/api/tests/sqlserver_ct_init.sql dataflow-sqlserver:/tmp/ct_init.sql
  docker cp apps/api/tests/sqlserver_cdc_init.sql dataflow-sqlserver:/tmp/cdc_init.sql
  mssql_sqlcmd -i /tmp/ct_init.sql
  # Native CDC capture jobs run under SQL Server Agent. Without it the live
  # capture-job tests cannot pass, so a missing Agent fails here, not later.
  wait_for "SQL Server Agent" 30 2 mssql_agent_running \
    || { docker logs dataflow-sqlserver || true; exit 1; }
  mssql_cdc_init
}

# sp_cdc_enable_table creates the capture job through msdb (sp_cdc_add_job).
# Agent can read "Running" in sys.dm_server_services and still refuse job
# operations while it starts: error 14258 "Cannot perform this operation while
# SQLServerAGENT is starting", surfaced as Msg 22832 (PR 424 CI). The init is
# idempotent, so retry it on exactly those errors, for at most 120 s.
mssql_cdc_init() {
  local deadline=$((SECONDS + 120)) out
  until out=$(mssql_sqlcmd -i /tmp/cdc_init.sql 2>&1); do
    if ! grep -qE "14258|22832" <<<"$out"; then
      echo "$out" >&2
      echo "SQL Server CDC init failed" >&2
      exit 1
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "$out" >&2
      echo "SQL Server Agent did not accept CDC job operations within 120 s (error 14258)" >&2
      docker logs --tail 80 dataflow-sqlserver || true
      exit 1
    fi
    echo "SQL Server Agent is still starting (14258/22832); retrying CDC init in 5 s"
    sleep 5
  done
  echo "$out"
}

for svc in "$@"; do
  case "$svc" in
    postgres) start_postgres ;;
    postgres-cdc) start_postgres_cdc ;;
    mysql) start_mysql ;;
    mongo) start_mongo ;;
    gcs) start_gcs ;;
    adls) start_adls ;;
    bigquery) start_bigquery ;;
    redis) start_redis ;;
    sqlserver) start_sqlserver ;;
    *) echo "unknown service: $svc" >&2; exit 2 ;;
  esac
done
