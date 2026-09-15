#!/usr/bin/env bash
# Start the CI databases that need a command line (GitHub `services:` cannot
# pass one), mirroring docker-compose.yml so CI proves the same topology the
# product ships against:
#   mysql — ROW binlog + GTID (CDC) and local_infile=1 (LOAD DATA LOCAL COPY path)
#   mongo — single-node replica set rs0 (snapshot read concern, change streams)
#   gcs   — fake-gcs-server (desktop lab postgresql->gcs execute cell)
#   adls  — Azurite blob (desktop lab postgresql->adls execute cell)
#   bigquery — goccy/bigquery-emulator (warehouse SKU proof); re-runnable:
#              an existing container is replaced with a fresh catalog
# Emulator greens are labelled emulator_not_customer_tenant by the lab; they
# are not customer-tenant certificates.
# Usage: start_ci_databases.sh [mysql] [mongo] [gcs] [adls] [bigquery]
set -euo pipefail

MYSQL_IMAGE="${MYSQL_IMAGE:-public.ecr.aws/docker/library/mysql:8.0}"
MONGO_IMAGE="${MONGO_IMAGE:-public.ecr.aws/docker/library/mongo:7}"
GCS_IMAGE="${GCS_IMAGE:-docker.io/fsouza/fake-gcs-server:latest}"
AZURITE_IMAGE="${AZURITE_IMAGE:-mcr.microsoft.com/azure-storage/azurite:latest}"
BIGQUERY_IMAGE="${BIGQUERY_IMAGE:-ghcr.io/goccy/bigquery-emulator:latest}"

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
}

start_adls() {
  AZURITE_IMAGE=$(pull_image "$AZURITE_IMAGE" "$AZURITE_IMAGE")
  docker run -d --name dataflow-ci-azurite -p 10000:10000 "$AZURITE_IMAGE" \
    azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --loose --skipApiVersionCheck
  # Azurite answers any path with a well-formed HTTP error once it is listening.
  wait_for "Azurite" 30 2 curl -s -o /dev/null http://127.0.0.1:10000/ \
    || { docker logs dataflow-ci-azurite || true; exit 1; }
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

for svc in "$@"; do
  case "$svc" in
    mysql) start_mysql ;;
    mongo) start_mongo ;;
    gcs) start_gcs ;;
    adls) start_adls ;;
    bigquery) start_bigquery ;;
    *) echo "unknown service: $svc" >&2; exit 2 ;;
  esac
done
