#!/usr/bin/env bash
# Start the CI databases that need a command line (GitHub `services:` cannot
# pass one), mirroring docker-compose.yml so CI proves the same topology the
# product ships against:
#   mysql — ROW binlog + GTID (CDC) and local_infile=1 (LOAD DATA LOCAL COPY path)
#   mongo — single-node replica set rs0 (snapshot read concern, change streams)
# Usage: start_ci_databases.sh [mysql] [mongo]
set -euo pipefail

MYSQL_IMAGE="${MYSQL_IMAGE:-public.ecr.aws/docker/library/mysql:8.0}"
MONGO_IMAGE="${MONGO_IMAGE:-public.ecr.aws/docker/library/mongo:7}"

wait_for() {  # wait_for <label> <retries> <sleep> <cmd...>
  local label=$1 retries=$2 pause=$3; shift 3
  for _ in $(seq 1 "$retries"); do
    if "$@" >/dev/null 2>&1; then echo "${label} ready"; return 0; fi
    sleep "$pause"
  done
  echo "${label} failed to become ready" >&2
  return 1
}

start_mysql() {
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

for svc in "$@"; do
  case "$svc" in
    mysql) start_mysql ;;
    mongo) start_mongo ;;
    *) echo "unknown service: $svc" >&2; exit 2 ;;
  esac
done
