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
#   oracle — Oracle Database 23ai Free (gvenzl/oracle-free slim-faststart: the
#              real database engine, not an emulator), switched to ARCHIVELOG
#              with supplemental logging, then tests/oracle_ci_init.sql creates
#              the PDB login DATAFLOW and the LogMiner common user C##DATAFLOW.
#              Their password is read from DATAFLOW_ORACLE_PASSWORD (per run,
#              masked by the workflow), passed on stdin only. Oracle Free Use
#              Terms permit development and testing use such as CI.
#   kafka  — apache/kafka KRaft single node (broker+controller), PLAINTEXT on
#              9092 and SASL_PLAINTEXT/PLAIN on 9094; the SASL password is read
#              from KAFKA_SASL_CI_PASSWORD.
# Emulator greens are labelled emulator_not_customer_tenant by the lab; they
# are not customer-tenant certificates.
# Usage: start_ci_databases.sh [mysql] [mongo] [gcs] [adls] [bigquery] [redis] [postgres] [sqlserver] [oracle] [kafka]
set -euo pipefail

POSTGRES_IMAGE="${POSTGRES_IMAGE:-public.ecr.aws/docker/library/postgres:16}"
MYSQL_IMAGE="${MYSQL_IMAGE:-public.ecr.aws/docker/library/mysql:8.0}"
MONGO_IMAGE="${MONGO_IMAGE:-public.ecr.aws/docker/library/mongo:7}"
GCS_IMAGE="${GCS_IMAGE:-docker.io/fsouza/fake-gcs-server:latest}"
AZURITE_IMAGE="${AZURITE_IMAGE:-mcr.microsoft.com/azure-storage/azurite:latest}"
BIGQUERY_IMAGE="${BIGQUERY_IMAGE:-ghcr.io/goccy/bigquery-emulator:latest}"
REDIS_IMAGE="${REDIS_IMAGE:-public.ecr.aws/docker/library/redis:7-alpine}"
MSSQL_IMAGE="${MSSQL_IMAGE:-mcr.microsoft.com/mssql/server:2022-latest}"
# Pinned: a new Oracle release must not change what CI proves unannounced.
# ghcr.io first (no Docker Hub anonymous-pull quota), Docker Hub as fallback.
ORACLE_IMAGE="${ORACLE_IMAGE:-ghcr.io/gvenzl/oracle-free:23.26.4-slim-faststart}"
ORACLE_IMAGE_FALLBACK="${ORACLE_IMAGE_FALLBACK:-docker.io/gvenzl/oracle-free:23.26.4-slim-faststart}"
KAFKA_IMAGE="${KAFKA_IMAGE:-docker.io/apache/kafka:3.9.0}"

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
  # mysqld restarts once during first-boot init (see mysql_serving); the
  # grants below then need the real server.
  wait_for "MySQL" 60 2 mysql_serving \
    || { docker logs dataflow-ci-mysql || true; exit 1; }
  docker exec dataflow-ci-mysql mysql -h 127.0.0.1 -u root -pdataflow -e "
    GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'dataflow'@'%';
    GRANT ALL PRIVILEGES ON dataflow.* TO 'dataflow'@'%';
    FLUSH PRIVILEGES;"
  docker exec dataflow-ci-mysql mysql -h 127.0.0.1 -u root -pdataflow -Ne \
    "SHOW VARIABLES WHERE Variable_name IN ('local_infile','log_bin','binlog_format')"
  # MySQL 8 generates a CA and server certificate on first boot. The TLS
  # tests (test_r14_mysql_ssl_mode_form_options / _tls_mode_truth) verify
  # against it: export its path for the job's later steps.
  if [ -n "${GITHUB_ENV:-}" ]; then
    local ca="${RUNNER_TEMP:-/tmp}/dataflow-ci-mysql-ca.pem"
    docker cp dataflow-ci-mysql:/var/lib/mysql/ca.pem "$ca"
    echo "DATAFLOW_TEST_MYSQL_CA=${ca}" >> "$GITHUB_ENV"
  fi
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
  wait_for "Mongo" 40 2 mongo_serving \
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
  wait_for "Redis" 30 2 redis_serving \
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
  # The emulator does not serve /; bigquery_serving reads the tests' dataset.
  wait_for "BigQuery" 30 2 bigquery_serving \
    || { docker logs dataflow-bigquery-emulator || true; exit 1; }
  docker inspect dataflow-bigquery-emulator \
    --format 'emulator restarts={{.RestartCount}} state={{.State.Status}} started={{.State.StartedAt}}'
}

# The postgres image first runs its init scripts on a temporary server that
# listens on the Unix socket only, and POSTGRES_DB does not exist yet; then it
# restarts the real server. pg_isready on the socket answers during that first
# phase, so the next query failed with 'database "dataflow" does not exist'
# (PR 426 and PR 422 CI). Ready means: the real server answers a query on
# POSTGRES_DB over TCP, which the temporary server never listens on.
pg_serving() {  # pg_serving <container>
  docker exec -e PGPASSWORD=dataflow "$1" psql -h 127.0.0.1 -U dataflow -d dataflow -Atqc "SELECT 1" >/dev/null 2>&1
}

# The same rule for the other engines: ready means the tests' database answers,
# as the tests' login, the way the tests reach it.
# MySQL's image also inits on a temporary server (--skip-networking) before
# MYSQL_DATABASE / MYSQL_USER exist, then restarts: TCP reaches only the real one.
mysql_serving() {
  docker exec -e MYSQL_PWD=dataflow dataflow-ci-mysql \
    mysql -h 127.0.0.1 -u dataflow -D dataflow -Nse "SELECT 1" >/dev/null 2>&1
}

# The replica-set URI the tests use (MONGO_URI), on a writable primary.
mongo_serving() {
  docker exec dataflow-ci-mongo mongosh --quiet "mongodb://localhost:27017/dataflow?replicaSet=rs0" \
    --eval 'quit(db.runCommand({ping: 1}).ok === 1 && db.hello().isWritablePrimary ? 0 : 1)' >/dev/null 2>&1
}

# redis-cli exits 0 on an error reply (e.g. "LOADING ..."): require PONG on db 0.
redis_serving() {
  docker exec dataflow-ci-redis redis-cli -h 127.0.0.1 -n 0 ping 2>/dev/null | grep -qx PONG
}

# The tests' database, created by the CT init, not just master.
mssql_serving() {
  mssql_sqlcmd -d dataflow -Q "SELECT 1" >/dev/null 2>&1
}

# The dataset the tests write to (--dataset=dataflow), not just the API.
bigquery_serving() {
  curl -fsS http://127.0.0.1:9050/bigquery/v2/projects/dataflow-test/datasets/dataflow >/dev/null 2>&1
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
  wait_for "Postgres" 60 2 pg_serving dataflow-ci-postgres \
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
  wait_for "Postgres CDC" 30 2 pg_serving dataflow-postgres-cdc \
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
  wait_for "SQL Server dataflow database" 30 2 mssql_serving \
    || { docker logs --tail 80 dataflow-sqlserver || true; exit 1; }
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

# sqlplus as SYSDBA inside the Oracle container; the script comes on stdin.
oracle_sysdba() {
  docker exec -i -u oracle dataflow-ci-oracle bash -lc "sqlplus -s / as sysdba"
}

# The image's own health check: the CDB and FREEPDB1 are open.
oracle_healthy() {
  docker exec dataflow-ci-oracle healthcheck.sh >/dev/null 2>&1
}

# The tests' login, the way the tests reach it (TCP listener, PDB service).
oracle_serving() {
  printf 'WHENEVER SQLERROR EXIT 1\nCONNECT DATAFLOW/"%s"@//127.0.0.1:1521/FREEPDB1\nSELECT 1 FROM DUAL;\nEXIT\n' \
    "$DATAFLOW_ORACLE_PASSWORD" \
    | docker exec -i -u oracle dataflow-ci-oracle bash -lc "sqlplus -s /nolog" >/dev/null 2>&1
}

start_oracle() {
  if [ -z "${DATAFLOW_ORACLE_PASSWORD:-}" ]; then
    echo "DATAFLOW_ORACLE_PASSWORD is empty: the workflow must generate it first" >&2
    exit 1
  fi
  local image started=$SECONDS
  image=$(pull_image "$ORACLE_IMAGE" "$ORACLE_IMAGE_FALLBACK")
  echo "Oracle image pulled in $((SECONDS - started)) s: ${image}"
  docker rm -f dataflow-ci-oracle >/dev/null 2>&1 || true
  # ORACLE_PASSWORD (SYS/SYSTEM) is required by the image; -e NAME copies the
  # value from this process's environment so it never reaches a command line.
  ORACLE_PASSWORD="$DATAFLOW_ORACLE_PASSWORD" docker run -d --name dataflow-ci-oracle \
    --shm-size=1g \
    -e ORACLE_PASSWORD \
    -p 1521:1521 \
    "$image"
  wait_for "Oracle" 120 2 oracle_healthy \
    || { docker logs --tail 80 dataflow-ci-oracle || true; exit 1; }
  echo "Oracle open in $((SECONDS - started)) s"
  # LogMiner needs archived redo: ARCHIVELOG mode is set at MOUNT, so the
  # instance restarts once. Archived logs go to a fixed local destination.
  docker exec -u oracle dataflow-ci-oracle mkdir -p /opt/oracle/oradata/FREE/archivelogs
  oracle_sysdba <<'SQL'
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SYSTEM SET log_archive_dest_1='LOCATION=/opt/oracle/oradata/FREE/archivelogs' SCOPE=SPFILE;
SHUTDOWN IMMEDIATE
STARTUP MOUNT
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
EXIT
SQL
  wait_for "Oracle (ARCHIVELOG)" 60 2 oracle_healthy \
    || { docker logs --tail 80 dataflow-ci-oracle || true; exit 1; }
  docker cp apps/api/tests/oracle_ci_init.sql dataflow-ci-oracle:/tmp/oracle_ci_init.sql
  docker exec -u root dataflow-ci-oracle chmod 0644 /tmp/oracle_ci_init.sql
  # The password reaches sqlplus as a substitution variable on stdin only.
  printf 'SET VERIFY OFF\nDEFINE ci_pw = "%s"\n@/tmp/oracle_ci_init.sql\n' "$DATAFLOW_ORACLE_PASSWORD" \
    | oracle_sysdba \
    || { echo "Oracle CI init (tests/oracle_ci_init.sql) failed" >&2; exit 1; }
  wait_for "Oracle DATAFLOW@FREEPDB1" 30 2 oracle_serving \
    || { docker logs --tail 80 dataflow-ci-oracle || true; exit 1; }
  oracle_sysdba <<'SQL'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0
SELECT 'log_mode=' || log_mode || ' supplemental_min=' || supplemental_log_data_min FROM v$database;
SELECT 'version=' || banner_full FROM v$version WHERE ROWNUM = 1;
EXIT
SQL
  echo "Oracle ready for the tests in $((SECONDS - started)) s"
}

kafka_serving() {  # kafka_serving <bootstrap> [command-config]
  docker exec dataflow-ci-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "$1" ${2:+--command-config "$2"} >/dev/null 2>&1
}

start_kafka() {
  if [ -z "${KAFKA_SASL_CI_PASSWORD:-}" ]; then
    echo "KAFKA_SASL_CI_PASSWORD is empty: the workflow must set it first" >&2
    exit 1
  fi
  local image started=$SECONDS jaas
  image=$(pull_image "$KAFKA_IMAGE" "$KAFKA_IMAGE")
  docker rm -f dataflow-ci-kafka >/dev/null 2>&1 || true
  # apache/kafka maps KAFKA_* env to server.properties. The SASL listener's
  # JAAS line carries the password, so it is passed by name (-e NAME).
  jaas="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"${KAFKA_SASL_CI_PASSWORD}\" user_admin=\"${KAFKA_SASL_CI_PASSWORD}\" user_dataflow=\"${KAFKA_SASL_CI_PASSWORD}\";"
  KAFKA_LISTENER_NAME_SASL_PLAIN_SASL_JAAS_CONFIG="$jaas" docker run -d --name dataflow-ci-kafka --network host \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
    -e KAFKA_LISTENERS=PLAINTEXT://127.0.0.1:9092,SASL://127.0.0.1:9094,CONTROLLER://127.0.0.1:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092,SASL://127.0.0.1:9094 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_SASL_ENABLED_MECHANISMS=PLAIN \
    -e KAFKA_LISTENER_NAME_SASL_PLAIN_SASL_JAAS_CONFIG \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
    "$image"
  wait_for "Kafka (PLAINTEXT :9092)" 60 2 kafka_serving 127.0.0.1:9092 \
    || { docker logs --tail 80 dataflow-ci-kafka || true; exit 1; }
  printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="%s";\n' \
    "$KAFKA_SASL_CI_PASSWORD" | docker exec -i dataflow-ci-kafka sh -c 'cat > /tmp/sasl.properties'
  wait_for "Kafka (SASL_PLAINTEXT :9094)" 30 2 kafka_serving 127.0.0.1:9094 /tmp/sasl.properties \
    || { docker logs --tail 80 dataflow-ci-kafka || true; exit 1; }
  echo "Kafka ready in $((SECONDS - started)) s"
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
    oracle) start_oracle ;;
    kafka) start_kafka ;;
    *) echo "unknown service: $svc" >&2; exit 2 ;;
  esac
done
