# Live matrix runbook

Most of the transfer suite skips itself when no database is listening. On a bare
runner that reads as a green suite, and it is how a set of real fidelity defects
stayed invisible: the tests that would have caught them had never executed.

Bringing the engines up moved **901 tests from skipped to executed** and surfaced
52 failures on paths that had reported nothing.

## Bring the engines up without Docker

Docker is unavailable in some runners; native packages work and the suite only
looks for listening ports.

```bash
sudo apt-get install -y postgresql postgresql-16-pgvector mysql-server redis-server
sudo service postgresql start && sudo service mysql start && sudo service redis-server start

# MongoDB ships outside the Ubuntu archive; mongod runs fine without systemd.
# Raise the descriptor limit first: MongoDB asks for 64000, and started from a
# shell with the default 1024 the server accepts connections for a while and
# then dies mid-suite with "Too many open files". That surfaces as a wave of
# Mongo failures plus a jump in skips, which reads like an engine regression.
sudo -u mongodb bash -c 'ulimit -n 64000; /usr/bin/mongod --dbpath /var/lib/mongodb \
  --logpath /var/log/mongodb/mongod.log --bind_ip 127.0.0.1 --port 27017 --fork'
```

Two credential conventions are both in use, so provision both or the suite
half-skips:

| Engine | User / password | Used by |
|--------|-----------------|---------|
| PostgreSQL | `dataflow` / `dataflow` | `docker-compose.yml` defaults |
| PostgreSQL | `postgres` / `admin` | `P2_PG_*` test defaults |
| MySQL | `dataflow` / `dataflow` | both |
| MySQL | `root` / `dataflow` | duplicate-key probe suites |

```bash
sudo -u postgres psql -c "CREATE USER dataflow WITH PASSWORD 'dataflow' SUPERUSER;"
sudo -u postgres psql -c "CREATE DATABASE dataflow OWNER dataflow;"
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'admin';"
sudo mysql -e "CREATE DATABASE IF NOT EXISTS dataflow;
  CREATE USER IF NOT EXISTS 'dataflow'@'localhost' IDENTIFIED BY 'dataflow';
  GRANT ALL PRIVILEGES ON *.* TO 'dataflow'@'localhost' WITH GRANT OPTION;
  -- Ubuntu's root authenticates over the unix socket, which pymysql cannot use.
  ALTER USER 'root'@'localhost' IDENTIFIED WITH caching_sha2_password BY 'dataflow';
  FLUSH PRIVILEGES;"
```

## Run it

```bash
redis-cli flushall          # see below
cd apps/api && python -m pytest tests -q -n 4 --dist loadfile
```

The suite does not isolate Redis between runs, and the destination keyspace
probe refuses to bind Map types against a prefix left over from an earlier run
rather than guess at their JSON types. After a few runs that surfaces as a wave
of `keyspace probe failed` errors on every `* → redis` route, which look like
engine regressions and are not. Flushing first is deliberate rather than
automatic: a conftest that wiped a developer's Redis would be worse than the
confusion it prevents.

Parallel is safe: each xdist worker gets its own fakesnow DuckDB catalog through
`FAKESNOW_DB_PATH`, since DuckDB permits a single writer per file.

## Reading a skip

A skip is an unproven combination, not a passing one. Aggregate the reasons
before quoting any coverage number:

```bash
python -m pytest tests -q -n 4 --dist loadfile -rs 2>&1 \
  | rg '^SKIPPED' | sed 's/^SKIPPED \[[0-9]*\] //' | sort | uniq -c | sort -rn
```

## Route families in `LIVE_MATRIX`

Declared routes by family — what the matrix *claims*, which is the denominator
for any coverage statement:

| Family | Routes | Family | Routes |
|--------|-------:|--------|-------:|
| db → db | 324 | db → object | 54 |
| file → db | 180 | object → db | 54 |
| db → file | 180 | file → warehouse | 30 |
| file → file | 100 | warehouse → file | 30 |
| warehouse → db | 54 | file → object | 30 |
| db → warehouse | 54 | object → file | 30 |
| object → warehouse | 9 | object → object | 9 |
| warehouse → object | 9 | warehouse → warehouse | 9 |

Total 1,156. `PRODUCTION_SKU` commits 75 of these to CI proof.

## Measured state (2026-08-12, this runner)

```
13575 passed, 12 failed, 1062 skipped
```

A fifteenth, `test_live_cross_engine_confirm_moves_every_row_intact[postgres_to_mysql]`,
fails intermittently under `-n 4` and passes in isolation: the Pilot wave93
fixtures share MySQL table names across workers. Re-run it alone before treating
it as a defect.

The remaining failures are live-path defects that the skipping suite never
reported, not regressions:

| Cluster | Tests | Note |
|---------|------:|------|
| Document / vector destinations | 7 | Mongo and Redis cross-schema mapping, Mongo→Snowflake ×2, pgvector read-back ×2, Redis overwrite |
| `test_typed_fidelity_transfer_matrix_e2e` | 2 | PG → Snowflake (fakesnow has no `SHOW GRANTS`), PG → MySQL TZ collapse |
| Single-test clusters | 3 | PG→MySQL SKU, PG/MySQL/Mongo matrix, Pilot→Mongo confirm |

### Where the engine is right and the expectation is not

* **PG → MySQL typed fidelity** fails on `timestamptz → DATETIME(6)`. MySQL has
  no timezone-aware type, so the conversion contract classes it
  `needs_user_approval` and Execute demands a signed Risk Contract. That is
  `MIGRATION_SCENARIO_MATRIX` GAP-7, not a defect in the write path.
* **PG → Snowflake** fails because fakesnow does not implement `SHOW GRANTS`,
  so the privilege probe cannot prove CREATE and fails closed. Relaxing that
  would weaken a real check on real Snowflake for an emulator's convenience.
* **pgvector per-column read-back** cannot exist: the table is a fixed vector
  schema (`id` / `content` / `embedding` / `metadata`), so mapped columns are
  JSONB payload rather than columns to select. A vector sink's honest assurance
  is `writer_ack`.

* **PostgreSQL `TIMESTAMP` → Mongo `date`** is a real collapse: BSON `date` is
  milliseconds since epoch, so a microsecond-precision source cannot round-trip.
  The remaining Pilot → Mongo confirm failure is this, and it wants a signed
  Risk Contract rather than a code change. (`DECIMAL(p,s)` into Mongo is no
  longer flagged — see the Decimal128 fix below.)

Anything quoted from this file must name the runner and date: these counts are
environment-specific, and the docs set already contains several stale ones.
