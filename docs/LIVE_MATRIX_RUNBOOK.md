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
sudo -u mongodb /usr/bin/mongod --dbpath /var/lib/mongodb \
  --logpath /var/log/mongodb/mongod.log --bind_ip 127.0.0.1 --port 27017 --fork
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
cd apps/api && python -m pytest tests -q -n 4 --dist loadfile
```

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
13535 passed, 27 failed, 1063 skipped
```

The remaining failures cluster as follows. They are live-path defects that the
skipping suite never reported, not regressions:

| Cluster | Tests | Note |
|---------|------:|------|
| `test_pilot_transfer_wave92` / `wave93` | 10 | Pilot NL plan/confirm against live PG, MySQL, Mongo |
| `test_typed_fidelity_transfer_matrix_e2e` | 3 | PG → Snowflake / MySQL / DuckDB typed carry |
| `test_source_duplicate_probe_live` | 2 | MySQL duplicate-key preflight |
| `test_production_sku_matrix` | 2 | PG → MySQL, PG → pgvector |
| `test_execute_tracked_schema_mapping_matrix` | 2 | Mongo, Redis cross-schema mapping |
| single-test clusters | 8 | Redis overwrite, Mongo→Snowflake, MySQL widen, locale dates, emulator pgvector |

Anything quoted from this file must name the runner and date: these counts are
environment-specific, and the docs set already contains several stale ones.
