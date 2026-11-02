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

## Object stores without a cloud account

S3 routes are provable locally. `moto` in server mode listens on a port, which
satisfies the matrix's reachability check, and the S3 connector already accepts
a custom endpoint through `resolve_endpoint_url`:

```bash
python -m moto.server -p 5000 &
```

Point the endpoint at it — `connection_string` carries the URL, `database` the
bucket, `table` the object key:

```python
EndpointConfig(kind="database", format="s3", host="127.0.0.1", port=5000,
               database="dfbucket", table="path/to/object.csv",
               username="test", password="test",
               connection_string="http://127.0.0.1:5000")
```

The matrices no longer need this started by hand: the `local_object_store`
session fixture runs `moto` in-process on an OS-assigned port and creates the
bucket, so S3 routes execute by default and skip honestly when moto is absent.
Set `DATAFLOW_TEST_S3_ENDPOINT` to point at MinIO or a real account instead.

This is how the all-text schema defect was found — object-store routes had never
executed, so nothing reported that the same CSV typed differently depending on
where it was read from. Azurite and fake-gcs-server would give ADLS and GCS the
same treatment, and DynamoDB needs its table provisioned with a key schema
before moto can serve it.

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
13632 passed, 11 failed, 1024 skipped
```

A fifteenth, `test_live_cross_engine_confirm_moves_every_row_intact[postgres_to_mysql]`,
fails intermittently under `-n 4` and passes in isolation: the Pilot wave93
fixtures share MySQL table names across workers. Re-run it alone before treating
it as a defect.

The remaining failures are live-path defects that the skipping suite never
reported, not regressions:

| Cluster | Tests | Note |
|---------|------:|------|
| Document / vector destinations | 6 | Mongo and Redis cross-schema mapping, Mongo→Snowflake ×2, pgvector read-back ×2 |
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
* **PostgreSQL bare `NUMERIC` → DynamoDB** is the same case as the MySQL one
  below: the matrix source is an unqualified `DECIMAL`, PostgreSQL creates it
  unbounded, and DynamoDB's `N` holds 38 significant digits. It only became
  visible once DynamoDB routes were given an endpoint to run against.
* **PostgreSQL bare `NUMERIC` → MySQL** is refused for the same reason. The
  matrix seeds `amount` as an unqualified `DECIMAL`, which PostgreSQL creates as
  `numeric` with no precision — unbounded to 131072 digits — while MySQL tops
  out at `DECIMAL(65,30)` and the invent is `DECIMAL(38,15)`. Passing this would
  mean sizing the carrier from sampled values, which is exactly the invent the
  declared-domain rule forbids, so the honest outcome is a Risk Contract.

### Open: reading DynamoDB as a source raises before any row moves

`dynamodb → *` routes fail with `unsupported format string passed to
NoneType.__format__`, recorded against the preflight phase. Writing to DynamoDB
works, and `read_source_database` against the same table returns rows and types
when called directly, so the defect is in the route rather than the driver — a
`None` reaches a numeric format specifier (`{x:,}`) somewhere between them. The
matrix skips these routes with that reason rather than reporting a red engine
for a fault nobody has located yet.

### Open: auto-map does not align to a discovered document schema

`test_intelligent_cross_schema_mapping[mongodb|redis]` pre-creates the
destination with different column names (`salary` → `compensation`) and expects
the auto-mapper to align to them. It does for SQL destinations; for MongoDB and
Redis the mapping stays identity, so the write is refused with "physical DDL
missing for mapped column(s) salary".

The fields are discoverable — `introspect_schema("mongodb", …)` against the
seeded collection returns `id`, `name`, `compensation`, `active` with inferred
types — so this is a document destination's schema not reaching the mapper
rather than a missing capability. Same shape as the pgvector introspect and the
Pilot existence gaps already fixed, and the likely fix is plumbing rather than
new logic.

Anything quoted from this file must name the runner and date: these counts are
environment-specific, and the docs set already contains several stale ones.


## Track C addendum — key-addressed expectations at scale

`docs/SCALE_MATRIX_NOSQL.md` records the >= 100K sweep for the non-relational
and analytical engines. Two rules that runbook readers keep getting wrong:

- `full_refresh_append` into a **key-addressed** destination lands `N`, not
  `2N`. The write target is the key (`HSET <prefix>:<id>`, `PutItem`, an
  `_id`-addressed upsert), so a second run rewrites the same keys. Take the
  classification from `services.primary_key.KEY_ADDRESSED_DESTS` — the canonical
  owner — and pass `key_addressed=True` to
  `tests.sync_mode_probe.expected_rows`; do not keep a local list.
- Redis and DynamoDB have no server-side cursor predicate, so
  `incremental_append` from them is filtered client-side by
  `services.sync_cursor`. A cell that lands `2N` there is replay, which is a
  defect, not an addressing quirk.

The `dynamodb → *` preflight `NoneType.__format__` fault recorded above did not
reproduce in this sweep; `dynamodb→postgresql` and `dynamodb→mysql` were
measured in all four modes.
