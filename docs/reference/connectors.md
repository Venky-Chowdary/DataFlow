# Connector capability matrix

This page says what each connector can do and which connectors CI has proven.
The matrix is generated from the code and from the committed CI evidence, so
it cannot drift:
`apps/api/tests/test_docs_connector_matrix.py` fails when the page and the code
disagree.

To regenerate, run from the repository root with the API's Python environment:

```
python docs/tools/gen_connector_matrix.py
```

## Read this first

- **A catalog tile is not a working connector.** The catalog lists many more
  names than there are drivers. Most tiles are marked planned. The "Counts"
  table below gives the exact figures at this commit.
- **The matrix shows declared capabilities**: what a server reports when every
  driver package is installed. A server that is missing a package reports
  less. `GET /api/v1/catalog/connectors` shows the live view for your server.
- **Certified means proven in CI.** A connector is Certified only when a CI
  job that proves its engine ran green with none of that engine's tests
  skipped. A connector that works but has no such run is "Implemented, not yet
  proven". What the CI workflows are configured to start never counts.
- **An emulator is not Certified.** Snowflake and BigQuery are proven in CI
  against emulators (fakesnow and the goccy BigQuery emulator), not real cloud
  accounts. Their tier reads "Proven on an emulator only, not on a real
  account" here and in the product.
- The CI evidence is the same file the product's public claims are generated
  from: `apps/api/data/connector_ci_evidence.json`. It names the CI run it came
  from (see the Counts table). A newer run changes this page only after that
  file is regenerated.
- One function answers for capabilities (CDC, upsert, merge):
  `get_connector_capability` in
  `apps/api/services/connector_capability_registry.py`.

## How to read the columns

| Column | Meaning | Source in code |
|---|---|---|
| Read, Write | The driver can act as a source, as a destination. | `_DRIVER_CAPS` and `_FILE_CAPS` in `src/transfer/connector_capabilities.py` |
| Incremental | The rule Validate applies. `needs cursor`: an incremental mode is refused unless the stream names a cursor column. `re-read, no cursor`: a file or object source is read again in full and matched on its key. `no (declared)`: the driver declares it cannot read incrementally. | `services/preflight_cursor_gate.py`, the driver's `incremental` field |
| CDC source | The source can drive change data capture. This is exactly the list Validate enforces. | `cdc_source_supported` in `services/preflight_cursor_gate.py` |
| CDC sink | The registry declares that this destination can receive a CDC stream. Declared, not derived from a gate. | `supports_cdc_sink` in the capability record |
| Upsert | The destination supports keyed upsert. | capability record |
| Merge strategy | The SQL statement family used for a keyed write. `-` means the dialect has no entry (non-SQL stores use their own keyed write). | `connectors/merge_registry.py` |
| Create-privilege probe | Validate measures whether the connector's account may write and create the destination object, from metadata, without writing a row. `no` means the check falls back to connectivity. | `_SUPPORTED` in `services/destination_privilege_probe.py` |
| Tier | The tier the driver's own catalog tile gets: Certified, Implemented (not yet proven), Source only, Connection test only, or Planned. `beta, tested against fixtures` means a source exercised only against canned responses. `not offered as transfer-ready` means read and write code exists but the driver is held back. | `certification_tier` and `enrich_catalog_entry` in `src/transfer/connector_capabilities.py` |
| CI evidence | Whether a CI job that proves the engine ran green. When the evidence is recorded under another engine (pgvector under `postgresql`, file formats under `files`), that engine is named. | `proof_for` in `services/connector_proof.py`, reading `data/connector_ci_evidence.json` |

"Needs cursor" is the gate's rule, not proof that every such source filters by
the cursor on the server. The code has no per-connector list for that.

## Matrix

<!-- BEGIN GENERATED: connector-matrix (docs/tools/gen_connector_matrix.py) -->

### Counts

These numbers count different things. Quote the first one as "connectors that can run a transfer".

| Count | Value | What it counts |
|---|---|---|
| Transfer-live drivers | 46 | Unique driver and file-format types that can run a transfer on this server right now (two-way, destination-only, file and first-class source-only), excluding fixture-only SDK sources. The public count. |
| CI-proven drivers | 22 | Transfer-live drivers whose proving CI job ran green with none of their tests skipped (avro, bigquery, csv, excel, fixed_width, json, jsonl, mongodb, mysql, ndjson, oracle, orc, parquet, pgvector, postgresql, redis, snowflake, sqlite, sqlserver, tsv, xml, yaml). The other 24 are implemented but not yet proven. Evidence: CI run 44932873967, recorded 2026-11-21. |
| Transfer-ready catalog drivers | 35 | Unique drivers behind those tiles. Smaller than transfer_live_drivers because several tiles share one driver and file formats are not tiles here. |
| Transfer-ready catalog ids | 82 | Catalog ids marked transfer-ready, hosted twins included. |
| CDC source engines | 5 | Registry entries the Validate gate accepts as a CDC source. (mongodb, mysql, oracle, postgresql, sqlserver) |
| Catalog tiles | 647 | All tiles in the connector catalog. By status: beta: 30, live: 111, planned: 506. |
| Catalog tiles by tier | - | certified: 69, implemented: 39, planned: 506, source_only: 33. Tiles whose own record says transfer-ready: 108 (a per-tile answer; the frozen id list above is the shorter one). |

### Drivers

| Connector | Read | Write | Incremental | CDC source | CDC sink | Upsert | Merge strategy | Create-privilege probe | Tier | CI evidence |
|---|---|---|---|---|---|---|---|---|---|---|
| `adls` | yes | yes | re-read, no cursor | no | no | no | - | yes | Implemented, not yet proven | not proven: no CI job proves it |
| `airtable` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `asana` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `bigcommerce` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `bigquery` | yes | yes | needs cursor | no | no | yes | merge | yes | Proven on an emulator only, not on a real account | proven (against an emulator) |
| `brevo` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `calendly` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `clickup` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `close` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `confluence` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `couchbase` | yes | no | needs cursor | no | no | n/a | - | n/a | Source only | not proven: no CI job proves it |
| `dynamodb` | yes | yes | needs cursor | no | no | yes | - | yes | Implemented, not yet proven | not proven: no CI job proves it |
| `elasticsearch` | yes | yes | needs cursor | no | no | yes | - | yes | Implemented, not yet proven | not proven: no CI job proves it |
| `email` | no | yes | n/a | no | no | no | - | no | Planned | not proven: no CI job proves it |
| `freshdesk` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `freshservice` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `gcs` | yes | yes | re-read, no cursor | no | no | no | - | yes | Implemented, not yet proven | not proven: no CI job proves it |
| `generic_sql` | yes | yes | needs cursor | no | no | yes | - | yes | Implemented, not yet proven | not proven: no CI job proves it |
| `github` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `gitlab` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `hubspot` | yes | yes | needs cursor | no | no | yes | - | no | Implemented, not yet proven | not proven: no CI job proves it |
| `iceberg` | yes | yes | needs cursor | no | no | yes | - | yes | Implemented, not yet proven | not proven: no CI job proves it |
| `influxdb` | yes | no | needs cursor | no | no | n/a | - | n/a | Source only | not proven: no CI job proves it |
| `intercom` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `jira` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `kafka` | yes | yes | needs cursor | no | yes | no | - | yes | Implemented, not yet proven | not proven: no result recorded for the proving job |
| `klaviyo` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `linear` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `mailchimp` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `mailjet` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `milvus` | no | yes | n/a | no | no | yes | - | yes | Implemented, not yet proven | not proven: no CI job proves it |
| `monday` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `mongodb` | yes | yes | needs cursor | yes | yes | yes | - | yes | Certified | proven |
| `mysql` | yes | yes | needs cursor | yes | yes | yes | insert_on_duplicate_key | yes | Certified | proven |
| `neo4j` | yes | no | needs cursor | no | no | n/a | - | n/a | Source only | not proven: no CI job proves it |
| `notion` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `oracle` | yes | yes | needs cursor | yes | yes | yes | merge | yes | Certified | proven |
| `pgvector` | yes | yes | needs cursor | no | yes | yes | - | yes | Certified | proven; engine `postgresql` |
| `pinecone` | no | yes | n/a | no | no | yes | - | yes | Implemented, not yet proven | not proven: no CI job proves it |
| `pipedrive` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `postgresql` | yes | yes | needs cursor | yes | yes | yes | insert_on_conflict | yes | Certified | proven |
| `posthog` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `qdrant` | yes | yes | needs cursor | no | yes | yes | - | yes | Implemented, not yet proven | not proven: no CI job proves it |
| `redis` | yes | yes | needs cursor | no | no | yes | - | yes | Certified | proven |
| `redshift` | yes | yes | needs cursor | no | no | yes | delete_insert_stage | yes | Planned; not offered as transfer-ready | not proven: no CI job proves it |
| `rest_api` | yes | no | needs cursor | no | no | n/a | - | n/a | Source only | not proven: no CI job proves it |
| `s3` | yes | yes | re-read, no cursor | no | no | no | - | yes | Implemented, not yet proven | not proven: no CI job proves it |
| `salesforce` | yes | yes | needs cursor | no | no | yes | - | no | Implemented, not yet proven | not proven: no CI job proves it |
| `sendgrid` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `servicenow` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `sftp` | yes | yes | re-read, no cursor | no | no | yes | - | yes | Implemented, not yet proven | not proven: no CI job proves it |
| `shopify` | yes | yes | needs cursor | no | no | no | - | no | Planned; not offered as transfer-ready | not proven: no CI job proves it |
| `singer_tap` | yes | no | needs cursor | no | no | n/a | - | n/a | Source only | not proven: no CI job proves it |
| `slack` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `snowflake` | yes | yes | needs cursor | no | no | yes | merge | yes | Proven on an emulator only, not on a real account | proven (against an emulator) |
| `sqlite` | yes | yes | needs cursor | no | no | yes | insert_on_conflict | yes | Certified | proven |
| `sqlserver` | yes | yes | needs cursor | yes | yes | yes | merge_holdlock | yes | Certified | proven |
| `stripe` | yes | yes | needs cursor | no | no | no | - | no | Implemented, not yet proven; destination role not offered as transfer-ready | not proven: no CI job proves it |
| `todoist` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `typeform` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `weaviate` | no | yes | n/a | no | no | yes | - | yes | Implemented, not yet proven | not proven: no CI job proves it |
| `woocommerce` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `zendesk` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |
| `zoho_crm` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only; beta, tested against fixtures | not proven: no CI job proves it |

### File formats

| Connector | Read | Write | Incremental | CDC source | CDC sink | Upsert | Merge strategy | Create-privilege probe | Tier | CI evidence |
|---|---|---|---|---|---|---|---|---|---|---|
| `avro` | yes | yes | re-read, no cursor | no | no | no | - | no | Certified | proven; engine `files` |
| `csv` | yes | yes | re-read, no cursor | no | no | no | - | no | Certified | proven; engine `files` |
| `docx` | yes | no | re-read, no cursor | no | no | n/a | - | n/a | Implemented, not yet proven | not proven: no CI job proves it |
| `excel` | yes | yes | re-read, no cursor | no | no | no | - | no | Certified | proven; engine `files` |
| `fixed_width` | yes | yes | re-read, no cursor | no | no | no | - | no | Certified | proven; engine `files` |
| `html` | yes | no | re-read, no cursor | no | no | n/a | - | n/a | Implemented, not yet proven | not proven: no CI job proves it |
| `json` | yes | yes | re-read, no cursor | no | no | no | - | no | Certified | proven; engine `files` |
| `jsonl` | yes | yes | re-read, no cursor | no | no | no | - | no | Certified | proven; engine `files` |
| `ndjson` | yes | yes | re-read, no cursor | no | no | no | - | no | Certified | proven; engine `files` |
| `orc` | yes | yes | re-read, no cursor | no | no | no | - | no | Certified | proven; engine `files` |
| `parquet` | yes | yes | re-read, no cursor | no | no | no | - | no | Certified | proven; engine `files` |
| `pdf` | yes | no | re-read, no cursor | no | no | n/a | - | n/a | Implemented, not yet proven | not proven: no CI job proves it |
| `tsv` | yes | yes | re-read, no cursor | no | no | no | - | no | Certified | proven; engine `files` |
| `xml` | yes | yes | re-read, no cursor | no | no | no | - | no | Certified | proven; engine `files` |
| `yaml` | yes | yes | re-read, no cursor | no | no | no | - | no | Certified | proven; engine `files` |

<!-- END GENERATED: connector-matrix -->

## Notes on specific connectors

- **CDC delivery is at-least-once.** A destination applies changes as keyed
  upserts. Nothing in the product claims exactly-once delivery.
- **CDC sources are five engines**: PostgreSQL, MySQL, MongoDB, Oracle and SQL
  Server, plus the hosted names of those engines that the gate lists. A product
  that only speaks the same wire protocol (TimescaleDB, CockroachDB, Citus,
  Greenplum, YugabyteDB) is judged by its own name and is not a CDC source.
- **Kafka, pgvector and Qdrant** are not CDC sources. Their registry entries
  declare that they can receive a CDC stream. For pgvector and Qdrant the
  entries say: at-least-once, deletes by document key, no log-position guard.
- **Redis TTL.** Key expiry (TTL / EXPIRE) is not preserved as a migration
  guarantee. Values transfer; Validate shows this as a warning on any route
  with Redis on either side. The Redis-to-Redis server-side copy path carries a
  TTL when the source key has one, but the row-by-row path does not. Set
  expiry in a step after the load if you need it.
- **Timestamps.** Before comparing source and destination, verification rounds
  the source timestamp to the precision the destination column can store. The
  rule is applied to both sides and only removes digits the destination cannot
  hold. Where the destination's precision is not known, the value is compared
  unrounded and any difference fails verification.
- **Email** is a send-only destination and is Planned. It is not offered as
  transfer-ready.
- **Redshift and Shopify** have read and write code but are Planned: they are
  not offered as transfer-ready.
- **Wire-compatible products are not proven by the engine they imitate.**
  MariaDB, DocumentDB, Azure Cosmos DB, the hosted Redis services, Oracle
  Autonomous Warehouse, SingleStore, CockroachDB, TiDB and similar tiles are
  Planned until CI evidence is recorded under their own id.
- **OpenSearch, Amazon OpenSearch Service and Redis Cluster** are Planned: the
  Elasticsearch 8 client refuses an OpenSearch server, and the Redis driver
  does not follow cluster redirects.
- **SDK SaaS sources** (Airtable, Asana, GitHub, Jira, Notion, Slack, Zendesk
  and others) are read-only, declare no incremental read, and carry the
  evidence label `synthetic-fixture`.

## SQL Server TLS and Entra ID

SQL Server connectors take these options (top level or under `extra`; the
Connectors form shows them as text fields). Code:
`apps/api/connectors/mssql_security.py`.

| Option | Values |
|---|---|
| `sqlserver_driver` | `auto` (default): Microsoft ODBC Driver 18/17 when installed, else pymssql (FreeTDS), unless the host sets `DATAFLOW_MSSQL_REQUIRE_VERIFIED_TLS=1`, which turns a missing driver into an error. `odbc`: require the Microsoft driver. `freetds`: use FreeTDS on purpose; **TLS certificate not verified**, no Entra ID. |
| `authentication` | `sql` (default). `entra_service_principal`: username is the application (client) id, password the client secret. `entra_managed_identity`: username empty (system-assigned) or a user-assigned identity's client id; no password. `entra_password`: an Entra user's name and password. `entra_access_token`: an access token for `https://database.windows.net/` in the password field, passed as `SQL_COPT_SS_ACCESS_TOKEN`; tokens expire in about an hour. The ODBC spellings (`ActiveDirectoryServicePrincipal`, `ActiveDirectoryMsi`, `ActiveDirectoryPassword`) are accepted. All Entra modes need the Microsoft driver. |
| `encrypt` | Sent as `yes` when unset (Driver 17's own default is `no`). `strict` needs Driver 18. `no` / `optional` only when declared. |
| `trust_server_certificate`, `server_certificate`, `hostname_in_certificate` | Unchanged: skip verification (declared), pin a certificate, or match an alias host name. |

Gate-8 read-back, the verification ladder and target samples follow the same
choice: they use the Microsoft driver whenever it is installed (unless the
connector chose `freetds`) and whenever the connector requires encryption, a
pinned certificate or Entra ID, and the SQL Server-to-SQL Server fast path
never retries a failed verified handshake over FreeTDS. A `mssql+pymssql://`
connection string is refused under `MSSQL_REQUIRE_VERIFIED_TLS`,
`sqlserver_driver=odbc` or Entra ID, like a host/port connector. Read-backs
send no default `sa` user (a system-assigned managed identity has none). Proven by unit tests (`tests/test_nf2_sqlserver_tls_and_entra.py`);
Entra ID sign-in has not been run against a tenant.

## Known inconsistencies

- The catalog id `dynamics365` resolves to the `s3` driver, because driver
  resolution falls back to a substring match. The tile is planned and not
  selectable, so no transfer can use it, but the capability record for that id
  is wrong.
- Two different figures are called "transfer-ready": the frozen id list
  (`TRANSFER_READY_CATALOG_IDS`) and the per-tile answer from
  `enrich_catalog_entry`. Both are shown in the Counts table with what each
  counts.

## Not covered

- Results newer than the committed CI evidence. The page shows the run named
  in the Counts table, not the latest CI run.
- Performance, volume limits and rate limits per connector.
- Live certification against real cloud accounts. No evidence for it is in the
  repository at this commit.
- Whether the destination table is created by each writer. The privilege probe
  column is the closest fact the code declares.
- Hosted variants one by one. They resolve to the driver shown here.

## Verified against

- `apps/api/src/transfer/connector_capabilities.py`
- `apps/api/services/connector_capability_registry.py`
- `apps/api/services/preflight_cursor_gate.py`, `apps/api/services/preflight_transfer_policy.py`
- `apps/api/connectors/merge_registry.py`
- `apps/api/services/destination_privilege_probe.py`
- `apps/api/services/policy_gate_notes.py`, `apps/api/services/copy_redis_redis.py` (Redis TTL)
- `apps/api/services/carrier_instant.py` (timestamp rounding)
- `apps/api/data/connector_catalog.json`
- `apps/api/data/connector_ci_evidence.json`, `apps/api/services/connector_proof.py`,
  `apps/api/services/connector_ci_evidence.py`, `apps/api/scripts/connector_claims_report.py`
- `apps/api/tests/test_connector_capability_single_source.py`
