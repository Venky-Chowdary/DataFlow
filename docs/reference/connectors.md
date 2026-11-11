# Connector capability matrix

This page says what each connector can do. The matrix is generated from the
code and from the CI configuration, so it cannot drift:
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
- **"Engine in CI" says whether CI starts that engine**, not which tests pass.
  An emulator is not the real service. "Fixture only" means the connector has
  only been exercised against canned responses.
- One function answers for all of this: `get_connector_capability` in
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
| Declared status | The product's own label for the driver. `not certified` means read and write exist but the driver is not offered as transfer-ready. | `capability_label`, `effective_status` |
| Engine in CI | What the CI workflows start for this engine. | `.github/workflows/`, `apps/api/requirements.txt` |

"Needs cursor" is the gate's rule, not proof that every such source filters by
the cursor on the server. The code has no per-connector list for that.

## Matrix

<!-- BEGIN GENERATED: connector-matrix (docs/tools/gen_connector_matrix.py) -->

### Counts

These numbers count different things. Quote the first one as "connectors that can run a transfer".

| Count | Value | What it counts |
|---|---|---|
| Transfer-live drivers | 46 | Unique driver and file-format types that can run a transfer on this server right now (two-way, destination-only, file and first-class source-only), excluding fixture-only SDK sources. The public count. |
| Transfer-ready catalog drivers | 35 | Unique drivers behind those tiles. Smaller than transfer_live_drivers because several tiles share one driver and file formats are not tiles here. |
| Transfer-ready catalog ids | 89 | Catalog ids marked transfer-ready, hosted twins included. |
| CDC source engines | 5 | Registry entries the Validate gate accepts as a CDC source. (mongodb, mysql, oracle, postgresql, sqlserver) |
| Catalog tiles | 647 | All tiles in the connector catalog. By status: beta: 30, live: 124, planned: 493. |
| Catalog tiles by tier | - | certified: 121, planned: 493, source_only: 33. Tiles whose own record says transfer-ready: 121 (a per-tile answer; the frozen id list above is the shorter one). |

### Drivers

| Connector | Read | Write | Incremental | CDC source | CDC sink | Upsert | Merge strategy | Create-privilege probe | Declared status | Engine in CI |
|---|---|---|---|---|---|---|---|---|---|---|
| `adls` | yes | yes | re-read, no cursor | no | no | no | - | yes | Full transfer (live) | emulator container, PR workflow |
| `airtable` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `asana` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `bigcommerce` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `bigquery` | yes | yes | needs cursor | no | no | yes | merge | yes | Full transfer (live) | emulator container, PR workflow |
| `brevo` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `calendly` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `clickup` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `close` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `confluence` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `couchbase` | yes | no | needs cursor | no | no | n/a | - | n/a | Source only (live) | not started by any workflow |
| `dynamodb` | yes | yes | needs cursor | no | no | yes | - | yes | Full transfer (live) | in-process emulator (moto) |
| `elasticsearch` | yes | yes | needs cursor | no | no | yes | - | yes | Full transfer (live) | not started by any workflow |
| `email` | no | yes | n/a | no | no | no | - | no | Roadmap (planned) | not started by any workflow |
| `freshdesk` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `freshservice` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `gcs` | yes | yes | re-read, no cursor | no | no | no | - | yes | Full transfer (live) | emulator container, PR workflow |
| `generic_sql` | yes | yes | needs cursor | no | no | yes | - | yes | Full transfer (live) | not started by any workflow |
| `github` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `gitlab` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `hubspot` | yes | yes | needs cursor | no | no | yes | - | no | Full transfer (live) | not started by any workflow |
| `iceberg` | yes | yes | needs cursor | no | no | yes | - | yes | Full transfer (live) | not started by any workflow |
| `influxdb` | yes | no | needs cursor | no | no | n/a | - | n/a | Source only (live) | not started by any workflow |
| `intercom` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `jira` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `kafka` | yes | yes | needs cursor | no | yes | no | - | yes | Full transfer (live) | live container, nightly workflow only |
| `klaviyo` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `linear` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `mailchimp` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `mailjet` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `milvus` | no | yes | n/a | no | no | yes | - | yes | Destination only (live) | not started by any workflow |
| `monday` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `mongodb` | yes | yes | needs cursor | yes | yes | yes | - | yes | Full transfer (live) | live container, PR workflow |
| `mysql` | yes | yes | needs cursor | yes | yes | yes | insert_on_duplicate_key | yes | Full transfer (live) | live container, PR workflow |
| `neo4j` | yes | no | needs cursor | no | no | n/a | - | n/a | Source only (live) | not started by any workflow |
| `notion` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `oracle` | yes | yes | needs cursor | yes | yes | yes | merge | yes | Full transfer (live) | opt-in job, skipped unless ENABLE_ORACLE_CDC is set, PR workflow |
| `pgvector` | yes | yes | needs cursor | no | yes | yes | - | yes | Full transfer (live) | live container (PostgreSQL image with pgvector), PR workflow |
| `pinecone` | no | yes | n/a | no | no | yes | - | yes | Destination only (live) | not started by any workflow |
| `pipedrive` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `postgresql` | yes | yes | needs cursor | yes | yes | yes | insert_on_conflict | yes | Full transfer (live) | live container, PR workflow |
| `posthog` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `qdrant` | yes | yes | needs cursor | no | yes | yes | - | yes | Full transfer (live) | not started by any workflow |
| `redis` | yes | yes | needs cursor | no | no | yes | - | yes | Full transfer (live) | live container, PR workflow |
| `redshift` | yes | yes | needs cursor | no | no | yes | delete_insert_stage | yes | Roadmap (planned); not certified | not started by any workflow |
| `rest_api` | yes | no | needs cursor | no | no | n/a | - | n/a | Source only (live) | not started by any workflow |
| `s3` | yes | yes | re-read, no cursor | no | no | no | - | yes | Full transfer (live) | in-process emulator (moto) |
| `salesforce` | yes | yes | needs cursor | no | no | yes | - | no | Full transfer (live) | not started by any workflow |
| `sendgrid` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `servicenow` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `sftp` | yes | yes | re-read, no cursor | no | no | yes | - | yes | Full transfer (live) | not started by any workflow |
| `shopify` | yes | yes | needs cursor | no | no | no | - | no | Roadmap (planned); not certified | not started by any workflow |
| `singer_tap` | yes | no | needs cursor | no | no | n/a | - | n/a | Source only (live) | not started by any workflow |
| `slack` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `snowflake` | yes | yes | needs cursor | no | no | yes | merge | yes | Full transfer (live) | in-process emulator (fakesnow) |
| `sqlite` | yes | yes | needs cursor | no | no | yes | insert_on_conflict | yes | Full transfer (live) | no service needed (file engine) |
| `sqlserver` | yes | yes | needs cursor | yes | yes | yes | merge_holdlock | yes | Full transfer (live) | live container, PR workflow |
| `stripe` | yes | yes | needs cursor | no | no | no | - | no | Source certified (live); destination not certified | not started by any workflow |
| `todoist` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `typeform` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `weaviate` | no | yes | n/a | no | no | yes | - | yes | Destination only (live) | not started by any workflow |
| `woocommerce` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `zendesk` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |
| `zoho_crm` | yes | no | no (declared) | no | no | n/a | - | n/a | Source only (beta) | fixture only (`synthetic-fixture`) |

### File formats

| Connector | Read | Write | Incremental | CDC source | CDC sink | Upsert | Merge strategy | Create-privilege probe | Declared status | Engine in CI |
|---|---|---|---|---|---|---|---|---|---|---|
| `avro` | yes | yes | re-read, no cursor | no | no | no | - | no | File transfer (live) | no service needed (file fixtures) |
| `csv` | yes | yes | re-read, no cursor | no | no | no | - | no | File transfer (live) | no service needed (file fixtures) |
| `docx` | yes | no | re-read, no cursor | no | no | n/a | - | n/a | File transfer (live) | no service needed (file fixtures) |
| `excel` | yes | yes | re-read, no cursor | no | no | no | - | no | File transfer (live) | no service needed (file fixtures) |
| `fixed_width` | yes | yes | re-read, no cursor | no | no | no | - | no | File transfer (live) | no service needed (file fixtures) |
| `html` | yes | no | re-read, no cursor | no | no | n/a | - | n/a | File transfer (live) | no service needed (file fixtures) |
| `json` | yes | yes | re-read, no cursor | no | no | no | - | no | File transfer (live) | no service needed (file fixtures) |
| `jsonl` | yes | yes | re-read, no cursor | no | no | no | - | no | File transfer (live) | no service needed (file fixtures) |
| `ndjson` | yes | yes | re-read, no cursor | no | no | no | - | no | File transfer (live) | no service needed (file fixtures) |
| `orc` | yes | yes | re-read, no cursor | no | no | no | - | no | File transfer (live) | no service needed (file fixtures) |
| `parquet` | yes | yes | re-read, no cursor | no | no | no | - | no | File transfer (live) | no service needed (file fixtures) |
| `pdf` | yes | no | re-read, no cursor | no | no | n/a | - | n/a | File transfer (live) | no service needed (file fixtures) |
| `tsv` | yes | yes | re-read, no cursor | no | no | no | - | no | File transfer (live) | no service needed (file fixtures) |
| `xml` | yes | yes | re-read, no cursor | no | no | no | - | no | File transfer (live) | no service needed (file fixtures) |
| `yaml` | yes | yes | re-read, no cursor | no | no | no | - | no | File transfer (live) | no service needed (file fixtures) |

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
- **Email** is a send-only destination labelled Roadmap. It is not offered as
  transfer-ready.
- **Redshift and Shopify** have read and write code but are marked not
  certified, so they are not offered as transfer-ready.
- **SDK SaaS sources** (Airtable, Asana, GitHub, Jira, Notion, Slack, Zendesk
  and others) are read-only, declare no incremental read, and carry the
  evidence label `synthetic-fixture`.

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

- Which tests pass for each connector. This page reads CI configuration, not
  CI results.
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
- `.github/workflows/ci.yml`, `.github/workflows/full-suite.yml`,
  `.github/actions/ci-services/action.yml`, `.github/scripts/start_ci_databases.sh`
- `apps/api/tests/test_connector_capability_single_source.py`
