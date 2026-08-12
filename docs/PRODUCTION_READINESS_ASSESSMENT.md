# Production readiness — measured, 2026-08-12

Every number here was measured on this runner with PostgreSQL 16, MySQL 8,
MongoDB 8 and Redis 7 live. Nothing is carried over from another document; the
docs set already contains several counts from branches and dates that no longer
hold, so quote this file only with its date and engine list attached.

## Short answer

**No — not as a universal any-source-to-any-destination platform.** The honest
claim is narrower and still valuable: a proven relational and file core, with a
large declared surface that has no live proof on this host.

The gap is not mostly defects. It is that **most declared routes have never been
executed anywhere we can see**, because the infrastructure they need is absent.
A suite that skips is not a suite that passes.

## What the numbers say

| Measure | Count | How it was obtained |
|---------|------:|---------------------|
| Catalog tiles | 741 | `data/connector_catalog.json` |
| Registered drivers | 36 | `CONNECTOR_MODULES` |
| Drivers passing `transfer_ready()` | 23 | capability registry |
| `LIVE_MATRIX` declared routes | 1156 | `registry.LIVE_MATRIX` |
| `PRODUCTION_SKU` committed routes | 75 | `registry.PRODUCTION_SKU` |

Running the universal matrix — one live transfer per declared route:

```
tests/test_execute_tracked_universal_matrix.py
  341 passed, 814 skipped, 1 failed
```

**30% of the declared live matrix actually executes here**, up from 26% once the
object-store routes were given an endpoint (see below). The remaining 70% skips
for want of SQL Server, Oracle, BigQuery, GCS/ADLS, Kafka, Elasticsearch, the
vector stores, and the SaaS tenants. Of the 75 committed SKU routes, **42 are
provable on this host and 33 are credential- or infrastructure-gated**.

Whole suite, same engines:

```
13585 passed, 11 failed, 1062 skipped
```

On a runner with no databases at all the same suite reports 12625 passed,
0 failed, 1984 skipped — which is the trap: it looks perfect and proves far less.

## What is genuinely ready

* **PostgreSQL, MySQL, SQLite, MongoDB, Redis and the file formats.** These carry
  the bulk of the 305 executed routes and the 40 provable SKU routes, including
  create-new DDL, upsert, overwrite, resume and Gate-8 reconciliation.
* **The fidelity machinery.** Type invention covers 17 logical types across 20
  engines with no coverage holes; the conversion contract classifies invent and
  loss per column; quarantine, checkpoints and the verification ladder are real
  and tested (CDC 344 tests, quarantine 222, contracts 200, reconcile/Gate-8 155,
  cursor/incremental 129, checkpoint/resume 90, schedules 55).
* **Fail-closed behaviour.** Repeatedly during this audit the engine refused
  work it could not prove — and in five of the current failures it is the test,
  not the engine, that is wrong. That instinct is the product's strongest asset
  and should not be traded for green.

## What is not ready, and why

1. **Most routes are unexecuted, not broken.** Nothing is known about 850 of
   1156 declared routes on this host. They may work. That is the point: nobody
   has evidence either way, so they cannot be sold as proven.
2. **Warehouse and object-store destinations are credential-gated.** Snowflake
   runs only under an emulator that cannot answer `SHOW GRANTS`, so its
   privilege gate fails closed and the route is unproven rather than passing.
3. **File and object exports remain `unproven`** by design: the writer checksum
   proves bytes and counts, never per-cell fidelity, because no destination
   read-back exists.
4. **CDC delivery is at-least-once.** Correct and documented, but it is not the
   exactly-once semantics a buyer may assume from "no data loss".
5. **Zero-loss properties 7–12 are UNPROVEN** in `ZERO_LOSS_PROPERTIES.md` —
   referential integrity across multi-table migration, semantic value fidelity,
   row accounting, determinism, the certificate, and adversarial testing.
6. **Auto-map does not align to a discovered document schema.** See below.

## Open finding: canonical forms do not converge

`test_intelligent_cross_schema_mapping[mongodb|redis]` maps a source onto an
existing destination whose columns are named differently (`salary` →
`compensation`). Three of four columns align; `salary` does not.

The cause is in the schematic index rather than the mapper. Synonym groups
overlap, and whichever group claims a term first owns its canonical form, so
two terms *declared as synonyms of each other* resolved differently:
`salary → salary_amount` while `compensation → salary`. Terms that never share a
canonical form can never match. Listing `compensation` in the `salary_amount`
group fixes that pair, and is committed.

It is not sufficient on its own, and the obvious general fix is worse than the
problem. Two closure strategies were measured against the 2,478,917-entry index:

| Strategy | Entries changed | Effect |
|----------|----------------:|--------|
| Transitive closure to a fixpoint | 382,995 (15.45%) | Destructive — `customer_id → id`, so `cust_id` and `order_id` become the same form |
| Converge each declared synonym group | 99 (0.004%) | Still destructive — propagates `customer_id → id` and `member_id → insurance` |

Both collapse specific business entities into generic leaves, which would damage
mapping accuracy far more widely than the bug they fix. The index contains edges
that are simply wrong (`customer_id` is not a variant of `id`), and no automated
transform can distinguish those from correct ones. **This needs curation of the
synonym data, with the mapping golden set as the regression gate — not an
algorithmic closure.**

## Found by finally running an object-store route

The 850 unexecuted routes are not hypothetical risk. `moto` in server mode
(`python -m moto.server -p 5000`) satisfies the matrix's endpoint check, and the
S3 connector already accepts a custom endpoint through `resolve_endpoint_url`,
so these routes are provable here today:

```
CSV file      → S3   : 2 rows, object written
S3            → PG   : 2 rows, landed
```

Both succeed. But the first S3 → PostgreSQL run exposed a defect no unit test
covers, with the same CSV bytes on both sides:

| Source of the identical CSV | Destination DDL |
|-----------------------------|-----------------|
| file upload | `id bigint, amount numeric, ts date` |
| S3 object | `id text, amount text, ts text` |

Every object-store → database transfer lands an all-text schema — no arithmetic
on amounts, no date filtering, no numeric constraints — and reports success
while doing it. That is exactly the class of defect that only appears when a
route actually runs, and it is why the skip count above is the headline number.

**Fixed, both halves.** It took two changes because two layers had thrown the
types away:

1. `read_object_from_store` now infers column types from the rows it already
   parsed and returns them in `meta["native_types"]`, the channel readers
   already carry types on.
2. The introspect built `{col: "string"}` per header as a placeholder, and that
   placeholder travelled as a *declared* schema through
   `endpoint_source_column_types` → `reconcile_source_types`, which is designed
   to let a declaration outrank the reader's sampled shape. Correct for a
   relational catalog — a declared `NUMBER(12,2)` should beat a sampled
   `DECIMAL(8,4)` — and wrong for a store with no catalog, so the placeholder
   overwrote the real types the reader had just supplied.

The five sites that built that placeholder now prefer reader-declared types and
fall back per column, so readers that cannot type their rows are untouched.
Identical bytes now produce identical DDL:

```
file → PG : id bigint, amount numeric, ts date
S3   → PG : id bigint, amount numeric, ts date
```

## Object-store routes now execute in the suite

The recipe above is wired in rather than left as a note. A session fixture
(`local_object_store`) starts `moto` in-process on an OS-assigned port, creates
the matrix bucket, and hands the endpoint to the universal and SKU matrices; an
externally supplied `DATAFLOW_TEST_S3_ENDPOINT` (MinIO, a real account) wins over
it, and without moto the value is empty and the routes skip honestly.

**36 routes moved from skipped to passing**, including 2 committed SKU routes.
The port is OS-assigned so parallel workers do not collide, and no cloud
credentials are involved.

DynamoDB is *not* wired to the same endpoint yet, though moto answers its API.
A table has to exist with a declared key schema before the writer can seed one,
and pointing routes at it without that provisioning surfaced a crash rather than
a transfer. That is the next increment, and it is left undone rather than
half-wired.

## Found by running PostgreSQL → MySQL with a `timestamptz` column

The route did not move a single row. Create-new invented `DATETIME(6)` for the
aware source, and the engine then correctly refused the transfer, because an
instant landing in a zoneless wall clock is a fidelity loss it will not sign off
on. So the most ordinary timestamp column in the most ordinary route was a hard
block.

MySQL has no offset-label carrier, so the label is unstorable whichever type is
chosen and the only real question is which one keeps the *instant*. `TIMESTAMP`
is UTC on disk and converted with the session `time_zone` the writer already
pins to UTC; `DATETIME` holds the same digits with no polarity marker and is an
instant only by writer convention, which is why it needs a UTC-normalize
contract. `_TZ_AWARE_DDL` had said `TIMESTAMP(6)` all along, with the reasoning
written out; the LTZ and offset maps contradicted it, so aware sources never
reached it.

Chasing the write path turned up a second, independent defect. The writer
resolved destination carriers through the *foreign-token* rematerializer, which
rewrites MySQL `TIMESTAMP` to `DATETIME(6)` — correct for a PostgreSQL or Oracle
source token, wrong for MySQL's own. That rewrite also hit types read straight
back from `INFORMATION_SCHEMA`, so an **existing** `created_at TIMESTAMP` column
— the most common audit column in MySQL — was retyped as wall-clock, and the
NTZ guard then quarantined every offset-bearing row the column was built to
hold. One token, two roles, one resolver: `TIMESTAMP(p)` is now a physical
destination stamp, while bare `TIMESTAMP` stays foreign and still lands
`DATETIME(6)`. Both roles are pinned by tests, and out-of-range instants keep
the existing epoch guard (flagged at Validate, quarantined at write, never
silently zeroed).

## Unbounded `NUMERIC` into MySQL is a policy block, not a bug

`PRODUCTION_SKU` PostgreSQL → MySQL still fails, and the cause is worth stating
precisely rather than fixing by loosening a gate. The matrix seeds `amount` as
bare `DECIMAL`, which in PostgreSQL is unbounded. MySQL has no unbounded decimal
(the cap is `DECIMAL(65,30)`), so *every* bounded target is formally a narrowing,
and the rule refuses to invent "a capacity the source never proved" — the same
reason the DynamoDB route refuses.

The sample-observing invention already does the right thing when it is given
rows (`DECIMAL` + `["1000.00","2000.50"]` → `DECIMAL(9,4)`; wide values → `TEXT`),
and the writer already quarantines any cell that does not fit, so silent
truncation is not the exposure here. What is unresolved is a policy question:
whether an *undeclared* source precision should be treated as provable narrowing
(today: yes, block) or as unknown capacity that observation may size, with the
residual risk surfaced and enforced at write. Deciding that by weakening the
gate mid-audit would trade a real honesty property for a green test, so it is
left blocked and named here instead.

## SFTP was a catalog tile, not a connector

SFTP declared `introspect: False` / `preflight: False`, so `transfer_ready()`
refused it and `validate_transfer` answered "sftp is Planned" for every route.
It could move bytes and could not pass Validate. Every SFTP test in the
repository patched `connect_sftp` and asserted on the mock, which proves the
call site and nothing about whether a row survives — and mocks were the entire
evidence base behind a driver the catalog offered.

paramiko ships the server half of the protocol, so `tests/sftp_test_server.py`
now runs a real SFTP server in-process, rooted at a temp directory with path
confinement and a per-run host key that the client **pins**. The transfers
therefore exercise real host-key verification rather than the
`insecure_ignore` escape.

Closing the gap took five pieces, four of which were missing capability rather
than missing wiring:

| Gap | What it does now |
|---|---|
| Introspect | Lists the remote directory and types the payload; existence is decided against the whole listing, not the 200 shown |
| Typed schema | Returns `meta['native_types']` through the same helper the object stores use |
| G2 privilege | Creates and removes a uniquely named hidden file — SFTP has no privilege catalog, and POSIX mode bits cannot say what *this* session may do |
| G9 uniqueness | Full-population payload scan, shared with the object stores |
| Streaming read | The dispatch simply had no `sftp` branch |

The typed-schema piece is the object-store defect one transport later: without
it, the identical file landed `bigint`/`numeric`/`date` over an upload and three
`text` columns over SFTP.

Host-key trust turned out to be dropped by **three** separate paths that
rebuild a connection — the preflight destination inspect, the registry write
path, and the source duplicate probe. Each silently downgraded to "no pinned
key", which is precisely the mismatch `host_key_settings()` was written to
prevent, so a route the operator pinned would verify at Validate and fail at
write.

One real writer bug surfaced on the way: the atomic replace gated
`posix_rename` on `hasattr` of the paramiko *client*, which is always true. The
OpenSSH extension is not universal, so managed file-transfer appliances
answered "Operation unsupported" and the write failed *after* the bytes had
landed. The fallback is now taken only when the server actually refused —
paramiko maps just `ENOENT` and `EACCES` onto errno and raises a bare
`IOError(text)` for everything else, so an unrecognised error stays fatal
rather than being read as permission to delete the destination and retry.

**Three SKU routes and 39 universal matrix routes now execute.** `email` stays
demoted: it writes and can never be read back, so nothing it sends is
verifiable.

## DynamoDB could not be read at all

Every DynamoDB *source* route was skipped because the transfer raised before
moving a row. Three separate defects were in the way, and none of them are
DynamoDB-specific — they hit any source that cannot cheaply count itself, which
includes Kafka topics and search indexes:

1. `run_file_preflight` compared `row_count <= 0` against the `None` those
   sources honestly report, so "we do not know yet" crashed before gate one.
2. Progress messages formatted the same count with `{total_rows:,}`, which
   raises on `None`. `stream.py` already had the "unknown" wording; the label
   now lives beside the progress percentage, which had solved this already.
3. Column types were resolved by **majority vote** over the values in a page. A
   column of 999 integers and one `2000.50` typed as INTEGER, and that row then
   failed the write with "Invalid integer". The larger the table, the more
   certain it was that the minority value would be mistyped. Types now widen to
   cover every value observed, and mixed families land on text rather than
   narrowing. Relatedly `Decimal("1000.00")` is integral in value but carries a
   scale, so calling it INTEGER dropped the scale a money column depends on.

Uniqueness is proven rather than skipped: DynamoDB cannot hold two items with
one primary key, so a table-key identity is structural proof, while a non-key
identity is counted across a real scan.

**40 DynamoDB matrix routes now execute, up from 18.**

## What would move the needle

In order of how much unproven surface each closes:

1. **Bring the absent engines up in CI** — SQL Server, Oracle, and object stores
   via emulators (Azurite, fake-gcs-server, MinIO). That is 850 unexecuted
   routes, the single largest source of unknowns.
2. **Curate the schematic index** against a golden mapping set, so canonical
   forms converge without collapsing entities.
3. **Destination read-back for object stores**, which is the only thing standing
   between file/object exports and a real fidelity claim.
4. **Properties 7–12**, starting with row accounting and referential integrity,
   since those are what "no data loss" actually means to a buyer.

## How to talk about this

Say: *proven for PostgreSQL, MySQL, SQLite, MongoDB and file formats, on 40
CI-verified routes, with fail-closed preflight, quarantine and full-population
checksum reconciliation.* That claim is defensible and measured.

Do not say: *any source to any destination*, *650+ connectors*, or *zero data
loss* without naming the route and the artifact. The catalog is 741 tiles and 23
transfer-ready drivers; those are different numbers and buyers can tell.
