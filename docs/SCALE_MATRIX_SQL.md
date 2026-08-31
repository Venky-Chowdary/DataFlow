# Track A — SQL engine duplex scale matrix (PostgreSQL, MySQL, SQL Server, SQLite, Oracle)

Measured evidence for the relational engines as **both source and destination**, run
through the real transfer engine (`src.transfer.engine.UniversalTransferEngine.execute_tracked`).
No bypass path, no writer acknowledgement used as proof: every destination number in
this document comes from a **separate driver connection** doing `COUNT(*)` plus an
order-independent checksum over the mapped projection, compared against a checksum
computed independently from the fixture generator.

## How to re-run

```bash
cd apps/api
DATAFLOW_SCALE_MATRIX=1 PYTHONPATH=. python -m tests.scale.run_matrix \
  --rows 100000 --out /tmp/matrix.json --markdown /tmp/matrix.md
```

The harness is env-gated: without `DATAFLOW_SCALE_MATRIX=1` it exits without touching
a database, and an engine whose service is not reachable is reported `skip` with the
exact connection error rather than silently dropped. Useful flags:
`--rows`, `--engines`, `--modes`, `--data-shapes`, `--blocking-shapes`, `--prefix`,
`--out`, `--markdown`.

## Fixture

One wide typed table, deterministic per row index (`apps/api/tests/scale/fixture.py`):

| column | type intent | notable values |
| --- | --- | --- |
| `id` | INT primary key | keyset watermark for `incremental_append` |
| `big_id` | BIGINT | values above 2^31 to catch int4 collapse |
| `amt_dec` | DECIMAL(28,9) | negative, zero, leading-zero (`0.000000001`), trailing-zero (`123.450000000`) |
| `amt_float` | FLOAT/DOUBLE | `1.5e300`, denormals, negative zero |
| `name_txt` | bounded VARCHAR(64) | CJK (`日本語`), emoji (`🚀`), RTL (`مرحبا`) |
| `note_null` | nullable text | SQL NULL, distinct from `note_empty` |
| `note_empty` | NOT NULL text | empty string (`''`) |
| `d_date` | DATE | |
| `ts_utc` | TIMESTAMP without time zone | microsecond precision |
| `ts_tz` | TIMESTAMP with time zone | non-UTC offsets |
| `flag` | BOOLEAN | |
| `uid` | UUID | |
| `payload_json` | JSON | nested object, numbers, Unicode, `null` |
| `blob_bin` | binary | high bytes, embedded `0x00` |

Columns an engine genuinely lacks are reported per cell as
`skip (engine has no X)` in `skipped_columns`, never silently omitted:

- MySQL: `ts_tz` — no timezone-aware timestamp type.
- SQLite: `ts_tz`, `payload_json`, exact `amt_dec` — no TZ timestamp, no JSON type,
  NUMERIC affinity is IEEE-754.
- SQL Server 2022: `payload_json` — no JSON data type (NVARCHAR carrier only).
- Oracle: `note_empty` — the engine stores `''` as NULL, so an empty-string domain
  cannot be proven round-trip.

## Matrix definition

Every ordered pair of live engines including same-engine pairs (25 routes) ×

- data-moving cells: `full_refresh_overwrite` (create_new), `full_refresh_append`
  (keyless append sink), `incremental_append` (create_new), `upsert` (create_new),
  `full_refresh_overwrite` × `dest_exists_compatible`,
  `full_refresh_overwrite` × `dest_exists_missing_column` (G13 extra source column
  against a destination that lacks it);
- blocking cells that must refuse rather than damage data:
  `dest_exists_narrower`, `g13_extra_source_column`, `g14_dest_notnull_no_default`.

Each data-moving cell runs **twice** and asserts second-run semantics:
overwrite stays N, append becomes 2N, incremental stays N, upsert stays N.
`dest_exists_compatible` builds the destination from what
`services.schema_introspect` reads on the source, put through the canonical inventor
`services.type_system.ddl_type` — i.e. the table create-new would have stamped —
so a "compatible" destination is not a second, hand-written type map.

## Results

Full grid, all five engines live, **200 rows** (`--rows 200`, run id prefix `jf5`,
2026-08-30 16:03–16:08 UTC):

**pass = 168 · fail = 57 · skip = 0** out of 225 cells.

| route | pass | fail | skip |
| --- | --- | --- | --- |
| mysql→mysql | 6 | 3 | 0 |
| mysql→oracle | 9 | 0 | 0 |
| mysql→postgresql | 9 | 0 | 0 |
| mysql→sqlite | 3 | 6 | 0 |
| mysql→sqlserver | 3 | 6 | 0 |
| oracle→mysql | 3 | 6 | 0 |
| oracle→oracle | 9 | 0 | 0 |
| oracle→postgresql | 9 | 0 | 0 |
| oracle→sqlite | 3 | 6 | 0 |
| oracle→sqlserver | 9 | 0 | 0 |
| postgresql→mysql | 9 | 0 | 0 |
| postgresql→oracle | 9 | 0 | 0 |
| postgresql→postgresql | 9 | 0 | 0 |
| postgresql→sqlite | 3 | 6 | 0 |
| postgresql→sqlserver | 6 | 3 | 0 |
| sqlite→mysql | 9 | 0 | 0 |
| sqlite→oracle | 9 | 0 | 0 |
| sqlite→postgresql | 9 | 0 | 0 |
| sqlite→sqlite | 3 | 6 | 0 |
| sqlite→sqlserver | 6 | 3 | 0 |
| sqlserver→mysql | 3 | 6 | 0 |
| sqlserver→oracle | 9 | 0 | 0 |
| sqlserver→postgresql | 9 | 0 | 0 |
| sqlserver→sqlite | 3 | 6 | 0 |
| sqlserver→sqlserver | 9 | 0 | 0 |

Every one of the 45 blocking cells (`dest_exists_narrower`,
`g13_extra_source_column`, `g14_dest_notnull_no_default`) refused the run and left
the destination at 0 rows — no truncation, no partial write.

Focused re-run of the Oracle ↔ PostgreSQL sub-grid after the Oracle fixes in this
branch (prefix `jf4`): **36 pass / 0 fail / 0 skip**, all checksums matching.

## Performance finding

**Every measured route is below the 2,000 rows/sec bar** — 93 of 93 data-moving
cells in the `jf5` run were flagged slow. Measured throughput:

| route sample | rows | rows/sec |
| --- | --- | --- |
| postgresql→postgresql overwrite/create_new | 200 | 106 |
| postgresql→mysql overwrite/create_new | 200 | 171 |
| oracle→oracle overwrite/create_new | 200 | 141 |
| postgresql→sqlite (refused, see below) | 200 | ~600 |
| postgresql→postgresql overwrite/create_new | 20,000 | 406 |

The per-cell figure includes preflight, mapping and reconciliation, so at 200 rows it
is dominated by fixed cost; the 20,000-row PostgreSQL→PostgreSQL measurement at
406 rows/sec is the honest steady-state number available so far, and it is still
5× under the bar. **No 100K figure is extrapolated from these numbers** — see
"Not yet proven".

## Defects found and fixed on this branch

Each fixed in the single canonical owner for that concern, then re-run.

1. **Oracle `TIMESTAMP WITH LOCAL TIME ZONE` read as NTZ** — SQLAlchemy carries Oracle
   TSLTZ awareness on `local_timezone` and leaves `timezone` False, so
   `_logical_type_from_sa` stamped `timestamp_ntz`; the writer then quarantined every
   timezone-aware value the column exists to hold. PostgreSQL→Oracle `upsert` and
   `full_refresh_append` refused 200/200 rows. Owner: `connectors/generic_sql.py`.
   After: both cells pass, checksum match.
2. **Oracle string invention defaulted to BYTE width** — a character-stated width
   (`VARCHAR(64)`, `CHAR(36)`) was invented as `... (64 BYTE)`, which is a real
   shrink for CJK (up to 4 bytes/char) and mismatched `CHAR(36 CHAR)` sources.
   Owner: `services/type_system.py`.
3. **Oracle native `JSON` unrecognised by SQLAlchemy reflection** — the column read as
   `NullType`, so a JSON document travelled the container/string wire and
   `{"i": 0}` arrived as `{"i": "0"}`. Registered an Oracle JSON type in
   `ischema_names`. Owner: `connectors/generic_sql.py`.
4. **JSON documents lost numeric polarity** — `json.dumps(..., default=json_default)`
   renders a `Decimal` as a JSON *string*, correct for a DECIMAL column on the string
   wire and wrong inside a document python-oracledb decoded from OSON. Added a
   document-specific serializer; the scalar DECIMAL contract is unchanged.
   Owner: `services/value_serializer.py` (+ `services/json_polarity.py` for
   MySQL/MariaDB `UNSIGNED INTEGER` number spelling).
5. **`NVARCHAR2` not recognised as a national carrier** during destination invention —
   an incomplete regex missed the Oracle spelling. Owner: `services/type_system.py`.
6. **SQL Server connection options dropped on the read path** —
   `trust_server_certificate` never reached readers/introspection, so a route whose
   writer connected was reported unreachable. Owners:
   `src/transfer/connector_dispatch.py`, `src/transfer/batch_readers.py`,
   `src/transfer/adapters_introspect.py`.
7. **SQL Server UUID destination stamped `VARCHAR(36)`** instead of
   `UNIQUEIDENTIFIER`. Owner: `services/type_system.py`.
8. **SQL Server `DATETIMEOFFSET` reflected/resolved as NTZ** (plus pyodbc `-155`
   output conversion), quarantining every aware value. Owner:
   `connectors/generic_sql.py`.
9. **SQL Server `NVARCHAR`/`NCHAR`/`NTEXT` classified as Latin-1**, which quarantined
   all CJK/emoji into a column that holds them. Owner:
   `services/encoding_capacity.py`.
10. **pyodbc short rowcount on all-or-nothing batches** counted committed rows wrong.
    Owner: `connectors/generic_sql.py`.
11. **BIGINT reflection collapsing to INTEGER**, giving int4 bounds to an int8 column.
    Owner: `connectors/generic_sql.py`.
12. **Oracle explicit `NUMBER(1,0)` widened through generic 64-bit integer inference**
    (`flag → DECIMAL(19,0)` overflow). Owner: `services/type_system.py`.
13. **Oracle identifier folding** — quoted lower-case names were looked up as distinct
    identifiers (`ORA-00942`); reflection now normalises to the stored spelling.
    Owner: `services/schema_introspect.py`.

## Still failing (57 cells, grouped by root cause)

These are open, not worked around. Each is a genuine product finding.

| cells | routes | measured message | assessment |
| --- | --- | --- | --- |
| 9 | postgresql/mysql/sqlite/sqlserver → sqlserver (append, dest_exists_*, create_new for mysql source) | `U+8A9E exceeds destination varchar capacity` (160/200 rows quarantined) | Destination invention chooses `VARCHAR` for a Unicode-capable source column, then correctly refuses CJK. The refusal is honest; the **type choice** is the defect — a Unicode source column must be stamped `NVARCHAR` on SQL Server. |
| 18 | * → sqlite | `uid UUID → TEXT` / `→ VARCHAR`, `CHAR(36) → TEXT` "collapse fidelity" | **Fixed (D3).** SQLite has one untyped TEXT affinity — a declared length is parsed and discarded and no carrier enforces a UUID domain, so `VARCHAR(36)` there enforces nothing `TEXT` does not. The gate now asks the destination whether a narrower text carrier exists (`dest_string_length_is_unenforced`): the route carries the value byte-exact, the missing domain is stated as a warn-level `uuid_carrier_equivalent` / `fixed_width_not_enforced` note, and no Migration Risk Contract is demanded. Other dialects keep the collapse. Live proof: `tests/test_sqlite_text_carrier_equivalence_d3.py`. These 18 cells need re-measuring in the next full matrix run. |
| 6 | sqlserver/oracle → mysql (create_new) | MySQL `1064 ... near 'CHARACTER SET utf8mb4 C...'` | Invented MySQL DDL emits a `CHARACTER SET`/`COLLATE` clause the server rejects (local server is MariaDB 11 — see caveat). DDL syntax defect in the MySQL create-new path. |
| 3 | mysql→mysql | `uid CHAR(36) COLLATE UTF8MB4_0900_BIN → CHAR(36) COLLATE UTF8MB4_0900_AI_CI` | Same-engine collation carry: the invented destination does not carry the source collation, then the gate blocks its own output. |
| 12 | mysql↔sqlserver, oracle→mysql (append, dest_exists_*) | collation/charset collapse (`UTF8MB4_0900_BIN → SQL_LATIN1_GENERAL_CP1_CI_AS`, `NVARCHAR(64 CHAR) → VARCHAR(64) COLLATE UTF8MB3_GENERAL_CI`) | Cross-engine collation mapping picks a weaker collation/charset than the source, and the gate correctly refuses. Fix belongs in `services/collation_carry.py` + destination invention. |
| 9 | remaining sqlserver/sqlite dest_exists cells | as above | same causes |

Environment caveat, stated because it changes how two of the groups must be read:
the compose "MySQL 8" service on this box reports **MariaDB** (`utf8mb4_0900_ai_ci`
present, `utf8mb3_general_ci` defaults). Two pre-existing repo tests
(`test_property8_unicode_form.py::test_mariadb_collation_unique_nfc_nfd_and_sharp_s`,
`test_typed_fidelity_transfer_matrix_e2e.py::test_postgresql_into_existing_mysql_timestamp_column`)
fail on this box for the same reason, on the base branch as well as this one.

## Not yet proven

Stated plainly rather than estimated:

- **100,000-row runs have not completed.** At the measured 106–406 rows/sec, one
  100K cell is ~8 minutes of wall clock for its two runs, so the 225-cell grid is
  ~40 hours. The grid above is a real run at 200 rows; no result in this document is
  scaled up from it, and no cell is marked pass on the strength of a smaller run.
  The largest completed single run so far is 20,000 rows
  (postgresql→postgresql, `full_refresh_overwrite`, 406 rows/sec).
- The 57 failing cells above are **not** fixed.
- Slow-route profiling is limited to end-to-end elapsed time per cell; no per-stage
  (read / transform / write / reconcile) breakdown has been captured yet.
- Binary (`blob_bin`) round-trip is exercised only where the engine has a binary type;
  it has not been separately proven byte-for-byte at scale.

## Addendum — after the collation / destination-invention fixes (200 rows)

Two more root causes were fixed after the grid above was measured:

| Defect | Root cause | Owner module changed |
| --- | --- | --- |
| A UUID carried as text lost its source collation, so a `CHAR(36) COLLATE utf8mb4_0900_bin` source landed in the destination's default `utf8mb4_0900_ai_ci` (case/accent-insensitive equality on hex text — two UUIDs differing in case collide), and the fidelity gate correctly refused the write | the exact-wire UUID branch of `ddl_type` returned the destination string spelling built from `strip_identity_qualifier(...)`, which drops `COLLATE`, bypassing the collation re-attacher | `services/decision_kernel/type_invent.py` (exact-wire spelling extracted to `_uuid_exact_wire_ddl`; `services.type_system._with_collation_clause` stays the single owner of destination-legal `COLLATE` spelling) |
| SQL Server dest-exists shapes were created with `VARCHAR`, so every CJK/emoji row was (correctly) quarantined as `U+8A9E exceeds destination varchar capacity` | harness defect, not product: the dest-exists table was stamped with `ddl_type` (no source-engine context) instead of the canonical create-new stamp, which promotes to `NVARCHAR` when the source can emit any code point | `apps/api/tests/scale/{fixture,engines,matrix}.py` — now stamps with `decision_kernel.create_new_mapping_target_type(..., source_db=<source engine>)` |

Focused re-runs after the fixes, real services, 200 rows, both directions, all four
modes, all shapes:

| Sub-grid | Before | After |
| --- | --- | --- |
| postgresql ↔ sqlserver | 9 fail | **pass=36 fail=0 skip=4** |
| postgresql ↔ mysql | 6 fail (DDL 1064) + 3 fail (collation) | **pass=36 fail=0 skip=4** |
| postgresql ↔ sqlite | 18 fail | **pass=38 fail=0 skip=2** |
| oracle ↔ postgresql | — | **pass=36 fail=0 skip=0** |

The 4 skips per sub-grid are the `domain_contract_unsigned` shape on routes where the
destination declares every mapped domain natively, so no Migration Risk Contract is
required to prove unsigned refusal; the exact reason is recorded per cell.

The full 225-cell re-run was **stopped at 122 cells** (`pass=100 fail=12 skip=10`) when
work was halted. Every one of the 12 failures in that partial run is a
`mysql ↔ sqlserver` cell — the one cross pair whose fix had not been re-measured — and
all 12 landed 0 destination rows (refused, not partially written). The complete
225-cell grid has **not** been re-measured after these fixes; the numbers in the main
table above are the last complete measurement.
