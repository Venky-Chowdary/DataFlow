# DataFlow / Datawrap — engineering handover

Integration branch: `feature/Venkat-Analysis` (**not** `main` — merges land
there, and no CI workflow triggers on it). Latest wave head:
`3e3dd8a4` on `devin/1787950000-transform-preload-ui`
([#71](https://github.com/Venky-Chowdary/DataFlow/pull/71)). Earlier accounts /
workspaces wave: `a1cc1f91` via
[#43](https://github.com/Venky-Chowdary/DataFlow/pull/43).

**Latest (this branch):** YAML and fixed-width are transfer-live **file sources**
on `feature/yaml-fixed-width-live`. YAML keeps scalar text (no `yes`→bool).
Fixed-width refuses a guessed layout. Do not fold D1 [#132], N2 [#133],
N4 [#134], or N5 [#135] into this tree. Next: 100K of these formats, then
other never-measured / fleet items.

This document is written so the next engineer can continue without re-deriving
anything. It separates **proven** (a command or artifact anyone can re-run) from
**open** (known defect) and **unproven** (never measured). Nothing here says the
product is deployment-ready; §7 lists exactly what is missing for that claim.

---

## 1. How to run it

```bash
# API — port 8001
cd apps/api
export MONGODB_URI=mongodb://127.0.0.1:27017
export DATAFLOW_ADMIN_EMAIL=operator@dataflow.test
export DATAFLOW_ADMIN_PASSWORD='Op3rator-Test-2026'
export DATAFLOW_ADMIN_NAME=Operator
export DATAFLOW_ADMIN_ROLE=admin
export DATAFLOW_REQUIRE_AUTH=1
../../.venv/bin/python -m uvicorn src.main:app --port 8001

# Web — port 5173
cd apps/web && npm run dev
```

On the Windows dev box the interpreter is `dfvenv` and the shell is PowerShell
(`&&` is not a separator; use `;` or two calls), and the API is started as
`python -m uvicorn main:app --host 127.0.0.1 --port 8001` from `apps/api`.
Run it **without `--reload`** and hard-restart it after any Python edit —
`--reload` has repeatedly served stale service modules while the tests passed,
which is how a fixed gate can still look broken in the browser.

Test commands:

```bash
cd apps/api
../../.venv/bin/python -m pytest tests -q                    # whole suite, ~31 min
../../.venv/bin/python -m pytest tests/test_team_access.py -q # accounts/roles, 33 tests

cd apps/web
npm test        # 592 tests
npm run build   # tsc -b && vite build

# CI gates (exact scopes CI enforces)
cd apps/api
../../.venv/bin/ruff check --config ruff.toml services/decision_kernel \
  services/auth_rate_limit.py services/auto_create_lifecycle.py \
  src/services/auth_service.py connectors/elasticsearch_writer.py \
  connectors/sqlite_writer.py
../../.venv/bin/python -m mypy --config-file mypy.ini --follow-imports=skip \
  services/decision_kernel services/type_system.py services/type_ddl_specialty.py
```

Pushing: the Devin git proxy returns **403** for this repository, so pushes go
direct to github.com with the stored token (`secret:org:GITHUB_PUSH_TOKEN`).
That token was pasted into chat and **must be rotated**.

---

## 2. Latest wave — Validate≡Execute, Transform (pre-load), RBAC (PRs #44–#71)

The theme of this wave is one rule per question. Every defect below was the same
shape: two layers answered the same question differently, so a run passed
Validate and died at the writer, or was blocked by a gate the operator had
already satisfied.

### Validate must promise only what the writer will accept

| Defect | Fix | PR |
| --- | --- | --- |
| `fits_integer("22.433332", "INT")` did `int(Decimal(v))` → 22, in range, pass — the writer refuses any fractional value, so a 1M-row MySQL load died at row 1 with 4,917 findings | fit and the write share one rule, across all five dialect families | [#67](https://github.com/Venky-Chowdary/DataFlow/pull/67) |
| `'ABC-1'`, `'NaN'`, `'--3'` into INT and `'maybe'` into BOOLEAN passed a bounds test | Validate asks the exact parser the write binds through; a parse bound with no width bound (BOOLEAN) is screened, not filed "undecidable". Also fixes a `NaN` cell making Transform preview 500 | [#70](https://github.com/Venky-Chowdary/DataFlow/pull/70) (**open**) |
| A too-wide decimal was judged on textual padding (`1.50000000` read as scale 8) | the gate measures the value, and a test asserts it agrees with the writer's `fits_decimal` on every sample | [#54](https://github.com/Venky-Chowdary/DataFlow/pull/54) |
| A held-out row was judged by whoever asked last: Validate said "27 will be quarantined", Execute aborted on the strict job posture | the signed Risk Contract on that column decides both | [#54](https://github.com/Venky-Chowdary/DataFlow/pull/54) |
| `5,000 quarantined / 0 findings`, and a rolled-back row counted as quarantine | a quarantined row is one with a persisted finding; a rolled-back row is named as one | [#68](https://github.com/Venky-Chowdary/DataFlow/pull/68) |

Still open in that family: **unparseable dates** (`2024-02-31`) are not decided
at Validate. Our date parser refuses ambiguous forms MySQL may accept, and
trading a real failure for a false refusal is worse; this needs a
dialect-truthful probe.

### Transform (pre-load) — the operator's own name for it

The step was called "Shape" (engine vocabulary) and shipped no stylesheet or
guidance. It is now **Transform (pre-load)** everywhere an operator reads it,
with on-screen rules, per-column finding charts and a recipe-identity badge.
Internal `shape*` names (`shapeSteps`, `ShapeStepWire`, `/shape/profile`,
`shape_recipe_hash`, `shape_refused_row`) are deliberately unchanged — renaming
them breaks stored recipes and running jobs.

Semantics, and they are the contract to preserve:

* The recipe runs **on the read**, before Map, Validate and the write. Only
  row-local deterministic ops are allowed in flight; joins/aggregates/pivots stay
  post-load and are refused at design time with a pointer there.
* Validate judges the **transformed image** (`services/shape_preflight.py`),
  assembled from the same `ShapeRecipe`/`ShapeEngine` Execute runs. No gate logic
  is duplicated.
* Type resolution is asymmetric on purpose — a sample is weaker evidence than a
  catalog: a column the recipe never wrote keeps its declared carrier; a column
  the recipe wrote (or created) is re-read from the transformed values.
* One recipe identity spans approval, Validate, Execute, Proof, reconciliation
  and replay. A changed recipe is a different recipe and must be revalidated; an
  empty or all-disabled recipe has **no** identity (an early build minted one and
  refused plain transfers at Execute).
* An unrunnable recipe, or one that refuses a sampled row, is a `400` — Validate
  never scores a program Execute would abort on.
* Sources are never mutated. A row the recipe removed is `rows_shaped_out`
  ("Removed by transform"), never a quarantine finding and never silent loss.
* A shaped DB run declines the source/destination cell ladder and says why: the
  source still holds pre-transform values, so cell equality would report every
  transformed cell as a mismatch. Its proof is the pinned hash plus the
  destination re-read.

PRs: [#65](https://github.com/Venky-Chowdary/DataFlow/pull/65) (execution on all
three read paths), [#66](https://github.com/Venky-Chowdary/DataFlow/pull/66)
(ledger term), [#71](https://github.com/Venky-Chowdary/DataFlow/pull/71) (naming,
UI, transformed-image Validate — **open**).

#### The four-layer "Case A" fix (head `3e3dd8a4`, browser-unverified)

A DECIMAL source column rounded to whole numbers for an existing `INT`/`int4`
destination was blocked with no operator remedy. Four layers each held a piece:

1. **Drift asked one question and used one answer for two meanings.**
   `detect_schema_drift()` now takes `declared_source_columns` /
   `declared_source_schema`, which answer *"did the source change under the
   stored revision?"*, while `source_columns` / `source_schema` are the
   transformed image and answer *"do these values fit the destination carrier?"*.
   Judging the fingerprint on the transformed image reported the operator's own
   recipe as source drift; judging the carrier on the declared type graded the run
   `hard_breaking:narrow_type` with `source_changed:false` — nothing to re-map.
   They default to each other, so an unshaped run is unchanged and an unshaped
   `DECIMAL → INT4` is still blocked.
2. **Map reported the create-new DDL carrier as the source's own.**
   `ddl_carrier_type` widens ambiguous `INTEGER` to `BIGINT` — correct for the
   CREATE question, wrong as a *report*, so an `INTEGER` source into an existing
   `int4` column read back as a `BIGINT → INT4` narrowing and demanded a Risk
   Contract for a path this same engine grades `preserve`.
   `_reported_source_carrier()` undoes only the ambiguous-width integer widening;
   every invent still uses the never-narrower carrier.
3. **`/preflight/preview-cells` scanned raw cells**, reporting
   `Invalid integer: '22.43'` on values the approved recipe rounds to 22. The
   recipe now travels with that request too, and an unrunnable one refuses `400`.
4. **A blank required option looked usable** in the step builder; `Add` is now
   disabled with the missing field named on the field and on the button.

Also: a row refused by the operator's own Refuse policy stopped the sample short
of a balance and the panel called that "a defect, do not approve". Only an
imbalance with **no** refusal is an accounting defect now.

### Scheduling, RBAC, tenants, file reads

* **The deterministic park never fired** — it compared a classification dict to
  the string `"deterministic"`, so every 2-minute beat re-ran a failing job and
  appended the same rows (5 → 25 at the destination). A failed beat now parks on
  one finding when the verdict cannot change by itself, which suppresses the
  cadence. Approve/reject/revoke also stopped 400-ing for a signed-in operator,
  and deleting the last schedule stays deleted.
  [#55](https://github.com/Venky-Chowdary/DataFlow/pull/55)
* **RBAC was cosmetic**: a viewer saw every control enabled, backend `403`s were
  swallowed and replaced with hardcoded placeholders, `Settings → General → Save`
  issued no request at all, a Team-created login landed with platform role
  `member`, the "change it at first sign-in" promise was false, and
  `GET /api/v1/auth/me` was a 404. Authority now resolves once per workspace on
  the server, every write surface refuses **in words**, and Settings saves.
  `POST /api/v1/fidelity/check` had no rule and fell through to the mutation
  default `connector.write`, refusing the **operator** — it is `job.run`.
  [#58](https://github.com/Venky-Chowdary/DataFlow/pull/58),
  [#59](https://github.com/Venky-Chowdary/DataFlow/pull/59)
* **The Excel/CSV reader always read `wb.active` with row 1 as header**, so a
  workbook with a title row or data on sheet 2 could not be loaded at all. Sheet,
  header row, skip rows, encoding and delimiter now govern the read, the count,
  the write and reconciliation. Browser testing on the same PR caught a worse
  defect: retargeting the destination table in a finished draft left "Transfer
  complete" showing for a table that did not exist.
  [#61](https://github.com/Venky-Chowdary/DataFlow/pull/61)
* **Tenant create/amend/delete/BYOK all 500'd** on one undefined name, and
  `level="warning"` is not this system's vocabulary (`warn`), so tenant deletion
  and an executed rollback were filed INFO and invisible under Warnings. The
  level is canonicalized at the write.
  [#63](https://github.com/Venky-Chowdary/DataFlow/pull/63)
* `sqlparse` CVE floor — the `security` CI job was failing on every branch
  because of it. [#56](https://github.com/Venky-Chowdary/DataFlow/pull/56)

### Open PRs in this wave (do not assume merged)

[#69](https://github.com/Venky-Chowdary/DataFlow/pull/69) (destination quarantine
DDL carrier), [#70](https://github.com/Venky-Chowdary/DataFlow/pull/70) (parser
parity), [#71](https://github.com/Venky-Chowdary/DataFlow/pull/71) (Transform).
No CI runs on PRs targeting `feature/Venkat-Analysis`; workflows trigger on
`main` only, so a green tick is not available on this base and its absence is not
a failure.

---

## 3. Accounts, workspaces and roles (wave `a1cc1f91`)

### What was broken

1. No screen ever called `createWorkspace`, so `/workspace/workspaces` returned
   `[]`, the Team tab had nothing selected, and **Add member returned without
   issuing a request** — the user's "I cannot add a member".
2. Accounts existed only as `DATAFLOW_ADMIN_*` / `DATAFLOW_AUTH_USERS`
   environment values, so an added member **could never sign in**.
3. Membership was a single blob document `team_store/primary`: two concurrent
   invites lost one, and one email could appear twice in a workspace.

### What it is now

| Concern | Owner |
| --- | --- |
| Accounts | `apps/api/services/user_store.py` (`platform_users`) |
| Workspaces + membership | `apps/api/services/team_store.py` (`workspaces`, `workspace_members`) |
| Password primitive | `apps/api/services/password_hash.py` (bcrypt; the only verifier) |
| Metadata persistence | `apps/api/services/metadata_backend.py` (Mongo, else locked JSON) |
| HTTP surface | `apps/api/src/routers/team_router.py` under `/api/v1/team` |
| Permissions | `apps/api/services/rbac.py` (middleware) + the store (workspace role) |
| UI | `apps/web/src/pages/settings/TeamSettings.tsx` |

* Roles are `admin` / `editor` / `viewer`; the pre-rename `owner` reads as
  `admin`, so a deployment written by an older build keeps working.
* Membership `_id` is `"<workspace_id>:<email>"` with a unique
  `(workspace_id, email)` index, so a duplicate invite is a replace, not a
  second row, and the write is atomic.
* Creating a workspace and its first admin is **one unit**: a MongoDB
  transaction on a replica set, and on a standalone server (no transactions) the
  workspace row is deleted again if the membership insert fails. No workspace can
  exist that nobody can administer or invite into.
* Refusals carry their reason, and the router maps each to its own status:
  unknown workspace `404`, missing member `404`, denied actor `403`, last admin
  `409`. Previously every refusal was an indistinguishable `403`.
* A workspace admin manages members **without** being a platform admin;
  deployment-level account and workspace creation stays platform-admin only.
* Rotating your own password is the `account.self` permission, so every
  authenticated role can do it without gaining workspace or connector write.
* Accounts created for a member return a **one-time temporary password**, shown
  once in the operator's own session; the hash is bcrypt and no plaintext is
  stored (asserted by `test_mongo_stores_no_plaintext_password`).

Proof: `tests/test_team_access.py` — **33 passed**, covering role capability,
editor limits, last-admin protection, workspace isolation, generated-account
sign-in, temporary-password rotation, reload persistence, distinguishable HTTP
statuses, audit attribution, live-Mongo relationship rows, blob migration, and
the compensating rollback.

---

## 3b. Transfer Studio parity wave — row-level mirror/SCD2 proof

The 60-case live matrix (`apps/api/scripts/live_studio_parity_matrix.py`, 13
routes × 7 sync modes over PostgreSQL, MySQL, MongoDB, CSV, Excel and file
export) stands at **58 pass, 2 blocked consistently, 0 parity breaks**. The two
blocked cases are the correct answer: a second `full_refresh_append` into a
destination that enforces uniqueness is refused at Validate *and* at Run with the
same reason.

Mirror and SCD2 no longer pass on a destination count. Per route the matrix
asserts deleted keys are the soft-deleted ones, survivors still hold their source
values, the updated key has exactly two versions (old closed with
`is_current=false` and `valid_to >= valid_from`, new current with `valid_to NULL`),
untouched keys stay on one version, and totals equal population plus changed
versions. Details in `docs/STUDIO_PARITY_MATRIX_EVIDENCE.md`.

| Defect found by that proof | Fix |
| --- | --- |
| Mirror key-staging tables accumulated in the customer's schema | `Connection.execution_options()` mutates the connection, so a streamed digest left `stream_results` set and the following `DROP TABLE` compiled as `DECLARE ... CURSOR FOR DROP TABLE`. Streaming is now statement-scoped; live re-run leaves **0** orphans and logs **0** drop failures |
| SCD2 history collapsed on MySQL (`valid_from == valid_to`) | MySQL `DATETIME` defaults to fsp 0. MySQL-family destinations carry `DATETIME(6)`; `DATE` stays `DATE` |
| A retry could drop the spool the first attempt was still reading; a source that died mid-stage leaked its spool | Spools are stamped per attempt, the stage runs inside the cleanup's try, and cleanup never masks the transfer's own error |
| Two engines each owned staging naming/reaping | One owner: `services.staging_reaper` (6h TTL, `keep` for the live table, bounded lock wait, catalog failure = skipped sweep) |
| An existing table outside the bounded object listing read as absent | One owner for qualified names: `split_object_namespace()` at the introspect entry |

Regressions: `tests/test_naive_datetime_subsecond_carrier.py` (18),
`tests/test_staging_spool_lifecycle.py` (8),
`tests/test_qualified_object_existence.py` (12). Full backend suite after the
wave: **16,365 passed, 1,532 skipped, 0 failed**; frontend `npm run build` clean;
CI ruff allowlist (now including `services/staging_reaper.py`) and the Decision
Kernel mypy gate clean.

Not yet measured for this wave: CDC on these routes at volume, a 1M-row run per
sync mode (only `full_refresh_append` at 221.5 s and `incremental_deduped` at
393.2 s are measured), and any Snowflake route (no live credentials/network).

---

## 3c. Number-locale + exact-Decimal wave (`4bfde98a` → this commit)

An ambiguous file number now settles once — declaration → typed wire → evidence
in the sample → US fallback — and inference, profiling, preflight, the writer
and Gate-8 all read that single owner (`services/transform_engine.py`). A typed
database numeric carrier is `WIRE`, so a faithful `NUMERIC(12,3)` route is no
longer refused as "ambiguous grouping", and `20.5` vs `20.500` is no longer
reconciled as corruption.

This commit closes the 12 type/decimal failures that predated the locale work
(verified failing at `1b2bf77a~1`), each a real defect, not a test edit:

| Defect | Fix |
| --- | --- |
| Currency text (`$1,234.56`) silently became `1234.56` at a DECIMAL bind nobody declared a conversion for | `coerce_decimal_wire` refuses the marker; a declared decimal transform hands bind an exact `Decimal` |
| A refused DynamoDB `NS` member fell through and landed the envelope as a Dynamo **map** — a declared number set silently became a document | only a failed JSON *parse* falls through; a refusal raises |
| Exported JSON/JSONL retyped every numeric column to text (`"1000.00"`) | `json_dumps_exact_numbers` writes exact digits as an unquoted literal; `float` is never involved |
| Generic-SQL JSON columns read back as a quoted JSON string | SQLAlchemy swaps `sa.JSON` for the dialect impl via `colspecs`, which re-serialized our canonical text — `_ExactJSON._gen_dialect_impl` keeps our processors |
| Structural attestation "unreadable" on a table that exists (SQLite-backed generic SQL) | a writer reports the db *file* as the schema; `_catalog_schema` maps a non-attached path to the default schema |
| Mapping hard-case golden at 92.9% | fixture semantics: a **proven-absent** table is a lossless identity CREATE (`create_new`, `identity_passthrough`, `equivalent_create_new`, approve-eligible). Existing-but-unreadable stays `pending_dest_schema`, `create_new=false`, review-required. The 100% floor was not lowered |

Measured: `1122 passed, 12 skipped` across
`test_cross_schema_edge_types / test_cross_type_accuracy / test_decimal_write_path /
test_dynamodb_ns_write_path / test_value_serializer / test_mapping_hard_case_accuracy /
test_object_store_materialize`, plus `35 passed` on
`test_physical_state_diff / test_generic_sql_json_exact_write_path`. CI ruff
allowlist clean; CI mypy shows the same 4 baseline errors as before this wave.

Not proven by this wave: the 60+ connector matrix, 100K/1M per connector, live
Snowflake, notification delivery, MCP from a real client.

---

## 4. Earlier waves on this branch (already pushed and proven)

| Area | Evidence |
| --- | --- |
| 710k append failing its own proof | Gate-8 compares the mapped delta; digest scope/basis stamped; `docs/RESYNC_MATRIX_EVIDENCE.md` (20/20 live) |
| `VARCHAR(16777216) → VARCHAR(16777216) loses fidelity` | Destination existence travels synchronously into Map; unread destination metadata is its own class with a real Reload control |
| 1M rows in ~20 min | **221.5 s, 4,515 rows/s**, destination `COUNT(*) = 1,000,000`, 0 rejected — `docs/THROUGHPUT_1M_EVIDENCE.md` (local PostgreSQL→MySQL fixture, not an SLA) |
| CDC silently degrading to cursor polling | Refused when delete capture would be lost; requested vs actual mode, cause, remedy and `cdc_delete_capture=false` stamped on the run |
| Structural schema attestation | 4/4 live — `docs/STRUCTURAL_ATTESTATION_EVIDENCE.md` |
| Naive timestamp → MongoDB dead end | Real IANA-zone Map control serializing `assume_timezone:<zone>` |
| G1–G9 stage names | Named technical stages with a live ticker |
| Pilot answering off-subject questions | Deterministic refusal, no sources, confidence 0.2, no navigation |
| Jobs counting only the loaded page | Counts the whole history; the row list states which slice it holds |

### Settings wave on `devin/settings-enterprise-1787535434` (PR #72, head `46cdaaeb`)

Each item was found by driving the tab in a browser and re-proven there after the fix
(recordings on the PR; latest run against `635676b2`).

| Defect | Fix and proof |
| --- | --- |
| Notification create/update always 422 | `apiFetch` labels a string body as JSON; channel create/edit/persist through F5 and an API restart |
| Slack/Teams "Test message sent" for any HTTP 2xx | Provider acknowledgement required (`ok` / `{"ok": true}`; `1` / 202 empty); an HTML 200 endpoint now reads *Test failed* |
| Missing SMTP reported as sent | `{"ok": false, "error": "SMTP host not configured"}` surfaced as *Test failed* |
| A duplicate invitation re-roled (and could demote) a member | 409 naming the role held today, before any account is created or password rotated |
| Editor UI contradicted the editor API | `Permission.MEMBER_INVITE` (editor + admin); granting the admin *role* stays `workspace.manage` |
| A tenant saved with no workspace could never be read back | Creation refuses an unnamed workspace and a body/header scope mismatch |
| A rejected password read as "Control plane unreachable" | `classifySignInFailure` by status/transport; every 5xx is the API, 503 is deployment config, an answered 401 is the credential |
| Enterprise showed "No tenant configured" over a saved tenant | An account with several memberships gets no `workspace_id` from `/auth/me`, so the client names one; the tab re-reads on `WORKSPACE_CHANGED_EVENT` and a successful re-read clears the banner |
| A workspace-less legacy tenant still answered host routing and CORS | `get_tenant_by_domain` and `cors_origins` both skip records with no `workspace_id` |

Still not proven for these tabs: real SMTP / Slack / Teams delivery, SSO/IdP, KMS/BYOK
material, and host routing in a real browser vhost (verified at service level only).

---

## 5. Open defects (found, reproduced, not yet fixed)

1. **`TIMESTAMPTZ → DATETIME(6)` refused as a fidelity collapse.** Three
   `tests/test_typed_fidelity_transfer_matrix_e2e.py` cases fail (PostgreSQL→
   MySQL typed, into an existing MySQL `TIMESTAMP(6)` column, PostgreSQL→Redis).
   An instant landing in an instant carrier should not need a Risk Contract.
   Verified pre-existing on the parent commit; this is the next fix to make.
2. **Test isolation.** `tests/test_pilot_llm_wave41.py::test_hybrid_footnote_on_auth_failure`
   passes alone and in its own file, fails only in whole-suite order — provider
   state leaks between tests.
3. **Scheduler shutdown logging.** Every suite run ends with
   `ValueError: I/O operation on closed file` from
   `services/transfer_scheduler.py:61`. Harmless in tests, wrong in a service.
4. **Host-fact tests.** `property8_unicode_form` / `property8_json_polarity`
   assert a MariaDB build without `utf8mb4_0900_ai_ci`, and two PostgreSQL cases
   need the `vector` extension. They should skip on capability, not fail.
5. **Pilot citations open the public docs shell.** Clicking a citation opens the
   right Help article but with the marketing header, so the operator leaves the
   authenticated workspace. Awaiting the user's decision.
6. **Map API vs UI type spelling.** The map API returns `TIMESTAMP_NTZ(6)` while
   the UI shows `DATETIME(6)`; a separate physical/native type through
   introspection was proposed and not yet decided.
7. **Case A is browser-unverified.** The four-layer fix at `3e3dd8a4` passes unit
   and API tests, but the last browser run (before it) showed Validate still
   blocking on `schema_drift`, so nothing yet proves the decimal→integer route
   reaches Execute in the real UI. Treat it as unproven until an independent SQL
   re-read shows whole-number values, the expected row count, and **rounding
   rather than truncation** (the fixture is chosen so the two differ: `SUM = 66`,
   not 65).
8. **A file-export destination is not approvable at all** — Map says
   "Destination schema not loaded", so the export retarget path is unit-tested
   only. Awaiting a decision on whether file export is meant to be live.
9. **Tenant delete and BYOK rotate have no UI surface** (API only). Awaiting a
   decision on whether they should be operator-reachable.
10. **`round_number` collapse through the profiler.** A literal
    `1.50000000 → NUMBER(9,2)` case is unreachable through the UI because the CSV
    profiler collapses the padded value to `DECIMAL(7,4)`, so a `(9,2)`
    destination is held earlier by the narrowing Risk-Contract gate. Awaiting a
    decision on whether the profiler should preserve declared scale.
11. **Environment failures that are not product defects** — do not "fix" these by
    changing production semantics: PyIceberg reads a Windows path `C:\...` as URI
    scheme `c` (7 `test_row_conservation.py` failures on this box), and the local
    MySQL fixture refuses `root@172.17.0.1`
    (`test_source_duplicate_probe_live`).

---

## 6. Unproven — never measured, must not be claimed

* Live Snowflake authentication and network (all Snowflake evidence to date is
  emulator or fixture).
* CDC and upsert throughput at volume; only the 1M PostgreSQL→MySQL append is
  measured.
* Platform-wide exactly-once. `PLATFORM_EXACTLY_ONCE_CLAIMED = False`; CDC is
  at-least-once upsert unless a named route proves otherwise.
* Every Settings tab other than Team, end to end in a browser (in progress).
* The chatbot/RAG path with a real configured OpenAI key — only the
  deterministic refusal and shipped-docs path are proven.
* MCP driven from a real MCP client, including per-role permissions.
* Per-client domains and host routing; tenant isolation under hard isolation.
* Email, Slack and Microsoft Teams delivery.
* Documentation screenshots and the per-section explainer videos.
* The connector-family matrix, the type-family matrix and the sync-mode matrix
  (append / overwrite-full-refresh / upsert-sync) across the 40 connectors.
* SFTP daily-Excel ingestion into an existing table under each sync mode, with a
  2-minute schedule replaying the same Transform recipe.
* Governance operations (mask / hash / redact) recorded in the audit certificate
  — designed, not built.
* SAML / single sign-on against a real IdP.
* 1M / 10M-row phase timing for this branch (the only measured throughput figure
  is the earlier `docs/THROUGHPUT_1M_EVIDENCE.md` append).

---

## 7. What "ready to deploy to a client" still needs

1. Fix defect §4.1 (typed instant route) — it blocks a common real route.
2. Browser proof for every Settings tab: load, save, reload, validation, secret
   masking, failure state, tenant scope.
3. Chatbot/RAG proven with a configured provider, including citations inside the
   authenticated shell and live-data answers.
4. MCP proven from a real client with viewer/editor/admin separation.
5. Notification delivery proven for email, Slack and Teams.
6. Per-client domain/host routing proven, plus audit and user logs.
7. Documentation refreshed with current screenshots; the requested ~1 minute
   explainer per section.
8. Rotate the GitHub token that was pasted into chat.
9. Case A (§5.7) proven in the browser on MySQL **and** PostgreSQL with an
   independent destination re-read, and B–H re-run after it.
10. The three matrices in §6 (connector family, type family, sync mode) run as
    named matrices with pass/fail/skip counts, not as a spot check.
11. SAML/SSO proven against a real IdP.

---

## 8. How to test this branch (what the last waves actually ran)

```powershell
# API, focused on this wave
cd apps\api
C:\Users\Administrator\dfvenv\Scripts\python.exe -m pytest tests -q `
  -k "shape or transform or conservation or certificate or reconcil or preflight or drift or mapping"

# Web, focused
cd apps\web
npx tsx --test src/lib/transferStudioChrome.test.ts src/lib/transformProfile.test.ts `
  src/lib/conservationLedger.test.ts src/lib/shape.test.ts
npx tsc -b; npx vite build
```

Browser evidence is driven with Playwright over CDP at `http://127.0.0.1:29229`;
write `.mjs` files rather than `node -e`, because PowerShell mangles inline JS.
Live fixtures are Docker containers `df-mysql` (3306), `df-pg-5432` (5432),
`df-pg` (5433), `df-mongo` and `df-redis`. Traps learned the hard way live in
`.agents/skills/testing-dataflow-ui/SKILL.md` — read it before writing browser
checks; Map's safe band is collapsed by default, the expand control exists both
inline and in a drawer outside `main`, and a role test must assert the signed-in
email before reporting anything about viewer behaviour.

**A passing test suite is not evidence a transfer works.** Every claim in §2 that
says "browser" means a real connector, a real Execute, and a destination re-read
issued independently of the app. Where that is missing, the row says so.

---

## 7. Conventions worth keeping

* One algorithm owner per concern. `services.semantic_mapper.map_columns` owns
  Map; `services.shape_contract.classify_dest_exists_shape` owns dest-exists;
  `services.password_hash` owns password verification (a test that patched
  `auth_service.is_production` was stale and was pointed at the real owner).
* A refusal names its reason and maps to a deliberate status; never return a
  bare `False`.
* Unknown destination metadata stays unknown — it must never become create-new
  or source-type passthrough.
* Proof means a named fixture or matrix with pass/fail/skip counts. A skip is
  not a pass, and a writer acknowledgement is not destination proof.
* The next correct operator action must be a real control, not prose.
