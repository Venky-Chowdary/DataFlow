# DataFlow / Datawrap — engineering handover

Branch `venkat` → remote `feature/Venkat-Analysis`, PR
[#43](https://github.com/Venky-Chowdary/DataFlow/pull/43). Head at time of
writing: `a1cc1f91`.

This document is written so the next engineer can continue without re-deriving
anything. It separates **proven** (a command or artifact anyone can re-run) from
**open** (known defect) and **unproven** (never measured). Nothing here says the
product is deployment-ready; §6 lists exactly what is missing for that claim.

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

## 2. Accounts, workspaces and roles (this wave, `a1cc1f91`)

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

## 3. Earlier waves on this branch (already pushed and proven)

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

---

## 4. Open defects (found, reproduced, not yet fixed)

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

---

## 5. Unproven — never measured, must not be claimed

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

---

## 6. What "ready to deploy to a client" still needs

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
