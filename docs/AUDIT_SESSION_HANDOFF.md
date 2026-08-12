# Audit Session Handoff — Standing Limitations & Next Moves

Branch of record: `devin/deep-audit-1784855991` (HEAD `41fd8a33`, "Close engine honesty
and Gate-8 assurance gaps (#36)").

This is the carry-forward state between audit sessions. Items marked **honesty lock**
are deliberate `unknown` / `unproven` / `Planned` labels. They are *not* TODOs to flip
green — flipping one requires the named proof artifact, not a wording change.

---

## 1. Open limitations

### 1. CHECK predicate AST equivalence per dialect — open
Structure fidelity's last soft spot. CHECK predicates are not compared as dialect-aware
ASTs, so semantically equal predicates written differently are not proven equivalent.
Carry/loss is certified rather than rewritten — see `services/schema_fidelity.py` and
Property 6 in [ZERO_LOSS_PROPERTIES.md](ZERO_LOSS_PROPERTIES.md).

### 2. Resume conservation for `job_id`-less callers — open decision
Callers that do not supply a `job_id` have no durable checkpoint identity. Undecided
whether to refuse resume outright or persist a lightweight local checkpoint. Checkpoints
are otherwise fail-closed: a job aborts rather than continuing with silent resume risk.

### 3. File/object exports remain `unproven` (operational pass only) — honesty lock
No destination cell read-back exists; the writer checksum proves bytes/count, never
per-cell fidelity. This is intentional honesty, not a TODO to flip green.
Code anchor: `apps/api/services/reconcile_coverage.py` (`is_unproven_export`,
`skipped_readback`, writer-digest-only detection).

### 4. CDC/resume default is at-least-once upsert — honesty lock
Holds until exactly-once is *measured*. The registry now says so; do not upgrade the
wording without an integration fixture.
Code anchor: `apps/api/services/cdc_effectively_once.py` — `DELIVERY_DEFAULT =
"at-least-once"`, `EXACTLY_ONCE_CLAIMED = False`, `honesty_dict()`.

### 5. Windows-only test teardown flakes (not logic)
4 tests fail *only* on `PermissionError: [WinError 32]` inside `tempfile.py` cleanup
(SQLite file handle not released before `TemporaryDirectory.__exit__`). Assertions pass
in isolation.

- `test_data_integrity_p0.py::test_stream_strict_fails_instead_of_silent_null[strict|maximum]`
- `test_data_integrity_p0.py::test_stream_balanced_holds_out_bad_row_and_records_rejection`
- `test_file_stream_path.py::test_stream_file_to_database_from_path`
- `test_stream_database_sqlite.py::test_stream_sqlite_includes_ddl_log_and_summary`

**Fix (next session):** close the SQLite connection (or `engine.dispose()`) in these
tests before the temp dir is torn down. Test hygiene only — no product impact.

---

## 2. Not committed on the audit branch

- `apps/web/public/brand/linkedin-posts/` (untracked marketing PNGs) — intentionally
  excluded from the engineering commit. Commit separately if desired.

---

## 3. Recommended next moves (prioritized, defend then expand)

1. **CHECK predicate AST equivalence** per dialect (close limitation #1) — the last
   soft spot in structure fidelity.
2. **Fix the 4 Windows teardown flakes** so CI on Windows is deterministically green
   (limitation #5). Trend teardown errors to zero.
3. **Reconcile matrix expansion** — extend `test_property6_schema_fidelity` and the
   Gate-8 conservation cases to real Postgres/MySQL/SQL Server services (emulator or
   containers), reporting pass/fail/skip counts. Today SQLite is the deepest live path.
4. **Resume conservation for job_id-less callers** — decide whether to refuse resume
   or persist a lightweight local checkpoint (limitation #2).
5. **CDC exactly-once** — until proven with an integration fixture, keep the
   at-least-once wording locked (limitation #4).

---

## 4. One-line honesty summary

A green run on this branch now means: the reader counted the source, every row was
delivered or quarantined-and-surfaced (including across resume), and PK/NN/DEFAULT/
UNIQUE/CHECK were re-read from the destination catalog — with the remaining edges in
§1 stated as `unknown`/`unproven`/`Planned` rather than painted green.

---

## Provenance

Sections 3–5 of §1 and all of §2–§4 are the verbatim carry-forward from the audit
session that produced `41fd8a33`. Limitations #1 and #2 were referenced by number in
that handoff but not restated there; their titles here are reconstructed from the
next-moves list and the code they point at, so treat their bodies as summary rather
than verbatim.

Verified against the tree at `41fd8a33`: all four flaky test functions exist at the
paths above, `linkedin-posts/` is untracked, and both code anchors (`reconcile_coverage.py`,
`cdc_effectively_once.py`) carry the stated constants/predicates.
