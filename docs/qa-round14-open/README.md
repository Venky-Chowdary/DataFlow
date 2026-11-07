# QA round 14: open items at budget stop (2026-11-06)

Everything finished is on `claude/qa-round13-integration`. The two files here are
unfinished drafts kept out of the code because their tests do not pass yet. They are
NOT applied. Each item below is still open.

## Drafts in this folder
- `security-review6-items-1-3.UNFINISHED.diff`: email-change retire moved to the
  confirm step (one test needs rewriting through the confirm flow); SCIM `active=false`
  / DELETE blocks SSO sign-in (3 tests failing, not debugged); one fail-closed rule with
  a 30 s cache for the deleted-account marker read. The diff also contains two changes
  that are already pushed (f08667467, 6e2c3d37b).
- `audit-chain-tail-truncation.UNFINISHED.py`: patch script for a MAC'd chain head
  high-water mark and `tail_truncated` reporting. Untested; no p3 test yet.

## Open items with no code written
- Audit chain (C4): re-anchor carries only the first 500 findings and does not walk the
  whole store; `kh_seq` / re-anchor scans not sorted or restricted; honesty text still
  claims removal is proven; CDC cursor-key scoping per workspace.
- Hashing key (Browser-2): legacy pinned `"deployment"` version breaks replays and warns
  falsely (BLOCKING on upgrade); per-quarantine-row key version and per-row replay pin;
  parent job with no recorded version; share bounds on the hold-out offer; more
  guessable-key forms (sha256(""), "\n" variants, sha512/sha3, uuid1, time_ns);
  atomic `_JobKey`; Studio balanced hint text for numeric holdouts.
- MySQL (C3 follow-ups): `source_procedure` missing from `_PLACEMENT_KEYS`; intake not
  run on resume / re-dispatch / orphan sweep; release note for the refused legacy shape;
  unselected bad contract should warn, not 422; schedule-save source kind; typed
  ssl_mode error; live MySQL e2e only run in full-suite.yml.
- Transforms: NUMERIC scale rounding on the PG COPY+swap path (`pg_kept_columns_narrower`
  ignores numeric); bad rows at the top of a REST upload still refused (xfail pinned);
  self-review of b4467bb97, 96eb3dc7c, bcddac8d0.
- Schedules (D7): camelCase / spaced PAN names; exclusion beyond the first word;
  cookware false positives; Indian tax PAN as PII; IMEI false positives.
- Mapping: MySQL UNIQUE/PRIMARY prefix keys (SUB_PART) at Validate and in the append
  probe; SET byte sizing; shorter DDL lock-wait per attempt.
- Verdict: `DATAFLOW_DEST_WRITE_LOCK` is off by default; its flag-on fixes (#1-#11:
  real relation key, exclusive only when a witness starts, pooler detection, lock
  re-check, waiting state, 60 s wait with unwitnessed fallback, disclosures) are not done.
- CI: the full API suite has not completed green on any recent head; pushes cancelled
  runs. Run it once on the final head and triage.
