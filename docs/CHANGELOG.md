# Changelog

A factual summary of what merged into the main line recently. It is taken from
the subjects of merge commits (`git log --merges --first-parent`). An entry
that links upgrade notes also says what operators will notice; those points
were checked against the code.

## 14 November 2026

- **Enterprise hardening** (PR 430). Operators should read the upgrade notes
  before deploying: see
  [operate/backup-and-upgrade.md](operate/backup-and-upgrade.md#enterprise-hardening-pr-430).
  - Connector egress is enforced at every checkpoint by default: private,
    loopback and carrier-grade NAT hosts are refused unless the workspace
    allowlist covers them; cloud metadata and link-local hosts are always
    refused.
  - Only a platform administrator can widen a workspace's egress allowlist
    (SEC-16). A workspace administrator can still narrow or clear it.
  - SQLite and DuckDB files are confined to the service's folders in every
    environment, not only in production. `DATAWRAP_SOURCE_PATH_ROOTS` adds a
    directory.
  - Error responses for failures the product can name carry a code, one next
    action and a support reference (`error.reference`, also in
    `X-Correlation-ID`). Driver text is reduced to one cleaned sentence.
  - Run needs the approval of the Validate that checked the plan, for the
    same destination and streams.
  - Sign-in lockouts are per account and address and can be shared through
    Redis; an administrator's password reset ends the person's sessions;
    non-upload request bodies are size-limited.
  - "Certified" connectors are those a green CI run proves; others that work
    are "Implemented, not yet proven". See
    [reference/connectors.md](reference/connectors.md).

## 13 November 2026

- **Exports, Map step and engine edge cases** (PR 427).
- **Transfer plans** (PR 422). Concurrent edits to a plan no longer overwrite
  each other.
- **Security follow-ups** (PR 421).
- **Destination write semantics** (PR 420).
- **Spreadsheet formula injection** (PR 419). XLSX, CSV and TSV, email and
  audit exports no longer carry formulas from the data.
- **Document Modeler core** (PR 417).
- **MongoDB upsert** (PR 416). An upsert keeps destination fields the mapping
  does not write.
- PR 428 (CI PostgreSQL readiness) changes CI only.

## 12 November 2026

- **Incremental syncs no longer skip late-committed rows** (PR 407). A
  commit-safe read window: the PostgreSQL snapshot horizon, an overlap re-read
  for other SQL sources, one total cursor order, refusals for text and file
  cursors that cannot be ordered, and an optional key census. Read the upgrade
  notes: see
  [operate/backup-and-upgrade.md](operate/backup-and-upgrade.md#incremental-syncs-pr-407).
- **Workspace isolation and residency** (PR 403). Deny-by-default residency
  addressing, per-workspace file volumes, and ownership checks on ids named
  in a request body. Operators should read the upgrade notes before
  deploying: see
  [operate/backup-and-upgrade.md](operate/backup-and-upgrade.md#workspace-isolation-and-residency-pr-403)
  and [operate/security.md](operate/security.md#data-residency).
- **Training runtime and egress** (PR 410). Training runs on its own thread
  with a stop event. The egress guard's skip of AWS region labels is strict.
- **Schedules** (PR 409). Fixes a race when a schedule run is finalized, and
  classifies a failure by an HTTP status code, not by a port that contains
  the same digits.
- **Reversibility Proof** (PR 402). Exit rehearsals, a reversibility gate, and
  an S3 Object Lock anchor for the audit chain (see
  [operate/security.md](operate/security.md#anchors)).
- **Ops reliability** (PR 406). Shared control-plane stores, worker drain on
  SIGTERM, bounded store calls and Helm checks. See
  [operate/deploy.md](operate/deploy.md#what-is-shared-between-replicas).
- **Security follow-ups to PR 403** (PR 415). Confinement in production,
  caller binding, platform credentials and URL parity.
- **Verification** (PR 411). Fingerprints v2, honest source filters and
  lossy-type flags.
- **Multi-stream runs** (PR 413). Each stream is written, verified, exported
  and measured on its own object.
- **File transfers** (PR 414). A file transfer's date order no longer stays
  pinned on its thread.
- **Pilot grounding** (PR 405). Workspace-isolated sources, an egress gate
  and accurate answers.
- PR 418 and PR 412 (test suite reliability) change tests only.

## 11 November 2026

- **Test database** (PR 408). Mail threads are kept out of the enterprise
  test database.
- **Trust Report states** (PR 401). The Trust Report shows honest states for
  failed and unverified runs. Destination labels are built from the parts of
  the destination that are set.
- **Jobs page** (PR 399). The Jobs page is an operations grid with a detail
  drawer.
- **Parity Studio** (PR 400). Adds the Trust Report, a loop for approving
  fixes, and signed sign-off by more than one role. See
  [use/parity-studio.md](use/parity-studio.md).
- **QA defect round 39** (PR 398). Fixes defects D352 to D361, of which D355
  was rated high, and changes when the append baseline is taken.

## 10 November 2026

- **File exports** (PR 397). Transfer Studio file exports go through a private
  bucket with signed, audited download links that expire.
- **QA defects** (PR 396). Fixes D349 (high), D350 and D351, and adds a SQL
  Server proof oracle.
- **QA defects** (PR 395). Fixes D325 to D348, D276 and D148. CORS accepts
  exact origins only. Local-disk destinations are turned off on the hosted
  service.
- **QA defects** (PR 394). Fixes 38 high-priority defects, one of them
  critical, and returns CI to green.

## 9 November 2026

- PR 392 and PR 391 merged. Their merge subjects carry no description.

## 7 November 2026

- Documentation that was not needed at run time was removed from the
  repository (commit `1415f1f0f`). The pages in this folder replace the
  operator and user parts of it.

## 1 November 2026

- 30 pull requests merged, numbered between 297 and 363, including the QA
  round 12 integration (PR 363). The other merge subjects carry only a number.

## 26 to 29 October 2026

- 11 pull requests merged. Their subjects carry branch names only.

## How to use this file

- For what a release changes for operators, read
  [operate/backup-and-upgrade.md](operate/backup-and-upgrade.md#things-to-check-after-an-upgrade).
- Defect numbers (D-numbers) refer to the QA defect register, which is not in
  this repository.
- To see the changes behind an entry, run `git log <merge>^1..<merge>^2`.
