# Changelog

A factual summary of what merged into the main line recently. It is taken from
the subjects of merge commits only (`git log --merges --first-parent`). It
does not describe changes that are not named there.

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
