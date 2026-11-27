# Mapping accuracy

What Map's automatic column mapping gets right on labelled data, how that was
measured, and what is not claimed. **The numbers below are dev and dev2
numbers: dev was tuned on and dev2 was inspected while tuning, so neither is
a blind measurement. The blind number is pending** (a sealed set from a
separate author, scored by the lead, aggregates only).

## How it is measured

- **Fixture**: `apps/api/tests/fixtures/mapping_holdout_v1.json`, 23 tables
  and 231 source columns: renames and synonyms, abbreviations, five languages
  (German, Spanish, French, Portuguese, Dutch), type-disambiguated names,
  nested JSON paths, public schemas (Synthea to OMOP person, Salesforce
  Contact to HubSpot, Stripe Charge to a payments table) and 25 columns that
  have no right destination column. One author.
- **Splits**, by table and stratified by kind: *dev* (9 tables, 98 columns),
  tuned on; *dev2* (14 tables, 133 columns), formerly called "test". dev2 was
  re-measured after each change and two of its rows were cited while tuning,
  so **dev2 is not blind**. The fixture was committed 28 minutes before tuning
  began; that it was written without reading the mapper's dictionaries cannot
  be verified from the repository.
- **Contamination**: the base code's dictionaries, synonym lists, schematic
  index and knowledge files already associated 58 of dev2's 100 non-trivial
  answers (29 by the whole source and destination name, the rest by a token).
  Those rows are not evidence of generalisation. CI fails on any new
  association (`tests/test_mapping_holdout_leak.py`, name and token level, any
  quoting, plurals).
- **Path measured**: Map's own pipeline (`run_mapping_pipeline`, no AI model),
  typed source columns with sample values into an existing PostgreSQL table
  with typed columns. *Auto-approved* means proposed onto an existing column,
  not flagged for review, confidence at least 0.85 (the default G4 floor).
- **Intervals**: 95%, cluster bootstrap over tables (columns of one table are
  not independent), fixed seed.

Reproduce from `apps/api`:
`PYTHONPATH=.:../../packages/preflight/src python scripts/run_mapping_holdout.py --split dev2`

## Results on dev2, Map's pipeline (110 columns with a right answer, 23 without)

| Measure | Before this work | Now | 95% interval (now) |
|---|---|---|---|
| Auto-approved mappings that were right | 9 of 9 | **9 of 9** auto-approved mappings were right | too few rows for a useful interval |
| Columns auto-approved (coverage) | 8% | 8% | 1% to 17% |
| Columns with no right answer auto-approved | 0 of 23 | 0 of 23 | |
| Columns with no right answer proposed for review | 2 of 23 | 1 of 23 | |
| Right column proposed first (top-1) | 56% | 67% | 50% to 84% |
| Right column among the first three (top-3) | 84% | 87% | 80% to 94% |
| Proposals onto an existing column that were right | 76% | 94% | 88% to 98% |

Map auto-approves few columns: most proposals, right or wrong, wait for a
person. The auto-approved rows are the same rows the code auto-approved
before this work: a CI invariant (`tests/test_mapping_policy_invariant.py`)
forbids any new rule from creating an auto-approval the base mapper did not
make.

On dev (tuned on, so in-sample) the pipeline's top-1 is 77% and 9 of 9
auto-approved rows were right.

By kind on dev2 (top-1, not separately gated, 1 to 5 tables each): nested
JSON paths 100%, abbreviations 87%, public schemas 80%, type-disambiguated
names 80%, renames and synonyms 68%, **languages other than English 18%**.

### Scorer-level numbers (map_columns alone, not what Map approves)

The scoring stage alone marks more rows auto-approvable than Map does (the
pipeline then demotes most of them to review): on dev2 38 of 38 right
(coverage 35%), top-1 66%, top-3 89%. These are scorer-level figures and are
not a claim about Map.

## What the CI gates hold

`mapping-evidence` job:

- `tests/test_mapping_holdout_gate.py`, floors in
  `services/mapping_holdout_eval.py` (raised only after a re-measurement), for
  the pipeline and, labelled, the scorer: auto-approve precision at least 0.98
  on dev, dev2 and both; no column without a right answer auto-approved;
  top-1, top-3, coverage and proposal-precision floors;
- the leak check above;
- `tests/test_mapping_policy_invariant.py`: base vs new over 400 generated
  cross-table cases, no new or changed auto-approval.

The per-kind figures and the intervals are reported, not gated.

## What we do not claim

- No blind accuracy figure yet; it is pending.
- Not "100% mapping accuracy". The in-house golden fixtures score 100%, but
  they were written next to the dictionaries.
- Not multilingual mapping (18% top-1 on dev2).
- No external benchmark (Valentine, MIMIC-OMOP) has been run.
- The auto-approve floor stays 0.85; the data cannot justify a lower one.

## Validate vs Execute

Every finished run records the verdict that predicted it and what the write
then did. `GET /api/v1/ops/validate-execute` returns, for your workspace, the
share of runs whose preflight passed and whose write then completed, and the
miss rate with a 95% interval, overall and per source/destination type. A run
Execute did not preflight is counted as unvalidated whatever its outcome. It
holds counts and connector types only, no table, column or user names. The
rate covers runs recorded since this ledger was added; no historical figure
is claimed.

## Verified against

- `apps/api/tests/fixtures/mapping_holdout_v1.json` (the labelled tables and
  their dev / dev2 splits)
- `apps/api/scripts/run_mapping_holdout.py`, `apps/api/services/mapping_holdout_eval.py`
  (the measurement and the gated floors; `tests/test_mapping_holdout_gate.py`)
- `apps/api/services/mapping_holdout_leak.py` (`tests/test_mapping_holdout_leak.py`)
- `apps/api/tests/test_mapping_policy_invariant.py` (no new auto-approval)
- `apps/api/services/validate_execute_ledger.py`, `apps/api/src/routers/ops_router.py`
  (`GET /api/v1/ops/validate-execute`)
- `.github/workflows/ci.yml` (the `mapping-evidence` job)
