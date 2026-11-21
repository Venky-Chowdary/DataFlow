# Mapping accuracy

What the automatic column mapping gets right, measured on data it was not
built from, and what it does not claim. Every number on this page is checked
by a CI gate; a change that makes one worse fails the build.

## How it is measured

- **Benchmark**: `apps/api/tests/fixtures/mapping_holdout_v1.json`, 23 tables
  and 231 source columns: renames and synonyms, abbreviations, five languages
  (German, Spanish, French, Portuguese, Dutch), type-disambiguated names,
  nested JSON paths, public schemas (Synthea to OMOP person, Salesforce
  Contact to HubSpot, Stripe Charge to a payments table) and 25 columns that
  have no right destination column at all.
- **Written blind**: the set was authored before reading the mapper's
  abbreviation, synonym or schematic dictionaries.
- **Split by table** into *dev* (9 tables, 98 columns) and *test* (14 tables,
  133 columns), stratified by kind. Changes are tuned on dev only. The numbers
  below are for **test**.
- **Leak check**: CI fails if a test answer (source column -> right
  destination column) is added to the abbreviation dictionary, synonym lists,
  training data, ML vocabulary or mapping fixtures. Overlaps that already
  existed when the set was frozen are listed in the fixture.
- **Setting**: typed source columns with sample values, mapped into an existing
  PostgreSQL table with typed columns, no AI model.
- **Auto-approved** means Map proposes an existing column with no review flag
  and confidence at or above 0.85, the default G4 floor. Everything else waits
  for a person.

Reproduce from `apps/api`:
`PYTHONPATH=.:../../packages/preflight/src python scripts/run_mapping_holdout.py --split test`

## Results (test split: 110 columns with a right answer, 23 without)

| Measure | Value |
|---|---|
| Auto-approved mappings that were right | **40 of 40** (95% lower bound 91%) |
| Columns with no right answer that were auto-mapped | **0 of 23** |
| Columns with no right answer that were proposed for review | 1 of 23 |
| Right column proposed first (top-1) | 66% (73 of 110) |
| Right column among the first three suggestions (top-3) | 89% |
| Proposals onto an existing column that were right | 92% (73 of 79) |
| Columns auto-approved (coverage) | 36% |

On dev and test together (231 columns) 76 of 76 auto-approved mappings were
right (95% lower bound 95%).

By kind on test (top-1): nested JSON paths 100%, abbreviations 87%,
type-disambiguated names 80%, renames and synonyms 68%, public schemas 60%,
**languages other than English 18%**.

## What the CI gates hold

`mapping-evidence` job (`tests/test_mapping_holdout_gate.py`, floors in
`services/mapping_holdout_eval.py`, raised only after a re-measurement):

- auto-approve precision at least 0.98 on test and on all columns;
- no column without a right answer is ever auto-approved, on any split;
- test top-1 at least 0.6636 and top-3 at least 0.8909;
- the leak check above.

`ai-offline-correctness` job: the mapping policy unit tests, the create-new
type grading sweep and the Gate-8 fingerprint property
(`tests/test_fingerprint_properties.py`).

## What we do not claim

- Not "100% mapping accuracy". The in-house golden fixtures score 100%, but
  they were written next to the dictionaries; on blind data the right column
  comes first about two times in three.
- Not multilingual mapping. Column names in other languages are mostly left
  for review (18% top-1).
- One author and about 230 columns: a 95% interval on test top-1 is roughly
  plus or minus 9 points. No external benchmark (Valentine, MIMIC-OMOP) has
  been run yet.
- The auto-approve floor stays 0.85. The dev data cannot justify a lower one
  (too few columns for a 95% lower bound of 0.95).

## Validate vs Execute

Every finished run records the verdict that predicted it (a reused Validate
run, else the preflight Execute ran before writing) and what the write then
did. `GET /api/v1/ops/validate-execute` returns, for your workspace, the
share of validated runs whose write completed and the miss rate with a 95%
interval, overall and per source/destination type. It holds counts and
connector types only, no table, column or user names. The rate covers runs
recorded since this ledger was added; no historical figure is claimed.
