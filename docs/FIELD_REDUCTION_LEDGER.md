# Field Reduction Ledger (G16)

A legacy screen has 10 fields. The replacement has 7. Three fields do not
survive the migration. The auditor's question is not "did you notice?" — G13
already answers that — it is **"why, who agreed, and on what evidence?"**

The Field Reduction Ledger records exactly one typed disposition for every
declared source field, and refuses to record a justification the data
contradicts.

## Dispositions

**Carried** — the value reaches the destination:

| Disposition | Meaning |
| --- | --- |
| `carried` | 1:1, no transform |
| `carried_transformed` | 1:1 through a transform (expression hashed) |
| `merged_into` | N:1 — this field is one of several sources of one target |
| `split_into` | 1:N — this field feeds more than one target |

**Reduced** — the value does not reach the destination:

| Reason code | Requires | Evidence kind |
| --- | --- | --- |
| `dropped_empty` | — | observed (claims the column holds no values) |
| `dropped_constant` | — | observed (claims one distinct value) |
| `dropped_redundant` | reason text | declared |
| `dropped_obsolete` | reason text | declared |
| `dropped_pii_minimization` | reason text | declared |
| `dropped_not_required` | reason text | declared |
| `archive_only` | reason text + `archive_reference` | declared |
| `deferred_phase` | reason text | declared |
| `dropped_unclassified` | — | none (a declared drop with no recorded reason) |

`unaccounted` is not a disposition an operator can choose: it is the absence of
a decision, and it blocks at G13.

## What the gate refuses

* **A factual claim the sample disproves.** `dropped_empty` on a column where
  12 of 500 sampled rows carry a value is a false record, not a decision. The
  operator must either fix the code or own it as a judgement
  (`dropped_not_required` / `dropped_redundant`) with a reason.
* **An archive claim with no archive.** "It is kept elsewhere" is only evidence
  when the elsewhere is named (`archive_reference`, optionally
  `retention_until`).
* **A reason code Datawrap cannot classify.** An unknown `dropped_*` code
  blocks rather than being coerced into a neighbouring meaning.
* **In strict mode** (`DATAWRAP_FIELD_REDUCTION_STRICT=true`): any reduction
  without a reason code or without a named approver. Off by default, because
  turning a legacy boolean `intentional_omit` into a hard block would fail jobs
  already approved under G13 — those surface as a `warn` and are recorded in the
  proof pack as `dropped_unclassified`.

## What it does *not* prove

* Justification statistics come from the Validate sample, not the population.
  An all-empty sample is **not** proof the column is empty; the ledger labels
  the basis (`sample`) and never claims population proof. Contradiction is
  conclusive in one direction only: a non-empty sample *does* disprove an
  "always empty" claim.
* Approvals are **recorded, not authenticated**. Identity comes from the
  caller's session; the signature binds the ledger to a job, not to a person.
* A signed ledger proves the reduction decisions were not edited after
  approval. It does not prove the reduction was a good idea.

## Recording a reduction in Map

Set `Transform → Omit` on the source row. The destination cell then asks for the
reduction itself:

| Control | Wire field | When |
| --- | --- | --- |
| Why is it dropped? | `omit_reason` | always |
| Note | `omit_reason_text` | required by every judgement code |
| Archive that holds it | `archive_reference` | required by `archive_only` |
| Retained until | `retention_until` | optional, `archive_only` |
| Accepted by | `omit_approved_by` | optional (required in strict mode) |

Map names the evidence G16 still needs on the row itself, so the gap is visible
before Validate. It deliberately does not predict the sample-contradiction
block: only the engine sees the sample, and a green row from a check Datawrap
never ran would be a false promise. Carrying the column again clears its
reduction evidence — a carried field has no reduction to justify.

## Where it appears

* Gate `g16_field_reduction` in Validate (hard gate; `warn` for unexplained
  legacy omissions, `block` for a contradicted or unclassifiable reduction).
* `preflight.field_reduction_ledger` and `proof_bundle.field_reduction_ledger`.
* `preflight_summary.field_reduction_ledger` inside the signed proof pack, so an
  exported pack shows *why* each field was not carried — not only that it was
  declared omitted.

Signing and verification use the same canonical-JSON + HMAC-SHA256 scheme as
the proof pack, with subject `field_reduction:<job_id>`.
