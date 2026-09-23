# Automated Rule Engine — fit analysis

**Branch:** `feature/automated-engine-for-rules`  
**Base:** `feature/Venkat-Analysis` @ `896ba87d`  
**Question:** if a customer uploads an Excel workbook of business rules (maps, lookups, formulas, validations), can Datawrap compile that into a transfer and execute it accurately? Does it belong on Transfer Studio **Transform**? Is it a good enterprise feature?

**Verdict.** Yes — this is a good feature, enterprises already buy adjacent products, and it is implementable **because most of the execution already exists**. It does **not** belong entirely inside the Transform step. Transform is the right *import surface* for pre-load shaping; the compiler must fan out onto Map, Validate, Run, and (for joins) Operations Transforms. Do not promise 100% automatic interpretation of arbitrary Excel. Promise 100% deterministic execution of rules the operator accepted, with proof.

---

## What the customer actually wants

Not “AI migrates my data.”

They already have the rules. They want:

> Turn this workbook into an executable, reviewable, proven migration.

Typical sheet:

| Source | Source Column | Destination | Destination Column | Rule |
| --- | --- | --- | --- | --- |
| Customer | fname | Customer | first_name | Direct |
| Customer | dob | Customer | birth_date | MM/DD/YYYY → YYYY-MM-DD |
| Customer | status | Customer | status | A → ACTIVE, I → INACTIVE |
| Customer | salary | Customer | annual_salary | salary × 12 |
| Customer | email | Customer | email | Lowercase + validate |

That is not a connector problem. It is a **compiler** problem.

---

## How Transfer Studio is built today

Wizard order is already the right pipeline (`apps/web/src/pages/transfer/studioConstants.ts`):

```
Source → Destination → Transform (pre-load) → Map → Validate → Run
                                              ↑
                                    Job Theater proof lives after Run
```

Transform sits **before** Map on purpose: Map, fidelity risk, and destination DDL are decided from the columns and values they are shown. Changing source-side truth after Map would lie.

There are **three separate transform planes**. Mixing them is how this feature would become dishonest.

| Plane | Where | What it can do | What it refuses |
| --- | --- | --- | --- |
| **Pre-load shape recipe** | Transfer step **Transform** | Row-local, deterministic steps on the read. Source file is never mutated. Identity is `recipe_hash`. | `join`, `lookup`, `aggregate`, `pivot`, `sort`, `dedupe`, `window` — each named and pointed at Operations Transforms (`shape_models.py` `_GLOBAL_OPS`) |
| **Map pair + write transform** | Transfer step **Map** | Source→dest edge, per-column `transform`, **code_crosswalk** (A→ACTIVE) with Gate **G20** coverage | Invented LLM transforms auto-apply (`docs/AI_GATE_POLICY.md`) |
| **Post-load SQL** | Operations → Transforms | Joins, aggregates, set-based SQL, dbt export | Not the Transfer Transform step |

The shape engine’s contract is already the one this feature needs (`shape_models.py`):

> A recipe is an ordered list of named steps applied to the source stream before Map sees it… the source file is never mutated.

And (`shape_expr.py`):

> Deterministic. No clock, no randomness, no I/O. The same row yields the same value in preview, in Validate and in Execute.

Validate already shapes first (`shape_preflight.py`). Execute already stamps `shape_recipe_hash` and `shape_proof`. A declared recipe that does not match the approved hash is refused.

**What is missing is only the front door:** ingest a customer workbook and compile it onto those three planes, with confidence and a review queue.

Excel as a *data source* already works. Excel as a *rule specification* has no importer.

---

## Does it fit the Transform step?

**Partially — and that is the correct answer.**

### Put on Transform (shape recipe)

Anything that changes the *source image* before Map:

- cleanse: trim, case, replace, null sentinels, pad
- parse: dates, numbers, booleans
- derive: `salary * 12`, concatenate, split, hash identity
- structure: rename / keep / drop / cast
- row policy: filter exclusions, divert bad rows to quarantine
- defaults: `default_if_null`, constant columns

These already exist as catalog ops (`STEP_CATALOG` in `shape_models.py`). A compiler emits `ShapeStepWire[]`. The existing preview, `recipe_hash`, Continue gate, Validate, and Execute path do the rest.

Hard limits that must be shown, not hidden:

- **100 steps** (`MAX_STEPS`). A 427-rule workbook cannot be dumped as 427 sequential steps.
- **Row-local only.** A VLOOKUP against another sheet is not a shape step.
- **CDC / SCD2 / mirror refuse pre-load** (`preloadTransformRefused`). History was not written by this recipe.

### Put on Map, not Transform

- Direct column pairs (`fname → first_name`)
- Closed code tables (`A → ACTIVE`) as `code_crosswalk` + G20
- Per-column write transforms already on the mapping row
- Unmapped destination fields, duplicate mappings, dest type conflicts

Putting `fname → first_name` into Transform as `rename_column` would steal Map’s job and break create-new / identity / risk-ack.

### Put on Operations Transforms, not Transfer Transform

- Joins, second-table lookups, aggregates, pivots, window functions
- Multi-object enrichment that cannot be evaluated on one streamed row

The engine already refuses these by name. The compiler must do the same and open a post-load draft, not invent a shape `lookup` op.

### Put on Validate / proof, not AI

- Mandatory fields, type narrowing, null policy, uniqueness
- Population coverage of every lookup code (G20)
- Checksum reconcile and quarantine
- Gate pass/fail — **AI never decides G1–G9** (`docs/AI_GATE_POLICY.md`)

---

## Recommended compiler (do not let the LLM touch rows)

```
Workbook / CSV / SQL / existing ETL map
        │
        ▼
  Rule Ingestion (parse sheets, not “understand the business”)
        │
        ▼
  Interpreter  ── structured only ──►  Canonical Rule IR
  (deterministic first; LLM only for leftover prose)
        │
        ▼
  Validator against live source + dest schemas
        │
        ├── executable now
        ├── needs confirmation
        └── conflict / unknown
        │
        ▼
  Fan-out (never one blob)
        ├── ShapeRecipe          → Transform step
        ├── Mapping + crosswalk  → Map step
        ├── Post-load SQL draft  → Operations Transforms
        └── Review queue         → operator Accept / Edit / Reject
        │
        ▼
  Existing Preview → Validate → Execute → Job Theater proof
```

The IR is the product. Example of a compiled lookup — **not** an LLM deciding each row:

```json
{
  "kind": "lookup",
  "source": "Customer.status",
  "destination": "Customer.status",
  "plane": "map.code_crosswalk",
  "mapping": { "A": "ACTIVE", "I": "INACTIVE", "P": "PENDING" },
  "confidence": 0.98,
  "provenance": { "sheet": "Rules", "row": 183 }
}
```

`salary × 12` compiles to a shape `derive_column` with `expression: salary * 12`.  
`MM/DD/YYYY → YYYY-MM-DD` compiles to `parse_date`.  
`Lowercase + validate email` compiles to `case` + a Validate / `divert_rows` rule, not a silent drop.

Ambiguous prose (“for legacy customers use the old number unless migrated”) stays in the review queue. That is the honesty bar, not a failure.

---

## Can it be 100%?

Two different claims:

| Claim | Honest answer |
| --- | --- |
| **100% execution** of an accepted, compiled rule | **Yes, and we already have this.** Same recipe in preview, Validate, and Execute. Hash mismatch refuses the run. |
| **100% interpretation** of any Excel the customer wrote | **No.** Ambiguous English, implicit joins, conflicting sheets, and “legacy unless migrated” cannot be auto-trusted. |

Do not market “upload Excel, migrate everything automatically.”  
Market: **interpret and operationalize the customer’s existing rules, with confidence and human approval for the rest, then prove every accepted rule on the population.**

That is also what `docs/AI_GATE_POLICY.md` already requires: AI may suggest; deterministic engines decide; invented transforms stay `requires_review`.

---

## Is this used in enterprise products already?

Yes. The *broad* idea is not new. The wedge is.

| Product | What they actually do | What they do not do (for this customer) |
| --- | --- | --- |
| **Informatica CLAIRE Automapping** | Suggests source↔target field matches from names, types, and learned patterns. Docs: [Automapping](https://docs.informatica.com/integration-cloud/data-integration/current-version/mappings/mappings/automapping.html). | Not “here is our 50-page rule workbook, compile it.” SQL-ELT mode has no automapping. |
| **Informatica CLAIRE Copilot** | Builds a mapping from a **natural-language prompt**; can add transformations and expressions. Docs: [CLAIRE Copilot](https://docs.informatica.com/integration-cloud/data-integration/current-version/claire-copilot-for-data-integration/using-claire-copilot-with-data-integration.html). | Informatica staff (March 2026) said Copilot **cannot create mapping tasks from a parameterized template or an Excel list of mappings** — “something that we are planning to add.” [Community thread](https://network.informatica.com/s/question/0D5VM00000xXigb0AC/claire-copilot-creating-mapping-tasks). |
| **PowerCenter Mapping Analyst for Excel** | Imports a **fixed Standard mapping-specification template** (Models / Sources / Targets / transformations) via Repository Manager. Guide: *Mapping Analyst for Excel 10.5.x*. | Customer must fill *Informatica’s* template. It is not freeform “status A means ACTIVE” business English, and it does not produce Datawrap-style population proof. |
| **AWS Glue DataBrew** | Reusable transformation recipes, hundreds of built-in cleanses, run as jobs. | Recipe authoring, not “compile our migration bible and prove the rules.” |
| **Datawrap today** | Semantic Map + pre-load shape recipe + G20 crosswalk + Validate + checksum proof. | No workbook importer. |

So: enterprises already accept AI-assisted mapping. They have **not** been given a simple path from *their existing Excel* to *executable rules + proof*. PowerCenter’s Excel add-in proves the demand; CLAIRE Copilot’s current gap (no Excel mapping list) is the opening.

Do not position as “we invented AI mapping.” Position as:

> **Turn your existing business-rule workbook into an executable, validated migration — with proof that those rules held.**

That is closer to xAQUA / QEagle *assurance* than to Airbyte / Fivetran *sync*, which is already this product’s charter (`docs/PRODUCT_ARCHITECTURE.md`).

---

## Is it good for *this* application?

**Yes, if it compiles onto the engines we already trust.** It is a bad idea if it becomes a fourth transform runtime or lets an LLM write destination rows.

Why it is a fit:

1. Transfer Studio already has the operator loop: Transform → Map → Validate → Run → proof.
2. Shape recipe + `recipe_hash` is already a compiler target.
3. `code_crosswalk` + G20 is already a lookup target.
4. Mapping lock / `requires_review` is already the human-in-the-loop.
5. Quarantine + reconcile already forbid silent loss.
6. Enterprise buyers of this product arrive with mapping spreadsheets. That is the job, not a side quest.

Why dumping it only into Transform would be wrong:

- Direct maps belong on Map (identity, create-new, risk-ack).
- Closed code tables belong on G20, not 50 `replace` steps.
- Joins cannot run on the stream (`_GLOBAL_OPS`).
- 427 rules vs `MAX_STEPS = 100`.
- CDC/SCD2 cannot take a pre-load recipe today.

---

## Smallest honest first slice (not the 50-page dream)

Do not start with PDF/Word/SQL archaeology. Start with one structured sheet the customer already has.

**Slice 1 — structured mapping workbook → IR → two planes**

Accepted columns (aliases allowed): source table, source column, dest table, dest column, rule.

Compile:

| Rule text (examples) | Target |
| --- | --- |
| Direct / 1:1 / map | Mapping pair, transform `none` |
| A → ACTIVE, I → INACTIVE | `code_crosswalk` on that pair |
| Lowercase / trim / date format | Shape steps on the source column |
| `salary * 12` / concat | `derive_column` / `concat_columns` |
| Unrecognised prose | Review queue, confidence < threshold |

UI: **Import rules** on the Transform step (the operator is already there to declare shaping). After compile:

- accepted shape steps populate `shapeSteps` and must pass the existing preview identity
- accepted pairs merge into Map via the same lock rules as today’s pipeline (`docs/MAPPING_ENGINE_CONTRACT.md` — operator locks are never overwritten)
- review items stay visible until Accept / Edit / Reject

Proof for slice 1: a fixture workbook of the Customer table above, compiled, previewed, Validated, executed on SQLite→SQLite, with:

- every Direct pair present on Map
- status crosswalk covered by G20
- `recipe_hash` on the run
- row counts + checksum
- one deliberately ambiguous row that did **not** auto-apply

That is a measured floor, not a marketing 100%.

**Later slices** (only after slice 1 has artifacts): multi-sheet lookups → G20 tables; join sheets → post-load draft; PDF/Word prose; Pilot “Analyze Migration Rules” briefing.

---

## What we will not do

- Let an LLM decide a cell at execute time.
- Auto-apply a rule below the confidence floor.
- Claim catalog-wide “any Excel, any dialect.”
- Add `lookup` / `join` to the pre-load catalog to make a demo green.
- Treat workbook tile count as “427 rules understood.” Count compiled IR objects, with executable / review / conflict buckets.

---

## Code seams (when implementation starts)

| Seam | File |
| --- | --- |
| Import API | new `services/rule_compiler/` + `POST /shape/import-spec` (or `/transfer/rules/import`) |
| Shape target | `services/shape_models.py` `ShapeRecipe` |
| Map + crosswalk target | `lib/mapping.ts`, `services/code_crosswalk.py`, `services/mapping_pipeline.py` |
| Transform UI hook | `TransferTransformStep.tsx` — Import rules, then existing preview |
| Map merge | `TransferPage.tsx` `applyPipelineMappings` |
| Approval / lock | `docs/MAPPING_ENGINE_CONTRACT.md`, `docs/AI_GATE_POLICY.md` |
| Proof | existing Validate + `engine_shape.py` + Job Theater |

No new execution engine is required for slice 1.

---

## Bottom line

| Question | Answer |
| --- | --- |
| Is this a good feature? | Yes. It is the difference between “another ETL” and “migration assurance.” |
| Can we implement it? | Yes. Compile onto shape + map + G20 + Validate. Do not rebuild Transform. |
| Do enterprises use this class of thing? | Yes — Informatica automapping/Copilot, PowerCenter Mapping Analyst for Excel, Glue DataBrew. |
| Can enterprises use *ours*? | Yes, if we stay proof-oriented and human-in-the-loop. |
| Does it live in the Transform step? | The **import and pre-load half** does. The rest must land on Map / Validate / Operations Transforms. |
| 100%? | 100% of accepted compiled rules: yes. 100% of arbitrary Excel: no. |
