# Parity at Scale: canonical encoding (CE1)

**Status: experimental, behind `DATAWRAP_PARITY_SCALE=1`.** This is an
engineering specification. Nothing in it changes a verdict, Gate-8, a proof
pack or the Trust Report yet; those read Parity at Scale only from phase 3 of
the design.

Parity at Scale compares every row of a table of any size inside the two
databases. Each engine turns every row into bytes by the rules on this page
(the canonical encoding, version **CE1**), hashes the bytes, and sums the
hashes per key range or hash bucket. Two units are equal when their row
counts and sums are equal. The Python reference of these rules is
`apps/api/services/parity_scale/ce1.py`; the golden byte vectors are
`apps/api/tests/data/ce1_vectors.json`.

## The one rule that must never break

> If two values encode equal under CE1, Gate-8's fingerprint (fingerprint
> rules version 2, `services.reconciliation.fingerprint_for_reconcile`) also
> calls them equal.

CE1 may be **stricter** than the fingerprint: two values the fingerprint
folds together may encode apart (letter case under a case-insensitive
collation, JSON key order, SQLite's two separator spellings of one instant, a
`numeric` read back as a SQLite `REAL`). Those rows are fetched and the cell
judge rules on them with the fingerprint itself; they count in
`ce_stricter_than_fp` and are not differences. CE1 may never be **looser**: a
looser encoding could call two populations equal that Gate-8 would fail.

`apps/api/tests/test_parity_scale_ce1_soundness.py` checks the rule with
Hypothesis against the real fingerprint functions, and for approved lossy
casts against the real writer (`map_rows_for_fingerprint` and a SQLite write
round trip).

## Row, field and hash

A row is the concatenation, in column-plan order, of one field per column.
Column names are not encoded, so a rename costs nothing.

```
field := 'N'                                   SQL NULL
       | 'V' <UTF-8 byte length> ':' <text>    inline canonical text
       | 'D' <64 lowercase hex>                SHA-256 of the canonical bytes
       | 'X' <class> ':' <byte length> ':' <text>
                                               a value whose storage class is
                                               not the column kind's own
```

Every alternative starts with a tag and carries a length or a fixed width, so
no value can forge a field boundary and the row encoding is injective.

- `H = SHA-256(row bytes)`.
- `a` = the first 8 bytes of `H` read big-endian, low 60 bits kept; `b` = the
  next 8 bytes, likewise.
- A unit's digest is `(row count, Σa, Σb)`, summed as exact integers: never
  modulo, never XOR. XOR would cancel a duplicated row; a sum keeps
  multiplicity, which is what lets a keyless table be compared as a multiset.
- Two different multisets give equal sums with probability at most 2⁻¹²⁰ per
  unit. This protects against **accidental** change only: an adversary who
  controls the data can forge an additive hash. Tamper evidence remains the
  signed proof pack and the audit chain.

Buckets (mode H):

- keyed table: bits `63−10i … 54−10i` of the first 64-bit word of
  `SHA-256(key bytes)` give the bucket (0–1023) at level `i`;
- keyless table: the same bits of the **third** 64-bit word of `H` (the first
  two words are `a` and `b`; bucketing on them would fix some of their bits
  inside a bucket and weaken the bound).

## Per-kind canonical text

The source value is encoded **as the destination stores it** under the write
contract that ran: the destination's declared type, its carrier precision and
the approved risk contract. The destination value is encoded by the same rule
without the projection.

| Kind | Canonical field | Notes |
|---|---|---|
| NULL | `N` | NULL and `''` stay distinct, except on a destination that stores `''` as NULL (Oracle): there a zero-**length** string is `N` (tested by length, never `= ''`, because PAD SPACE collations make `'' = '   '` true). |
| Integer | `V` + decimal ASCII, `-` sign, no leading zeros | A boolean read as an integer is `1` / `0`. |
| Boolean | `V1:1` / `V1:0` | An integer in a boolean carrier keeps its value: `2` stays `2`. |
| Exact decimal | `V` + minimal exact decimal: no exponent, no trailing fractional zeros, `-0` → `0`, `NaN`, `Infinity`, `-Infinity` | `12.3400` and `12.34` encode equal (scale zeros are not significant, as in fingerprint v2). PostgreSQL: `trim_scale(x)::text`. |
| Text | `V` + the exact UTF-8 bytes | No Unicode normalisation and no case, accent, width or kana fold: those are the judge's (a deliberate stricter-than-fingerprint choice). |
| Fixed-width CHAR | as text, right-trimmed of U+0020 only | The destination's blank pad is not the value. |
| Date | `V` + integer days since 1970-01-01; `+inf` / `-inf` | |
| Timestamp (naive) | `V` + integer epoch units at the comparison precision `d`; `+inf` / `-inf` | `d` is the destination carrier's fractional digits (`carrier_instant.carrier_instant_digits`, capped at 6). The wall clock is read as if it were UTC. |
| Timestamp (aware) | as naive, after conversion to UTC | Naive and aware values with equal UTC components encode equal, as in fingerprint v2. |
| Time | `V` + integer units since midnight at `d` | |
| UUID | `V` + lowercase `8-4-4-4-12` | |
| Binary | `D` + SHA-256 of the raw bytes | |
| Same-engine native | `V` + the engine's own text rendering | Only when source and destination are one engine family and the column's canonical declared type is identical on both sides (PostgreSQL `x::text` under pinned session settings). Covers `jsonb`, floats, intervals and arrays on PostgreSQL→PostgreSQL. |
| JSON `null` from a file (D144) | `N` on a scalar column, `V4:null` on a document column | Only a stream side (a file) can produce it; the reference follows `services.file_json_null.json_null_reads_as_sql_null`. |

**Width.** Text declared wider than 64 characters (or unbounded) is encoded
by digest (`D`), so a row stays bounded. Binary is always a digest.

**Values of another storage class (`X`).** An engine without enforced column
types (SQLite) can hold an integer in a text column. Such a value is encoded
with its class (`Xinteger:1:5`, `Xreal:3:1.5`, `Xtext:…`, `Xblob:<hex>`), so it
never encodes like the text `'5'`. A value finer than the comparison
precision that no projection removed (a microsecond on a `timestamp(3)`
destination) is encoded at microseconds as `Xus:…` instead of being folded.

## Projections: replaying the write contract

Projections apply to the **source** side only. They replay what the writer
did; they never round and never invent a value the writer did not write.

**Instants.** The source instant is quantised exactly as
`carrier_instant.quantize_instant_for_carrier` does for the destination
carrier: drop a `datetime2(7)` 100 ns tick when `d ≤ 6`, then round the
microsecond field half **up**. On the epoch axis that is
`floor((u + step/2) / step)` for instants before 1970 as well. (The design
text said "half away from zero"; the code rounds half up on the timeline, and
CE1 follows the code.) A `TIME` that would round past midnight keeps its
second, as the carrier does.

**Approved lossy decimal casts (fit-or-contract).** For a destination
`NUMERIC(p,s)` narrower than the source:

- a value that fits (`connectors.writer_common.fits_decimal`) is encoded
  exactly;
- otherwise the writer's own action decides: the cell is written as NULL
  (`N`), or the row is held out of the destination and leaves the expected
  source population (the source query filters it out, and the scan reports
  how many rows the predicate excluded, for the quarantine-ledger cross-check
  in phase 3).

The plan does not assume the action from the contract's wording. It **reads
it from the real writer** at plan time
(`plan.probe_decimal_fit`: `map_rows_for_fingerprint` on one value of each
non-fitting class — fraction too long, integer part too long, both, `NaN`,
infinity — under the run's number locale; a typed database source reads as
`WIRE`). Each class's action is `keep`, `null` or `hold`. The property tests
check that the projection and the writer agree value by value, and a SQLite
write round trip checks what is actually stored.

`fits_decimal` counts integer digits of the *normalised* value, and zero
normalises to one integer digit: `0` does not fit `NUMERIC(p,p)` in the
writer, and the PostgreSQL projection replays that too.

## What goes to the streamed fingerprint path

These columns are not encoded in-engine in phase 1. Both sides stream
`(key, columns)`, fingerprint them with Gate-8's own
`fingerprint_for_reconcile`, spill `(key, SHA-256 of the fingerprints)` to a
temporary on-disk SQLite file and merge there, so fingerprint v2 semantics
hold exactly and memory stays bounded:

- binary floats, JSON, intervals and arrays when source and destination are
  not one engine family with an identical declared type (cross-engine pairs
  are phase 2);
- any column with a non-identity transform (trim, upper, `hash_pii`,
  crosswalks, recipes): the judge replays the transform;
- a rule contract (its held-out rows are phase 3);
- a risk contract that is not a decimal narrowing;
- a fixed-width source into a varying destination;
- naive↔aware instant pairs and time with time zone;
- a column whose catalog type is unknown.

A keyed table whose key cannot be encoded in-engine (a lossy key projection,
a `native` key outside SQLite) is compared entirely on the streamed path; so is
a keyless table with any streamed column (as a multiset of row fingerprints).

A declared omission is `excluded`: outside the population by rule.

## Keyless tables

A keyless table is compared as a multiset of row contents ("Content match (no
key)"). A match proves the destination holds exactly the same rows with the
same multiplicity. A difference is reported as rows of content missing from
and extra in the destination; it is never attributed to a key, and a changed
cell reads as one missing plus one extra row.

## Divergences the tests found in the product (not in CE1)

CE1 follows Gate-8. Where the tests showed Gate-8's model and an engine or a
writer disagree, the scan reports a difference — the honest direction — and
the divergence is recorded here for the owner of that code:

- **PostgreSQL rounds a narrowed instant half away from zero before 1970**
  (`timestamp(6)` → `timestamp(3)`: `1969-12-31 23:59:59.9995` is stored
  `…59.999`), while `carrier_instant` rounds half up on the timeline (`00:00:00.000`).
  Instants after 1970 agree. The live rendering test is restricted to
  post-1970 instants for that reason.
- **A `NaN` into a SQLite `NUMERIC(p,s)`**: Gate-8's source replay
  (`map_rows_for_fingerprint`) holds the row out, while the SQLite writer
  stores it with NULL.
- **A signed `CAST_AND_CONTINUE` contract with `COERCE_NULL` does not mean
  "NULL" everywhere.** Read from the writer (under the `WIRE` number locale a
  typed database source uses): into `NUMERIC(10,2)` every non-fitting value is
  held out; into `NUMERIC(6,0)` a value whose fraction does not fit (or that
  is `NaN` or infinite) is written as NULL, while one whose integer part does
  not fit is held out. A PostgreSQL `NaN` fits `NUMERIC(10,2)` and is kept.
  The projection reads each class's action from the writer per column, so it
  follows whatever the writer does.

## Versioning

The CE version is stamped on every scan's evidence. A change to any rule on
this page that could make two values encode equal that encoded apart before
is a new version. A fold added to fingerprint v2 must either bump CE or be
shown to be a fold CE1 leaves to the judge.

## Verified against

- `apps/api/services/parity_scale/ce1.py` (the Python reference) and
  `apps/api/services/parity_scale/plan.py` (classification, writer probe)
- `apps/api/tests/data/ce1_vectors.json`, generated by
  `apps/api/tests/data/gen_ce1_vectors.py`
- `apps/api/tests/test_parity_scale_ce1.py` (golden vectors, plan)
- `apps/api/tests/test_parity_scale_ce1_soundness.py` (Hypothesis: soundness
  against `fingerprint_for_reconcile`, projection against the real writer)
