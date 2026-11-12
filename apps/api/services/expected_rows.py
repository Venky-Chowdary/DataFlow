"""How many rows a run is expected to leave at the destination.

One cardinality identity, used by Gate-8 (``reconcile``), verification ladder
L1 and row conservation. When each kept its own copy they disagreed: the
ladder had no term for rows a declared source filter removed on the read, so
a correct filtered load passed the digest and failed L1.

    expected = source + expanded + retained - held_out - skipped
               - shaped_out - source_filtered

* ``source``          rows the reader counted (before any filter or recipe);
* ``expanded``        extra rows a recipe produced (explode / unpivot);
* ``retained``        SCD2 keys absent from the snapshot that stay current;
* ``held_out``        rejected rows that did not land. A row whose bad cell was
                      coerced to NULL landed, so it is not held out;
* ``skipped``         read but neither written nor rejected (stale CDC redelivery);
* ``shaped_out``      rows an approved recipe filtered or diverted on the read;
* ``source_filtered`` rows a declared ``source_filter`` removed on the read.
"""

from __future__ import annotations


def held_out_rows(rejected_rows: int, coerced_null_rows: int) -> int:
    """Rejected rows the destination does not hold (coerced-null rows landed)."""
    return max(max(int(rejected_rows or 0), 0) - max(int(coerced_null_rows or 0), 0), 0)


def expected_destination_rows(
    source_rows: int,
    *,
    rejected_rows: int = 0,
    coerced_null_rows: int = 0,
    rows_skipped: int = 0,
    rows_shaped_out: int = 0,
    rows_source_filtered: int = 0,
    rows_expanded: int = 0,
    retained_rows: int = 0,
    clamp: bool = True,
) -> int:
    """Rows this run should leave at the destination (see the module identity).

    ``clamp=False`` keeps a negative result, for a ledger that reports by how
    many rows the terms over-account.
    """
    expected = (
        int(source_rows or 0)
        + max(int(rows_expanded or 0), 0)
        + max(int(retained_rows or 0), 0)
        - held_out_rows(rejected_rows, coerced_null_rows)
        - max(int(rows_skipped or 0), 0)
        - max(int(rows_shaped_out or 0), 0)
        - max(int(rows_source_filtered or 0), 0)
    )
    return max(expected, 0) if clamp else expected
