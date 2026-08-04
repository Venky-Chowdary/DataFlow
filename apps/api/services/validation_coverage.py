"""Validation Coverage Contract — sample must never claim population proof.

Module 4 SSOT for DataWrap Migration Assurance.

Every validation surface stamps:

* ``layer`` — schema | sample | population | execution | post_write
* ``population_proof`` — True only when layer is population AND probe completed
* ``guarantees`` / ``non_guarantees`` — operator-facing honesty
* optional row counts for explainability

Gate-8 full_checksum coverage is post-write digest equality — still not RI proof.
"""

from __future__ import annotations

from typing import Any

VALIDATION_LAYERS = frozenset(
    {"schema", "sample", "population", "execution", "post_write"}
)

_DEFAULT_NON_GUARANTEES = {
    "schema": [
        "Schema metadata does not prove row fidelity or referential integrity.",
        "Population orphan detection is not proven from FK hints alone.",
    ],
    "sample": [
        "Sample validation does not prove full population correctness.",
        "Rows outside the examined sample are unproven.",
    ],
    "population": [
        "Population proof is scoped to the selected transfer keys/tables only.",
    ],
    "execution": [
        "Execution checks do not replace post-write reconciliation.",
    ],
    "post_write": [
        "Post-write sample coverage is not full population proof.",
        "Checksum match does not prove referential integrity or constraint coverage.",
    ],
}

_DEFAULT_GUARANTEES = {
    "schema": ["Declared types and mapping targets were inspected."],
    "sample": ["Examined rows were checked under the active validation mode."],
    "population": ["Full selected population probe completed for the stated check."],
    "execution": ["Write-path policies were applied for accepted risk contracts."],
    "post_write": ["Post-write evidence was produced for the stated coverage level."],
}


def stamp_validation_coverage(
    *,
    layer: str,
    population_proof: bool = False,
    rows_examined: int | None = None,
    estimated_population: int | None = None,
    guarantees: list[str] | None = None,
    non_guarantees: list[str] | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    """Build an immutable-style coverage stamp. Never invent population proof."""
    layer_n = (layer or "").strip().lower()
    if layer_n not in VALIDATION_LAYERS:
        raise ValueError(f"Unknown validation layer: {layer!r}")
    if population_proof and layer_n != "population":
        raise ValueError(
            f"population_proof=True is illegal for layer={layer_n!r}; "
            "only layer=population may claim population proof"
        )
    if population_proof is False and layer_n == "population":
        # Population layer without proven flag is allowed (failed / incomplete probe).
        pass
    stamp: dict[str, Any] = {
        "layer": layer_n,
        "population_proof": bool(population_proof) if layer_n == "population" else False,
        "guarantees": list(guarantees if guarantees is not None else _DEFAULT_GUARANTEES[layer_n]),
        "non_guarantees": list(
            non_guarantees if non_guarantees is not None else _DEFAULT_NON_GUARANTEES[layer_n]
        ),
    }
    if rows_examined is not None:
        stamp["rows_examined"] = int(rows_examined)
    if estimated_population is not None:
        stamp["estimated_population"] = int(estimated_population)
    if note:
        stamp["note"] = note
    elif layer_n == "sample":
        stamp["note"] = (
            "Sample coverage only — does not represent full population correctness."
        )
    return stamp


def assert_no_sample_population_lie(stamp: dict[str, Any]) -> None:
    """Hard guard for tests and callers — sample cannot claim population proof."""
    if stamp.get("layer") == "sample" and stamp.get("population_proof"):
        raise AssertionError("Sample coverage claimed population_proof — forbidden")
    if stamp.get("layer") != "population" and stamp.get("population_proof"):
        raise AssertionError(
            f"layer={stamp.get('layer')!r} claimed population_proof — forbidden"
        )
