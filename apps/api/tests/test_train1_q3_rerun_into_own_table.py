"""Q3 (train review): a re-run / retry into the plan's own table is not drift.

#435 ran an exact-DDL comparison before the create-new dest-exists hold, so a
create-new artifact executed into its own table was refused as "dest schema
drifted" when the table carried a column Datawrap adds itself (the approved
``_dw_row_id``, mirror ``_deleted``) or when the catalog spelled a planned
type its own way (``timestamp with time zone``, ``decimal(10, 2)``). The
comparison now ignores Datawrap-owned columns, folds catalog spellings with
the dialect's read-back canonicaliser, and is the D407 retry exception's own
condition only.
"""

from __future__ import annotations

import pytest

from services.decision_kernel.execute_gate import (
    _datawrap_owned_columns,
    build_artifact_from_mappings,
    enforce_decision_artifact,
    planned_ddl_matches_live,
)
from services.schema_fingerprint import fingerprint_schema

_MAPS = [
    {"source": "id", "target": "id", "source_type": "BIGINT", "target_type": "BIGINT", "create_new": True},
    {"source": "at", "target": "at", "source_type": "TIMESTAMPTZ", "target_type": "TIMESTAMPTZ",
     "create_new": True},
    {"source": "amt", "target": "amt", "source_type": "NUMERIC(10,2)", "target_type": "NUMERIC(10,2)",
     "create_new": True},
]


def _artifact(mode: str = "incremental_append"):
    return build_artifact_from_mappings(
        _MAPS, dest_db="postgresql", source_db="postgresql", source_fingerprint="",
        dest_fingerprint="", sync_mode=mode, route_id="validate:postgresql",
        artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00",
    )


def _gate(live: dict[str, str], mode: str = "incremental_append", **extra):
    art = _artifact(mode)
    args = dict(
        mappings=_MAPS, dest_db="postgresql", source_db="postgresql",
        approved_content_hash=art.content_hash, artifact_payload=art.to_dict(),
        dest_fingerprint=fingerprint_schema(list(live), live), destination_table_exists=True,
        dest_column_names=list(live), source_column_names=["id", "at", "amt"],
        live_column_types=live, sync_mode=mode, error_policy="quarantine",
    )
    args.update(extra)
    return enforce_decision_artifact(**args)


_CATALOG = {"id": "bigint", "at": "timestamp with time zone", "amt": "numeric(10, 2)"}


@pytest.mark.parametrize("owned, mode, surrogate", [
    ({"_dw_row_id": "bigint"}, "incremental_append", True),
    ({"_deleted": "boolean"}, "mirror", False),
    ({"_deleted": "boolean", "_deleted_at": "timestamptz"}, "mirror", False),
])
def test_a_create_new_artifact_runs_into_its_table_with_datawrap_columns(owned, mode, surrogate):
    err, art = _gate({**_CATALOG, **owned}, mode, surrogate_approved=surrogate)
    assert err is None, err
    assert art is not None
    # And the D407 retry exception holds on its own terms too.
    err, art = _gate({**_CATALOG, **owned}, mode, surrogate_approved=surrogate,
                     plan_created_destination=True)
    assert err is None, err
    # The same column where this plan does not add it is drift (review 3 item 2).
    other = "incremental_append" if mode == "mirror" else "mirror"
    err, art = _gate({**_CATALOG, **owned}, other)
    assert err and "drifted" in err.lower(), err
    assert art is None


def test_each_bookkeeping_column_is_ignored_only_in_the_mode_that_adds_it():
    """Review 2 item 9: SCD2 columns for scd2, soft-delete columns for mirror,
    the surrogate only when this run's DDL approval let the writer add it."""
    planned = {"id": "BIGINT", "v": "TEXT"}
    base = {"id": "bigint", "v": "text"}
    cases = [
        ({"is_current": "boolean", "valid_from": "timestamptz", "valid_to": "timestamptz",
          "row_hash": "text"}, "scd2", False),
        ({"_deleted": "boolean", "_deleted_at": "timestamptz"}, "mirror", False),
        ({"_dw_row_id": "bigint"}, "incremental_append", True),
    ]
    for extra, mode, surrogate in cases:
        live = {**base, **extra}
        owned = _datawrap_owned_columns(mode, surrogate_approved=surrogate)
        assert planned_ddl_matches_live(planned, live, dest_db="postgresql", owned_columns=owned), mode
        for other in ("incremental_append", "upsert", "scd2", "mirror"):
            if other == mode:
                continue
            assert not planned_ddl_matches_live(
                planned, live, dest_db="postgresql", owned_columns=_datawrap_owned_columns(other)
            ), (mode, other)
    assert not planned_ddl_matches_live(
        planned, {**base, "_dw_row_id": "bigint"}, dest_db="postgresql",
        owned_columns=_datawrap_owned_columns("incremental_append"),
    )


def test_catalog_spellings_of_the_planned_types_match():
    planned = dict(_artifact().ddl.column_ddl)
    assert planned_ddl_matches_live(planned, _CATALOG, dest_db="postgresql")
    # Not folded for this check: json vs jsonb, BIT(1) vs tinyint(1).
    assert not planned_ddl_matches_live({"j": "JSON"}, {"j": "jsonb"}, dest_db="postgresql")
    assert not planned_ddl_matches_live({"b": "BIT(1)"}, {"b": "tinyint(1)"}, dest_db="mysql")
    assert planned_ddl_matches_live({"d": "DECIMAL(10,2)"}, {"d": "decimal(10, 2)"}, dest_db="mysql")
    assert planned_ddl_matches_live({"f": "BOOLEAN"}, {"f": "tinyint(1)"}, dest_db="mysql")
    # Widths and scales are still compared; a user column is still drift.
    assert not planned_ddl_matches_live({"d": "DECIMAL(10,2)"}, {"d": "decimal(12,2)"}, dest_db="mysql")
    assert not planned_ddl_matches_live(planned, {**_CATALOG, "note": "text"}, dest_db="postgresql")
    # A Datawrap column the plan maps itself is compared like any other.
    assert not planned_ddl_matches_live({"_deleted": "BOOLEAN"}, {"_deleted": "text"}, dest_db="postgresql",
                                        owned_columns=_datawrap_owned_columns("mirror"))
