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


def _artifact():
    return build_artifact_from_mappings(
        _MAPS, dest_db="postgresql", source_db="postgresql", source_fingerprint="",
        dest_fingerprint="", sync_mode="incremental_append", route_id="validate:postgresql",
        artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00",
    )


def _gate(live: dict[str, str], **extra):
    art = _artifact()
    args = dict(
        mappings=_MAPS, dest_db="postgresql", source_db="postgresql",
        approved_content_hash=art.content_hash, artifact_payload=art.to_dict(),
        dest_fingerprint=fingerprint_schema(list(live), live), destination_table_exists=True,
        dest_column_names=list(live), source_column_names=["id", "at", "amt"],
        live_column_types=live, sync_mode="incremental_append", error_policy="quarantine",
    )
    args.update(extra)
    return enforce_decision_artifact(**args)


_CATALOG = {"id": "bigint", "at": "timestamp with time zone", "amt": "numeric(10, 2)"}


@pytest.mark.parametrize("owned", [
    {"_dw_row_id": "bigint"}, {"_deleted": "boolean"}, {"_deleted": "boolean", "_deleted_at": "timestamptz"},
])
def test_a_create_new_artifact_runs_into_its_table_with_datawrap_columns(owned):
    err, art = _gate({**_CATALOG, **owned})
    assert err is None, err
    assert art is not None
    # And the D407 retry exception holds on its own terms too.
    err, art = _gate({**_CATALOG, **owned}, plan_created_destination=True)
    assert err is None, err


def test_catalog_spellings_of_the_planned_types_match():
    planned = dict(_artifact().ddl.column_ddl)
    assert planned_ddl_matches_live(planned, _CATALOG, dest_db="postgresql")
    assert planned_ddl_matches_live({"d": "DECIMAL(10,2)"}, {"d": "decimal(10, 2)"}, dest_db="mysql")
    assert planned_ddl_matches_live({"f": "BOOLEAN"}, {"f": "tinyint(1)"}, dest_db="mysql")
    # Widths and scales are still compared; a user column is still drift.
    assert not planned_ddl_matches_live({"d": "DECIMAL(10,2)"}, {"d": "decimal(12,2)"}, dest_db="mysql")
    assert not planned_ddl_matches_live(planned, {**_CATALOG, "note": "text"}, dest_db="postgresql")
    # A Datawrap column the plan maps itself is compared like any other.
    assert not planned_ddl_matches_live({"_deleted": "BOOLEAN"}, {"_deleted": "text"}, dest_db="postgresql")
