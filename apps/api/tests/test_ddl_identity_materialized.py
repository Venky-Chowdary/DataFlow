"""DDL identity is the physical destination column, not the stamp spelling.

Validate greened a Postgres->MySQL job 13/13 and Execute then refused it with
"DDL identity mismatch" and wrote nothing: Execute's own preflight had bound
the live MySQL catalog stamps (``VARCHAR(255) COLLATE utf8mb4_0900_ai_ci``,
``TIMESTAMP_NTZ(6)``) and assigned transforms, then hashed those against the
operator's Map rows. Both sets materialize to the same columns.
"""

from __future__ import annotations

import pytest

from services.decision_kernel import (
    DdlIdentityError,
    approved_mapping_ddl_fingerprint,
    assert_ddl_identity,
    ddl_identity_columns,
    ddl_identity_divergence,
    ddl_identity_report,
)

OPERATOR_MAPS = [
    {"source": "id", "target": "id", "target_type": "BIGINT", "transform": "none"},
    {
        "source": "email",
        "target": "email",
        "target_type": "VARCHAR(255)",
        "transform": "none",
    },
    {
        "source": "updated_at",
        "target": "updated_at",
        "target_type": "TIMESTAMP",
        "transform": "none",
    },
]

# What Execute re-derives from the live MySQL catalog before its own preflight.
ENGINE_MAPS = [
    {"source": "id", "target": "id", "target_type": "BIGINT", "transform": "integer"},
    {
        "source": "email",
        "target": "email",
        "target_type": "VARCHAR(255) COLLATE utf8mb4_0900_ai_ci",
        "transform": "none",
    },
    {
        "source": "updated_at",
        "target": "updated_at",
        "target_type": "TIMESTAMP_NTZ(6)",
        "transform": "datetime",
    },
]


def test_live_catalog_stamps_are_not_drift():
    approved = approved_mapping_ddl_fingerprint(OPERATOR_MAPS, dest_db="mysql")
    assert (
        assert_ddl_identity(approved, ENGINE_MAPS, dest_db="mysql") == approved
    )


def test_transform_change_alone_is_not_ddl_drift():
    approved = approved_mapping_ddl_fingerprint(OPERATOR_MAPS, dest_db="mysql")
    retransformed = [dict(m, transform="trim") for m in OPERATOR_MAPS]
    assert assert_ddl_identity(approved, retransformed, dest_db="mysql") == approved


def test_narrowed_column_still_refused():
    approved = approved_mapping_ddl_fingerprint(OPERATOR_MAPS, dest_db="mysql")
    narrowed = [
        dict(m, target_type="VARCHAR(32)") if m["source"] == "email" else dict(m)
        for m in OPERATOR_MAPS
    ]
    with pytest.raises(DdlIdentityError):
        assert_ddl_identity(approved, narrowed, dest_db="mysql")


def test_added_column_still_refused():
    approved = approved_mapping_ddl_fingerprint(OPERATOR_MAPS, dest_db="mysql")
    extra = [*OPERATOR_MAPS, {"source": "ssn", "target": "ssn", "target_type": "TEXT"}]
    with pytest.raises(DdlIdentityError):
        assert_ddl_identity(approved, extra, dest_db="mysql")


def test_dropped_column_still_refused():
    approved = approved_mapping_ddl_fingerprint(OPERATOR_MAPS, dest_db="mysql")
    with pytest.raises(DdlIdentityError):
        assert_ddl_identity(approved, OPERATOR_MAPS[:2], dest_db="mysql")


def test_mismatch_names_the_diverged_columns():
    approved_cols = ddl_identity_columns(OPERATOR_MAPS, dest_db="mysql")
    approved = approved_mapping_ddl_fingerprint(OPERATOR_MAPS, dest_db="mysql")
    narrowed = [
        dict(m, target_type="VARCHAR(32)") if m["source"] == "email" else dict(m)
        for m in OPERATOR_MAPS
    ]
    with pytest.raises(DdlIdentityError) as err:
        assert_ddl_identity(
            approved, narrowed, dest_db="mysql", approved_columns=approved_cols
        )
    assert "email: VARCHAR(255) → VARCHAR(32)" in str(err.value)


def test_divergence_is_silent_without_approved_columns():
    """A hash alone cannot name a cause — never invent one."""
    assert ddl_identity_divergence(None, OPERATOR_MAPS, dest_db="mysql") == []


def test_unstamped_column_never_equals_a_stamped_one():
    unstamped = [dict(m, target_type="") for m in OPERATOR_MAPS]
    assert ddl_identity_columns(unstamped, dest_db="mysql")[0]["materialized_ddl"] == ""
    assert approved_mapping_ddl_fingerprint(
        unstamped, dest_db="mysql"
    ) != approved_mapping_ddl_fingerprint(OPERATOR_MAPS, dest_db="mysql")


def test_report_carries_columns_for_the_execute_diff():
    report = ddl_identity_report(OPERATOR_MAPS, dest_db="mysql")
    assert report["ddl_identity_hash"]
    targets = [c["target"] for c in report["columns"]]
    assert targets == ["email", "id", "updated_at"]


def test_omitted_columns_stay_out_of_identity():
    with_omit = [
        *OPERATOR_MAPS,
        {
            "source": "notes",
            "target": "notes",
            "target_type": "TEXT",
            "intentional_omit": True,
        },
    ]
    assert approved_mapping_ddl_fingerprint(
        with_omit, dest_db="mysql"
    ) == approved_mapping_ddl_fingerprint(OPERATOR_MAPS, dest_db="mysql")
