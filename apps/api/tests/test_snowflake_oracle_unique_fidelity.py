"""Snowflake UNIQUE catalog + Oracle GENERATED/NLSSORT — enterprise SSOT."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.schema_introspect import _snowflake_fetch_unique_keys  # noqa: E402
from services.type_system import (  # noqa: E402
    is_generated_always_column,
    parse_case_insensitive_index_expression,
    row_matches_unique_filter,
    unique_key_forces_casefold,
)


def test_snowflake_fetch_unique_keys_hybrid_enforced():
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("SYS_PK", "PRIMARY KEY", "ID", 1, "YES"),
        ("UQ_EMAIL", "UNIQUE", "EMAIL", 1, "YES"),
    ]
    meta = _snowflake_fetch_unique_keys(cur, "PUBLIC", "USERS")
    assert meta["primary_key_columns"] == ["ID"]
    names = {u["name"]: u for u in meta["unique_keys"]}
    assert names["UQ_EMAIL"]["enforced"] is True
    assert names["UQ_EMAIL"]["columns"] == ["EMAIL"]


def test_snowflake_not_enforced_flagged():
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("UQ_EMAIL", "UNIQUE", "EMAIL", 1, "NO"),
    ]
    meta = _snowflake_fetch_unique_keys(cur, "PUBLIC", "USERS")
    assert meta["unique_keys"][0]["enforced"] is False


def test_integrity_skips_snowflake_not_enforced_unique():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "email", "target": "EMAIL"}],
        [{"email": "a"}, {"email": "a"}],
        "strict",
        dest_kind="snowflake",
        primary_key="email",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "UQ_EMAIL",
                "columns": ["EMAIL"],
                "enforced": False,
            }
        ],
        target_types={"EMAIL": "VARCHAR"},
    )
    # Advisory UNIQUE must not invent a transfer block.
    assert result["passed"] is True


def test_integrity_does_not_block_on_a_stamped_snowflake_enforced_flag():
    """Dialect name does not say hybrid. A stamped True is not a write block."""
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "email", "target": "EMAIL"}],
        [{"email": "a"}, {"email": "a"}],
        "strict",
        dest_kind="snowflake",
        primary_key="id",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "UQ_EMAIL",
                "columns": ["EMAIL"],
                "enforced": True,
            }
        ],
        target_types={"EMAIL": "VARCHAR", "ID": "INTEGER"},
    )
    assert result["passed"] is True


def test_measured_hybrid_table_blocks_a_duplicate_enforced_key():
    """IS_HYBRID YES plus ENFORCED YES is a write rule. Either fact alone is not."""
    from services.data_integrity import _check_duplicate_keys

    rows = [{"email": "a"}, {"email": "a"}]
    mappings = [{"source": "email", "target": "EMAIL"}]
    types = {"EMAIL": "VARCHAR", "ID": "INTEGER"}

    def _run(**key):
        return _check_duplicate_keys(
            mappings,
            rows,
            "strict",
            dest_kind="snowflake_aws",
            primary_key="id",
            sync_mode="append",
            destination_unique_keys=[
                {
                    "name": "UQ_EMAIL",
                    "columns": ["EMAIL"],
                    "enforced": True,
                    **key,
                }
            ],
            target_types=types,
        )

    hybrid = _run(table_kind="YES")
    assert hybrid["passed"] is False
    assert hybrid["blocks_transfer"] is True
    assert any("UQ_EMAIL" in issue for issue in hybrid["issues"])

    standard = _run(table_kind="NO")
    assert standard["passed"] is True

    iceberg = _run(table_kind="iceberg")
    assert iceberg["passed"] is True

    missing_enforced = _check_duplicate_keys(
        mappings,
        rows,
        "strict",
        dest_kind="snowflake",
        primary_key="id",
        sync_mode="append",
        destination_unique_keys=[
            {"name": "UQ_EMAIL", "columns": ["EMAIL"], "table_kind": "hybrid"}
        ],
        target_types=types,
    )
    assert missing_enforced["passed"] is True

    other_engine = _check_duplicate_keys(
        mappings,
        rows,
        "strict",
        dest_kind="bigquery",
        primary_key="id",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "UQ_EMAIL",
                "columns": ["EMAIL"],
                "enforced": True,
                "table_kind": "hybrid",
            }
        ],
        target_types=types,
    )
    assert other_engine["passed"] is True


def test_snowflake_fetch_records_is_hybrid():
    """The table-kind read is INFORMATION_SCHEMA.TABLES.IS_HYBRID, after the keys."""
    cur = MagicMock()
    cur.fetchall.side_effect = [
        [("UQ_EMAIL", "UNIQUE", "EMAIL", 1, "YES")],
        [("YES",)],
    ]
    meta = _snowflake_fetch_unique_keys(cur, "PUBLIC", "ORDERS")
    assert meta["table_kind"] == "hybrid"
    assert meta["unique_keys"][0]["table_kind"] == "hybrid"
    assert meta["unique_keys"][0]["enforced"] is True
    sql = " ".join(str(call.args[0]) for call in cur.execute.call_args_list)
    assert "is_hybrid" in sql
    assert "information_schema.tables" in sql

    failed = MagicMock()
    failed.fetchall.return_value = [("UQ_EMAIL", "UNIQUE", "EMAIL", 1, "NO")]
    failed.execute.side_effect = [
        None,
        RuntimeError("table kind columns unavailable"),
        RuntimeError("IS_HYBRID unavailable"),
    ]
    unread = _snowflake_fetch_unique_keys(failed, "PUBLIC", "ORDERS")
    assert unread["table_kind"] == ""
    assert "table_kind" not in unread["unique_keys"][0]
    assert unread["unique_keys"][0]["enforced"] is False
    assert "rely" not in unread["unique_keys"][0]


def test_snowflake_fetch_records_rely_without_treating_it_as_enforced():
    """``TABLE_CONSTRAINTS.RELY`` is stored beside ``ENFORCED``. It does not replace it.

    A five-column row is an account that did not return RELY. A missing RELY
    column falls back to the enforced select and leaves the key unmarked.
    """
    from services.unique_key_introspect import _snowflake_rely

    assert _snowflake_rely("YES") is True
    assert _snowflake_rely("no") is False
    assert _snowflake_rely(None) is None
    assert _snowflake_rely("MAYBE") is None

    cur = MagicMock()
    cur.fetchall.side_effect = [
        [
            ("UQ_EMAIL", "UNIQUE", "EMAIL", 1, "NO", "YES"),
            ("UQ_EMAIL", "UNIQUE", "ORG", 2, "NO", "NO"),
        ],
        [("NO", "NO", "NO", "NO")],
    ]
    meta = _snowflake_fetch_unique_keys(cur, "PUBLIC", "USERS")
    key = meta["unique_keys"][0]
    assert key["enforced"] is False
    assert key["rely"] is True
    assert "tc.rely" in str(cur.execute.call_args_list[0].args[0]).lower()

    narrow = MagicMock()
    narrow.fetchall.return_value = [("UQ_EMAIL", "UNIQUE", "EMAIL", 1, "NO")]
    narrow_meta = _snowflake_fetch_unique_keys(narrow, "PUBLIC", "USERS")
    assert narrow_meta["unique_keys"][0]["enforced"] is False
    assert "rely" not in narrow_meta["unique_keys"][0]

    fallback = MagicMock()
    fallback.fetchall.return_value = [("UQ_EMAIL", "UNIQUE", "EMAIL", 1, "NO")]
    fallback.execute.side_effect = [
        RuntimeError("invalid identifier 'RELY'"),
        None,
        RuntimeError("table kind columns unavailable"),
        RuntimeError("IS_HYBRID unavailable"),
    ]
    fallen = _snowflake_fetch_unique_keys(fallback, "PUBLIC", "USERS")
    assert fallen["unique_keys"][0]["enforced"] is False
    assert "rely" not in fallen["unique_keys"][0]
    assert fallen["table_kind"] == ""
    attempted = " ".join(str(call.args[0]) for call in fallback.execute.call_args_list).lower()
    assert "tc.rely" in attempted
    assert "coalesce(tc.enforced" in attempted


def test_rely_yes_does_not_block_and_does_not_cancel_hybrid_enforcement():
    """RELY YES is an optimizer hint. Hybrid plus ENFORCED YES still blocks."""
    from services.data_integrity import _check_duplicate_keys

    rows = [{"email": "a"}, {"email": "a"}]
    mappings = [{"source": "email", "target": "EMAIL"}]
    types = {"EMAIL": "VARCHAR", "ID": "INTEGER"}

    def _run(**key):
        return _check_duplicate_keys(
            mappings,
            rows,
            "strict",
            dest_kind="snowflake",
            primary_key="id",
            sync_mode="append",
            destination_unique_keys=[
                {
                    "name": "UQ_EMAIL",
                    "columns": ["EMAIL"],
                    **key,
                }
            ],
            target_types=types,
        )

    hinted = _run(enforced=False, rely=True, table_kind="hybrid")
    assert hinted["passed"] is True
    assert hinted["blocks_transfer"] is False
    assert any("RELY is an optimizer hint" in warning for warning in hinted["warnings"])
    assert any("NOT ENFORCED" in warning for warning in hinted["warnings"])

    rely_without_kind = _run(enforced=True, rely=True)
    assert rely_without_kind["passed"] is True
    assert any("RELY is an optimizer hint" in warning for warning in rely_without_kind["warnings"])

    rely_no = _run(enforced=False, rely=False, table_kind="hybrid")
    assert rely_no["passed"] is True
    assert not any("RELY" in warning for warning in rely_no["warnings"])

    enforced = _run(enforced=True, rely=True, table_kind="YES")
    assert enforced["passed"] is False
    assert enforced["blocks_transfer"] is True
    assert any("UQ_EMAIL" in issue for issue in enforced["issues"])

    # BUILD VALIDATION FAILURE still rejects a new write. SHOW INDEXES is
    # the existing-row proof, not this duplicate block.
    still_enforced = _run(
        enforced=True,
        table_kind="YES",
        index_status="BUILD VALIDATION FAILURE",
    )
    assert still_enforced["passed"] is False
    assert still_enforced["blocks_transfer"] is True


def test_oracle_not_validated_is_recorded_and_still_blocks_a_new_duplicate():
    """``VALIDATED`` is not ``STATUS``. NOT VALIDATED still rejects a new row."""
    from services.data_integrity import _check_duplicate_keys, _unique_constraint_enforced
    from services.unique_key_introspect import (
        _oracle_fetch_unique_keys,
        oracle_uniqueness_proof,
        read_oracle_uniqueness_rows,
    )

    assert oracle_uniqueness_proof(
        [
            ("PK_EMP", "P", "ID", 1, "NOT VALIDATED"),
            ("PK_EMP", "P", "ORG", 2, "VALIDATED"),
        ]
    ) == {frozenset({"id", "org"}): "not_checked"}
    assert oracle_uniqueness_proof(
        [("UQ_EMAIL", "U", "EMAIL", 1, "VALIDATED")]
    ) == {frozenset({"email"}): ""}

    cur = MagicMock()
    cur.execute.return_value.fetchall.side_effect = [
        [("PK_EMP", "P", "ID", 1, "NOT VALIDATED")],
        [],
    ]
    meta = _oracle_fetch_unique_keys(cur, "HR", "EMP")
    key = meta["unique_keys"][0]
    assert key["validated"] is False
    assert key["primary"] is True
    assert key["columns"] == ["ID"]
    sql = str(cur.execute.call_args_list[0].args[0]).lower()
    assert "ac.validated" in sql
    assert "ac.status" in sql
    assert "status = 'enabled'" not in sql

    narrow = MagicMock()
    narrow.execute.return_value.fetchall.side_effect = [
        [("PK_EMP", "P", "ID", 1)],
        [],
    ]
    unmarked = _oracle_fetch_unique_keys(narrow, "HR", "EMP")
    assert "validated" not in unmarked["unique_keys"][0]

    class _Broken:
        def execute(self, sql, params=()):
            raise RuntimeError("ORA-00904: VALIDATED")

    assert read_oracle_uniqueness_rows(_Broken(), "HR", "EMP") is None

    assert _unique_constraint_enforced(
        {"name": "PK_EMP", "columns": ["ID"], "validated": False},
        dest_kind="oracle",
    ) is True
    blocked = _check_duplicate_keys(
        [{"source": "email", "target": "EMAIL"}],
        [{"email": "a"}, {"email": "a"}],
        "strict",
        dest_kind="oracle",
        primary_key="id",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "UQ_EMAIL",
                "columns": ["EMAIL"],
                "validated": False,
            }
        ],
        target_types={"EMAIL": "VARCHAR"},
    )
    assert blocked["passed"] is False
    assert blocked["blocks_transfer"] is True


def test_oracle_disabled_unique_stays_visible_and_does_not_block():
    """``STATUS`` ``DISABLED`` is not a dropped key and not a write block.

    A five-column row did not ask for ``STATUS``. An enabled constraint on
    the same columns keeps the existing-row proof.
    """
    from services.data_integrity import _check_duplicate_keys, _unique_constraint_enforced
    from services.unique_key_introspect import (
        _oracle_fetch_unique_keys,
        oracle_uniqueness_proof,
    )

    assert oracle_uniqueness_proof(
        [("UQ_EMAIL", "U", "EMAIL", 1, "VALIDATED", "DISABLED")]
    ) == {frozenset({"email"}): "disabled"}
    assert oracle_uniqueness_proof(
        [
            ("UQ_EMAIL", "U", "EMAIL", 1, "VALIDATED", "ENABLED"),
            ("UQ_EMAIL_OFF", "U", "EMAIL", 1, "VALIDATED", "DISABLED"),
        ]
    ) == {frozenset({"email"}): ""}
    assert oracle_uniqueness_proof(
        [("PK_EMP", "P", "ID", 1, "NOT VALIDATED")]
    ) == {frozenset({"id"}): "not_checked"}

    cur = MagicMock()
    cur.execute.return_value.fetchall.side_effect = [
        [("UQ_EMAIL", "U", "EMAIL", 1, "VALIDATED", "DISABLED")],
        [],
    ]
    key = _oracle_fetch_unique_keys(cur, "HR", "EMP")["unique_keys"][0]
    assert key["disabled"] is True
    assert key["enforced"] is False
    assert _unique_constraint_enforced(key, dest_kind="oracle") is False
    warned = _check_duplicate_keys(
        [{"source": "email", "target": "EMAIL"}],
        [{"email": "a"}, {"email": "a"}],
        "strict",
        dest_kind="oracle",
        primary_key="id",
        sync_mode="append",
        destination_unique_keys=[key],
        target_types={"EMAIL": "VARCHAR"},
    )
    assert warned["passed"] is True
    assert warned["blocks_transfer"] is False
    assert any("STATUS" in warning and "DISABLED" in warning for warning in warned["warnings"])


def test_oracle_nlssort_binary_ci_forces_casefold():
    expr = "NLSSORT(\"EMAIL\",'NLS_SORT=BINARY_CI')"
    assert parse_case_insensitive_index_expression(expr) == ["EMAIL"]
    uks = [
        {
            "name": "UQ_EMAIL_CI",
            "columns": [],
            "expression_columns": ["EMAIL"],
            "case_insensitive": True,
        }
    ]
    assert unique_key_forces_casefold("EMAIL", unique_keys=uks) is True


def test_oracle_virtual_identity_generated_tokens():
    assert is_generated_always_column("NUMBER GENERATED ALWAYS") is True
    assert is_generated_always_column("NUMBER GENERATED BY DEFAULT") is False


def test_partial_unique_and_conjunction():
    pred = "([active]=(1) AND [email] IS NOT NULL)"
    assert row_matches_unique_filter({"active": 1, "email": "a@b.c"}, pred)
    assert not row_matches_unique_filter({"active": 0, "email": "a@b.c"}, pred)
    assert not row_matches_unique_filter({"active": 1, "email": None}, pred)
