"""MySQL character-set / collation DDL truth (defects D4, D5).

Three separate claims are covered:

1. A ``CHARACTER SET`` / ``COLLATE`` clause is only legal on a MySQL character
   column, and only before ``NOT NULL`` / ``DEFAULT``. Emitted anywhere else it
   is a hard refusal (1064 / 1253), not a warning, so the whole CREATE fails.
2. MySQL ``NVARCHAR``/``NCHAR`` are aliases for ``CHARACTER SET utf8mb3``,
   which is BMP-only: a national source column carrying an astral scalar must
   not land in one, or the write is refused with 1366.
3. A collation must belong to the character set the column actually stores —
   ``utf8mb4_bin`` on a utf8mb3 column is refused.
"""

from __future__ import annotations

import re

import pytest
from services.collation_carry import plan_collation_carry
from services.decision_kernel.invent import invent_dest_type
from services.encoding_capacity import BMP_MAX, classify_capacity
from services.schema_fidelity import (
    SourceSchemaCatalog,
    plan_create_new_fidelity,
    render_create_column_defs,
)
from services.source_engine_scope import bind_source_engine
from services.type_system import national_charset_would_collapse

_CHARSET_RE = re.compile(r"(?:CHARACTER\s+SET|CHARSET)\s+", re.I)
_COLLATE_RE = re.compile(r"COLLATE\s+", re.I)


def _mysql_type(source_type: str, *, source_engine: str = "sqlserver") -> str:
    """Invented MySQL create-new stamp for ``source_type``.

    The source engine is bound because a national carrier's repertoire is the
    source engine's answer, not the type name's: SQL Server ``NVARCHAR`` is
    UTF-16 (every scalar), MySQL's own ``NVARCHAR`` is the utf8mb3 alias.
    """
    with bind_source_engine(source_engine):
        return str(invent_dest_type(source_type, dest_db="mysql", context="create_new"))


def _plan_for(
    source_types: dict[str, str],
    *,
    dialect: str = "sqlserver",
    collations: dict[str, str] | None = None,
) -> tuple[str, list[str]]:
    """Rendered MySQL column body plus the invented types."""
    types = [_mysql_type(t, source_engine=dialect) for t in source_types.values()]
    catalog = SourceSchemaCatalog(
        dialect=dialect,
        columns=list(source_types),
        column_types=dict(source_types),
        nullable=dict.fromkeys(source_types, True),
        primary_key=[],
        unique_keys=[],
        collations=collations or {},
        charsets={},
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="mysql",
        target_columns=list(source_types),
        target_types=types,
        source_to_target={c: c for c in source_types},
    )
    body = render_create_column_defs(
        columns=plan.dest_columns, types=types, plan=plan, dialect="mysql"
    )
    return body, types


@pytest.mark.parametrize(
    ("column", "source_type"),
    [
        ("n", "INT"),
        ("d", "DECIMAL(12,2)"),
        ("t", "DATETIME2"),
        ("b", "BIT"),
        ("bin", "VARBINARY(MAX)"),
    ],
)
def test_non_character_column_gets_no_charset_or_collation(
    column: str, source_type: str
) -> None:
    """A collated source column landing on a non-character MySQL type.

    MySQL answers ``INT CHARACTER SET utf8mb4`` with a 1064 syntax error and
    ``LONGBLOB COLLATE utf8mb4_bin`` with 1253, so the source's collation is
    recorded as not carried rather than stamped onto the column.
    """
    body, _ = _plan_for(
        {column: source_type},
        collations={column: "Latin1_General_BIN"},
    )
    assert not _CHARSET_RE.search(body), body
    assert not _COLLATE_RE.search(body), body


def test_non_character_destination_records_an_unsupported_decision() -> None:
    """The refusal is stated, not silently dropped."""
    catalog = SourceSchemaCatalog(
        dialect="sqlserver",
        columns=["payload"],
        column_types={"payload": "VARCHAR(64)"},
        nullable={"payload": True},
        primary_key=[],
        unique_keys=[],
        collations={"payload": "Latin1_General_BIN"},
        charsets={},
    )
    plan = plan_collation_carry(
        catalog=catalog,
        dest_dialect="mysql",
        dest_name_for_source=lambda c: c,
        dest_type_for_column=lambda _c: "JSON",
        unique_or_pk=set(),
    )
    assert not plan.column_prefixes
    decision = next(d for d in plan.decisions if d.dest_column == "payload")
    assert decision.status == "unsupported"
    assert "character carrier" in decision.reason


def test_character_column_clauses_precede_not_null() -> None:
    """``CHARACTER SET`` / ``COLLATE`` bind to the type, not to the constraint.

    ``VARCHAR(32) NOT NULL CHARACTER SET utf8mb4`` is a syntax error; the
    clauses must sit between the type and ``NOT NULL``.
    """
    types = ["VARCHAR(32)"]
    catalog = SourceSchemaCatalog(
        dialect="sqlserver",
        columns=["code"],
        column_types={"code": "VARCHAR(32)"},
        nullable={"code": False},
        primary_key=[],
        unique_keys=[],
        collations={"code": "Latin1_General_BIN"},
        charsets={},
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="mysql",
        target_columns=["code"],
        target_types=types,
        source_to_target={"code": "code"},
    )
    body = render_create_column_defs(
        columns=plan.dest_columns, types=types, plan=plan, dialect="mysql"
    )
    charset_at = body.upper().find("CHARACTER SET")
    collate_at = body.upper().find("COLLATE")
    not_null_at = body.upper().find("NOT NULL")
    assert charset_at != -1 and collate_at != -1 and not_null_at != -1, body
    assert charset_at < collate_at < not_null_at, body


def test_charset_is_stated_once() -> None:
    """One CHARACTER SET clause per column — a repeat is a syntax error."""
    body, _ = _plan_for(
        {"code": "NVARCHAR(32)"},
        collations={"code": "Latin1_General_BIN"},
    )
    assert len(_CHARSET_RE.findall(body)) == 1, body


def test_national_source_lands_in_utf8mb4_not_the_nvarchar_alias() -> None:
    """MySQL ``NVARCHAR`` is utf8mb3 — an astral scalar would be refused.

    Preserving the national *spelling* narrowed the repertoire from the
    source's UTF-16 to three-byte utf8mb3, so a 4-byte scalar died at the
    write with 1366. The national polarity is preserved as capacity instead.
    """
    stamped = _mysql_type("NVARCHAR(32)")
    assert not re.match(r"^\s*N", stamped, re.I), stamped
    assert "utf8mb4" in stamped.lower(), stamped
    assert classify_capacity("mysql", stamped).form == "utf8"


def test_unbounded_national_source_states_its_charset() -> None:
    """An unstamped ``TEXT`` inherits the server default, which may be latin1."""
    stamped = _mysql_type("NVARCHAR(MAX)")
    assert "utf8mb4" in stamped.lower(), stamped


def test_mysql_source_national_keeps_its_own_spelling() -> None:
    """A MySQL source's ``NVARCHAR`` really is utf8mb3 — widening it would lie.

    The widen exists to carry a repertoire the source actually had. MySQL to
    MySQL has none to carry, so restating the column as utf8mb4 would report a
    capacity change the source never made, and would silently change the
    equality domain of a column the operator matched on.
    """
    stamped = _mysql_type("NVARCHAR(32)", source_engine="mysql")
    assert stamped.upper().startswith("NVARCHAR"), stamped
    assert "utf8mb4" not in stamped.lower(), stamped


def test_unknown_source_engine_does_not_invent_a_widen() -> None:
    """Unknown means unmeasured: keep the source's spelling, decide nothing."""
    stamped = _mysql_type("NVARCHAR(32)", source_engine="")
    assert stamped.upper().startswith("NVARCHAR"), stamped


def test_mysql_national_alias_is_measured_as_bmp_only() -> None:
    """Reading the alias as 'national' promised UTF-16 capacity it lacks."""
    cap = classify_capacity("mysql", "NVARCHAR(32)")
    assert cap.form == "utf8mb3"
    assert cap.max_code_point == BMP_MAX


def test_utf8mb4_target_is_not_graded_a_national_collapse() -> None:
    """A stamped utf8mb4 column keeps every scalar the national source held."""
    assert not national_charset_would_collapse(
        "NVARCHAR(32)",
        "VARCHAR(32) CHARACTER SET utf8mb4",
        dest_db="mysql",
    )
    # MySQL information_schema reports COLLATE and omits CHARACTER SET.
    assert not national_charset_would_collapse(
        "NVARCHAR(32) COLLATE Latin1_General_100_BIN2",
        "VARCHAR(32) COLLATE utf8mb4_bin",
        dest_db="mysql",
    )
    # An unstamped latin1-defaulting carrier is still a collapse.
    assert national_charset_would_collapse(
        "NVARCHAR(32)",
        "VARCHAR(32) CHARACTER SET latin1",
        dest_db="mysql",
    )
    # Bare VARCHAR is the server default — not a measured utf8mb4 capacity.
    assert national_charset_would_collapse(
        "NVARCHAR(32)",
        "VARCHAR(32)",
        dest_db="mysql",
    )


def test_collation_matches_the_charset_the_column_stores() -> None:
    """A utf8mb4 collation on a utf8mb3 column is refused by the engine (1253)."""
    catalog = SourceSchemaCatalog(
        dialect="sqlserver",
        columns=["code"],
        column_types={"code": "NVARCHAR(32)"},
        nullable={"code": True},
        primary_key=[],
        unique_keys=[],
        collations={"code": "Latin1_General_BIN"},
        charsets={},
    )
    plan = plan_collation_carry(
        catalog=catalog,
        dest_dialect="mysql",
        dest_name_for_source=lambda c: c,
        # A pre-existing destination column that really is the utf8mb3 alias.
        dest_type_for_column=lambda _c: "NVARCHAR(32)",
        unique_or_pk=set(),
    )
    clauses = " ".join(
        frag for frags in plan.column_prefixes.values() for frag in frags
    ).lower()
    assert "utf8mb4" not in clauses, clauses
    assert "character set" not in clauses, clauses
    if clauses:
        assert "utf8mb3_bin" in clauses, clauses
