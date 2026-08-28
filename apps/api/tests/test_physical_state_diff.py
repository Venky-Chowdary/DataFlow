"""Constraints, indexes, nullability and defaults compared against real catalogs.

Every case runs against a live SQLite catalog rather than a stubbed inspector:
the whole value of this module is that it reads what the database actually
stored, so a mock would prove nothing.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from services.migration_certificate import physical_state_findings
from services.physical_state_diff import (
    ADVISORY_ASPECTS,
    ASPECTS,
    compare_physical_state,
    _has_catalog_supplied_value,
    _normalize_predicate,
    read_physical_state,
    verify_physical_state,
)

FULL = """
CREATE TABLE {name} (
  id INTEGER PRIMARY KEY,
  code TEXT NOT NULL UNIQUE,
  parent_id INTEGER REFERENCES parent(id),
  note TEXT DEFAULT 'n',
  qty INTEGER CHECK (qty > 0)
)
"""
BARE = (
    "CREATE TABLE {name} "
    "(id INTEGER, code TEXT, parent_id INTEGER, note TEXT, qty INTEGER)"
)


def _db(tmp_path: Path, *statements: str) -> dict[str, str]:
    path = str(tmp_path / "cat.db")
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        for stmt in statements:
            conn.execute(stmt)
    return {"type": "sqlite", "database": path}


def _verify(cfg: dict[str, str], src: str, dest: str) -> dict:
    return verify_physical_state(
        source_db_type="sqlite",
        source_cfg=cfg,
        source_table=src,
        dest_db_type="sqlite",
        dest_cfg=cfg,
        dest_table=dest,
    )


def test_faithful_copy_verifies_every_aspect(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        FULL.format(name="src"),
        "CREATE INDEX ix_src ON src (note)",
        FULL.format(name="dst"),
        "CREATE INDEX ix_dst ON dst (note)",
    )
    result = _verify(cfg, "src", "dst")
    assert result["verified"] is True
    assert result["absent"] == []
    assert set(result["aspects"]) == set(ASPECTS) | set(ADVISORY_ASPECTS)
    assert {a["status"] for a in result["aspects"].values()} == {"carried"}


def test_dropped_constraints_are_reported_absent(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        FULL.format(name="src"),
        "CREATE INDEX ix_src ON src (note)",
        BARE.format(name="dst"),
    )
    result = _verify(cfg, "src", "dst")
    assert result["verified"] is False
    assert set(result["absent"]) == set(ASPECTS)
    assert result["aspects"]["primary_key"]["missing"] == ["id"]
    assert result["aspects"]["foreign_keys"]["missing"] == ["parent_id->parent->id"]
    assert result["aspects"]["not_null"]["missing"] == ["code"]
    assert result["aspects"]["defaults"]["missing"] == ["note"]


def test_missing_primary_key_alone_fails(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        "CREATE TABLE src (id INTEGER PRIMARY KEY, code TEXT)",
        "CREATE TABLE dst (id INTEGER, code TEXT)",
    )
    result = _verify(cfg, "src", "dst")
    assert result["absent"] == ["primary_key"]
    assert result["aspects"]["indexes"]["status"] == "carried"


def test_extra_destination_index_does_not_fail(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        "CREATE TABLE src (id INTEGER PRIMARY KEY, note TEXT)",
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, note TEXT)",
        "CREATE INDEX ix_dst ON dst (note)",
    )
    result = _verify(cfg, "src", "dst")
    assert result["verified"] is True
    assert result["aspects"]["indexes"]["extra"] == ["note"]


def test_absent_destination_table_is_unreadable_not_carried(tmp_path: Path) -> None:
    cfg = _db(tmp_path, FULL.format(name="src"))
    result = _verify(cfg, "src", "nope")
    assert result["verified"] is False
    assert "not found" in result["reason"]
    assert not result.get("aspects")


def test_case_folded_table_and_columns_match(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        'CREATE TABLE "SRC" (ID INTEGER PRIMARY KEY, Code TEXT NOT NULL)',
        'CREATE TABLE "dst" (id INTEGER PRIMARY KEY, code TEXT NOT NULL)',
    )
    result = _verify(cfg, "src", "DST")
    assert result["verified"] is True


def test_unreadable_source_is_not_a_pass(tmp_path: Path) -> None:
    cfg = _db(tmp_path, FULL.format(name="dst"))
    result = verify_physical_state(
        source_db_type="sqlite",
        source_cfg={"type": "sqlite", "database": str(tmp_path / "missing.db")},
        source_table="src",
        dest_db_type="sqlite",
        dest_cfg=cfg,
        dest_table="dst",
    )
    assert result["verified"] is False


def test_read_state_reports_the_stored_facts(tmp_path: Path) -> None:
    cfg = _db(tmp_path, FULL.format(name="src"), "CREATE INDEX ix_src ON src (note)")
    state = read_physical_state("sqlite", cfg, table="src")
    assert state.found and state.readable
    assert state.primary_key == ("id",)
    assert ("code",) in state.unique_constraints
    assert state.not_null >= {"code"}
    assert "note" in state.defaults


def test_compare_is_symmetric_about_direction(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        FULL.format(name="src"),
        BARE.format(name="dst"),
    )
    src = read_physical_state("sqlite", cfg, table="src")
    dst = read_physical_state("sqlite", cfg, table="dst")
    forward = compare_physical_state(src, dst)
    backward = compare_physical_state(dst, src)
    assert forward["verified"] is False
    # Nothing is missing when the destination is the richer table.
    assert backward["absent"] == []


def test_certificate_carries_schema_object_findings() -> None:
    recon = {
        "physical_state": {
            "schema_objects": {
                "verified": False,
                "absent": ["primary_key"],
                "aspects": {"primary_key": {"status": "absent", "missing": ["id"]}},
            }
        }
    }
    findings = physical_state_findings(recon)
    assert findings["schema_objects"]["absent"] == ["primary_key"]
    assert findings["schema_objects"]["verified"] is False


def test_certificate_marks_missing_comparison_unverified() -> None:
    findings = physical_state_findings({})
    assert findings["schema_objects"]["verified"] is False
    assert "not compared" in findings["schema_objects"]["reason"]


CHECKED = (
    "CREATE TABLE {name} (id INTEGER PRIMARY KEY, qty INTEGER CHECK (qty > 0))"
)
UNCHECKED = "CREATE TABLE {name} (id INTEGER PRIMARY KEY, qty INTEGER)"


def test_dropped_check_constraint_is_reported_absent(tmp_path: Path) -> None:
    """A CHECK that did not survive lets bad values in tomorrow."""
    cfg = _db(tmp_path, CHECKED.format(name="src"), UNCHECKED.format(name="dst"))
    result = _verify(cfg, "src", "dst")
    assert result["verified"] is False
    assert "check_constraints" in result["absent"]
    assert result["aspects"]["check_constraints"]["missing"] == ["qty>0"]


def test_check_constraint_spelling_differences_still_match(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        CHECKED.format(name="src"),
        'CREATE TABLE dst (id INTEGER PRIMARY KEY, qty INTEGER CHECK ( ("qty") > 0 ))',
    )
    result = _verify(cfg, "src", "dst")
    assert result["aspects"]["check_constraints"]["status"] == "carried"


def test_triggers_are_reported_but_never_block_the_verdict(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        UNCHECKED.format(name="src"),
        UNCHECKED.format(name="dst"),
        "CREATE TRIGGER trg_src AFTER INSERT ON src BEGIN SELECT 1; END",
    )
    result = _verify(cfg, "src", "dst")
    triggers = result["aspects"]["triggers"]
    assert triggers["advisory"] is True
    assert triggers["status"] == "absent"
    assert triggers["missing"] == ["trg_src (after insert)"]
    assert result["advisory"] == {"triggers": "absent"}
    assert result["cutover_recreate"] == [
        {
            "kind": "trigger",
            "name": "trg_src (after insert)",
            "action": "recreate_before_cutover",
        }
    ]
    # Every blocking aspect carried, so the move is still verified.
    assert result["absent"] == []
    assert result["verified"] is True


def test_matching_triggers_are_carried(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        UNCHECKED.format(name="src"),
        UNCHECKED.format(name="dst"),
        "CREATE TRIGGER trg_src AFTER INSERT ON src BEGIN SELECT 1; END",
        "CREATE TRIGGER trg_dst AFTER INSERT ON dst BEGIN SELECT 1; END",
    )
    result = _verify(cfg, "src", "dst")
    assert result["aspects"]["triggers"]["status"] == "carried"
    assert result["advisory"] == {}
    assert result["cutover_recreate"] == []

def test_trigger_body_events_do_not_shadow_the_declared_event(tmp_path: Path) -> None:
    """SQLite hands back the whole CREATE statement; the header event wins."""
    cfg = _db(
        tmp_path,
        UNCHECKED.format(name="src"),
        UNCHECKED.format(name="dst"),
        "CREATE TRIGGER trg_src AFTER INSERT ON src "
        "BEGIN UPDATE src SET qty = qty; END",
        "CREATE TRIGGER trg_dst AFTER INSERT ON dst BEGIN SELECT 1; END",
    )
    state = read_physical_state(db_type="sqlite", cfg=cfg, table="src")
    assert state.triggers == frozenset({("trg_src", "after", "insert")})
    assert _verify(cfg, "src", "dst")["aspects"]["triggers"]["status"] == "carried"


def test_dependent_view_is_named_for_cutover_and_does_not_block(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        UNCHECKED.format(name="src"),
        UNCHECKED.format(name="dst"),
        "CREATE VIEW v_src_open AS SELECT id, qty FROM src WHERE qty > 0",
    )
    result = _verify(cfg, "src", "dst")
    views = result["aspects"]["views"]
    assert views["advisory"] is True
    assert views["status"] == "absent"
    assert views["missing"] == ["v_src_open"]
    assert result["verified"] is True
    assert "views" not in result["absent"]
    assert result["cutover_recreate"] == [
        {
            "kind": "view",
            "name": "v_src_open",
            "action": "recreate_before_cutover",
        }
    ]


def test_same_named_dependent_view_is_present_not_a_body_claim(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        UNCHECKED.format(name="src"),
        UNCHECKED.format(name="dst"),
        "CREATE VIEW v_open AS SELECT id, qty FROM src WHERE qty > 0",
        "CREATE VIEW v_open_dst AS SELECT id, qty FROM dst WHERE qty > 0",
    )
    # Same SQLite file cannot reuse the view name; name presence is dest-side.
    result = _verify(cfg, "src", "dst")
    assert result["aspects"]["views"]["status"] == "absent"
    assert "v_open" in result["aspects"]["views"]["missing"]


def test_unrelated_view_is_not_attributed_to_the_table(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        UNCHECKED.format(name="src"),
        UNCHECKED.format(name="dst"),
        "CREATE TABLE other (id INTEGER PRIMARY KEY, qty INTEGER)",
        "CREATE VIEW v_other AS SELECT id FROM other",
    )
    src = read_physical_state("sqlite", cfg, table="src")
    assert src.views == frozenset()


# --- cross-engine catalog spelling ------------------------------------------
#
# Two engines record the same guarantee in their own words. A comparison that
# reads the words instead of the rule reports a phantom dropped constraint on
# every cross-engine move, and the operator then cannot tell a real loss from
# the destination's punctuation. Measured live: PostgreSQL stores the source
# CHECK as ``status::text <> ''::text`` while MySQL stores the very constraint
# it created as ``(`status` <> _utf8mb4'')``.


def test_postgresql_cast_and_mysql_charset_introducer_are_the_same_check() -> None:
    assert _normalize_predicate("status::text <> ''::text") == _normalize_predicate(
        "(`status` <> _utf8mb4'')"
    )


def test_multi_word_type_cast_is_stripped_whole() -> None:
    assert _normalize_predicate(
        "ts::timestamp without time zone > '2020-01-01'"
    ) == _normalize_predicate("[ts] > '2020-01-01'")
    assert _normalize_predicate("qty::numeric(10,2) > 0") == _normalize_predicate(
        "qty > 0"
    )


def test_a_cast_never_swallows_the_operator_that_follows_it() -> None:
    """``x::text and y`` must keep its ``and``: a cast is not a word eater."""
    assert _normalize_predicate("x::text and y > 0") == _normalize_predicate(
        "x and y > 0"
    )


def test_literal_content_is_never_treated_as_a_cast_or_introducer() -> None:
    """Real drift inside a literal must still be visible."""
    assert _normalize_predicate("note <> 'a::b'") != _normalize_predicate(
        "note <> 'a'"
    )
    assert _normalize_predicate("note <> '_utf8mb4'") != _normalize_predicate(
        "note <> ''"
    )


def test_a_carried_generator_is_not_reported_as_a_dropped_default() -> None:
    """MySQL exposes AUTO_INCREMENT with no column default at all.

    Reflection shapes, verbatim from the two dialects: a PostgreSQL identity
    column and the MySQL AUTO_INCREMENT column created from it must both count
    as "the catalog fills this in", or a faithful create-new load reports its
    carried generator as a lost default.
    """
    pg_identity = {"name": "id", "default": None, "identity": {"start": 1}}
    mysql_auto = {"name": "id", "default": None, "autoincrement": True}
    pg_serial = {"name": "id", "default": "nextval('t_id_seq'::regclass)"}
    plain = {"name": "code", "default": None}
    assert _has_catalog_supplied_value(pg_identity)
    assert _has_catalog_supplied_value(mysql_auto)
    assert _has_catalog_supplied_value(pg_serial)
    assert not _has_catalog_supplied_value(plain)
