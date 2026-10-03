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
    PhysicalState,
    catalog_default_fact,
    catalog_index_fact,
    compare_physical_state,
    foreign_keys_from_catalog_state,
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


def test_unparsed_foreign_key_token_is_kept() -> None:
    """A token that is not a relationship must not vanish into 'no foreign keys'."""
    keys, unparsed = foreign_keys_from_catalog_state(
        {"foreign_keys": ["parent_id->parent->id", "not-a-relationship"]}
    )
    assert keys == [
        {
            "constrained_columns": ["parent_id"],
            "referred_schema": "",
            "referred_table": "parent",
            "referred_columns": ["id"],
        }
    ]
    assert unparsed == ["not-a-relationship"]
    qualified, broken = foreign_keys_from_catalog_state(
        {
            "foreign_key_facts": [
                {
                    "constrained_columns": ["parent_id"],
                    "referred_schema": "sales",
                    "referred_table": "parent",
                    "referred_columns": ["id"],
                }
            ]
        }
    )
    assert broken == []
    assert qualified[0]["referred_schema"] == "sales"
    assert qualified[0]["referred_table"] == "parent"


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
    facts = result["source"]["foreign_key_facts"]
    assert facts == [
        {
            "constrained_columns": ["parent_id"],
            "referred_schema": "",
            "referred_table": "parent",
            "referred_columns": ["id"],
            "row_proof_gap": "",
            "match": "",
            "on_delete": "NO ACTION",
            "on_update": "NO ACTION",
            "deferral": "not_deferrable",
        }
    ]
    assert result["aspects"]["not_null"]["missing"] == ["code"]
    assert result["aspects"]["defaults"]["missing"] == ["note=n"]


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
    assert ("note", "n") in state.defaults
    assert state.foreign_key_proof == ("",)
    assert state.foreign_key_match == ("",)
    assert state.foreign_key_on_delete == ("NO ACTION",)
    assert state.foreign_key_on_update == ("NO ACTION",)
    assert state.uniqueness_proof == ()
    assert state.check_proof == ()


def test_file_path_schema_is_not_read_as_a_catalog_qualifier(tmp_path: Path) -> None:
    """A SQLite-backed writer reports the db *file* as the schema it wrote.

    SQLite reads a schema as an ATTACHed database name, so that path turned
    every reflection into ``"/tmp/x.db".sqlite_master`` and structural
    attestation came back unreadable on a table that is right there.
    """
    cfg = _db(tmp_path, FULL.format(name="src"))
    state = read_physical_state(
        "sqlite", cfg, schema=str(cfg["database"]), table="src"
    )
    assert state.found and state.readable
    assert state.primary_key == ("id",)


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
    assert result["destination"]["check_proof"] == []


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


def test_sqlite_has_no_routines_and_does_not_invent_unreadable(tmp_path: Path) -> None:
    cfg = _db(tmp_path, UNCHECKED.format(name="src"), UNCHECKED.format(name="dst"))
    src = read_physical_state("sqlite", cfg, table="src")
    assert src.routines == frozenset()
    result = _verify(cfg, "src", "dst")
    assert result["aspects"]["routines"]["status"] == "carried"
    assert result["aspects"]["routines"]["advisory"] is True
    assert "routines" not in result.get("unreadable", [])


def test_named_routine_is_advisory_cutover_and_never_blocks() -> None:
    """Name presence only — missing dest routine must not veto verified."""
    src = PhysicalState(found=True, readable=True, routines=frozenset({"sp_refresh"}))
    dst = PhysicalState(found=True, readable=True)
    result = compare_physical_state(src, dst)
    assert result["verified"] is True
    assert "routines" not in result["absent"]
    assert result["aspects"]["routines"]["status"] == "absent"
    assert result["aspects"]["routines"]["advisory"] is True
    assert result["cutover_recreate"] == [
        {
            "kind": "routine",
            "name": "sp_refresh",
            "action": "recreate_before_cutover",
        }
    ]


def test_same_named_routine_is_presence_not_a_body_claim() -> None:
    src = PhysicalState(found=True, readable=True, routines=frozenset({"sp_refresh"}))
    dst = PhysicalState(found=True, readable=True, routines=frozenset({"sp_refresh"}))
    result = compare_physical_state(src, dst)
    assert result["aspects"]["routines"]["status"] == "carried"
    assert result["cutover_recreate"] == []
    assert result["verified"] is True


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


def _fk_state(
    *facts: tuple[tuple[str, ...], str, str, tuple[str, ...]],
    proof: tuple[str, ...] = (),
    dialect: str = "",
    match: tuple[str, ...] = (),
    on_delete: tuple[str, ...] = (),
    on_update: tuple[str, ...] = (),
    deferral: tuple[str, ...] = (),
) -> PhysicalState:
    return PhysicalState(
        found=True,
        readable=True,
        foreign_key_facts=tuple(facts),
        foreign_key_proof=proof,
        foreign_key_match=match,
        foreign_key_on_delete=on_delete,
        foreign_key_on_update=on_update,
        foreign_key_deferral=deferral,
        dialect=dialect,
    )


def test_uniqueness_is_the_column_set_not_the_catalog_order() -> None:
    """UNIQUE (b, a) and PRIMARY KEY (code, id) are the same rules reversed."""
    src = PhysicalState(
        found=True,
        readable=True,
        primary_key=("code", "id"),
        unique_constraints=frozenset({("b", "a"), ("note",)}),
    )
    dst = PhysicalState(
        found=True,
        readable=True,
        primary_key=("id", "code"),
        unique_constraints=frozenset({("a", "b"), ("note",)}),
    )
    result = compare_physical_state(src, dst)
    assert result["aspects"]["primary_key"]["status"] == "carried"
    assert result["aspects"]["primary_key"]["missing"] == []
    assert result["aspects"]["unique_constraints"]["status"] == "carried"
    assert "primary_key" not in result["absent"]
    assert "unique_constraints" not in result["absent"]


def test_informational_warehouse_primary_key_is_not_carried_row_proof() -> None:
    """A matching key on an engine that accepts duplicates is unchecked.

    Column order still matches first. An empty dialect and Postgres stay
    carried, because those callers did not name an informational engine.
    These are catalog-shaped states, not a live warehouse.
    """
    src = PhysicalState(
        found=True,
        readable=True,
        primary_key=("code", "id"),
        unique_constraints=frozenset({("b", "a")}),
    )
    bigquery = PhysicalState(
        found=True,
        readable=True,
        dialect="bigquery",
        primary_key=("id", "code"),
        unique_constraints=frozenset({("a", "b")}),
    )
    result = compare_physical_state(src, bigquery)
    assert result["verified"] is False
    assert result["absent"] == []
    assert "primary_key" in result["unchecked"]
    assert "unique_constraints" in result["unchecked"]
    assert "foreign_keys" not in result["unchecked"]
    pk = result["aspects"]["primary_key"]
    assert pk["status"] == "unchecked"
    assert pk["missing"] == []
    assert pk["unchecked"] == ["code+id"]
    assert "does not enforce" in pk["reasons"][0]
    assert "BigQuery" in pk["reasons"][0]
    assert "NOT ENFORCED" in pk["reasons"][0]
    unique = result["aspects"]["unique_constraints"]
    assert unique["status"] == "unchecked"
    assert unique["unchecked"] == ["a+b"]

    snowflake = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="snowflake_aws",
            primary_key=("id", "code"),
            unique_constraints=frozenset({("a", "b")}),
        ),
    )
    reason = snowflake["aspects"]["primary_key"]["reasons"][0]
    assert "Snowflake" in reason
    assert "standard table" in reason
    assert "hybrid" in reason
    assert snowflake["aspects"]["primary_key"]["status"] == "unchecked"

    for dialect in ("", "postgresql", "sqlite"):
        carried = compare_physical_state(
            src,
            PhysicalState(
                found=True,
                readable=True,
                dialect=dialect,
                primary_key=("id", "code"),
                unique_constraints=frozenset({("a", "b")}),
            ),
        )
        assert carried["aspects"]["primary_key"]["status"] == "carried", dialect
        assert carried["aspects"]["unique_constraints"]["status"] == "carried", dialect
        assert carried["aspects"]["primary_key"]["unchecked"] == [], dialect
        assert "primary_key" not in carried["unchecked"], dialect


def test_snowflake_catalog_read_asks_is_hybrid() -> None:
    """The certificate read uses INFORMATION_SCHEMA.TABLES.IS_HYBRID."""
    from services.physical_state_diff import _read_snowflake_table_kind

    class _Cur:
        def __init__(self, rows):
            self.rows = rows
            self.sql = ""
            self.params: tuple = ()

        def execute(self, sql, params=()):
            self.sql = str(sql)
            self.params = tuple(params)

        def fetchall(self):
            return list(self.rows)

    yes = _Cur([("YES",)])
    assert _read_snowflake_table_kind(yes, "PUBLIC", "ORDERS") == "hybrid"
    assert "is_hybrid" in yes.sql
    assert "information_schema.tables" in yes.sql
    assert yes.params == ("PUBLIC", "ORDERS")
    assert _read_snowflake_table_kind(_Cur([("NO",)]), "PUBLIC", "ORDERS") == "standard"
    iceberg = _Cur([("NO", "YES", "NO", "NO")])
    assert _read_snowflake_table_kind(iceberg, "PUBLIC", "ORDERS") == "iceberg"
    assert "is_iceberg" in iceberg.sql
    assert "is_dynamic" in iceberg.sql
    assert "is_immutable" in iceberg.sql
    assert _read_snowflake_table_kind(_Cur([("NO", "NO", "YES", "NO")]), "PUBLIC", "T") == (
        "dynamic"
    )
    assert _read_snowflake_table_kind(_Cur([("YES", "YES", "NO", "NO")]), "PUBLIC", "T") == (
        "hybrid"
    )
    assert _read_snowflake_table_kind(_Cur([]), "PUBLIC", "ORDERS") == ""

    class _Broken:
        def execute(self, sql, params=()):
            raise RuntimeError("column is_hybrid does not exist")

        def fetchall(self):
            return []

    assert _read_snowflake_table_kind(_Broken(), "PUBLIC", "ORDERS") == ""


def test_measured_hybrid_primary_key_is_carried_row_proof() -> None:
    """IS_HYBRID YES makes a matching Snowflake key carried. The label does not travel."""
    src = PhysicalState(
        found=True,
        readable=True,
        primary_key=("id",),
        unique_constraints=frozenset({("email",)}),
    )
    hybrid = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="snowflake_aws",
            table_kind="YES",
            index_status="ACTIVE",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
        ),
    )
    assert hybrid["aspects"]["primary_key"]["status"] == "carried"
    assert hybrid["aspects"]["unique_constraints"]["status"] == "carried"
    assert hybrid["aspects"]["primary_key"]["unchecked"] == []
    assert "primary_key" not in hybrid["unchecked"]
    assert hybrid["destination"]["table_kind"] == "hybrid"
    assert hybrid["destination"]["index_status"] == "active"

    unread = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="snowflake",
            table_kind="hybrid",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
        ),
    )
    assert unread["aspects"]["primary_key"]["status"] == "unchecked"
    assert "SHOW INDEXES" in unread["aspects"]["primary_key"]["reasons"][0]

    failed_build = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="snowflake",
            table_kind="hybrid",
            index_status="BUILD VALIDATION FAILURE",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
        ),
    )
    assert failed_build["aspects"]["primary_key"]["status"] == "unchecked"
    assert "BUILD VALIDATION FAILURE" in failed_build["aspects"]["primary_key"]["reasons"][0]
    assert failed_build["destination"]["index_status"] == "failed"
    assert "status_info" not in failed_build["aspects"]["primary_key"]["reasons"][0]

    explained = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="snowflake",
            table_kind="hybrid",
            index_status="BUILD VALIDATION FAILURE",
            index_detail="existing customer row 4",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
        ),
    )
    explained_reason = explained["aspects"]["primary_key"]["reasons"][0]
    assert "existing customer row 4" in explained_reason
    assert "SHOW INDEXES status_info" in explained_reason
    assert explained["destination"]["index_detail"] == "existing customer row 4"

    standard = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="snowflake",
            table_kind="NO",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
        ),
    )
    assert standard["aspects"]["primary_key"]["status"] == "unchecked"
    assert "IS_HYBRID" in standard["aspects"]["primary_key"]["reasons"][0]

    iceberg_state = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="snowflake",
            table_kind="iceberg",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
        ),
    )
    assert iceberg_state["aspects"]["primary_key"]["status"] == "unchecked"
    assert "IS_ICEBERG" in iceberg_state["aspects"]["primary_key"]["reasons"][0]
    assert iceberg_state["destination"]["table_kind"] == "iceberg"

    borrowed = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="bigquery",
            table_kind="hybrid",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
        ),
    )
    assert borrowed["aspects"]["primary_key"]["status"] == "unchecked"
    assert "BigQuery" in borrowed["aspects"]["primary_key"]["reasons"][0]


def test_a_missing_unique_stays_absent_on_an_informational_engine() -> None:
    """The object that never arrived is absent. The gap does not hide that."""
    src = PhysicalState(
        found=True, readable=True, unique_constraints=frozenset({("a", "b")})
    )
    dst = PhysicalState(
        found=True,
        readable=True,
        dialect="bigquery",
        unique_constraints=frozenset({("a",)}),
    )
    result = compare_physical_state(src, dst)
    assert result["aspects"]["unique_constraints"]["status"] == "absent"
    assert result["aspects"]["unique_constraints"]["missing"] == ["a+b"]
    assert result["aspects"]["unique_constraints"]["extra"] == ["a"]
    assert result["aspects"]["unique_constraints"]["unchecked"] == []
    assert "unique_constraints" in result["absent"]


def test_a_different_unique_column_set_is_absent() -> None:
    src = PhysicalState(
        found=True, readable=True, unique_constraints=frozenset({("a", "b")})
    )
    dst = PhysicalState(
        found=True, readable=True, unique_constraints=frozenset({("a",)})
    )
    result = compare_physical_state(src, dst)
    assert result["aspects"]["unique_constraints"]["status"] == "absent"
    assert result["aspects"]["unique_constraints"]["missing"] == ["a+b"]
    assert result["aspects"]["unique_constraints"]["extra"] == ["a"]


def test_index_column_order_stays_part_of_the_index() -> None:
    """(b, a) does not serve the lookups (a, b) does. Order stays on indexes."""
    src = PhysicalState(
        found=True,
        readable=True,
        indexes=frozenset({(False, ("b", "a"), "", False)}),
    )
    dst = PhysicalState(
        found=True,
        readable=True,
        indexes=frozenset({(False, ("a", "b"), "", False)}),
    )
    result = compare_physical_state(src, dst)
    assert result["aspects"]["indexes"]["status"] == "absent"
    assert result["aspects"]["indexes"]["missing"] == ["b+a"]
    assert result["aspects"]["indexes"]["extra"] == ["a+b"]


def test_a_unique_index_is_not_a_plain_index(tmp_path: Path) -> None:
    """CREATE UNIQUE INDEX is a different guarantee from CREATE INDEX."""
    cfg = _db(
        tmp_path,
        "CREATE TABLE src (id INTEGER PRIMARY KEY, email TEXT)",
        "CREATE UNIQUE INDEX ux_src ON src (email)",
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, email TEXT)",
        "CREATE INDEX ix_dst ON dst (email)",
    )
    result = _verify(cfg, "src", "dst")
    assert result["verified"] is False
    assert result["aspects"]["indexes"]["status"] == "absent"
    assert result["aspects"]["indexes"]["missing"] == ["unique(email)"]
    assert result["aspects"]["indexes"]["extra"] == ["email"]


def test_a_partial_index_is_not_a_full_index(tmp_path: Path) -> None:
    """The WHERE clause decides which rows the index covers."""
    cfg = _db(
        tmp_path,
        "CREATE TABLE src (id INTEGER PRIMARY KEY, note TEXT)",
        "CREATE INDEX ix_src ON src (note) WHERE note IS NOT NULL",
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, note TEXT)",
        "CREATE INDEX ix_dst ON dst (note)",
    )
    result = _verify(cfg, "src", "dst")
    assert result["verified"] is False
    assert result["aspects"]["indexes"]["missing"] == ["note where noteisnotnull"]
    assert result["aspects"]["indexes"]["extra"] == ["note"]


def test_the_same_unique_partial_index_is_carried(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        "CREATE TABLE src (id INTEGER PRIMARY KEY, email TEXT)",
        "CREATE UNIQUE INDEX ux_src ON src (email) WHERE email IS NOT NULL",
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, email TEXT)",
        "CREATE UNIQUE INDEX ux_dst ON dst (email) WHERE email IS NOT NULL",
    )
    result = _verify(cfg, "src", "dst")
    assert result["aspects"]["indexes"]["status"] == "carried"
    assert result["aspects"]["indexes"]["missing"] == []
    assert result["verified"] is True


def test_direction_expression_and_nulls_stay_on_the_index() -> None:
    """Postgres reflection reports these. A column list alone is not the index."""
    plain = catalog_index_fact(
        {"name": "ix", "unique": False, "column_names": ["email"]}
    )
    descending = catalog_index_fact(
        {
            "name": "ix",
            "unique": False,
            "column_names": ["email"],
            "column_sorting": {"email": ("desc",)},
        }
    )
    expression = catalog_index_fact(
        {
            "name": "ix",
            "unique": False,
            "column_names": [None],
            "expressions": ['lower("email")'],
        }
    )
    same_expression = catalog_index_fact(
        {
            "name": "ix_other",
            "unique": False,
            "column_names": [None],
            "expressions": ["lower(email)"],
        }
    )
    distinct_nulls = catalog_index_fact(
        {
            "name": "ux",
            "unique": True,
            "column_names": ["email"],
            "dialect_options": {"postgresql_nulls_not_distinct": True},
        }
    )
    many_nulls = catalog_index_fact(
        {"name": "ux", "unique": True, "column_names": ["email"]}
    )
    assert expression == same_expression
    order = compare_physical_state(
        PhysicalState(found=True, readable=True, indexes=frozenset({descending})),
        PhysicalState(found=True, readable=True, indexes=frozenset({plain})),
    )
    assert order["aspects"]["indexes"]["missing"] == ["email desc"]
    expr = compare_physical_state(
        PhysicalState(found=True, readable=True, indexes=frozenset({expression})),
        PhysicalState(found=True, readable=True, indexes=frozenset({plain})),
    )
    assert expr["aspects"]["indexes"]["status"] == "absent"
    assert expr["aspects"]["indexes"]["missing"] == ["lower(email)"]
    nulls = compare_physical_state(
        PhysicalState(found=True, readable=True, indexes=frozenset({distinct_nulls})),
        PhysicalState(found=True, readable=True, indexes=frozenset({many_nulls})),
    )
    assert nulls["aspects"]["indexes"]["missing"] == ["unique(email) nulls not distinct"]


def test_covering_method_prefix_and_invalid_stay_on_the_index() -> None:
    """INCLUDE, gin, FULLTEXT, a prefix length, and INVALID are the access path.

    Postgres and MySQL report these on the inspector dict. A column list alone
    would certify a covering gin index as a plain btree index.
    """
    plain = catalog_index_fact(
        {"name": "ix", "unique": False, "column_names": ["email"]}
    )
    covering = catalog_index_fact(
        {
            "name": "ix",
            "unique": False,
            "column_names": ["email"],
            "dialect_options": {"postgresql_include": ["Phone", "name"]},
        }
    )
    same_covering = catalog_index_fact(
        {
            "name": "ix_other",
            "unique": False,
            "column_names": ["email"],
            "dialect_options": {"postgresql_include": ["name", "phone"]},
        }
    )
    gin = catalog_index_fact(
        {
            "name": "ix",
            "unique": False,
            "column_names": ["body"],
            "dialect_options": {"postgresql_using": "gin"},
        }
    )
    fulltext = catalog_index_fact(
        {
            "name": "ix",
            "unique": False,
            "column_names": ["body"],
            "dialect_options": {"mysql_prefix": "FULLTEXT", "mysql_with_parser": "ngram"},
        }
    )
    prefix = catalog_index_fact(
        {
            "name": "ix",
            "unique": False,
            "column_names": ["email"],
            "dialect_options": {"mysql_length": {"email": 10}},
        }
    )
    opclass = catalog_index_fact(
        {
            "name": "ix",
            "unique": False,
            "column_names": ["email"],
            "dialect_options": {"postgresql_ops": {"email": "text_pattern_ops"}},
        }
    )
    invalid = catalog_index_fact(
        {
            "name": "ix",
            "unique": False,
            "column_names": ["email"],
            "dialect_options": {"postgresql_invalid": True},
        }
    )
    assert covering == same_covering
    covered = compare_physical_state(
        PhysicalState(found=True, readable=True, indexes=frozenset({covering})),
        PhysicalState(found=True, readable=True, indexes=frozenset({plain})),
    )
    assert covered["aspects"]["indexes"]["missing"] == ["email include name+phone"]
    btree = catalog_index_fact(
        {"name": "ix", "unique": False, "column_names": ["body"]}
    )
    method = compare_physical_state(
        PhysicalState(found=True, readable=True, indexes=frozenset({gin})),
        PhysicalState(found=True, readable=True, indexes=frozenset({btree})),
    )
    assert method["aspects"]["indexes"]["missing"] == ["body using gin"]
    assert method["aspects"]["indexes"]["extra"] == ["body"]
    parsed = compare_physical_state(
        PhysicalState(found=True, readable=True, indexes=frozenset({fulltext})),
        PhysicalState(found=True, readable=True, indexes=frozenset({btree})),
    )
    assert parsed["aspects"]["indexes"]["missing"] == ["body using fulltext ngram"]
    assert compare_physical_state(
        PhysicalState(found=True, readable=True, indexes=frozenset({prefix})),
        PhysicalState(found=True, readable=True, indexes=frozenset({plain})),
    )["aspects"]["indexes"]["missing"] == ["email(10)"]
    assert compare_physical_state(
        PhysicalState(found=True, readable=True, indexes=frozenset({opclass})),
        PhysicalState(found=True, readable=True, indexes=frozenset({plain})),
    )["aspects"]["indexes"]["missing"] == ["email text_pattern_ops"]
    assert compare_physical_state(
        PhysicalState(found=True, readable=True, indexes=frozenset({invalid})),
        PhysicalState(found=True, readable=True, indexes=frozenset({plain})),
    )["aspects"]["indexes"]["missing"] == ["email invalid"]


def test_an_expression_index_the_driver_skips_is_not_carried(tmp_path: Path) -> None:
    """SQLite reflection drops lower(email). That must not look like no index."""
    cfg = _db(
        tmp_path,
        "CREATE TABLE src (id INTEGER PRIMARY KEY, email TEXT)",
        "CREATE INDEX ix_src ON src (lower(email))",
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, email TEXT)",
    )
    result = _verify(cfg, "src", "dst")
    assert result["verified"] is False
    assert "indexes" in result["unreadable"]


def test_the_same_expression_index_on_both_sides_is_still_unreadable(
    tmp_path: Path,
) -> None:
    """A driver that skips the expression cannot certify that the two match."""
    cfg = _db(
        tmp_path,
        "CREATE TABLE src (id INTEGER PRIMARY KEY, email TEXT)",
        "CREATE INDEX ix_src ON src (lower(email))",
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, email TEXT)",
        "CREATE INDEX ix_dst ON dst (lower(email))",
    )
    result = _verify(cfg, "src", "dst")
    assert result["verified"] is False
    assert "indexes" in result["unreadable"]


def test_catalog_diff_uses_the_orphan_scan_relationship_identity() -> None:
    """Schema, qualification, and column order follow one identity.

    SQLite cannot store two schemas, so this comparison is pure. The live
    catalog test still checks the empty-schema wire ``parent_id->parent->id``.
    An unqualified name matches one qualified name: engines omit the default
    schema, and the orphan scan uses that same rule. Two qualified names with
    different schemas do not match.
    """
    sales = (("parent_id",), "sales", "parent", ("id",))
    archive = (("parent_id",), "archive", "parent", ("id",))
    drifted = compare_physical_state(_fk_state(sales), _fk_state(archive))
    assert drifted["verified"] is False
    assert drifted["aspects"]["foreign_keys"]["status"] == "absent"
    assert drifted["aspects"]["foreign_keys"]["missing"] == [
        "parent_id->sales.parent->id"
    ]
    assert drifted["aspects"]["foreign_keys"]["extra"] == [
        "parent_id->archive.parent->id"
    ]

    public = (("parent_id",), "public", "parent", ("id",))
    unqualified = (("Parent_Id",), "", "Parent", ("ID",))
    carried = compare_physical_state(_fk_state(public), _fk_state(unqualified))
    assert carried["aspects"]["foreign_keys"]["status"] == "carried"
    assert carried["aspects"]["foreign_keys"]["missing"] == []

    forward = (("a", "b"), "", "parent", ("x", "y"))
    reversed_pairs = (("b", "a"), "", "parent", ("y", "x"))
    order = compare_physical_state(_fk_state(forward), _fk_state(reversed_pairs))
    assert order["aspects"]["foreign_keys"]["status"] == "carried"
    assert order["verified"] is True

    incomplete = ((), "", "parent", ())
    blank = compare_physical_state(_fk_state(incomplete), _fk_state(incomplete))
    assert blank["aspects"]["foreign_keys"]["status"] == "absent"


def test_catalog_diff_does_not_treat_an_unchecked_foreign_key_as_carried() -> None:
    """A matching relationship is not row proof when the destination gap is open.

    PostgreSQL NOT VALID, an unreported validation bit, and a Redshift
    constraint all leave the object in place. The diff lists it and does not
    call it carried, and it does not call the object absent. The source gap
    does not veto a destination that recorded the check. These facts are
    catalog-shaped; a live Postgres or Redshift server was not used.
    """
    fact = (("parent_id",), "public", "parent", ("id",))
    not_valid = compare_physical_state(
        _fk_state(fact, proof=("",)),
        _fk_state(fact, proof=("not_checked",)),
    )
    fk = not_valid["aspects"]["foreign_keys"]
    assert fk["status"] == "unchecked"
    assert fk["missing"] == []
    assert fk["unchecked"] == ["parent_id->public.parent->id"]
    assert "NOT VALID" in fk["reasons"][0]
    assert not_valid["verified"] is False
    assert not_valid["unchecked"] == ["foreign_keys"]
    assert "foreign_keys" not in not_valid["absent"]

    redshift = compare_physical_state(
        _fk_state(fact, proof=("",)),
        _fk_state(fact, proof=("unenforced",), dialect="redshift"),
    )
    reason = redshift["aspects"]["foreign_keys"]["reasons"][0]
    assert redshift["aspects"]["foreign_keys"]["status"] == "unchecked"
    assert "does not enforce" in reason
    assert "Redshift" in reason
    assert redshift["verified"] is False
    assert "foreign_keys" not in redshift["absent"]

    snowflake = compare_physical_state(
        _fk_state(fact, proof=("",)),
        _fk_state(fact, proof=("unenforced",), dialect="snowflake_aws"),
    )
    snow_reason = snowflake["aspects"]["foreign_keys"]["reasons"][0]
    assert snowflake["aspects"]["foreign_keys"]["status"] == "unchecked"
    assert "Snowflake" in snow_reason
    assert "standard table" in snow_reason
    assert "hybrid" in snow_reason
    assert snowflake["verified"] is False

    checked = compare_physical_state(
        _fk_state(fact, proof=("unreported",)),
        _fk_state(fact, proof=("",)),
    )
    assert checked["aspects"]["foreign_keys"]["status"] == "carried"
    assert checked["verified"] is True

    unreported = compare_physical_state(
        _fk_state(fact),
        _fk_state(fact, proof=("unreported",)),
    )
    assert unreported["aspects"]["foreign_keys"]["status"] == "unchecked"
    assert "did not say" in unreported["aspects"]["foreign_keys"]["reasons"][0]

    misaligned = compare_physical_state(
        _fk_state(fact),
        _fk_state(fact, proof=("not_checked", "extra")),
    )
    assert misaligned["aspects"]["foreign_keys"]["status"] == "unchecked"
    assert misaligned["verified"] is False


def test_catalog_fact_keeps_the_match_the_catalog_named() -> None:
    """The structured fact is what the orphan scan reads. Match must survive it."""
    keys, unparsed = foreign_keys_from_catalog_state(
        {
            "foreign_key_facts": [
                {
                    "constrained_columns": ["parent_id"],
                    "referred_schema": "public",
                    "referred_table": "parent",
                    "referred_columns": ["id"],
                    "match": "FULL",
                }
            ]
        }
    )
    assert unparsed == []
    assert keys[0]["match"] == "full"
    _kept, conflict = foreign_keys_from_catalog_state(
        {
            "foreign_key_facts": [
                {
                    "constrained_columns": ["a", "b"],
                    "referred_schema": "public",
                    "referred_table": "parent",
                    "referred_columns": ["x", "y"],
                    "match": "FULL",
                    "options": {"match": "SIMPLE"},
                }
            ]
        }
    )
    assert _kept == []
    assert conflict and "two match types" in conflict[0]


def test_catalog_diff_compares_match_type_after_relationship_identity() -> None:
    """MATCH FULL and MATCH SIMPLE are one relationship and two rules.

    Column order still matches. An empty match list is a comparison that did
    not measure the type, so those states stay on identity and the row-proof
    gap. A destination MATCH FULL keeps an unreported source promise.
    MATCH PARTIAL is stored and is not a completed comparison. These facts
    are catalog-shaped; a live Postgres server was not used.
    """
    fact = (("parent_id",), "public", "parent", ("id",))
    full_to_simple = compare_physical_state(
        _fk_state(fact, match=("full",)),
        _fk_state(fact, match=("simple",)),
    )
    fk = full_to_simple["aspects"]["foreign_keys"]
    assert fk["status"] == "unchecked"
    assert fk["missing"] == []
    assert fk["unchecked"] == ["parent_id->public.parent->id"]
    assert fk["unchecked"].count("parent_id->public.parent->id") == 1
    assert "MATCH FULL" in fk["match_reasons"][0]
    assert "MATCH SIMPLE" in fk["match_reasons"][0]
    assert fk["proof_reasons"] == []
    assert full_to_simple["verified"] is False
    assert "foreign_keys" not in full_to_simple["absent"]

    spelled = compare_physical_state(
        _fk_state(fact, match=("f",)),
        _fk_state(fact, match=("MATCH FULL",)),
    )
    assert spelled["aspects"]["foreign_keys"]["status"] == "carried"
    assert spelled["verified"] is True

    stricter = compare_physical_state(
        _fk_state(fact, match=("",)),
        _fk_state(fact, match=("full",)),
    )
    assert stricter["aspects"]["foreign_keys"]["status"] == "carried"

    unreported_dest = compare_physical_state(
        _fk_state(fact, match=("full",)),
        _fk_state(fact, match=("",)),
    )
    dest_reason = unreported_dest["aspects"]["foreign_keys"]["match_reasons"][0]
    assert unreported_dest["aspects"]["foreign_keys"]["status"] == "unchecked"
    assert "MATCH FULL" in dest_reason
    assert "unreported" in dest_reason

    partial = compare_physical_state(
        _fk_state(fact, match=("partial",)),
        _fk_state(fact, match=("partial",)),
    )
    partial_reason = partial["aspects"]["foreign_keys"]["match_reasons"][0]
    assert partial["aspects"]["foreign_keys"]["status"] == "unchecked"
    assert "MATCH PARTIAL" in partial_reason
    assert "not a completed comparison" in partial_reason
    assert partial["verified"] is False

    forward = (("a", "b"), "", "parent", ("x", "y"))
    reversed_pairs = (("b", "a"), "", "parent", ("y", "x"))
    order = compare_physical_state(
        _fk_state(forward, match=("full",)),
        _fk_state(reversed_pairs, match=("full",)),
    )
    assert order["aspects"]["foreign_keys"]["status"] == "carried"

    both = compare_physical_state(
        _fk_state(fact, proof=("",), match=("full",)),
        _fk_state(fact, proof=("not_checked",), match=("simple",)),
    )
    both_fk = both["aspects"]["foreign_keys"]
    assert both_fk["status"] == "unchecked"
    assert both_fk["unchecked"] == ["parent_id->public.parent->id"]
    assert len(both_fk["proof_reasons"]) == 1
    assert len(both_fk["match_reasons"]) == 1
    assert "NOT VALID" in both_fk["proof_reasons"][0]

    absent = compare_physical_state(
        _fk_state(fact, match=("full",)),
        _fk_state(match=("simple",)),
    )
    assert absent["aspects"]["foreign_keys"]["status"] == "absent"
    assert absent["aspects"]["foreign_keys"]["match_reasons"] == []

    misaligned = compare_physical_state(
        _fk_state(fact, match=("full",)),
        _fk_state(fact, match=("simple", "extra")),
    )
    assert misaligned["aspects"]["foreign_keys"]["status"] == "unchecked"
    assert "could not be read" in misaligned["aspects"]["foreign_keys"]["match_reasons"][0]

    legacy = compare_physical_state(_fk_state(fact), _fk_state(fact))
    assert legacy["aspects"]["foreign_keys"]["status"] == "carried"
    assert legacy["verified"] is True


def test_catalog_fact_keeps_the_referential_actions() -> None:
    """The structured fact keeps ON DELETE and ON UPDATE for the diff."""
    keys, unparsed = foreign_keys_from_catalog_state(
        {
            "foreign_key_facts": [
                {
                    "constrained_columns": ["parent_id"],
                    "referred_schema": "public",
                    "referred_table": "parent",
                    "referred_columns": ["id"],
                    "on_delete": "cascade",
                    "onupdate": "no action",
                }
            ]
        }
    )
    assert unparsed == []
    assert keys[0]["on_delete"] == "CASCADE"
    assert keys[0]["on_update"] == "NO ACTION"


def test_catalog_diff_compares_referential_actions_after_identity() -> None:
    """CASCADE and NO ACTION are one relationship and two rules.

    Column order still matches. An empty action list is a comparison that
    did not measure the actions, so those states stay on identity, the
    row-proof gap, and the match type. An unreported action is NO ACTION.
    These facts are catalog-shaped; a live Postgres server was not used.
    """
    fact = (("parent_id",), "public", "parent", ("id",))
    weakened = compare_physical_state(
        _fk_state(fact, on_delete=("CASCADE",), on_update=("CASCADE",)),
        _fk_state(fact, on_delete=("NO ACTION",), on_update=("NO ACTION",)),
    )
    fk = weakened["aspects"]["foreign_keys"]
    assert fk["status"] == "unchecked"
    assert fk["missing"] == []
    assert fk["unchecked"] == ["parent_id->public.parent->id"]
    assert fk["unchecked"].count("parent_id->public.parent->id") == 1
    assert "ON DELETE NO ACTION" in fk["action_reasons"][0]
    assert "ON DELETE CASCADE" in fk["action_reasons"][0]
    assert "ON UPDATE" in fk["action_reasons"][0]
    assert fk["proof_reasons"] == []
    assert fk["match_reasons"] == []
    assert weakened["verified"] is False
    assert "foreign_keys" not in weakened["absent"]

    same = compare_physical_state(
        _fk_state(fact, on_delete=("cascade",), on_update=("NO ACTION",)),
        _fk_state(fact, on_delete=("CASCADE",), on_update=("",)),
    )
    assert same["aspects"]["foreign_keys"]["status"] == "carried"
    assert same["verified"] is True

    unreported_dest = compare_physical_state(
        _fk_state(fact, on_delete=("SET NULL",), on_update=("CASCADE",)),
        _fk_state(fact, on_delete=("",), on_update=("",)),
    )
    assert unreported_dest["aspects"]["foreign_keys"]["status"] == "unchecked"
    assert "SET NULL" in unreported_dest["aspects"]["foreign_keys"]["action_reasons"][0]
    assert "unreported" in unreported_dest["aspects"]["foreign_keys"]["action_reasons"][0]

    forward = (("a", "b"), "", "parent", ("x", "y"))
    reversed_pairs = (("b", "a"), "", "parent", ("y", "x"))
    order = compare_physical_state(
        _fk_state(forward, on_delete=("CASCADE",), on_update=("RESTRICT",)),
        _fk_state(reversed_pairs, on_delete=("CASCADE",), on_update=("RESTRICT",)),
    )
    assert order["aspects"]["foreign_keys"]["status"] == "carried"

    both = compare_physical_state(
        _fk_state(fact, proof=("",), on_delete=("CASCADE",), on_update=("CASCADE",)),
        _fk_state(
            fact,
            proof=("not_checked",),
            on_delete=("NO ACTION",),
            on_update=("NO ACTION",),
        ),
    )
    both_fk = both["aspects"]["foreign_keys"]
    assert both_fk["status"] == "unchecked"
    assert both_fk["unchecked"] == ["parent_id->public.parent->id"]
    assert len(both_fk["proof_reasons"]) == 1
    assert len(both_fk["action_reasons"]) == 1

    absent = compare_physical_state(
        _fk_state(fact, on_delete=("CASCADE",), on_update=("CASCADE",)),
        _fk_state(on_delete=("NO ACTION",), on_update=("NO ACTION",)),
    )
    assert absent["aspects"]["foreign_keys"]["status"] == "absent"
    assert absent["aspects"]["foreign_keys"]["action_reasons"] == []

    misaligned = compare_physical_state(
        _fk_state(fact, on_delete=("CASCADE",), on_update=("CASCADE",)),
        _fk_state(fact, on_delete=("NO ACTION", "extra"), on_update=("NO ACTION",)),
    )
    assert misaligned["aspects"]["foreign_keys"]["status"] == "unchecked"
    assert "could not be read" in misaligned["aspects"]["foreign_keys"]["action_reasons"][0]

    legacy = compare_physical_state(_fk_state(fact), _fk_state(fact))
    assert legacy["aspects"]["foreign_keys"]["status"] == "carried"


def test_arrow_in_a_column_name_stays_one_wire_tuple(tmp_path: Path) -> None:
    """The report wire is three parts even when a column name contains ``->``."""
    path = str(tmp_path / "arrow.db")
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE parent ("i->d" INTEGER PRIMARY KEY)')
        conn.execute(
            'CREATE TABLE child (id INTEGER, "a->b" INTEGER REFERENCES parent("i->d"))'
        )
    state = read_physical_state(
        "sqlite", {"type": "sqlite", "database": path}, table="child"
    )
    assert state.found is True
    wires = list(state.foreign_keys)
    assert len(wires) == 1
    assert len(wires[0]) == 3
    assert wires[0] == ("a->b", "parent", "i->d")


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
    serial = PhysicalState(
        found=True,
        readable=True,
        defaults=frozenset({catalog_default_fact(pg_serial)}),
    )
    identity = PhysicalState(
        found=True,
        readable=True,
        defaults=frozenset({catalog_default_fact(pg_identity)}),
    )
    auto = PhysicalState(
        found=True,
        readable=True,
        defaults=frozenset({catalog_default_fact(mysql_auto)}),
    )
    assert compare_physical_state(serial, auto)["aspects"]["defaults"]["status"] == "carried"
    assert compare_physical_state(identity, auto)["aspects"]["defaults"]["status"] == "carried"
    literal = PhysicalState(
        found=True,
        readable=True,
        defaults=frozenset({catalog_default_fact({"name": "id", "default": "'n'"})}),
    )
    drifted = compare_physical_state(literal, auto)
    assert drifted["aspects"]["defaults"]["status"] == "absent"
    assert drifted["aspects"]["defaults"]["missing"] == ["id=n"]
    # DEFAULT NULL is the literal null, not an empty string and not "no default".
    assert catalog_default_fact({"name": "note", "default": "NULL"}) == ("note", "null")
    assert catalog_default_fact({"name": "note", "default": "''"}) == ("note", "")


def test_a_different_default_expression_is_absent(tmp_path: Path) -> None:
    """DEFAULT 'n' is not DEFAULT 'x'. The column name alone is not the rule."""
    cfg = _db(
        tmp_path,
        "CREATE TABLE src (id INTEGER PRIMARY KEY, note TEXT DEFAULT 'n')",
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, note TEXT DEFAULT 'x')",
    )
    result = _verify(cfg, "src", "dst")
    assert result["verified"] is False
    assert result["aspects"]["defaults"]["status"] == "absent"
    assert result["aspects"]["defaults"]["missing"] == ["note=n"]
    assert result["aspects"]["defaults"]["extra"] == ["note=x"]


def test_equivalent_default_spellings_are_the_same_rule(tmp_path: Path) -> None:
    """Case, wrapping parens and numeric scale are one rule in the live catalog."""
    cfg = _db(
        tmp_path,
        "CREATE TABLE src ("
        "id INTEGER PRIMARY KEY, "
        "note TEXT DEFAULT 'N', "
        "qty NUMERIC DEFAULT 1.0"
        ")",
        "CREATE TABLE dst ("
        "id INTEGER PRIMARY KEY, "
        "note TEXT DEFAULT ('n'), "
        "qty NUMERIC DEFAULT 1"
        ")",
    )
    result = _verify(cfg, "src", "dst")
    assert result["aspects"]["defaults"]["status"] == "carried"
    assert result["aspects"]["defaults"]["missing"] == []
    assert result["verified"] is True


def test_clock_and_cast_spellings_are_the_same_default_rule() -> None:
    """The catalog diff uses the shared expression rule, not a second copy of it.

    ``CURRENT_TIMESTAMP(6)`` and ``now()`` are one clock. A cast and a national
    string are one literal. SQLite will not store ``now()`` as a default, so
    these are the texts the inspector returns on the engines that do.
    """
    source = PhysicalState(
        found=True,
        readable=True,
        defaults=frozenset(
            {
                catalog_default_fact(
                    {"name": "ts", "default": "CURRENT_TIMESTAMP(6)"}
                ),
                catalog_default_fact(
                    {"name": "status", "default": "('active'::character varying)"}
                ),
            }
        ),
    )
    destination = PhysicalState(
        found=True,
        readable=True,
        defaults=frozenset(
            {
                catalog_default_fact({"name": "ts", "default": "now()"}),
                catalog_default_fact({"name": "status", "default": "N'active'"}),
            }
        ),
    )
    result = compare_physical_state(source, destination)
    assert result["aspects"]["defaults"]["status"] == "carried"
    assert result["aspects"]["defaults"]["missing"] == []
    assert result["verified"] is True


def test_catalog_fact_keeps_the_deferral_mode() -> None:
    """The structured fact keeps when the constraint is checked."""
    keys, unparsed = foreign_keys_from_catalog_state(
        {
            "foreign_key_facts": [
                {
                    "constrained_columns": ["parent_id"],
                    "referred_schema": "public",
                    "referred_table": "parent",
                    "referred_columns": ["id"],
                    "deferral": "DEFERRABLE INITIALLY DEFERRED",
                }
            ]
        }
    )
    assert unparsed == []
    assert keys[0]["deferral"] == "deferred"


def test_catalog_diff_compares_deferral_after_identity() -> None:
    """INITIALLY DEFERRED and NOT DEFERRABLE are one relationship and two rules.

    Column order still matches. An empty deferral list is a comparison that
    did not measure the mode, so those states stay on identity, the row-proof
    gap, the match type, and the referential actions. An unreported mode is
    NOT DEFERRABLE. These facts are catalog-shaped; a live server was not used.
    """
    fact = (("parent_id",), "public", "parent", ("id",))
    postponed = compare_physical_state(
        _fk_state(fact, deferral=("deferred",)),
        _fk_state(fact, deferral=("not_deferrable",)),
    )
    fk = postponed["aspects"]["foreign_keys"]
    assert fk["status"] == "unchecked"
    assert fk["missing"] == []
    assert fk["unchecked"] == ["parent_id->public.parent->id"]
    assert fk["unchecked"].count("parent_id->public.parent->id") == 1
    assert "DEFERRABLE INITIALLY DEFERRED" in fk["deferral_reasons"][0]
    assert "NOT DEFERRABLE" in fk["deferral_reasons"][0]
    assert fk["proof_reasons"] == []
    assert fk["match_reasons"] == []
    assert fk["action_reasons"] == []
    assert postponed["verified"] is False
    assert "foreign_keys" not in postponed["absent"]

    sooner = compare_physical_state(
        _fk_state(fact, deferral=("not_deferrable",)),
        _fk_state(fact, deferral=("immediate",)),
    )
    assert sooner["aspects"]["foreign_keys"]["status"] == "unchecked"
    assert "INITIALLY IMMEDIATE" in sooner["aspects"]["foreign_keys"]["deferral_reasons"][0]
    assert "NOT DEFERRABLE" in sooner["aspects"]["foreign_keys"]["deferral_reasons"][0]

    same = compare_physical_state(
        _fk_state(fact, deferral=("DEFERRABLE INITIALLY DEFERRED",)),
        _fk_state(fact, deferral=("deferred",)),
    )
    assert same["aspects"]["foreign_keys"]["status"] == "carried"
    assert same["verified"] is True

    unreported_dest = compare_physical_state(
        _fk_state(fact, deferral=("deferred",)),
        _fk_state(fact, deferral=("",)),
    )
    assert unreported_dest["aspects"]["foreign_keys"]["status"] == "unchecked"
    assert "unreported" in unreported_dest["aspects"]["foreign_keys"]["deferral_reasons"][0]

    legacy = compare_physical_state(_fk_state(fact), _fk_state(fact))
    assert legacy["aspects"]["foreign_keys"]["status"] == "carried"
    assert legacy["verified"] is True

    misaligned = compare_physical_state(
        _fk_state(fact, deferral=("deferred",)),
        _fk_state(fact, deferral=("deferred", "extra")),
    )
    assert misaligned["aspects"]["foreign_keys"]["status"] == "unchecked"
    assert "could not be read" in misaligned["aspects"]["foreign_keys"]["deferral_reasons"][0]

    forward = (("a", "b"), "", "parent", ("x", "y"))
    reversed_pairs = (("b", "a"), "", "parent", ("y", "x"))
    order = compare_physical_state(
        _fk_state(forward, deferral=("immediate",)),
        _fk_state(reversed_pairs, deferral=("immediate",)),
    )
    assert order["aspects"]["foreign_keys"]["status"] == "carried"


def test_oracle_not_validated_key_is_not_existing_row_proof() -> None:
    """ENABLED is the new-write rule. VALIDATED is the existing-row proof.

    A hand-built Oracle state with an empty proof tuple did not measure the
    column, so it stays on the older carried verdict. A live read attaches
    one gap per reflected key.
    """
    from services.physical_state_diff import _oracle_uniqueness_proof

    src = PhysicalState(
        found=True,
        readable=True,
        primary_key=("id",),
        unique_constraints=frozenset({("email",)}),
    )
    unmeasured = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="oracle",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
        ),
    )
    assert unmeasured["aspects"]["primary_key"]["status"] == "carried"
    assert unmeasured["verified"] is True

    validated = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="oracle",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
            uniqueness_proof=((("id",), ""), (("email",), "")),
        ),
    )
    assert validated["aspects"]["primary_key"]["status"] == "carried"
    assert validated["aspects"]["unique_constraints"]["status"] == "carried"
    assert validated["verified"] is True

    not_validated = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="oracle",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
            uniqueness_proof=((("id",), "not_checked"), (("email",), "")),
        ),
    )
    primary = not_validated["aspects"]["primary_key"]
    assert primary["status"] == "unchecked"
    assert primary["missing"] == []
    assert "id" in primary["unchecked"]
    assert "NOT VALIDATED" in primary["reasons"][0]
    assert "ENABLED" in primary["reasons"][0]
    assert not_validated["aspects"]["unique_constraints"]["status"] == "carried"
    assert not_validated["verified"] is False
    assert "primary_key" in not_validated["unchecked"]
    assert "primary_key" not in not_validated["absent"]

    unread = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="oracle",
            primary_key=("id",),
            uniqueness_proof=((("id",), "unreported"),),
        ),
    )
    assert unread["aspects"]["primary_key"]["status"] == "unchecked"
    assert "was not read" in unread["aspects"]["primary_key"]["reasons"][0]

    rows = [
        ("PK_ID", "P", "ID", 1, "VALIDATED"),
        ("UQ_EMAIL", "U", "EMAIL", 1, "NOT VALIDATED"),
    ]
    proof = _oracle_uniqueness_proof("oracle", ("id",), {("email",)}, rows)
    assert dict(proof) == {("email",): "not_checked", ("id",): ""}
    failed = _oracle_uniqueness_proof("oracle", ("id",), {("email",)}, None)
    assert dict(failed) == {("email",): "unreported", ("id",): "unreported"}
    assert _oracle_uniqueness_proof("postgresql", ("id",), {("email",)}, rows) == ()


def test_sqlserver_disabled_unique_index_is_not_row_proof() -> None:
    """``is_disabled = 1`` is not a write rule and not existing-row proof.

    A hand-built SQL Server state with an empty proof tuple did not measure
    the column, so it stays on the older carried verdict.
    """
    from services.physical_state_diff import _measured_uniqueness_proof

    src = PhysicalState(
        found=True,
        readable=True,
        primary_key=("id",),
        unique_constraints=frozenset({("email",)}),
    )
    unmeasured = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="mssql",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
        ),
    )
    assert unmeasured["aspects"]["primary_key"]["status"] == "carried"
    assert unmeasured["verified"] is True

    disabled = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="sqlserver",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
            uniqueness_proof=((("id",), ""), (("email",), "not_checked")),
        ),
    )
    unique = disabled["aspects"]["unique_constraints"]
    assert unique["status"] == "unchecked"
    assert unique["missing"] == []
    assert "email" in unique["unchecked"]
    assert "is_disabled" in unique["reasons"][0]
    assert disabled["aspects"]["primary_key"]["status"] == "carried"
    assert disabled["verified"] is False
    assert "unique_constraints" not in disabled["absent"]

    rows = [
        ("PK_ID", True, "ID", 1, None, None, 0),
        ("UQ_EMAIL", False, "EMAIL", 1, None, None, 1),
    ]
    proof = _measured_uniqueness_proof(
        "azure_sql", ("id",), {("email",)}, None, rows
    )
    assert dict(proof) == {("email",): "not_checked", ("id",): ""}
    failed = _measured_uniqueness_proof(
        "mssql", ("id",), {("email",)}, None, None
    )
    assert dict(failed) == {("email",): "unreported", ("id",): "unreported"}
    assert _measured_uniqueness_proof("sqlite", ("id",), {("email",)}, rows, rows) == ()


def test_postgres_invalid_unique_index_is_not_existing_row_proof() -> None:
    """``indisvalid`` is the existing-row proof. ``indisready`` is the write rule.

    A hand-built PostgreSQL state with an empty proof tuple did not measure
    the columns, so it stays on the older carried verdict. A live read
    attaches one gap per reflected key. A valid index on the same columns
    keeps that proof.
    """
    from services.physical_state_diff import _measured_uniqueness_proof

    src = PhysicalState(
        found=True,
        readable=True,
        primary_key=("id",),
        unique_constraints=frozenset({("email",)}),
    )
    unmeasured = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="postgres",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
        ),
    )
    assert unmeasured["aspects"]["primary_key"]["status"] == "carried"
    assert unmeasured["verified"] is True

    invalid = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="azure_postgres",
            primary_key=("id",),
            unique_constraints=frozenset({("email",)}),
            uniqueness_proof=((("id",), ""), (("email",), "not_checked")),
        ),
    )
    unique = invalid["aspects"]["unique_constraints"]
    assert unique["status"] == "unchecked"
    assert unique["missing"] == []
    assert "email" in unique["unchecked"]
    assert "indisvalid" in unique["reasons"][0]
    assert "indisready is true" in unique["reasons"][0]
    assert invalid["aspects"]["primary_key"]["status"] == "carried"
    assert invalid["verified"] is False
    assert "unique_constraints" not in invalid["absent"]

    not_ready = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="postgresql",
            primary_key=("id",),
            uniqueness_proof=((("id",), "not_ready"),),
        ),
    )
    primary = not_ready["aspects"]["primary_key"]
    assert primary["status"] == "unchecked"
    assert "indisready" in primary["reasons"][0]
    assert "does not reject" in primary["reasons"][0]
    assert not_ready["verified"] is False

    rows = [
        ("users_pkey", "id", True, True),
        ("users_email_invalid", "email", False, True),
    ]
    proof = _measured_uniqueness_proof(
        "postgres", ("id",), {("email",)}, None, None, rows
    )
    assert dict(proof) == {("email",): "not_checked", ("id",): ""}
    cancelled = _measured_uniqueness_proof(
        "postgresql",
        ("id",),
        {("email",)},
        None,
        None,
        [
            ("users_email_invalid", "email", False, True),
            ("users_email_key", "email", True, True),
        ],
    )
    assert dict(cancelled) == {("email",): "", ("id",): "unreported"}
    failed = _measured_uniqueness_proof(
        "neon", ("id",), {("email",)}, None, None, None
    )
    assert dict(failed) == {("email",): "unreported", ("id",): "unreported"}
    assert _measured_uniqueness_proof(
        "redshift", ("id",), {("email",)}, None, None, rows
    ) == ()


def test_postgres_not_valid_check_is_not_existing_row_proof() -> None:
    """``NOT VALID`` is not a dropped check. New rows are still rejected.

    A hand-built PostgreSQL state with an empty proof tuple did not measure
    the flag, so it stays on the older carried verdict. SQLAlchemy omits
    ``dialect_options.not_valid`` when the reflected text is valid.
    """
    from services.physical_state_diff import _measured_check_proof

    src = PhysicalState(
        found=True,
        readable=True,
        check_constraints=frozenset({"qty>0"}),
    )
    unmeasured = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="postgres",
            check_constraints=frozenset({"qty>0"}),
        ),
    )
    assert unmeasured["aspects"]["check_constraints"]["status"] == "carried"
    assert unmeasured["verified"] is True

    not_valid = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="azure_postgres",
            check_constraints=frozenset({"qty>0"}),
            check_proof=(("qty>0", "not_checked"),),
        ),
    )
    check = not_valid["aspects"]["check_constraints"]
    assert check["status"] == "unchecked"
    assert check["missing"] == []
    assert check["unchecked"] == ["qty>0"]
    assert "NOT VALID" in check["reasons"][0]
    assert "still rejected" in check["reasons"][0]
    assert not_valid["verified"] is False
    assert "check_constraints" not in not_valid["absent"]
    assert "check_constraints" in not_valid["unchecked"]

    unread = compare_physical_state(
        src,
        PhysicalState(
            found=True,
            readable=True,
            dialect="postgresql",
            check_constraints=frozenset({"qty>0"}),
            check_proof=(("qty>0", "unreported"),),
        ),
    )
    assert unread["aspects"]["check_constraints"]["status"] == "unchecked"
    assert "was not read" in unread["aspects"]["check_constraints"]["reasons"][0]

    reflected = _measured_check_proof(
        "postgres",
        [
            {"sqltext": "qty > 0"},
            {
                "sqltext": "(email <> '')",
                "dialect_options": {"not_valid": True},
            },
        ],
    )
    assert dict(reflected) == {"qty>0": "", "email<>''": "not_checked"}
    assert _measured_check_proof("sqlite", [{"sqltext": "qty > 0"}]) == ()
    assert _measured_check_proof(
        "redshift",
        [{"sqltext": "qty > 0", "dialect_options": {"not_valid": True}}],
    ) == ()
