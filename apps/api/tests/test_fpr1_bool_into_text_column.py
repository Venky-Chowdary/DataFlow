"""PR 411 review 1, H1: a typed boolean into an existing TEXT column.

The writer stores what it binds for the boolean: ``'1'`` for a MySQL
``tinyint(1)`` cell, ``'true'`` for a PostgreSQL ``boolean``. Gate-8 rendered the
source bool as ``'true'`` and failed the MySQL one as corruption on a correct
load. The fold applies only when the source value is a typed bool (or the
mapping's boolean cast makes it one) landing in a character column: text into
text stays exact, so a destination ``'1'`` where the source text was ``'true'``
is still a mismatch.
"""

from __future__ import annotations

import uuid

import pytest

from services.fingerprint_rules import mark_typed_text_carriers
from services.reconciliation import checksum_rows, sample_compare_rows
from tests._r4_temporal_harness import transfer_client  # noqa: F401  (fixture)

_BOOL_TO_TEXT = {
    "source": "b",
    "target": "b",
    "source_type": "BOOLEAN",
    "target_type": "TEXT",
    "transform": "boolean",
}
_ID = {"source": "id", "target": "id", "target_type": "INT4", "transform": "integer"}


def _compare(src_b, dst_b, mapping=None, engine="postgresql"):
    mappings = [_ID, dict(mapping or _BOOL_TO_TEXT)]
    catalog = {"id": "int", "b": str((mapping or _BOOL_TO_TEXT)["source_type"]).lower()}
    dest_types = mark_typed_text_carriers({"id": "INT4", "b": "TEXT"}, mappings, catalog)
    sample = sample_compare_rows(
        [{"id": "0", "b": src_b}],
        [{"id": 0, "b": dst_b}],
        mappings,
        sort_key="id",
        dest_db_type=engine,
        dest_types=dest_types,
    )
    # The digest sides fingerprint under the same basis and must agree with it.
    # (the write pass digests the mapped cell: the boolean cast made '1' True)
    typed = {"1": True, "0": False}.get(src_b, src_b) if mapping is None else src_b
    source_rows = [{"id": 0, "b": typed}]
    digests_equal = checksum_rows(
        source_rows, ["id", "b"], dest_db_type=engine, dest_types=dest_types
    ) == checksum_rows([{"id": 0, "b": dst_b}], ["id", "b"], dest_db_type=engine, dest_types=dest_types)
    assert digests_equal is sample["passed"], (sample, digests_equal)
    return sample


def test_only_a_catalog_boolean_source_marks_the_text_column():
    mappings = [
        _ID,
        _BOOL_TO_TEXT,
        # A text column of 'true'/'false': sampling calls it BOOLEAN, the catalog TEXT.
        {"source": "t", "target": "t", "source_type": "BOOLEAN", "target_type": "TEXT"},
        {"source": "n", "target": "n", "source_type": "BOOLEAN"},
    ]
    basis = {"id": "INT4", "b": "TEXT", "t": "VARCHAR(10)", "n": "INT4"}
    catalog = {"id": "int", "b": "tinyint(1)", "t": "text", "n": "boolean"}
    assert mark_typed_text_carriers(basis, mappings, catalog) == {
        "id": "INT4",
        "b": "TEXT CARRYING SOURCE_BOOLEAN",
        "t": "VARCHAR(10)",
        "n": "INT4",
    }
    # No catalog (a file, a query): nothing is marked.
    assert mark_typed_text_carriers(basis, mappings, {}) == basis


def test_catalog_declared_types_ignore_sample_refined_inference(monkeypatch):
    import services.dest_physical_types as dpt

    records = [
        {"name": "t", "inferred_type": "BOOLEAN", "data_type": "text", "sample_refined": True},
        {"name": "b", "inferred_type": "BOOLEAN", "data_type": "tinyint(1)"},
        {"name": "x", "inferred_type": "DATETIME", "sample_refined": True},
        {"name": "y", "inferred_type": "INT4"},
    ]
    monkeypatch.setattr(dpt, "_catalog_columns", lambda *a, **k: records)
    assert dpt.catalog_declared_types("postgresql", {}, table="t") == {
        "t": "text",
        "b": "tinyint(1)",
        "y": "INT4",
    }
    assert dpt.physical_column_types("postgresql", {}, table="t")["t"] == "BOOLEAN"


@pytest.mark.parametrize("engine", ["postgresql", "mysql"])
@pytest.mark.parametrize(
    "src_b,dst_b",
    [
        ("1", "1"),  # MySQL tinyint(1) wire + the mapping's boolean cast
        ("0", "0"),
        (True, "1"),
        (False, "0"),
        (True, "true"),  # PostgreSQL boolean source
        (False, "false"),
    ],
)
def test_typed_bool_matches_the_text_the_writer_stored(engine, src_b, dst_b):
    result = _compare(src_b, dst_b, engine=engine)
    assert result["passed"] is True, result


@pytest.mark.parametrize(
    "src_b,dst_b",
    [(True, "0"), (False, "true"), ("1", "0"), (True, "yes"), (True, "t"), (True, " 1")],
)
def test_typed_bool_against_other_text_still_fails(src_b, dst_b):
    assert _compare(src_b, dst_b)["passed"] is False


def test_text_into_text_stays_exact():
    text_to_text = {"source": "b", "target": "b", "source_type": "TEXT", "target_type": "TEXT"}
    assert _compare("true", "1", mapping=text_to_text)["passed"] is False
    assert _compare("true", "true", mapping=text_to_text)["passed"] is True


# ---------------------------------------------------------------- live


def _live_bool_into_existing_text(client, h, dest):
    t = f"fpr1_b_{uuid.uuid4().hex[:6]}"
    out = f"{t}_o"
    h.mysql_exec(f"CREATE TABLE `{t}` (id int primary key, v double, b tinyint(1))")
    try:
        for i, (v, b) in enumerate([(1 / 3, 1), (0.1, 0), (None, None), (2.5, 1)]):
            h.mysql_exec(f"INSERT INTO `{t}` VALUES (%s, %s, %s)", (i, v, b))
        if dest == "postgresql":
            h.pg_exec(f'CREATE TABLE public."{out}" (id int primary key, v numeric, b text)')
        else:
            h.mysql_exec(f"CREATE TABLE `{out}` (id int primary key, v decimal(30,20), b text)")
        staged = h.stage(
            source_connector_id=h.connector("mysql", "source"),
            source_table=t,
            dest_connector_id=h.connector(dest, "destination"),
            dest_table=out,
            primary_key="id",
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
            schema_drift_acknowledged=True,
            risk_acceptance={
                "approved_by": "qa@example.com",
                "reason": "existing destination",
                "execution_policy": "CAST_AND_CONTINUE",
            },
        )
        assert staged.success, h.staged_summary(staged)
        job = h.confirm(client, staged)
        stored = (
            h.pg_exec(f'SELECT b FROM public."{out}" ORDER BY id')
            if dest == "postgresql"
            else h.mysql_exec(f"SELECT b FROM `{out}` ORDER BY id")
        )
        return job, [r[0] for r in stored]
    finally:
        h.mysql_exec(f"DROP TABLE IF EXISTS `{t}`")
        if dest == "postgresql":
            h.pg_exec(f'DROP TABLE IF EXISTS public."{out}"')
        else:
            h.mysql_exec(f"DROP TABLE IF EXISTS `{out}`")


@pytest.mark.parametrize("dest", ["postgresql", "mysql"])
def test_live_mysql_tinyint1_into_existing_text_passes_strict(transfer_client, dest):
    from tests import _r4_temporal_harness as h

    job, stored = _live_bool_into_existing_text(transfer_client, h, dest)
    assert stored == ["1", "0", None, "1"]
    assert str(job.get("status") or "").startswith("completed"), (
        job.get("error") or job.get("message")
    )


def test_live_text_true_stored_as_1_still_fails(transfer_client):
    """PG text 'true' -> PG text; a trigger stores '1'. Text compares exactly."""
    from tests import _r4_temporal_harness as h

    t = f"fpr1_bt_{uuid.uuid4().hex[:6]}"
    out = f"{t}_o"
    fn = f"{t}_fn"
    h.pg_exec(f'CREATE TABLE public."{t}" (id int primary key, b text)')
    try:
        h.pg_exec(f"INSERT INTO public.\"{t}\" VALUES (0, 'true'), (1, 'false')")
        h.pg_exec(f'CREATE TABLE public."{out}" (id int primary key, b text)')
        h.pg_exec(
            f'CREATE FUNCTION public."{fn}"() RETURNS trigger AS $$ BEGIN '
            "IF NEW.b = 'true' THEN NEW.b := '1'; END IF; RETURN NEW; END $$ LANGUAGE plpgsql"
        )
        h.pg_exec(
            f'CREATE TRIGGER "{t}_tg" BEFORE INSERT OR UPDATE ON public."{out}" '
            f'FOR EACH ROW EXECUTE FUNCTION public."{fn}"()'
        )
        staged = h.stage(
            source_connector_id=h.connector("postgresql", "source"),
            source_table=t,
            dest_connector_id=h.connector("postgresql", "destination"),
            dest_table=out,
            primary_key="id",
            sync_mode="incremental_append",
            cursor_column="id",
            cursor_semantics="monotonic_sequence",
            validation_mode="strict",
        )
        assert staged.success, h.staged_summary(staged)
        job = h.confirm(transfer_client, staged)
        # Failed and undone: the append wrote '1' where the source said 'true'.
        assert job.get("status") == "failed", job
        assert "'1'" in str(job.get("error") or ""), job.get("error")
    finally:
        h.pg_exec(f'DROP TABLE IF EXISTS public."{out}"')
        h.pg_exec(f'DROP FUNCTION IF EXISTS public."{fn}"() CASCADE')
        h.pg_exec(f'DROP TABLE IF EXISTS public."{t}"')
