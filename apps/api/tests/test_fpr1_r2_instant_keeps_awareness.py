"""PR 411 review 2, F1: a naive instant never equals text that carries an offset.

A text column whose source column is a typed datetime compares as the instant
it spells (``CARRYING SOURCE_INSTANT``). Folding offset text to UTC wall clock
and leaving the naive source alone made naive ``12:00:00`` equal
``'07:00:00-05:00'``, ``'13:00:00+01:00'`` and ``'12:00:00Z'``, and a stripped
parse made it equal ``'12:00:00\\n'``. Aware and naive now stay apart in the
canonical form (an aware value or offset text is UTC wall clock plus a marker),
and text with surrounding whitespace is not folded at all.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
import uuid

import pytest

from services.fingerprint_rules import mark_typed_text_carriers
from services.reconciliation import fingerprint_for_reconcile, normalize_cell
from services.verification_ladder import run_five_layer_verification
from tests._r4_temporal_harness import transfer_client  # noqa: F401  (fixture)

_CARRIED = "TEXT CARRYING SOURCE_INSTANT"
_MAPPINGS = [
    {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INT4"},
    {"source": "c", "target": "c", "source_type": "DATETIME", "target_type": "TEXT"},
]
_UTC = dt.timezone.utc
_NAIVE = dt.datetime(2024, 1, 1, 12)  # noqa: DTZ001
_AWARE = dt.datetime(2024, 1, 1, 12, tzinfo=_UTC)

# Each spells another instant than naive 12:00, or is not how a writer stores it.
_NOT_NAIVE_NOON = [
    "2024-01-01 07:00:00-05:00",
    "2024-01-01 13:00:00+01:00",
    "2024-01-01T12:00:00Z",
    "2024-01-01 12:00:00+00:00",
    "2024-01-01 12:00:00\n",
    "2024-01-01 12:00:00 ",
    "\t2024-01-01 12:00:00",
]
_NAIVE_NOON = ["2024-01-01 12:00:00", "2024-01-01T12:00:00", "2024-01-01 12:00:00.000000"]


def _fp(value, engine="postgresql"):
    return fingerprint_for_reconcile(value, ddl_type=_CARRIED, engine=engine)


def _ladder(source_c, target_c, *, catalog_c="datetime(6)"):
    types = mark_typed_text_carriers(
        {"id": "INT4", "c": "TEXT"}, _MAPPINGS, {"id": "int", "c": catalog_c}
    )
    report = run_five_layer_verification(
        source_rows=[{"id": i, "c": v} for i, v in enumerate(source_c)],
        target_rows=[{"id": str(i), "c": v} for i, v in enumerate(target_c)],
        columns=["id", "c"],
        pk_column="id",
        source_row_count=len(source_c),
        target_row_count=len(target_c),
        dest_db_type="postgresql",
        dest_types=types,
    )
    return bool(report.get("passed")) if isinstance(report, dict) else bool(report.passed)


@pytest.mark.parametrize("engine", ["postgresql", "mysql", "sqlite"])
@pytest.mark.parametrize("stored", _NOT_NAIVE_NOON)
def test_naive_source_never_equals_offset_or_padded_text(stored, engine):
    for source in (_NAIVE, "2024-01-01T12:00:00", "2024-01-01 12:00:00"):
        assert _fp(source, engine) != _fp(stored, engine), (source, stored)
        assert normalize_cell(source, ddl_type=_CARRIED, engine=engine) != normalize_cell(
            stored, ddl_type=_CARRIED, engine=engine
        )
    assert normalize_cell(_NAIVE, ddl_type=_CARRIED, engine=engine) != normalize_cell(
        stored, ddl_type=_CARRIED, engine=engine
    )


@pytest.mark.parametrize("stored", _NAIVE_NOON)
def test_naive_source_equals_the_naive_spellings_a_writer_stores(stored):
    for source in (_NAIVE, "2024-01-01T12:00:00"):
        assert _fp(source) == _fp(stored)
    assert normalize_cell(_NAIVE, ddl_type=_CARRIED) == normalize_cell(stored, ddl_type=_CARRIED)


@pytest.mark.parametrize(
    "stored", ["2024-01-01T12:00:00Z", "2024-01-01 12:00:00+00:00", "2024-01-01 13:00:00+01:00"]
)
def test_aware_source_equals_the_same_instant_with_an_offset(stored):
    for source in (_AWARE, "2024-01-01T12:00:00+00:00", "2024-01-01T12:00:00Z"):
        assert _fp(source) == _fp(stored), (source, stored)
    assert normalize_cell(_AWARE, ddl_type=_CARRIED) == normalize_cell(stored, ddl_type=_CARRIED)


@pytest.mark.parametrize(
    "stored", ["2024-01-01T13:00:00Z", "2024-01-01 12:00:00", "2024-01-01T12:00:00", "2024-01-01T12:00:00Z\n"]
)
def test_aware_source_differs_from_a_shifted_or_naive_instant(stored):
    for source in (_AWARE, "2024-01-01T12:00:00+00:00"):
        assert _fp(source) != _fp(stored), (source, stored)
    assert normalize_cell(_AWARE, ddl_type=_CARRIED) != normalize_cell(stored, ddl_type=_CARRIED)


@pytest.mark.parametrize("stored", _NOT_NAIVE_NOON)
def test_ladder_fails_offset_or_padded_text_against_a_naive_source(stored):
    assert not _ladder(["2024-01-01T12:00:00", None], [stored, None])
    assert not _ladder([_NAIVE, None], [stored, None])


@pytest.mark.parametrize("stored", _NAIVE_NOON)
def test_ladder_passes_the_naive_spellings(stored):
    assert _ladder(["2024-01-01T12:00:00", None], [stored, None])


# ---------------------------------------------------------------- live

_RISK = {
    "approved_by": "qa@example.com",
    "reason": "existing destination",
    "execution_policy": "CAST_AND_CONTINUE",
}


def _run(client, h, *, source, dest, stored_sql, sql_source=None):
    """``source`` builds the source table; a trigger rewrites row 1's stored text."""
    t = f"ff2_i_{uuid.uuid4().hex[:6]}"
    out = f"{t}_o"
    db = str(client.sqlite_root / "ff2.db")
    dest_extra = {"database": db} if dest == "sqlite" else {}
    source_kind = source(t)
    try:
        if dest == "postgresql":
            h.pg_exec(f'CREATE TABLE public."{out}" (id int primary key, c text)')
            if stored_sql:
                h.pg_exec(
                    f'CREATE FUNCTION public."{out}_fn"() RETURNS trigger AS $$ BEGIN '
                    f"IF NEW.id = 1 THEN NEW.c := {stored_sql}; END IF; "
                    "RETURN NEW; END $$ LANGUAGE plpgsql"
                )
                h.pg_exec(
                    f'CREATE TRIGGER "{out}_tg" BEFORE INSERT OR UPDATE ON public."{out}" '
                    f'FOR EACH ROW EXECUTE FUNCTION public."{out}_fn"()'
                )
        else:
            con = sqlite3.connect(db)
            con.execute(f'CREATE TABLE "{out}" (id INTEGER PRIMARY KEY, c TEXT)')
            if stored_sql:
                con.execute(
                    f'CREATE TRIGGER "{out}_tg" AFTER INSERT ON "{out}" WHEN NEW.id = 1 '
                    f'BEGIN UPDATE "{out}" SET c = {stored_sql} WHERE id = 1; END'
                )
            con.commit()
            con.close()
        staged = h.stage(
            source_connector_id=h.connector(source_kind, "source"),
            source_table=t,
            dest_connector_id=h.connector(dest, "destination", **dest_extra),
            dest_table=out,
            primary_key="id",
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
            schema_drift_acknowledged=True,
            risk_acceptance=_RISK,
        )
        assert staged.success, h.staged_summary(staged)
        job = h.confirm(client, staged)
        if dest == "postgresql":
            stored = h.pg_exec(f'SELECT c FROM public."{out}" WHERE id = 1')
        else:
            con = sqlite3.connect(db)
            stored = con.execute(f'SELECT c FROM "{out}" WHERE id = 1').fetchall()
            con.close()
        return job, (stored[0][0] if stored else None)
    finally:
        if source_kind == "mysql":
            h.mysql_exec(f"DROP TABLE IF EXISTS `{t}`")
        else:
            h.pg_exec(f'DROP TABLE IF EXISTS public."{t}"')
        if dest == "postgresql":
            h.pg_exec(f'DROP TABLE IF EXISTS public."{out}"')
            h.pg_exec(f'DROP FUNCTION IF EXISTS public."{out}_fn"() CASCADE')


def _mysql_naive_noon(h):
    def build(t):
        h.mysql_exec(f"CREATE TABLE `{t}` (id int primary key, c datetime(6))")
        h.mysql_exec(f"INSERT INTO `{t}` VALUES (1, '2024-01-01 12:00:00'), (2, NULL)")
        return "mysql"

    return build


def _pg_aware_noon(h):
    def build(t):
        h.pg_exec(f'CREATE TABLE public."{t}" (id int primary key, c timestamptz)')
        h.pg_exec(f"INSERT INTO public.\"{t}\" VALUES (1, '2024-01-01 12:00:00+00'), (2, NULL)")
        return "postgresql"

    return build


def _status(job):
    return str(job.get("status") or ""), job.get("error") or job.get("message")


@pytest.mark.parametrize(
    "stored_sql",
    [
        "'2024-01-01 07:00:00-05:00'",
        "'2024-01-01 13:00:00+01:00'",
        "'2024-01-01T12:00:00Z'",
        "E'2024-01-01 12:00:00\\n'",
    ],
    ids=["minus5", "plus1", "Z", "newline"],
)
def test_live_naive_mysql_datetime_fails_offset_or_padded_text(transfer_client, stored_sql):
    from tests import _r4_temporal_harness as h

    job, stored = _run(
        transfer_client, h, source=_mysql_naive_noon(h), dest="postgresql", stored_sql=stored_sql
    )
    assert stored is not None and stored != "2024-01-01 12:00:00"
    status, why = _status(job)
    assert status == "failed", why


@pytest.mark.parametrize(
    "stored_sql",
    [None, "'2024-01-01 12:00:00'", "'2024-01-01T12:00:00'", "'2024-01-01 12:00:00.000000'"],
    ids=["writer", "space", "iso", "micro"],
)
def test_live_naive_mysql_datetime_passes_the_naive_spellings(transfer_client, stored_sql):
    from tests import _r4_temporal_harness as h

    job, _stored = _run(
        transfer_client, h, source=_mysql_naive_noon(h), dest="postgresql", stored_sql=stored_sql
    )
    status, why = _status(job)
    assert status.startswith("completed"), why


@pytest.mark.parametrize(
    "stored_sql,ok",
    [
        (None, True),
        ("'2024-01-01 13:00:00+01:00'", True),
        ("'2024-01-01T13:00:00Z'", False),
        ("'2024-01-01 12:00:00'", False),
    ],
    ids=["writer", "same-instant", "shifted", "naive"],
)
def test_live_aware_pg_timestamptz_into_sqlite_text(transfer_client, stored_sql, ok):
    from tests import _r4_temporal_harness as h

    job, stored = _run(
        transfer_client, h, source=_pg_aware_noon(h), dest="sqlite", stored_sql=stored_sql
    )
    status, why = _status(job)
    if ok:
        assert status.startswith("completed"), (why, stored)
    else:
        assert status == "failed", (why, stored)
