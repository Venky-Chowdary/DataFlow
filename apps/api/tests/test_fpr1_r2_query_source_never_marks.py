"""PR 411 review 2, F2: only a plain table read marks typed-source text carriers.

The catalog of the source table decides whether a text column compares as a
boolean or an instant. In query mode the stream passed the query's first FROM
table, so ``SELECT id, CAST(c AS CHAR) AS c FROM t`` marked a pure text
projection whenever ``t.c`` was tinyint(1) or datetime: ``'1'`` verified as
``'true'`` and ``'2024-01-01 12:00:00'`` as ``'2024-01-01T12:00:00'``. A query
or procedure source has no catalog column behind its output, so it never marks.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

import services.dest_physical_types as dpt
from tests._r4_temporal_harness import transfer_client  # noqa: F401  (fixture)

_MAPPINGS = [
    {"source": "id", "target": "id", "target_type": "INT4"},
    {"source": "b", "target": "b", "target_type": "TEXT"},
    {"source": "c", "target": "c", "target_type": "TEXT"},
]
_BASIS = {"id": "INT4", "b": "TEXT", "c": "TEXT"}


@pytest.fixture
def catalog(monkeypatch):
    reads: list[str] = []

    def fake(db_type, cfg, *, table, columns=None):
        reads.append(table)
        return {"id": "int", "b": "tinyint(1)", "c": "datetime(6)"}

    monkeypatch.setattr(dpt, "catalog_declared_types", fake)
    return reads


def test_a_plain_table_read_marks(catalog):
    source = SimpleNamespace(table="t", extra={})
    out = dpt.mark_typed_source_text_carriers(_BASIS, _MAPPINGS, "mysql", {}, "t", source=source)
    assert out == {
        "id": "INT4",
        "b": "TEXT CARRYING SOURCE_BOOLEAN",
        "c": "TEXT CARRYING SOURCE_INSTANT",
    }
    assert catalog == ["t"]


@pytest.mark.parametrize(
    "extra",
    [
        {"source_query": "SELECT id, CAST(b AS CHAR) AS b, CAST(c AS CHAR) AS c FROM t"},
        {"source_read_mode": "query", "source_query": "SELECT id, b, c FROM t"},
        {"source_read_mode": "procedure", "procedure_call": "CALL p()"},
        {"procedure_call": "CALL p()"},
    ],
    ids=["query", "query-word", "procedure-word", "procedure"],
)
def test_a_query_or_procedure_source_never_marks(catalog, extra):
    endpoint = SimpleNamespace(table="t", extra=dict(extra))
    assert (
        dpt.mark_typed_source_text_carriers(_BASIS, _MAPPINGS, "mysql", {}, "t", source=endpoint)
        == _BASIS
    )
    # The connector config alone says so too (the stream checks both).
    assert dpt.mark_typed_source_text_carriers(_BASIS, _MAPPINGS, "mysql", dict(extra), "t") == _BASIS
    assert catalog == []


def test_no_named_table_never_marks(catalog):
    assert dpt.mark_typed_source_text_carriers(_BASIS, _MAPPINGS, "mysql", {}, "") == _BASIS
    assert catalog == []


# ---------------------------------------------------------------- live

_RISK = {
    "approved_by": "qa@example.com",
    "reason": "existing destination",
    "execution_policy": "CAST_AND_CONTINUE",
}


def _run(client, h, *, styp, value, stored_sql, query):
    t = f"ff2_q_{uuid.uuid4().hex[:6]}"
    out = f"{t}_o"
    h.mysql_exec(f"CREATE TABLE `{t}` (id int primary key, c {styp})")
    try:
        h.mysql_exec(f"INSERT INTO `{t}` VALUES (1, {value}), (2, NULL)")
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
        src = {"source_query": query.format(t=t)} if query else {"source_table": t}
        staged = h.stage(
            source_connector_id=h.connector("mysql", "source"),
            dest_connector_id=h.connector("postgresql", "destination"),
            dest_table=out,
            primary_key="id",
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
            schema_drift_acknowledged=True,
            risk_acceptance=_RISK,
            **src,
        )
        assert staged.success, h.staged_summary(staged)
        job = h.confirm(client, staged)
        stored = h.pg_exec(f'SELECT c FROM public."{out}" WHERE id = 1')
        return job, stored[0][0]
    finally:
        h.mysql_exec(f"DROP TABLE IF EXISTS `{t}`")
        h.pg_exec(f'DROP TABLE IF EXISTS public."{out}"')
        h.pg_exec(f'DROP FUNCTION IF EXISTS public."{out}_fn"() CASCADE')


@pytest.mark.parametrize(
    "styp,value,stored_sql",
    [
        ("tinyint(1)", "1", "'true'"),
        ("datetime", "'2024-01-01 12:00:00'", "'2024-01-01T12:00:00'"),
        ("datetime", "'2024-01-01 12:00:00'", "'2024-01-01 07:00:00-05:00'"),
    ],
    ids=["bool-as-true", "dt-as-iso", "dt-as-offset"],
)
def test_live_text_projection_from_a_bare_table_name_stays_exact(
    transfer_client, styp, value, stored_sql
):
    from tests import _r4_temporal_harness as h

    job, stored = _run(
        transfer_client,
        h,
        styp=styp,
        value=value,
        stored_sql=stored_sql,
        query="SELECT id, CAST(c AS CHAR) AS c FROM {t}",
    )
    assert stored == stored_sql.strip("'")
    assert job.get("status") == "failed", job.get("error") or job.get("message")


@pytest.mark.parametrize(
    "styp,value", [("tinyint(1)", "1"), ("datetime(6)", "'2024-01-01 12:00:00'")]
)
@pytest.mark.parametrize("query", [None, "SELECT id, c FROM {t}"], ids=["table", "query"])
def test_live_correct_loads_still_pass(transfer_client, styp, value, query):
    from tests import _r4_temporal_harness as h

    job, _stored = _run(
        transfer_client, h, styp=styp, value=value, stored_sql=None, query=query
    )
    assert str(job.get("status") or "").startswith("completed"), (
        job.get("error") or job.get("message")
    )
