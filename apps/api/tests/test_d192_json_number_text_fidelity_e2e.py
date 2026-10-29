"""D192 - a JSON number keeps the spelling the source gave it (``1.10`` stays ``1.10``).

Root cause: the JSON write path (``transform_engine._parse_json``) parsed the
document text and dumped it again. ``json_loads_exact`` demotes every number
binary64 can hold to a float, so ``1.10`` came back as ``1.1``, ``2.50`` as
``2.5`` and ``1e2`` as ``100.0`` before the writer bound the cell. A
destination that stores JSON as *text* (SQL Server NVARCHAR(MAX), PG ``json``,
MySQL LONGTEXT) then held a different document than the source.

Fix: valid JSON text is written as its own tokens with only insignificant
whitespace removed (``value_serializer.compact_json_text``).

Native JSON types normalise on store, and that is the engine's documented
behaviour, not DataFlow's (docs/JSON_FIDELITY.md): PG ``jsonb`` keeps every
digit and scale but rewrites ``1e2`` -> ``100`` and ``-0.0`` -> ``0.0``; MySQL
``JSON`` stores numbers as doubles (``1.10`` -> ``1.1``). Those are asserted
value-equal; text carriers are asserted byte-equal.

Route: ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job, PG source,
twice (run 2 lands in the same table). NULL, JSON strings "" / whitespace /
unicode inside the document, and a TEXT column beside it holding SQL NULL,
'', whitespace and unicode.
"""

from __future__ import annotations

import json
import uuid
from decimal import Decimal

import pytest

from tests import value_fidelity_route as r

_DOC = (
    '{"price": 1.10, "list": [1.0, 2.50, 123.4500, 0.000, 1e2, -0.0], '
    '"s": "1.10", "u": "Łukasz 山田 🎉", "e": "", "w": "  ", "n": null}'
)
_COMPACT = (
    '{"price":1.10,"list":[1.0,2.50,123.4500,0.000,1e2,-0.0],'
    '"s":"1.10","u":"Łukasz 山田 🎉","e":"","w":"  ","n":null}'
)
# id -> (json text, note)
_ROWS = {
    1: (_DOC, "plain"),
    2: (None, None),
    3: ('"1.10"', ""),
    4: ("[1.10, 2.500]", "   "),
    5: ('{"deep": {"x": [0.10, {"y": 10.0}]}}', "Łukasz 山田"),
}
_COMPACT_ROWS = {
    1: _COMPACT,
    2: None,
    3: '"1.10"',
    4: "[1.10,2.500]",
    5: '{"deep":{"x":[0.10,{"y":10.0}]}}',
}

# (dest kind, pre-existing DDL or None for "DataFlow creates it", how to compare)
_CASES = {
    "sqlserver-created": ("sqlserver", None, "text"),
    "sqlserver-existing-nvarchar": (
        "sqlserver", "CREATE TABLE {t} (id BIGINT PRIMARY KEY, doc NVARCHAR(MAX), note NVARCHAR(MAX))",
        "text",
    ),
    "postgresql-existing-json": (
        "postgresql", "CREATE TABLE {t} (id BIGINT PRIMARY KEY, doc JSON, note TEXT)", "text",
    ),
    "postgresql-created-jsonb": ("postgresql", None, "jsonb"),
    "mysql-existing-longtext": (
        "mysql", "CREATE TABLE {t} (id BIGINT PRIMARY KEY, doc LONGTEXT, note LONGTEXT) "
        "CHARACTER SET utf8mb4", "text",
    ),
    "mysql-created-json": ("mysql", None, "native"),
}


@pytest.fixture
def client(tmp_path, monkeypatch):
    with r.make_client(tmp_path, monkeypatch) as c:
        yield c


def _read(kind: str, table: str) -> dict[int, tuple]:
    doc = "doc::text" if kind == "postgresql" else "doc"
    rows = r.query(kind, f"SELECT id, {doc}, note FROM {r.quote(kind, table)}")  # nosec B608
    return {int(a): (b, c) for a, b, c in rows}


def _assert_doc(compare: str, got, want_compact) -> None:
    if want_compact is None:
        assert got is None
        return
    if compare == "text":
        # Byte-equal: every number keeps its spelling.
        assert got == want_compact
    elif compare == "jsonb":
        # jsonb keeps digits and scale; it reorders keys and rewrites 1e2/-0.0.
        assert json.loads(got, parse_float=Decimal) == json.loads(want_compact, parse_float=Decimal)
        for token in ("1.10", "2.50", "123.4500", "0.000", "0.10", "10.0", "2.500"):
            if token in want_compact:
                assert token in got, (token, got)
    else:
        # MySQL JSON stores doubles: value-equal is all the engine can hold.
        assert json.loads(got) == json.loads(want_compact)


@pytest.mark.parametrize("case", list(_CASES))
def test_json_number_spelling_survives_two_runs(client, case):
    kind, ddl, compare = _CASES[case]
    pg = r.pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"d192_{kind[:5]}_{token}"
    dest_table = table + "_out"
    cur = pg.cursor()
    try:
        cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, doc JSON, note TEXT)")
        for i, (doc, note) in _ROWS.items():
            cur.execute(f"INSERT INTO {table} VALUES (%s, %s, %s)", (i, doc, note))  # nosec B608
        src_id, dst_id = r.connectors(token, kind)
        if ddl:
            r.execute(kind, ddl.format(t=r.quote(kind, dest_table)))
        for _ in range(2):
            r.run(
                client,
                source_connector_id=src_id, source_table=table,
                dest_connector_id=dst_id, dest_table=dest_table,
                sync_mode="full_refresh_overwrite", primary_key="id",
            )
            out = _read(kind, dest_table)
            assert set(out) == set(_ROWS)
            for i, (_doc, note) in _ROWS.items():
                _assert_doc(compare, out[i][0], _COMPACT_ROWS[i])
                assert out[i][1] == note, (i, out[i][1], note)
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        r.drop(kind, dest_table)


def test_compact_json_text_keeps_every_token():
    from services.value_serializer import compact_json_text

    assert compact_json_text(_DOC) == _COMPACT
    # Whitespace inside strings, escaped quotes and backslashes are content.
    raw = '{ "a" : "x \\" y  \\\\" , "b":[ 1.10 ,\n\t-0 ] }'
    assert compact_json_text(raw) == '{"a":"x \\" y  \\\\","b":[1.10,-0]}'
    assert json.loads(compact_json_text(raw)) == json.loads(raw)


@pytest.mark.parametrize("text", ["1.10", "[1.10, 1e2]", '{"k": 2.50}', '"  "', '""'])
def test_json_transform_writes_the_source_tokens(text):
    from services.transform_engine import apply_transform
    from services.value_serializer import compact_json_text

    out, err = apply_transform(text, "json")
    assert err is None
    assert out == compact_json_text(text)


def _jsonb_source(cur, table: str) -> None:
    cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, doc JSONB, note TEXT)")
    for i, (doc, note) in _ROWS.items():
        cur.execute(f"INSERT INTO {table} VALUES (%s, %s, %s)", (i, doc, note))  # nosec B608


def test_qa_case_jsonb_numeric_scale_survives_pg_to_pg(client):
    """QA D192: jsonb ``1.10`` arrived as ``1.1`` on PG -> PG with a green checksum."""
    pg = r.pg_conn()
    token = uuid.uuid4().hex[:8]
    table, dest_table = f"d192_jb_{token}", f"d192_jb_{token}_out"
    cur = pg.cursor()
    try:
        _jsonb_source(cur, table)
        cur.execute(f"SELECT id, doc::text FROM {table}")  # nosec B608
        want = {int(a): b for a, b in cur.fetchall()}
        src_id, dst_id = r.connectors(token, "postgresql")
        for _ in range(2):
            job = r.run(
                client, source_connector_id=src_id, source_table=table,
                dest_connector_id=dst_id, dest_table=dest_table,
                sync_mode="full_refresh_overwrite", primary_key="id",
            )
            assert (job.get("reconciliation") or {}).get("passed") is not False
            out = _read("postgresql", dest_table)
            assert {i: v[0] for i, v in out.items()} == want  # jsonb::text, byte-equal
            assert "1.10" in out[1][0] and "123.4500" in out[1][0]
            assert {i: v[1] for i, v in out.items()} == {i: v[1] for i, v in _ROWS.items()}
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        r.drop("postgresql", dest_table)


def test_gate8_fails_a_write_that_drops_json_numeric_scale(client, monkeypatch):
    """The checksum must see ``1.10`` -> ``1.1``: re-introduce the old re-dump in
    the writer only, and the job must fail verification instead of reporting a
    match (QA: "checksums report a match")."""
    import services.value_serializer as vs

    def _old_redump(text: str) -> str:  # the pre-fix write: parse + dump
        return vs.json_dumps_exact_numbers(vs.json_loads_exact(text), separators=(",", ":"))

    pg = r.pg_conn()
    token = uuid.uuid4().hex[:8]
    table, dest_table = f"d192_g8_{token}", f"d192_g8_{token}_out"
    cur = pg.cursor()
    try:
        _jsonb_source(cur, table)
        src_id, dst_id = r.connectors(token, "postgresql")
        monkeypatch.setattr(vs, "compact_json_text", _old_redump)
        staged, _plan, blockers = r.stage(
            source_connector_id=src_id, source_table=table,
            dest_connector_id=dst_id, dest_table=dest_table,
            sync_mode="full_refresh_overwrite", primary_key="id",
        )
        assert staged.success and not blockers, (staged.error, blockers)
        resp = client.post(
            "/api/v1/copilot/confirm",
            json={"ack_id": staged.output["ack_id"], "reason": "d192-gate8"},
        )
        assert resp.status_code == 200 and resp.json().get("ok") is True, resp.text
        job = r.wait_job(str(resp.json().get("job_id") or ""))
        assert str(job.get("status")) == "failed", job.get("status")
        assert "verification" in str(job.get("error") or "").lower() or "mismatch" in str(
            job.get("error") or ""
        ).lower(), job.get("error")
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        r.drop("postgresql", dest_table)


@pytest.mark.parametrize("engine,ddl", [("postgresql", "JSONB"), ("postgresql", "JSON"),
                                        ("sqlserver", "NVARCHAR(MAX)"), ("mysql", "LONGTEXT")])
def test_fingerprint_keeps_json_numeric_scale_where_the_engine_does(engine, ddl):
    from services.reconciliation import fingerprint_for_reconcile as fp

    a, b = '{"a": 1.10}', '{"a":1.1}'
    assert fp(a, ddl_type=ddl, engine=engine) != fp(b, ddl_type=ddl, engine=engine)
    # jsonb's own rewrites are the same stored value.
    assert fp('[1e2, -0.0, 2.50e1]', ddl_type=ddl, engine=engine) == fp(
        "[100, 0.0, 25.0]", ddl_type=ddl, engine=engine
    )


def test_fingerprint_stays_value_equal_where_the_engine_stores_doubles():
    from services.reconciliation import fingerprint_for_reconcile as fp

    assert fp('{"a": 1.10}', ddl_type="JSON", engine="mysql") == fp(
        '{"a": 1.1}', ddl_type="JSON", engine="mysql"
    )
