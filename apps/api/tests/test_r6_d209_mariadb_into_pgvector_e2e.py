"""D209 (verification, MariaDB source) — pgvector destination honours its plan.

QA (production 557f556c): "pgvector as a destination puts columns only inside
the metadata, now also with a MariaDB source". The D209 fix (one destination
contract for plan, preflight, writer and Gate-8) is pinned for a PostgreSQL
source in ``test_d209_pgvector_destination_contract_e2e.py``. This drives the
same route from a real MariaDB table (``start_transfer`` -> Confirm -> job),
run 1 and run 2:

* destination opting into the document layout: the plan names the
  document layout and its columns honestly; every source column lands in
  ``metadata`` with NULL kept as JSON null, and Gate-8 proves each value;
* ``vector_layout=table``: the typed table the plan promised, with every source
  column as a real column and every cell equal (NULL, '', unicode).

With no layout given a new table is typed (owner decision, round 8): see
``test_r8_pgvector_typed_default_mariadb_e2e.py``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from decimal import Decimal

import pytest

from tests.test_d209_pgvector_destination_contract_e2e import (  # noqa: F401
    _canon_type,
    _connector,
    _pg,
    _physical,
    _run,
    _sql,
    client,
)

pymysql = pytest.importorskip("pymysql")

_COLS = ("id", "name", "long_txt", "amount", "seen_at", "flag", "note")
_ROWS = [
    (1, "Zoë ✓", "Ünïcødé long text " + "x" * 40, Decimal("12.50"),
     datetime(2026, 1, 2, 3, 4, 5), 1, None),
    (2, "", None, None, None, 0, ""),
    (3, None, "", Decimal("-0.01"), datetime(2026, 10, 30, 23, 59, 59), None, "  "),
    (4, "日本語", "short", Decimal("1000000.00"), None, 1, "n4"),
]


def _assert_plan_is_the_table(plan: dict, table: str) -> dict:
    """The plan's promised columns are the created columns, name and type.

    PostgreSQL spells ``DECIMAL(p,s)`` as ``numeric(p,s)``: the same type.
    """
    contract = plan.get("destination_contract") or {}
    assert contract, "the plan names no destination contract"

    def canon(declared: str) -> str:
        return _canon_type(declared).replace("decimal(", "numeric(")

    promised = [(c["name"], canon(c["type"])) for c in contract.get("columns") or []]
    assert promised == _physical(table), {"promised": promised, "created": _physical(table)}
    pre = plan.get("preflight") or {}
    assert pre.get("destination_contract") == contract
    assert any(w.get("id") == "pgvector_destination_layout" for w in pre.get("warnings") or [])
    return contract


def _maria():
    pw = os.environ.get("DATAFLOW_TEST_MARIADB_PASSWORD", "")
    port = int(os.environ.get("DATAFLOW_TEST_MARIADB_PORT", "3307"))
    if not pw:
        pytest.skip("MariaDB not configured (DATAFLOW_TEST_MARIADB_*, :3307)")
    try:
        return pymysql.connect(
            host="127.0.0.1", port=port, password=pw, charset="utf8mb4", autocommit=True,
            user=os.environ.get("DATAFLOW_TEST_MARIADB_USER", "dataflow"),
            database=os.environ.get("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
            connect_timeout=3,
        ), port, pw
    except pymysql.err.OperationalError as exc:
        pytest.skip(f"MariaDB not reachable: {exc}")


def _maria_sql(statement: str, params=None) -> None:
    conn, _port, _pw = _maria()
    try:
        conn.cursor().execute(statement, params)
    finally:
        conn.close()


@pytest.fixture
def tables():
    _pg().close()
    conn, port, pw = _maria()
    names = {"src": "d209m_" + uuid.uuid4().hex[:8], "dst": "d209md_" + uuid.uuid4().hex[:8]}
    cur = conn.cursor()
    cur.execute(
        f"CREATE TABLE `{names['src']}` (id BIGINT PRIMARY KEY, name VARCHAR(40) NULL, "
        "long_txt TEXT NULL, amount DECIMAL(12,2) NULL, seen_at DATETIME NULL, "
        "flag TINYINT(1) NULL, note VARCHAR(40) NULL, rev INT NOT NULL DEFAULT 1)"
    )
    for row in _ROWS:
        cur.execute(f"INSERT INTO `{names['src']}` ({', '.join(_COLS)}) VALUES "
                    "(%s, %s, %s, %s, %s, %s, %s)", row)
    conn.close()
    from services.connector_store import create_connector

    names["source"] = str(create_connector({
        "name": "d209m-maria-" + uuid.uuid4().hex[:6], "type": "mariadb", "role": "source",
        "host": "127.0.0.1", "port": port, "password": pw, "ssl": False, "workspace_id": "",
        "database": os.environ.get("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
        "username": os.environ.get("DATAFLOW_TEST_MARIADB_USER", "dataflow"),
    }).id)
    try:
        yield names
    finally:
        _maria_sql(f"DROP TABLE IF EXISTS `{names['src']}`")
        _sql(f"DROP TABLE IF EXISTS {names['dst']}")


def _source_rows(src: str) -> list[tuple]:
    conn, _port, _pw = _maria()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT {', '.join(_COLS)}, rev FROM `{src}` ORDER BY id")
        return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _same(stored, value) -> bool:
    """A metadata JSON value equals the source cell (JSON has no DECIMAL/DATETIME)."""
    if value is None:
        return stored is None
    if isinstance(value, Decimal):
        return Decimal(str(stored)) == value
    if isinstance(value, datetime):
        return datetime.fromisoformat(str(stored).replace("Z", "")).replace(tzinfo=None) == value
    if isinstance(stored, bool):
        return int(stored) == int(value)
    return stored == value


def _assert_documents(src: str, dst: str) -> None:
    rows = _sql(f"SELECT source_id, id, metadata FROM {dst} ORDER BY source_id, chunk_index")
    docs: dict[str, tuple] = {}
    for source_id, doc_id, meta in rows:
        docs.setdefault(str(source_id), (doc_id, meta))
    source = _source_rows(src)
    assert sorted(docs) == sorted(str(r[0]) for r in source)
    for row in source:
        doc_id, meta = docs[str(row[0])]
        assert doc_id == str(row[0])
        for key, value in zip(_COLS + ("rev",), row):
            assert key in meta, (row[0], key, meta)  # NULL kept as JSON null
            assert _same(meta[key], value), (row[0], key, meta[key], value)


def test_document_opt_in_from_mariadb_is_the_document_layout_it_plans(client, tables):
    src, dst = tables["src"], tables["dst"]
    dest = _connector("pgvector", "destination", {"vector_layout": "document"})
    args = dict(source_connector_id=tables["source"], source_table=src,
                dest_connector_id=dest, dest_table=dst)

    plan, job = _run(client, sync_mode="full_refresh_overwrite", **args)
    contract = _assert_plan_is_the_table(plan, dst)
    assert (contract["layout"], contract["layout_reason"]) == ("document", "declared")
    assert [f["key"] for f in contract["metadata_fields"]] == list(_COLS) + ["rev"]
    _assert_documents(src, dst)
    recon = job.get("reconciliation") or {}
    proof = recon.get("document_value_proof") or {}
    assert recon.get("passed") is True and proof.get("passed") is True, recon
    assert proof.get("unmatched_source_rows") == 0, proof

    _maria_sql(f"UPDATE `{src}` SET name = NULL, note = 'é', rev = 2 WHERE id = 4")
    plan2, job2 = _run(client, sync_mode="incremental_upsert", primary_key="id",
                       cursor_column="rev", cursor_semantics="monotonic_sequence", **args)
    contract2 = _assert_plan_is_the_table(plan2, dst)
    assert contract2["layout"] == "document"
    _assert_documents(src, dst)
    assert ((job2.get("reconciliation") or {}).get("document_value_proof") or {}).get("passed")


def test_vector_layout_table_from_mariadb_creates_every_column(client, tables):
    src, dst = tables["src"], tables["dst"]
    dest = _connector("pgvector", "destination", {"vector_layout": "table"})
    args = dict(source_connector_id=tables["source"], source_table=src,
                dest_connector_id=dest, dest_table=dst)

    def check() -> None:
        physical = [name for name, _t in _physical(dst)]
        for col in _COLS + ("rev",):
            assert col in physical, physical
        got = _sql(f"SELECT {', '.join(_COLS)}, rev FROM {dst} ORDER BY id")
        want = _source_rows(src)
        assert len(got) == len(want)
        for g, w in zip(got, want):
            for col, gv, wv in zip(_COLS + ("rev",), g, w):
                assert _same(gv, wv), (w[0], col, gv, wv)

    plan, _job = _run(client, sync_mode="full_refresh_overwrite", **args)
    contract = _assert_plan_is_the_table(plan, dst)
    assert contract["layout"] == "table"
    check()

    _maria_sql(f"UPDATE `{src}` SET long_txt = NULL, amount = 3.10, rev = 2 WHERE id = 1")
    _maria_sql(f"INSERT INTO `{src}` (id, rev) VALUES (5, 2)")
    plan2, _job2 = _run(client, sync_mode="incremental_upsert", primary_key="id",
                        cursor_column="rev", cursor_semantics="monotonic_sequence", **args)
    _assert_plan_is_the_table(plan2, dst)
    check()
