"""D349 (non-PG) on the QA route into Oracle: every flattened child stays protected.

``column_transforms {payload: <tf>}`` on a MongoDB field the reader flattens
into ``payload_k`` / ``payload_nested`` / ``payload_nested_a`` / ``payload_s``.
On Mongo -> PostgreSQL and Mongo -> MySQL every child inherits the protection
(tests/test_d349_flatten_children_inherit_protection.py,
tests/test_p2_d349_d147_mongo_to_mysql_route.py). The story asked for Oracle
too, where the DDL and writer paths differ; it stayed "needs live retest on
Oracle" until the oracle-live CI job started an Oracle Database.

Live MongoDB + Oracle through the strict plan -> confirm route; skipped when
either is absent (the oracle-live job starts both).
"""

from __future__ import annotations

import json
import socket
import uuid

import pytest

# Build Settings before ``transfer_client`` blanks DATAFLOW_SEED_DEMO.
import services.config  # noqa: F401,E402
from services.brand_env import getenv_brand
from tests.test_d190_d194_d197_declared_string_types_e2e import (
    _mongo_source,
    _run,
    transfer_client,  # noqa: F401 - fixture
)
from tests.test_d349_flatten_children_inherit_protection import (
    _CHILDREN,
    _assert_no_clear_payload,
    _docs,
    mongo_coll,  # noqa: F401 - fixture
)

pytestmark = pytest.mark.timeout(300)


def _ora_cfg() -> dict:
    return {
        "host": getenv_brand("ORACLE_HOST", "127.0.0.1") or "127.0.0.1",
        "port": int(getenv_brand("ORACLE_PORT", "1521") or 1521),
        "service": getenv_brand("ORACLE_SERVICE") or getenv_brand("ORACLE_DATABASE") or "FREEPDB1",
        "user": getenv_brand("ORACLE_USER", "") or "",
        "password": getenv_brand("ORACLE_PASSWORD", "") or "",
        "schema": (getenv_brand("ORACLE_SCHEMA") or getenv_brand("ORACLE_USER", "") or "").upper(),
    }


def _ora():
    oracledb = pytest.importorskip("oracledb")
    c = _ora_cfg()
    if not (c["user"] and c["password"]):
        pytest.skip("Oracle not configured (DATAFLOW_ORACLE_USER / DATAFLOW_ORACLE_PASSWORD)")
    try:
        with socket.create_connection((c["host"], c["port"]), timeout=2):
            pass
        conn = oracledb.connect(
            user=c["user"], password=c["password"], dsn=f"{c['host']}:{c['port']}/{c['service']}"
        )
    except Exception as exc:  # environmental
        pytest.skip(f"Oracle not reachable: {type(exc).__name__}")
    conn.autocommit = True
    return conn, c


def _sql(sql: str, params=None) -> list[tuple]:
    conn, _c = _ora()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            if cur.description is None:
                return []
            return [tuple(_plain(v) for v in row) for row in cur.fetchall()]
    finally:
        conn.close()


def _plain(value):
    return value.read() if hasattr(value, "read") else value


def _ora_connector(tag: str) -> str:
    from services.connector_store import create_connector

    conn, c = _ora()
    conn.close()
    return str(
        create_connector(
            {
                "name": f"{tag}-ora-{uuid.uuid4().hex[:6]}", "type": "oracle",
                "role": "destination", "host": c["host"], "port": c["port"],
                "database": c["service"], "username": c["user"],
                "password": c["password"], "schema": c["schema"],
                "ssl": False, "workspace_id": "",
            }
        ).id
    )


def _stored_table(table: str) -> str:
    """The table's name as Oracle stores it (the product may quote the source case)."""
    rows = _sql(
        "SELECT table_name FROM user_tables WHERE UPPER(table_name) = UPPER(:t)", {"t": table}
    )
    assert rows, f"{table} was not created"
    return str(rows[0][0])


def _columns(stored: str) -> dict[str, tuple[str, str]]:
    """lower(column) -> (stored column name, data_type)."""
    return {
        str(r[0]).lower(): (str(r[0]), str(r[1]).upper())
        for r in _sql(
            "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :t",
            {"t": stored},
        )
    }


def _rows(stored: str, cols: dict[str, tuple[str, str]], names: list[str]) -> list[dict]:
    quoted = ", ".join('"' + cols[n][0].replace('"', '""') + '"' for n in names)
    id_col = '"' + cols["_id"][0] + '"'
    out = []
    for r in _sql(f'SELECT {quoted} FROM "{stored}" ORDER BY {id_col}'):  # nosec B608 - test-owned
        row = dict(zip(names, r))
        row["_id"] = int(row["_id"])
        out.append(row)
    return out


def _drop(table: str) -> None:
    for name in (table, f"{table}_df_quarantine"):
        for stored in {name, name.upper()}:
            try:
                _sql(f'DROP TABLE "{stored}" PURGE')  # nosec B608 - test-owned name
            except Exception:  # noqa: BLE001 - absent is fine
                pass


def _text(value):
    """A JSON carrier may hold the redaction token as a JSON string."""
    if isinstance(value, str) and value.startswith('"') and value.endswith('"'):
        return json.loads(value)
    return value


@pytest.mark.parametrize("parent_tf", ["redact", "hash_pii", "mask_pii", "omit"])
def test_mongo_to_oracle_protects_every_flattened_child(
    transfer_client, mongo_coll, parent_tf  # noqa: F811
):
    _ora()[0].close()
    docs = _docs()
    mongo_coll.insert_many([dict(d) for d in docs])
    table = mongo_coll.name + "_ora"
    _drop(table)
    try:
        plan, _job = _run(
            transfer_client,
            source_connector_id=_mongo_source(mongo_coll),
            source_table=mongo_coll.name,
            dest_connector_id=_ora_connector("d349"),
            dest_table=table,
            sync_mode="full_refresh_overwrite",
            column_transforms={"payload": parent_tf},
            pii_acknowledgement={"approved_by": "qa@example.com", "reason": "d349"},
        )
        stored = _stored_table(table)
        cols = _columns(stored)
        if parent_tf == "omit":
            assert not any(c.startswith("payload") and c != "payload_id" for c in cols), cols
            return
        assert set(cols) >= _CHILDREN | {"payload"}, cols
        names = ["_id"] + sorted(c for c in cols if c.startswith("payload"))
        rows = _rows(stored, cols, names)
        assert len(rows) == len(docs)
        _assert_no_clear_payload(rows, docs, parent_tf)
        if parent_tf == "redact":
            for row in rows:
                for col in _CHILDREN | {"payload"}:
                    assert _text(row[col]) == "<redacted>", (col, row[col])
        inherited = {i["column"] for i in plan.get("inherited_protection") or []}
        assert inherited == _CHILDREN
    finally:
        _drop(table)
