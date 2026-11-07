"""D120 — PostgreSQL -> Snowflake end to end, under real Snowflake semantics.

Two defects made every plain-table Snowflake load fail:

(a) the writer creates folded ``"ID"`` columns while the Gate-8 readers
    quoted the logical ``"id"`` — on Snowflake ``000904 invalid identifier``;
(b) a literal spelling ``__DF_SQL_NULL__`` binds as ``WireLiteral`` and the
    connector refused "Binding data in type (wireliteral) is not supported".

fakesnow binds client-side through the connector's own converter, so (b)
reproduces as-is. It runs on DuckDB, where quoted identifiers are
case-insensitive, so ``snowflake_semantics`` restores Snowflake's rule: a
quoted identifier must match the stored case exactly. The rows also
carry the D106 (NULL in a short VARCHAR) and D109 (sentinel-spelling
literal) arms.
"""

from __future__ import annotations

import re
import uuid
from typing import Any

import psycopg2
import pytest

pytest.importorskip("fakesnow")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

PG_CFG = dict(host="127.0.0.1", port=5432, database="dataflow",
              username="dataflow", password="dataflow")

# A quoted identifier that is not the target of an ``AS`` alias.
_QUOTED = re.compile(r'(?<![Aa][Ss] )"((?:[^"]|"")+)"')


@pytest.fixture()
def snowflake_semantics(monkeypatch):
    """Real-Snowflake rules fakesnow lacks.

    Quoted identifiers are case-sensitive. ``executemany`` reports the batch's
    total rowcount, as the connector does — fakesnow runs one statement per
    parameter set and keeps only the last statement's count.
    """
    import snowflake.connector
    from fakesnow.cursor import FakeSnowflakeCursor

    orig_execute = FakeSnowflakeCursor.execute

    def _strict_miss(token: str, stored: set[str]) -> bool:
        return token not in stored and token.upper() in {s.upper() for s in stored}

    def execute(self, command: str, *args: Any, **kwargs: Any) -> Any:
        # User tables only: fakesnow's own ``_fs_*`` catalog has a lower-case
        # ``name`` column that would flag every ``"NAME"`` reference.
        rows = self._duck_conn.cursor().execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE NOT starts_with(table_catalog, '_fs') "
            "AND NOT starts_with(table_schema, '_fs') "
            "AND lower(table_schema) <> 'information_schema'"
        ).fetchall()
        tables = {str(r[0]) for r in rows}
        columns = {str(r[1]) for r in rows}
        sql = command or ""
        # A CREATE naming a differently-cased table makes a second table on
        # Snowflake rather than failing, so only references are checked.
        creates = sql.lstrip().upper().startswith("CREATE")
        for tok in _QUOTED.findall(sql):
            if _strict_miss(tok, columns):
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"SQL compilation error: invalid identifier '\"{tok}\"'",
                    errno=904,
                    sqlstate="42000",
                )
            if not creates and _strict_miss(tok, tables):
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"SQL compilation error: Object '\"{tok}\"' does not exist "
                    "or not authorized.",
                    errno=2003,
                    sqlstate="42S02",
                )
        return orig_execute(self, command, *args, **kwargs)

    def executemany(self, command: str, seqparams: Any, **kwargs: Any) -> Any:
        total = 0
        for params in seqparams:
            self.execute(command, params, **kwargs)
            total += max(int(self._rowcount or 0), 0)
        self._rowcount = total
        return self

    monkeypatch.setattr(FakeSnowflakeCursor, "execute", execute)
    monkeypatch.setattr(FakeSnowflakeCursor, "executemany", executemany)


@pytest.fixture()
def engine_env(tmp_path, monkeypatch, snowflake_semantics):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.fixture()
def pg_source():
    try:
        conn = psycopg2.connect(
            host=PG_CFG["host"], port=PG_CFG["port"], dbname=PG_CFG["database"],
            user=PG_CFG["username"], password=PG_CFG["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    table = "d120_src_" + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {table} (id INT PRIMARY KEY, code VARCHAR(5), "
            "label TEXT, amt NUMERIC(12,2))"
        )
        cur.execute(
            f"INSERT INTO {table} VALUES "
            "(1, 'ab', 'ok', 10.50), "
            "(2, NULL, '__DF_SQL_NULL__', 20.00), "
            "(3, 'xyz', NULL, NULL)"
        )
    try:
        yield table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def _snowflake_rows(table: str) -> dict[int, tuple]:
    from connectors.snowflake_conn import get_connection

    conn = get_connection(
        account="localhost", username="test", password="test",
        database="dataflow", schema="PUBLIC", warehouse="", connection_string="",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT "ID", "CODE", "LABEL", "AMT" FROM "PUBLIC"."{table.upper()}"'
            )
            return {int(r[0]): tuple(r[1:]) for r in cur.fetchall()}
    finally:
        conn.close()


@pytest.mark.parametrize("sync_mode", ["full_refresh_overwrite", "upsert"])
def test_d120_pg_to_snowflake_plain_table_verifies(engine_env, pg_source, sync_mode):
    dst = "d120_dst_" + uuid.uuid4().hex[:8]
    job_id = "d120" + uuid.uuid4().hex[:16]
    engine_env.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", schema="public",
            table=pg_source, **PG_CFG,
        ),
        destination=EndpointConfig(
            kind="database", format="snowflake", host="localhost", port=443,
            database="dataflow", username="test", password="test",
            schema="public", table=dst,
        ),
        sync_mode=sync_mode,
        stream_contracts=[{
            "name": pg_source, "sync_mode": sync_mode,
            "primary_key": "id", "selected": True,
        }],
    )
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success is True, result.error
    assert result.records_transferred == 3
    recon = result.reconciliation or {}
    assert recon.get("passed") is True, recon
    assert recon.get("source_checksum") == recon.get("target_checksum")
    rows = _snowflake_rows(dst)
    # D109: the literal lands as text; D106: NULL in VARCHAR(5) is NULL.
    assert rows[2][0] is None and rows[2][1] == "__DF_SQL_NULL__"
    assert rows[1][:2] == ("ab", "ok")
    assert rows[3][1] is None and rows[3][2] is None
