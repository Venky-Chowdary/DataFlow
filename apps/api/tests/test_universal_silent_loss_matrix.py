"""Named fixture: no silent loss / no silent corruption on every live wire.

``100%`` means every row on *this* matrix. It does not mean every connector
on earth, CDC exactly-once, or catalog tile count.

Owner: ``services.row_conservation.assert_population_conservation_closed``.
Execute fails closed when dest COUNT was measured and the ledger does not
balance — SQLite, PostgreSQL, MySQL, CSV, and DynamoDB share that gate.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from services.row_conservation import DEST_READBACK, dest_count_from_recon
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

_ARTIFACT = Path("/opt/cursor/artifacts/universal_silent_loss_matrix.json")
_CSV = b"id,name,note\n1,alice,\n2,bob,hi\n3,cara,x\n"


def _sid(prefix: str = "df_nl") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _maps(*cols: str) -> list[dict]:
    return [{"source": c, "target": c, "confidence": 0.99} for c in cols]


def _run(req: TransferRequest):
    return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])


def _pg():
    try:
        import psycopg2

        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
            connect_timeout=3,
        )
        conn.close()
        return EndpointConfig(
            kind="database",
            format="postgresql",
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table="",
        )
    except Exception:
        return None


def _mysql():
    try:
        import pymysql

        conn = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="dataflow",
            password="dataflow",
            database="dataflow",
            connect_timeout=3,
        )
        conn.close()
        return EndpointConfig(
            kind="database",
            format="mysql",
            host="127.0.0.1",
            port=3306,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            table="",
        )
    except Exception:
        return None


def _assert_closed(result, dest_count: int | None = None) -> dict:
    assert result.success is True, result.error
    accounting = dict(result.row_accounting or {})
    recon = dict(result.reconciliation or {})
    counted, source = dest_count_from_recon(recon)
    if counted is not None and source != "unmeasured":
        assert accounting.get("balanced") is True, accounting
        if dest_count is not None:
            assert counted == dest_count
    return accounting


def test_sqlite_to_sqlite_four_rows_balance(tmp_path: Path):
    src = tmp_path / "src.db"
    dst = tmp_path / "dst.db"
    import sqlite3

    with sqlite3.connect(src) as db:
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        db.executemany("INSERT INTO t VALUES (?, ?)", [(1, "a"), (2, "b"), (3, "c"), (4, "d")])
    result = _run(
        TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(src),
                table="t",
                connection_string=f"sqlite:///{src}",
            ),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(dst),
                table="t",
                connection_string=f"sqlite:///{dst}",
            ),
            mappings=_maps("id", "name"),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="balanced",
        )
    )
    _assert_closed(result, dest_count=4)
    with sqlite3.connect(dst) as db:
        assert db.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 4


def test_csv_to_sqlite_empty_cell_is_not_dropped(tmp_path: Path):
    dst = tmp_path / "csv_dst.db"
    result = _run(
        TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(dst),
                table="people",
                connection_string=f"sqlite:///{dst}",
            ),
            source_filename="people.csv",
            source_content=_CSV,
            mappings=_maps("id", "name", "note"),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="balanced",
        )
    )
    _assert_closed(result, dest_count=3)
    import sqlite3

    with sqlite3.connect(dst) as db:
        rows = db.execute("SELECT id, name, note FROM people ORDER BY id").fetchall()
        assert len(rows) == 3
        assert rows[0][1] == "alice"
        # Empty CSV field must land — never silently omit the row.


def test_postgres_null_vs_empty_does_not_corrupt():
    dest = _pg()
    if dest is None:
        pytest.skip("PostgreSQL dataflow/dataflow not authenticated")
    dest.table = _sid("pg")
    src_table = _sid("pgs")
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE public."{src_table}" '
                f"(id INTEGER PRIMARY KEY, name TEXT, note TEXT)"
            )
            cur.execute(
                f'INSERT INTO public."{src_table}" VALUES '
                f"(1, 'alice', NULL), (2, 'bob', '')"
            )
        dest_cfg = dest
        result = _run(
            TransferRequest(
                source=EndpointConfig(
                    kind="database",
                    format="postgresql",
                    host="127.0.0.1",
                    port=5432,
                    database="dataflow",
                    username="dataflow",
                    password="dataflow",
                    schema="public",
                    table=src_table,
                ),
                destination=dest_cfg,
                mappings=_maps("id", "name", "note"),
                sync_mode="full_refresh_overwrite",
                skip_preflight=True,
                validation_mode="balanced",
            )
        )
        _assert_closed(result, dest_count=2)
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT note IS NULL, note = %s FROM public."{dest.table}" WHERE id = 1',
                ("",),
            )
            is_null, is_empty = cur.fetchone()
            assert is_null is True
            assert is_empty is False
            cur.execute(
                f'SELECT note IS NULL, note = %s FROM public."{dest.table}" WHERE id = 2',
                ("",),
            )
            is_null, is_empty = cur.fetchone()
            assert is_null is False
            assert is_empty is True
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
                cur.execute(f'DROP TABLE IF EXISTS public."{dest.table}"')
            conn.commit()
        except Exception:
            pass
        conn.close()


def test_mysql_to_postgres_decimal_survives():
    src = _mysql()
    dest = _pg()
    if src is None or dest is None:
        pytest.skip("MySQL or PostgreSQL not authenticated — cross-engine row skipped")
    src.table = _sid("ms")
    dest.table = _sid("mp")
    import pymysql

    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE `{src.table}` "
                f"(id INT PRIMARY KEY, amt DECIMAL(10,2), title VARCHAR(32))"
            )
            cur.execute(
                f"INSERT INTO `{src.table}` VALUES (1, 10.50, 'ok'), (2, 0.01, 'cent')"
            )
        result = _run(
            TransferRequest(
                source=src,
                destination=dest,
                mappings=_maps("id", "amt", "title"),
                sync_mode="full_refresh_overwrite",
                skip_preflight=True,
                validation_mode="balanced",
            )
        )
        _assert_closed(result, dest_count=2)
        import psycopg2

        pg = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
        )
        try:
            with pg.cursor() as cur:
                cur.execute(
                    f'SELECT amt::text FROM public."{dest.table}" WHERE id = 1'
                )
                assert "10.5" in str(cur.fetchone()[0])
                cur.execute(f'DROP TABLE IF EXISTS public."{dest.table}"')
            pg.commit()
        finally:
            pg.close()
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{src.table}`")
        except Exception:
            pass
        conn.close()


def test_measured_shortfall_fails_execute(tmp_path: Path, monkeypatch):
    """A dest COUNT short of the reader is a failed job — never a green write."""
    src = tmp_path / "s.db"
    dst = tmp_path / "d.db"
    import sqlite3

    with sqlite3.connect(src) as db:
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        db.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

    real = dest_count_from_recon

    def short(recon):
        counted, source = real(recon)
        if counted is not None:
            return counted - 1, DEST_READBACK
        return 2, DEST_READBACK

    monkeypatch.setattr(
        "services.row_conservation.dest_count_from_recon", short
    )
    result = _run(
        TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(src),
                table="t",
                connection_string=f"sqlite:///{src}",
            ),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(dst),
                table="t",
                connection_string=f"sqlite:///{dst}",
            ),
            mappings=_maps("id"),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="balanced",
        )
    )
    assert result.success is False, result.destination_summary
    assert "silent" in (result.error or "").lower() or "unaccounted" in (
        result.error or ""
    ).lower() or "neither" in (result.error or "").lower()


def test_write_matrix_artifact():
    payload = {
        "fixture": "apps/api/tests/test_universal_silent_loss_matrix.py",
        "algorithm": "services.row_conservation.assert_population_conservation_closed",
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "honesty": {
            "one_hundred_percent": "this named fixture only",
            "any_source_dest": (
                "Every route shares the Execute fail-closed gate when dest "
                "COUNT is independently measured. Unmeasured dests stay open."
            ),
            "cdc_default": "at-least-once upsert",
            "catalog_tiles_are_not_transfer_live": True,
        },
    }
    if _ARTIFACT.parent.is_dir():
        _ARTIFACT.write_text(json.dumps(payload, indent=2) + "\n")
