"""Inferred-delete (full_refresh_mirror) transfer tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _csv_bytes(rows: list[tuple[str, str]]) -> bytes:
    lines = ["id,name"]
    for rid, name in rows:
        lines.append(f"{rid},{name}")
    return "\n".join(lines).encode("utf-8")


def _active_rows(db_path: Path) -> list[tuple]:
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute("SELECT id, name, _deleted FROM mirror_test ORDER BY id")
        return cur.fetchall()
    finally:
        conn.close()


def test_file_to_sqlite_mirror_soft_deletes_and_reactivates(tmp_path: Path) -> None:
    db_path = tmp_path / "mirror.db"
    engine = UniversalTransferEngine()

    # First snapshot: ids 1, 2, 3
    request1 = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=_csv_bytes([("1", "Alice"), ("2", "Bob"), ("3", "Charlie")]),
        source_filename="snapshot1.csv",
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            connection_string=str(db_path),
            table="mirror_test",
        ),
        sync_mode="full_refresh_mirror",
        skip_preflight=True,
        validation_mode="strict",
    )
    result1 = engine.execute_tracked(request1, f"mirror_01_{os.getpid():06d}")
    assert result1.success, result1.error

    rows1 = _active_rows(db_path)
    assert len(rows1) == 3
    assert {str(r[0]) for r in rows1} == {"1", "2", "3"}
    assert all(r[2] in (0, False, None) for r in rows1)

    # Second snapshot: 1 is gone, 2 and 3 updated, 4 is new
    request2 = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=_csv_bytes([("2", "Bob2"), ("3", "Charlie2"), ("4", "Dave")]),
        source_filename="snapshot2.csv",
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            connection_string=str(db_path),
            table="mirror_test",
        ),
        sync_mode="full_refresh_mirror",
        skip_preflight=True,
        validation_mode="strict",
    )
    result2 = engine.execute_tracked(request2, f"mirror_02_{os.getpid():06d}")
    assert result2.success, result2.error

    conn = __import__("sqlite3").connect(str(db_path))
    try:
        cur = conn.execute("SELECT id, name, _deleted FROM mirror_test ORDER BY id")
        all_rows = cur.fetchall()
    finally:
        conn.close()

    active = [r for r in all_rows if r[2] in (0, False, None)]
    deleted = [r for r in all_rows if r[2] not in (0, False, None)]

    assert len(active) == 3, active
    assert {str(r[0]) for r in active} == {"2", "3", "4"}
    assert {r[1] for r in active} == {"Bob2", "Charlie2", "Dave"}
    assert len(deleted) == 1
    assert str(deleted[0][0]) == "1"

    ledger = result2.row_accounting or {}
    assert ledger.get("conservation_kind") == "mirror", ledger
    assert ledger.get("balanced") is True, ledger
    assert ledger.get("active_count") == 3, ledger
    assert ledger.get("rows_written") == 3, ledger
    assert ledger.get("dest_count") == 4, ledger
    assert ledger.get("inferred_deletes") == 1, ledger
    assert ledger.get("reactivated") == 0, ledger
    assert ledger.get("rows_written_source") == "gate8_dest_active_readback", ledger

    # Bringing id 1 back must land as active AND census a this-run reactivate.
    # Upsert does not own ``_deleted`` (native SET excluded; no-unique fallback
    # is UPDATE+INSERT, never delete+insert DEFAULT). Dest-after currently
    # deleted ∩ snapshot equals dest-before tombstone ∩ snapshot.
    request3 = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=_csv_bytes([("1", "Alice"), ("2", "Bob2"), ("3", "Charlie2"), ("4", "Dave")]),
        source_filename="snapshot3.csv",
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            connection_string=str(db_path),
            table="mirror_test",
        ),
        sync_mode="full_refresh_mirror",
        skip_preflight=True,
        validation_mode="strict",
    )
    result3 = engine.execute_tracked(request3, f"mirror_03_{os.getpid():06d}")
    assert result3.success, result3.error
    rows3 = _active_rows(db_path)
    assert {str(r[0]) for r in rows3} == {"1", "2", "3", "4"}
    assert all(r[2] in (0, False, None) for r in rows3)
    ledger3 = result3.row_accounting or {}
    assert ledger3.get("inferred_deletes") == 0, ledger3
    assert ledger3.get("reactivated") == 1, ledger3
    assert ledger3.get("active_count") == 4, ledger3


def test_staging_inferred_deletes_count_transitions_not_already_active(tmp_path: Path) -> None:
    """Dest-engine COUNT of transitions: already-active keys in staging are not reactivates."""
    import sqlalchemy as sa

    from services.mirror_engine import apply_inferred_deletes_via_staging

    db = tmp_path / "mirror_staging.db"
    engine = sa.create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        conn.execute(sa.text("CREATE TABLE dst (id TEXT, name TEXT, _deleted INTEGER)"))
        conn.execute(sa.text("CREATE TABLE stg (id TEXT, name TEXT)"))
        conn.execute(
            sa.text(
                "INSERT INTO dst (id, name, _deleted) VALUES "
                "('1','a',0),('2','b',0),('3','c',1)"
            )
        )
        conn.execute(
            sa.text("INSERT INTO stg (id, name) VALUES ('1','a'),('3','c'),('4','d')")
        )
        conn.commit()
        census = apply_inferred_deletes_via_staging(
            conn, "dst", "stg", ["id"], dialect="sqlite"
        )
        conn.commit()
        rows = {
            str(r[0]): int(r[1])
            for r in conn.execute(sa.text("SELECT id, _deleted FROM dst")).fetchall()
        }
    assert census["reactivated"] == 1
    assert census["soft_deleted"] == 1
    assert census["physical_rows"] == 3
    assert census["rows_scanned"] == 3
    assert rows == {"1": 0, "2": 1, "3": 0}


def test_buffered_mirror_census_is_dest_engine_staging_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Buffered this-run census is dest-engine COUNT, not dest scan + rowcount."""
    import sqlite3

    from src.transfer.models import EndpointConfig
    from services.mirror_engine import apply_inferred_soft_deletes

    db = tmp_path / "buffered_mirror.db"
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "CREATE TABLE dst (id TEXT, name TEXT, _deleted INTEGER DEFAULT 0)"
        )
        conn.execute(
            "INSERT INTO dst (id, name, _deleted) VALUES "
            "('1','a',0),('2','b',0),('3','c',1)"
        )
        conn.commit()

    def _no_scan(*_a, **_k):
        raise AssertionError("buffered mirror census must not scan dest rows")

    monkeypatch.setattr(
        "services.reconciliation_api.iter_select_row_dicts", _no_scan
    )
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=str(db),
        table="dst",
    )
    summary = apply_inferred_soft_deletes(
        dest,
        [{"id": "1", "name": "a"}, {"id": "3", "name": "c"}, {"id": "4", "name": "d"}],
        ["id", "name"],
        {"id": "string", "name": "string"},
        [
            {"source": "id", "target": "id", "transform": "direct"},
            {"source": "name", "target": "name", "transform": "direct"},
        ],
        ["id"],
    )
    assert summary["reactivated"] == 1
    assert summary["soft_deleted"] == 1
    assert summary["physical_rows"] == 3
    assert summary["rows_scanned"] == 3
    with sqlite3.connect(str(db)) as conn:
        rows = {
            str(r[0]): int(r[1])
            for r in conn.execute("SELECT id, _deleted FROM dst").fetchall()
        }
        leftovers = [
            n
            for (n,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '_df_mirrorkeys_%'"
            ).fetchall()
        ]
    assert rows == {"1": 0, "2": 1, "3": 0}
    assert leftovers == []
    assert summary["source_key_rows"] == 3


def test_buffered_mirror_census_from_key_spool_with_empty_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After engine spill+clear, inferred-delete streams the keys-only spool."""
    import sqlite3

    from connectors.engine_record_spill import spill_engine_write_records
    from src.transfer.models import EndpointConfig
    from services.mirror_engine import apply_inferred_soft_deletes

    db = tmp_path / "spool_mirror.db"
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "CREATE TABLE dst (id TEXT, name TEXT, _deleted INTEGER DEFAULT 0)"
        )
        conn.execute(
            "INSERT INTO dst (id, name, _deleted) VALUES "
            "('1','a',0),('2','b',0),('3','c',1)"
        )
        conn.commit()

    def _no_scan(*_a, **_k):
        raise AssertionError("buffered mirror census must not scan dest rows")

    monkeypatch.setattr(
        "services.reconciliation_api.iter_select_row_dicts", _no_scan
    )
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=str(db),
        table="dst",
    )
    records = [
        {"id": "1", "name": "a"},
        {"id": "3", "name": "c"},
        {"id": "4", "name": "d"},
    ]
    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "name", "target": "name", "transform": "direct"},
    ]
    spill = spill_engine_write_records(
        records,
        ["id", "name"],
        mappings,
        extra={},
        collect_pk_sources=["id"],
        clear_records=True,
    )
    try:
        assert records == []
        summary = apply_inferred_soft_deletes(
            dest,
            records,
            ["id", "name"],
            {"id": "string", "name": "string"},
            mappings,
            ["id"],
            source_spool=spill.spool,
            source_key_spool=spill.key_spool,
            pk_sources=spill.pk_sources,
        )
    finally:
        spill.close()
    assert summary["reactivated"] == 1
    assert summary["soft_deleted"] == 1
    assert summary["physical_rows"] == 3
    assert summary["source_key_rows"] == 3
    with sqlite3.connect(str(db)) as conn:
        rows = {
            str(r[0]): int(r[1])
            for r in conn.execute("SELECT id, _deleted FROM dst").fetchall()
        }
    assert rows == {"1": 0, "2": 1, "3": 0}


def test_mirror_fail_closed_on_empty_key_set(tmp_path: Path) -> None:
    import sqlite3

    from src.transfer.models import EndpointConfig
    from services.mirror_engine import apply_inferred_soft_deletes

    db = tmp_path / "empty_keys.db"
    with sqlite3.connect(str(db)) as conn:
        conn.execute("CREATE TABLE dst (id TEXT, name TEXT)")
        conn.commit()
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=str(db),
        table="dst",
    )
    with pytest.raises(ValueError, match="non-empty source key set"):
        apply_inferred_soft_deletes(
            dest,
            [{"id": None, "name": "x"}],
            ["id", "name"],
            {"id": "string", "name": "string"},
            [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
            ["id"],
        )


def test_lattice_probe_uses_physical_table_not_mapped_write_table(
    tmp_path: Path,
) -> None:
    """Mapped write Table is id,name; physical dest has _deleted after ALTER."""
    import sqlalchemy as sa

    from services.mirror_engine import lattice_columns_on_table

    db = tmp_path / "lattice_probe.db"
    engine = sa.create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        conn.execute(sa.text("CREATE TABLE dst (id TEXT, name TEXT)"))
        conn.execute(sa.text("ALTER TABLE dst ADD COLUMN _deleted INTEGER DEFAULT 0"))
        conn.commit()
        mapped = sa.Table(
            "dst",
            sa.MetaData(),
            sa.Column("id", sa.Text),
            sa.Column("name", sa.Text),
        )
        found = lattice_columns_on_table(conn, mapped)
    assert found == ("_deleted",)


def test_strip_lattice_from_upsert_drops_deleted_from_set_and_insert() -> None:
    from services.mirror_engine import strip_lattice_from_upsert

    rows, update_cols, target_cols = strip_lattice_from_upsert(
        [{"id": "1", "name": "a", "_deleted": 0}],
        ["name", "_deleted"],
        ["id", "name", "_deleted"],
        ("_deleted",),
    )
    assert rows == [{"id": "1", "name": "a"}]
    assert update_cols == ["name"]
    assert target_cols == ["id", "name"]


def test_upsert_without_unique_preserves_tombstone(tmp_path: Path) -> None:
    """No unique index → portable UPDATE+INSERT, never delete+insert DEFAULT.

    The write Table is Map columns only (id, name). Physical dest has
    ``_deleted`` from the inferred-delete ALTER. Probe the physical table.
    """
    import sqlalchemy as sa

    from connectors.generic_sql import _upsert_batch

    db = tmp_path / "lattice_fallback.db"
    engine = sa.create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                "CREATE TABLE dst (id TEXT, name TEXT, _deleted INTEGER DEFAULT 0)"
            )
        )
        conn.execute(
            sa.text("INSERT INTO dst (id, name, _deleted) VALUES ('1', 'a', 1)")
        )
        conn.commit()
        mapped = sa.Table(
            "dst",
            sa.MetaData(),
            sa.Column("id", sa.Text),
            sa.Column("name", sa.Text),
        )
        written = _upsert_batch(
            conn,
            mapped,
            [{"id": "1", "name": "b"}, {"id": "2", "name": "c"}],
            ["id"],
            ["id", "name"],
            "sqlite",
        )
        conn.commit()
        rows = {
            str(r[0]): (r[1], int(r[2]))
            for r in conn.execute(sa.text("SELECT id, name, _deleted FROM dst")).fetchall()
        }
    assert written == 2
    assert rows["1"] == ("b", 1)
    assert rows["2"] == ("c", 0)


def test_native_upsert_does_not_set_lattice_when_unique_exists(tmp_path: Path) -> None:
    """ON CONFLICT SET must not include ``_deleted`` even when the payload does."""
    import sqlalchemy as sa

    from connectors.generic_sql import _upsert_batch

    db = tmp_path / "lattice_native.db"
    engine = sa.create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                "CREATE TABLE dst ("
                "id TEXT PRIMARY KEY, name TEXT, _deleted INTEGER DEFAULT 0)"
            )
        )
        conn.execute(
            sa.text("INSERT INTO dst (id, name, _deleted) VALUES ('1', 'a', 1)")
        )
        conn.commit()
        table = sa.Table("dst", sa.MetaData(), autoload_with=conn)
        _upsert_batch(
            conn,
            table,
            [{"id": "1", "name": "b", "_deleted": 0}],
            ["id"],
            ["id", "name", "_deleted"],
            "sqlite",
        )
        conn.commit()
        row = conn.execute(
            sa.text("SELECT name, _deleted FROM dst WHERE id = '1'")
        ).fetchone()
    assert row is not None
    assert row[0] == "b"
    assert int(row[1]) == 1


def test_upsert_set_columns_excludes_lattice() -> None:
    from services.mirror_engine import upsert_insert_columns, upsert_set_columns

    assert upsert_set_columns(["id", "name", "_deleted"], ["id"]) == ["name"]
    assert upsert_insert_columns(["id", "name", "_deleted"]) == ["id", "name"]
    assert upsert_set_columns(["id", "name"], ["id"], ("_deleted",)) == ["name"]


def test_mysql_on_duplicate_sql_does_not_set_lattice() -> None:
    """Dedicated MySQL writer must not SET dest-owned ``_deleted``."""
    from connectors.mysql_writer import _mysql_insert_sql

    sql = _mysql_insert_sql(
        table_q="`dst`",
        target_cols=["id", "name", "_deleted"],
        write_mode="upsert",
        conflict_columns=["id"],
    )
    insert, _, dup = sql.partition("ON DUPLICATE KEY UPDATE")
    assert dup, sql
    assert "`name`=VALUES(`name`)" in dup.replace(" ", "") or "`name`=VALUES(`name`)" in dup
    assert "_deleted" not in dup


def test_mysql_writer_upsert_does_not_undelete_lattice() -> None:
    """Live MariaDB/MySQL: payload ``_deleted=0`` must not revive a tombstone."""
    import socket
    import uuid

    import pytest

    try:
        with socket.create_connection(("127.0.0.1", 3306), timeout=1):
            pass
    except OSError:
        pytest.skip("MySQL/MariaDB not reachable on localhost:3306")

    import pymysql

    from connectors.mysql_writer import write_mapped_rows

    table = "mirror_lat_my_" + uuid.uuid4().hex[:8]
    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        database="dataflow",
        user="dataflow",
        password="dataflow",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE `{table}` ("
                "id VARCHAR(32) PRIMARY KEY, name VARCHAR(64), "
                "_deleted TINYINT NOT NULL DEFAULT 0)"
            )
            cur.execute(
                f"INSERT INTO `{table}` (id, name, _deleted) VALUES ('1', 'a', 1)"
            )
        result = write_mapped_rows(
            host="127.0.0.1",
            port=3306,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="dataflow",
            connection_string="",
            ssl=False,
            table_name=table,
            headers=["id", "name", "_deleted"],
            data_rows=[["1", "b", "0"], ["2", "c", "0"]],
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.95},
                {"source": "name", "target": "name", "confidence": 0.95},
                {"source": "_deleted", "target": "_deleted", "confidence": 0.95},
            ],
            column_types={"id": "string", "name": "string", "_deleted": "integer"},
            create_table=False,
            write_mode="upsert",
            conflict_columns=["id"],
        )
        assert result.ok, result.error
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, name, _deleted FROM `{table}` ORDER BY id")
            rows = {str(r[0]): (r[1], int(r[2])) for r in cur.fetchall()}
        assert rows["1"] == ("b", 1)
        assert rows["2"] == ("c", 0)
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        except Exception:
            pass
        conn.close()


def test_postgresql_writer_upsert_does_not_undelete_lattice() -> None:
    """Live PostgreSQL: ON CONFLICT SET must not revive a tombstone."""
    import socket
    import uuid

    import pytest

    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL not reachable on localhost:5432")

    import psycopg2

    from connectors.postgresql_writer import write_mapped_rows

    table = "mirror_lat_pg_" + uuid.uuid4().hex[:8]
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE public."{table}" ('
                "id TEXT PRIMARY KEY, name TEXT, "
                "_deleted INTEGER NOT NULL DEFAULT 0)"
            )
            cur.execute(
                f'INSERT INTO public."{table}" (id, name, _deleted) '
                "VALUES ('1', 'a', 1)"
            )
        result = write_mapped_rows(
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            connection_string="",
            ssl=False,
            table_name=table,
            headers=["id", "name", "_deleted"],
            data_rows=[["1", "b", "0"], ["2", "c", "0"]],
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.95},
                {"source": "name", "target": "name", "confidence": 0.95},
                {"source": "_deleted", "target": "_deleted", "confidence": 0.95},
            ],
            column_types={"id": "string", "name": "string", "_deleted": "integer"},
            create_table=False,
            write_mode="upsert",
            conflict_columns=["id"],
        )
        assert result.ok, result.error
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT id, name, _deleted FROM public."{table}" ORDER BY id'
            )
            rows = {str(r[0]): (r[1], int(r[2])) for r in cur.fetchall()}
        assert rows["1"] == ("b", 1)
        assert rows["2"] == ("c", 0)
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        except Exception:
            pass
        conn.close()
