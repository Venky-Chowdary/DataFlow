"""D122 — a failed strict upsert into a PG table with a user trigger must
restore the pre-run rows for a non-superuser role, or refuse up front.

The D111 restore suppressed triggers with ``session_replication_role``, which
only a superuser may set. For an ordinary role the restore replayed the backup
through the BEFORE trigger, its verify saw the rewrite, and it rolled back:
the job FAILED with the rejected batch left in place and the original rows
only in ``__dfbak``. The verify also used set ``EXCEPT`` (duplicates collapse)
and raised on ``json`` / ``point`` columns, which have no equality operator.

Live PostgreSQL, a dedicated NOSUPERUSER role:
* owner role — the restore suspends the table's own triggers inside the
  restore transaction and lands the backup, verified as a multiset;
* non-owner role — it cannot suspend them, so the strict keyed run is refused
  before any write.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

ADMIN = dict(host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow")


def _reachable() -> bool:
    try:
        with socket.create_connection((ADMIN["host"], ADMIN["port"]), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _reachable(), reason="PostgreSQL not reachable")


def _sql(sql: str, *, user: str | None = None, password: str | None = None) -> list[tuple]:
    import psycopg2

    params = dict(ADMIN)
    if user:
        params.update(user=user, password=password)
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall()) if cur.description else []
    finally:
        conn.close()


_PRE = [(1, "pre one"), (2, "pre two"), (3, "pre three")]


@pytest.fixture()
def role():
    sfx = uuid.uuid4().hex[:8]
    name, pw = f"d122_r_{sfx}", f"pw_{sfx}"
    _sql(f"CREATE ROLE {name} LOGIN NOSUPERUSER PASSWORD '{pw}'")
    _sql(f"GRANT CONNECT, TEMPORARY, CREATE ON DATABASE dataflow TO {name}")
    _sql(f"GRANT USAGE, CREATE ON SCHEMA public TO {name}")
    yield {"name": name, "pw": pw, "sfx": sfx}
    _sql(f"DROP OWNED BY {name} CASCADE")
    _sql(f"DROP ROLE IF EXISTS {name}")


def _make_table(table: str, sfx: str, *, user: str | None, password: str | None) -> None:
    ddl = f"""
        CREATE TABLE public."{table}" (id BIGINT PRIMARY KEY, name TEXT);
        INSERT INTO public."{table}" VALUES {", ".join(f"({i}, '{n}')" for i, n in _PRE)};
        CREATE FUNCTION public.d122_f_{sfx}() RETURNS trigger AS $$
        BEGIN NEW.name := upper(NEW.name); RETURN NEW; END $$ LANGUAGE plpgsql;
        CREATE TRIGGER d122_t_{sfx} BEFORE INSERT OR UPDATE ON public."{table}"
            FOR EACH ROW EXECUTE FUNCTION public.d122_f_{sfx}();
    """
    _sql(ddl, user=user, password=password)


def _snapshot(table: str) -> list[tuple]:
    return _sql(f'SELECT id, name FROM public."{table}" ORDER BY id')


def _backups(table: str) -> list[tuple]:
    return _sql(
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = 'public' AND table_name LIKE '{table}__dfbak_%'"
    )


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, tmp_path: Path, table: str, user: str, password: str):
    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?)",
            [(1, "new one"), (2, "new two"), (4, "new four")],
        )
    conn.close()
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(
            kind="database", format="postgresql", host=ADMIN["host"], port=ADMIN["port"],
            database="dataflow", username=user, password=password, schema="public", table=table,
        ),
        sync_mode="incremental_upsert",
        stream_contracts=[
            {
                "name": "t",
                "cursor_field": "id",
                "cursor_semantics": "monotonic_sequence",
                "primary_key": ["id"],
                "sync_mode": "incremental_upsert",
                "selected": True,
            }
        ],
        validation_mode="strict",
    )
    job_id = "d122" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def test_owner_role_failed_strict_upsert_restores_pre_run_rows(tmp_path, isolated, role):
    table = f"d122_own_{role['sfx']}"
    _make_table(table, role["sfx"], user=role["name"], password=role["pw"])
    before = _snapshot(table)

    result = _run(isolated, tmp_path, table, role["name"], role["pw"])

    # The trigger rewrote the batch, so the strict proof must fail ...
    assert not result.success
    # ... and the destination is exactly what it was, triggers intact.
    assert _snapshot(table) == before
    assert _backups(table) == []
    assert _sql(
        f"SELECT tgenabled FROM pg_trigger WHERE tgname = 'd122_t_{role['sfx']}'"
    ) == [("O",)]


def test_non_owner_role_is_refused_before_any_write(tmp_path, isolated, role):
    table = f"d122_foreign_{role['sfx']}"
    _make_table(table, role["sfx"], user=None, password=None)
    _sql(f'GRANT SELECT, INSERT, UPDATE, DELETE ON public."{table}" TO {role["name"]}')
    before = _snapshot(table)
    try:
        result = _run(isolated, tmp_path, table, role["name"], role["pw"])
        error = str(result.error or "")
        assert not result.success
        assert "trigger" in error.lower(), error
        assert _snapshot(table) == before
        assert _backups(table) == []
    finally:
        _sql(f'DROP TABLE IF EXISTS public."{table}" CASCADE')
        _sql(f"DROP FUNCTION IF EXISTS public.d122_f_{role['sfx']}()")


def test_restore_verifies_a_multiset_with_json_and_point(role):
    """No key, a duplicated row, json and point: the verify must be EXCEPT ALL
    over a text image, not set EXCEPT over types without equality."""
    from connectors.table_manager import create_pg_table_backup, restore_pg_route_backups

    sfx, user, pw = role["sfx"], role["name"], role["pw"]
    table = f"d122_ms_{sfx}"
    _sql(
        f"""
        CREATE TABLE public."{table}" (n INT, doc JSON, p POINT);
        INSERT INTO public."{table}" VALUES
            (1, '{{"a": 1}}', '(1,2)'), (1, '{{"a": 1}}', '(1,2)'), (2, 'null', NULL);
        CREATE FUNCTION public.d122_m_{sfx}() RETURNS trigger AS $$
        BEGIN NEW.n := NEW.n + 100; RETURN NEW; END $$ LANGUAGE plpgsql;
        CREATE TRIGGER d122_mt_{sfx} BEFORE INSERT ON public."{table}"
            FOR EACH ROW EXECUTE FUNCTION public.d122_m_{sfx}();
        """,
        user=user,
        password=pw,
    )
    cfg = dict(host=ADMIN["host"], port=ADMIN["port"], database="dataflow",
               username=user, password=pw, schema="public")
    image = f'SELECT n, doc::text, p::text FROM public."{table}" ORDER BY 1, 2'
    before = _sql(image)
    pair = create_pg_table_backup(cfg, schema="public", table=table, job_id="d122ms00")
    assert pair
    # The failed run's damage: one duplicate gone, a stray row added.
    _sql(
        f"DELETE FROM public.\"{table}\" WHERE ctid = (SELECT min(ctid) FROM public.\"{table}\" WHERE n = 1);"
        f"INSERT INTO public.\"{table}\" VALUES (7, '[]', '(0,0)')",
        user=user,
        password=pw,
    )
    assert restore_pg_route_backups(cfg, {"r": pair}) == ["r"]
    assert _sql(image) == before
    assert _backups(table) == []
