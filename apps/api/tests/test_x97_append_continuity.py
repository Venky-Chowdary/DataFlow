"""X-97: an append must not accept a destination tampered since the last run.

Run 1 is a clean append. The destination is then tampered (one value
changed, one row deleted, one stray row added) and run 2 appends new source
rows. Run 2 must fail verification naming the table and the mismatch, and the
destination must be exactly its pre-run-2 state (rows and digest).

Live engines: PostgreSQL, MySQL and MongoDB are required. An unreachable
engine fails the test; set ``DATAFLOW_ALLOW_LIVE_SKIP=1`` to skip instead
(the skip names the engine).
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Callable

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.sync_cursor as sc  # noqa: E402
import src.transfer.engine as engine_mod  # noqa: E402
from services import append_continuity as ac  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402

PG = {
    "host": os.environ.get("PG_HOST", "127.0.0.1"),
    "port": int(os.environ.get("PG_PORT", "5432")),
    "database": os.environ.get("PG_DB", "dataflow"),
    "username": os.environ.get("PG_USER", "dataflow"),
    "password": os.environ.get("PG_PASSWORD", "dataflow"),
}
MYSQL = {
    "host": os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306")),
    "database": os.environ.get("DATAFLOW_TEST_MYSQL_DB", "dataflow"),
    "username": os.environ.get("DATAFLOW_TEST_MYSQL_USER", "root"),
    "password": os.environ.get("DF_TEST_MYSQL_PASSWORD", "dataflow"),
}
MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")


def _unavailable(engine: str, exc: Exception) -> None:
    msg = f"X-97 live {engine} destination unreachable: {exc}"
    if os.environ.get("DATAFLOW_ALLOW_LIVE_SKIP") == "1":
        pytest.skip(msg)
    pytest.fail(msg + " (set DATAFLOW_ALLOW_LIVE_SKIP=1 to skip explicitly)")


# --- verifier unit tests ----------------------------------------------------

_BASE = {"rows": 10, "checksum": "a" * 64, "job_id": "job-1", "recorded_at": "t"}


def test_compare_identical_passes():
    assert ac.compare("t", _BASE, {"rows": 10, "checksum": "a" * 64}) is None


def test_compare_same_count_changed_values_fails_with_checksum_mismatch():
    v = ac.compare("orders", _BASE, {"rows": 10, "checksum": "b" * 64})
    assert v is not None
    assert v.details["mismatch"] == "checksum"
    assert v.details["table"] == "orders"
    assert v.details["verification_status"] == "failed"
    assert "'orders'" in v.message and "Nothing was written" in v.message


@pytest.mark.parametrize(
    ("rows", "word"), [(9, "1 row removed"), (12, "2 rows added")]
)
def test_compare_count_drift_names_direction(rows, word):
    v = ac.compare("orders", _BASE, {"rows": rows, "checksum": "b" * 64})
    assert v is not None
    assert v.details["mismatch"] == "row_count"
    assert v.details["expected_rows"] == 10 and v.details["found_rows"] == rows
    assert word in v.message


def test_compare_unreadable_destination_fails_closed():
    v = ac.compare("orders", _BASE, None)
    assert v is not None and v.details["mismatch"] == "unreadable"


def test_baseline_store_roundtrip_and_clear():
    ac.save_baseline("k", {"rows": 3, "checksum": "c" * 64}, "job-9")
    got = ac.load_baseline("k")
    assert got and got["rows"] == 3 and got["job_id"] == "job-9"
    ac.clear_baseline("k")
    assert ac.load_baseline("k") is None


def test_dsn_identity_drops_credentials():
    ident = ac._dsn_identity("postgresql://u:s3cret@db.example:5432/app")
    assert "s3cret" not in ident and "db.example:5432/app" in ident


# --- live destination matrix -------------------------------------------------


class _Dest:
    """Engine adapter: endpoint, full snapshot, tamper, teardown."""

    def __init__(self, engine: str, tmp_path: Path):
        self.engine = engine
        self.table = f"x97_{uuid.uuid4().hex[:10]}"
        self.tmp_path = tmp_path
        self._mongo: Any = None
        if engine == "postgresql":
            try:
                import psycopg2

                self._conn = lambda: psycopg2.connect(
                    host=PG["host"], port=PG["port"], dbname=PG["database"],
                    user=PG["username"], password=PG["password"], connect_timeout=5,
                )
                self._conn().close()
            except Exception as exc:  # noqa: BLE001
                _unavailable(engine, exc)
        elif engine == "mysql":
            try:
                import pymysql

                self._conn = lambda: pymysql.connect(
                    host=MYSQL["host"], port=MYSQL["port"], database=MYSQL["database"],
                    user=MYSQL["username"], password=MYSQL["password"],
                    connect_timeout=5, autocommit=True,
                )
                self._conn().close()
            except Exception as exc:  # noqa: BLE001
                _unavailable(engine, exc)
        elif engine == "mongodb":
            try:
                import pymongo

                client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
                client.admin.command("ping")
                self._mongo = client["x97_continuity"][self.table]
            except Exception as exc:  # noqa: BLE001
                _unavailable(engine, exc)
        elif engine == "duckdb":
            self.path = tmp_path / "dest.duckdb"
        else:
            self.path = tmp_path / "dest.sqlite"

    def endpoint(self) -> EndpointConfig:
        if self.engine in {"sqlite", "duckdb"}:
            return EndpointConfig(
                kind="database", format=self.engine, database=str(self.path), table=self.table
            )
        if self.engine == "mongodb":
            from urllib.parse import urlsplit

            u = urlsplit(MONGO_URI)
            return EndpointConfig(
                kind="database", format="mongodb", host=u.hostname or "localhost",
                port=u.port or 27017, database="x97_continuity",
                collection=self.table, table=self.table,
            )
        cfg = PG if self.engine == "postgresql" else MYSQL
        return EndpointConfig(
            kind="database", format=self.engine, table=self.table,
            schema="public" if self.engine == "postgresql" else "", **cfg,
        )

    def _sql(self, sql: str, params: tuple = ()) -> list[tuple]:
        if self.engine == "duckdb":
            import duckdb

            conn = duckdb.connect(str(self.path))
            try:
                conn.execute(sql, list(params))
                return list(conn.fetchall()) if conn.description else []
            finally:
                conn.close()
        if self.engine == "sqlite":
            conn = sqlite3.connect(str(self.path))
        else:
            conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            out = list(cur.fetchall()) if cur.description else []
            conn.commit()
            return out
        finally:
            conn.close()

    def _ph(self) -> str:
        return "?" if self.engine in {"sqlite", "duckdb"} else "%s"

    def snapshot(self) -> list[tuple]:
        if self.engine == "mongodb":
            return sorted(
                (d["id"], d["seq"], d["v"], str(d["_id"]))
                for d in self._mongo.find({})
            )
        q = '"' if self.engine != "mysql" else "`"
        return sorted(
            tuple(r)
            for r in self._sql(f"SELECT id, seq, v FROM {q}{self.table}{q}")  # nosec B608
        )

    def tamper(self) -> None:
        if self.engine == "mongodb":
            self._mongo.update_one({"id": 2}, {"$set": {"v": "TAMPER"}})
            self._mongo.delete_one({"id": 3})
            self._mongo.insert_one({"id": 999, "seq": 999, "v": "stray"})
            return
        q = '"' if self.engine != "mysql" else "`"
        t, p = f"{q}{self.table}{q}", self._ph()
        self._sql(f"UPDATE {t} SET v={p} WHERE id={p}", ("TAMPER", 2))  # nosec B608
        self._sql(f"DELETE FROM {t} WHERE id={p}", (3,))  # nosec B608
        self._sql(f"INSERT INTO {t} (id, seq, v) VALUES ({p},{p},{p})", (999, 999, "stray"))  # nosec B608

    def tamper_one(self, kind: str) -> None:
        """One mutation only: a cell at the same count, a delete, or an insert."""
        if self.engine == "mongodb":
            if kind == "cell":
                self._mongo.update_one({"id": 2}, {"$set": {"v": "TAMPER"}})
            elif kind == "delete":
                self._mongo.delete_one({"id": 3})
            else:
                self._mongo.insert_one({"id": 999, "seq": 999, "v": "stray"})
            return
        q = '"' if self.engine != "mysql" else "`"
        t, p = f"{q}{self.table}{q}", self._ph()
        if kind == "cell":
            self._sql(f"UPDATE {t} SET v={p} WHERE id={p}", ("TAMPER", 2))  # nosec B608
        elif kind == "delete":
            self._sql(f"DELETE FROM {t} WHERE id={p}", (3,))  # nosec B608
        else:
            self._sql(f"INSERT INTO {t} (id, seq, v) VALUES ({p},{p},{p})", (999, 999, "stray"))  # nosec B608

    def fingerprint(self) -> dict | None:
        target = ac.resolve_target(self.endpoint())
        assert target is not None, f"{self.engine} must be continuity-covered"
        return ac.fingerprint(target)

    def drop(self) -> None:
        try:
            if self.engine == "mongodb":
                self._mongo.drop()
            elif self.engine in {"postgresql", "mysql"}:
                q = '"' if self.engine == "postgresql" else "`"
                self._sql(f"DROP TABLE IF EXISTS {q}{self.table}{q}")
        except Exception as exc:  # noqa: BLE001 — cleanup must not mask the assertion
            print(f"X-97 cleanup of {self.engine}.{self.table} failed: {exc}")


@pytest.fixture
def fake_store(monkeypatch, tmp_path):
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _source(tmp_path: Path, *, pk: bool) -> Path:
    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(src))
    key = "INTEGER PRIMARY KEY" if pk else "INTEGER NOT NULL"
    conn.execute(f"CREATE TABLE t (id {key}, seq INTEGER NOT NULL, v TEXT)")
    conn.executemany("INSERT INTO t VALUES (?,?,?)", [(i, i, f"v{i}") for i in range(1, 11)])
    conn.commit()
    conn.close()
    return src


def _add_source_rows(src: Path, lo: int, hi: int) -> None:
    conn = sqlite3.connect(str(src))
    conn.executemany("INSERT INTO t VALUES (?,?,?)", [(i, i, f"v{i}") for i in range(lo, hi)])
    conn.commit()
    conn.close()


def _runner(fake, src: Path, dest: _Dest, *, pk: bool) -> Callable[[], Any]:
    contract: dict[str, Any] = {
        "name": "t", "sync_mode": "incremental_append", "selected": True,
        "cursor_field": "seq",
    }
    if pk:
        contract["primary_key"] = ["id"]

    def run():
        jid = "x97-" + uuid.uuid4().hex[:12]
        fake.update_job_status(jid, "pending", transfer_request={})
        req = TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
            destination=dest.endpoint(),
            sync_mode="incremental_append",
            stream_contracts=[contract],
            skip_preflight=True,
            validation_mode="strict",
        )
        return UniversalTransferEngine().execute_tracked(req, jid)

    return run


ENGINES = ["sqlite", "duckdb", "postgresql", "mysql", "mongodb"]


@pytest.mark.parametrize("engine", sorted(ac.CONTINUITY_ENGINES))
def test_every_covered_engine_resolves_a_continuity_target(engine, tmp_path):
    """A covered engine the resolver drops is silently unchecked (DuckDB fail-open)."""
    ep = EndpointConfig(
        kind="database", format=engine, host="h.example", port=1,
        database=str(tmp_path / "d.db"), schema="s", table="t.csv",
    )
    target = ac.resolve_target(ep)
    assert target is not None, f"{engine} is in CONTINUITY_ENGINES but is not resolved"
    assert target.db_type == engine or target.db_type in ac.CONTINUITY_ENGINES


@pytest.mark.parametrize("kind", ["cell", "delete", "insert"])
@pytest.mark.parametrize("engine", ENGINES)
def test_single_tamper_fails_run2_and_is_left_unchanged(engine, kind, tmp_path, fake_store):
    dest = _Dest(engine, tmp_path)
    try:
        src = _source(tmp_path, pk=True)
        run = _runner(fake_store, src, dest, pk=True)
        r1 = run()
        assert r1.success, r1.error
        base = ac.load_baseline(_key(dest))
        assert base and not base.get("unreadable"), f"{engine}: no baseline after run 1"
        dest.tamper_one(kind)
        before = dest.snapshot()
        _add_source_rows(src, 11, 16)
        r2 = run()
        assert not r2.success, f"{engine}/{kind}: tampered destination accepted"
        assert r2.error_details.get("reason") == "destination_continuity_mismatch"
        assert f"'{dest.table}'" in r2.error
        assert dest.snapshot() == before
    finally:
        dest.drop()


def test_unfingerprintable_destination_fails_next_append_loudly(tmp_path, fake_store, monkeypatch):
    dest = _Dest("sqlite", tmp_path)
    src = _source(tmp_path, pk=True)
    run = _runner(fake_store, src, dest, pk=True)
    real = ac.fingerprint
    monkeypatch.setattr(ac, "fingerprint", lambda *a, **k: None)
    assert run().success
    base = ac.load_baseline(_key(dest))
    assert base and base.get("unreadable"), "an unreadable destination must not drop its baseline"
    monkeypatch.setattr(ac, "fingerprint", real)
    before = dest.snapshot()
    _add_source_rows(src, 11, 16)
    r2 = run()
    assert not r2.success
    assert r2.error_details.get("mismatch") == "no_baseline_fingerprint"
    assert "could not be fingerprinted" in r2.error
    assert dest.snapshot() == before


@pytest.mark.parametrize("pk", [True, False], ids=["pk", "nopk"])
@pytest.mark.parametrize("engine", ENGINES)
def test_tampered_destination_fails_run2_and_is_left_unchanged(
    engine, pk, tmp_path, fake_store
):
    dest = _Dest(engine, tmp_path)
    try:
        src = _source(tmp_path, pk=pk)
        run = _runner(fake_store, src, dest, pk=pk)
        r1 = run()
        assert r1.success, r1.error
        assert len(dest.snapshot()) == 10
        assert ac.load_baseline(_key(dest)), "baseline key must match a fresh client endpoint"

        dest.tamper()
        before_rows, before_fp = dest.snapshot(), dest.fingerprint()
        assert len(before_rows) == 10
        _add_source_rows(src, 11, 16)

        r2 = run()
        assert not r2.success, "tampered destination was accepted as success"
        assert r2.error_details.get("reason") == "destination_continuity_mismatch"
        assert r2.error_details.get("table") == dest.table
        assert r2.error_details.get("mismatch") == "checksum"
        assert f"'{dest.table}'" in r2.error
        assert dest.snapshot() == before_rows
        assert dest.fingerprint() == before_fp
    finally:
        dest.drop()


@pytest.mark.parametrize("engine", ENGINES)
def test_clean_append_runs_stay_green_and_idempotent(engine, tmp_path, fake_store):
    dest = _Dest(engine, tmp_path)
    try:
        src = _source(tmp_path, pk=True)
        run = _runner(fake_store, src, dest, pk=True)
        assert run().success
        _add_source_rows(src, 11, 16)
        r2 = run()
        assert r2.success, r2.error
        after2 = dest.snapshot()
        assert [r[0] for r in after2] == list(range(1, 16))
        r3 = run()
        assert r3.success, r3.error
        assert dest.snapshot() == after2
    finally:
        dest.drop()


# --- Upgrade path, state location, recovery, failed-run concurrency --------


def _key(dest: _Dest) -> str:
    target = ac.resolve_target(dest.endpoint())
    assert target is not None
    return target.key


def test_store_lives_under_shared_data_dir(monkeypatch, tmp_path):
    import inspect

    from services import platform_config

    src = inspect.getsource(ac)
    assert "from services.platform_config import data_dir" in src
    assert "__file__" not in src, "store path must not be module-relative"
    monkeypatch.setattr(ac, "data_dir", platform_config.data_dir)
    monkeypatch.delenv("DATAWRAP_DATA_DIR", raising=False)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "dd1"))
    assert ac.store_path() == tmp_path / "dd1" / "append_continuity.json"
    ac.save_baseline("k", {"rows": 1, "checksum": "c"}, "j")
    assert (tmp_path / "dd1" / "append_continuity.json").exists()
    # Resolved per call — a later DATAFLOW_DATA_DIR change is honoured.
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "dd2"))
    assert ac.load_baseline("k") is None
    ac.save_baseline("k", {"rows": 2, "checksum": "d"}, "j2")
    assert (tmp_path / "dd2" / "append_continuity.json").exists()


def test_failed_result_never_moves_baseline(monkeypatch):
    class _Failed:
        success = False

    def _boom(*a, **k):
        raise AssertionError("baseline written for a failed run")

    monkeypatch.setattr(ac, "save_baseline", _boom)
    monkeypatch.setattr(ac, "clear_baseline", _boom)
    monkeypatch.setattr(ac, "resolve_target", _boom)
    ac.record_after_run(object(), "j", _Failed())


@pytest.mark.parametrize("engine", ENGINES)
def test_upgrade_path_existing_destination_without_baseline_records_and_passes(
    engine, tmp_path, fake_store
):
    dest = _Dest(engine, tmp_path)
    try:
        src = _source(tmp_path, pk=True)
        run = _runner(fake_store, src, dest, pk=True)
        assert run().success
        # Pipeline that ran before X-97 was deployed: data, no baseline.
        ac.clear_baseline(_key(dest))
        assert ac.load_baseline(_key(dest)) is None
        _add_source_rows(src, 11, 16)
        r2 = run()
        assert r2.success, r2.error
        assert len(dest.snapshot()) == 15
        base = ac.load_baseline(_key(dest))
        assert base and base["job_id"] == r2.job_id and base["rows"] == 15
        assert base["checksum"] == ac.fingerprint(ac.resolve_target(dest.endpoint()))["checksum"]
    finally:
        dest.drop()


@pytest.mark.parametrize("failure", ["raises", "returns_failed"])
@pytest.mark.parametrize("engine", ENGINES)
def test_failed_run_does_not_move_baseline(engine, failure, tmp_path, fake_store, monkeypatch):
    from src.transfer.models import TransferResult

    dest = _Dest(engine, tmp_path)
    try:
        src = _source(tmp_path, pk=True)
        run = _runner(fake_store, src, dest, pk=True)
        assert run().success
        b1 = ac.load_baseline(_key(dest))
        before = dest.snapshot()
        _add_source_rows(src, 11, 16)

        def _fail(self, request, job_id, *a, **k):
            if failure == "raises":
                raise RuntimeError("writer crashed; transaction rolled back")
            return TransferResult(success=False, job_id=job_id, error="rolled back")

        with monkeypatch.context() as m:
            m.setattr(UniversalTransferEngine, "_execute_tracked_inner", _fail)
            try:
                r2 = run()
            except RuntimeError:
                r2 = None
        assert r2 is None or not r2.success
        assert ac.load_baseline(_key(dest)) == b1
        assert dest.snapshot() == before
        r3 = run()
        assert r3.success, r3.error
        assert len(dest.snapshot()) == 15
    finally:
        dest.drop()


def _rebaseline_client():
    from fastapi.testclient import TestClient

    from src.main import app

    return TestClient(app, raise_server_exceptions=False)


def _endpoint_body(dest: _Dest) -> dict[str, Any]:
    import dataclasses

    return {k: v for k, v in dataclasses.asdict(dest.endpoint()).items() if v not in (None, "", {}, [])}


@pytest.mark.parametrize("engine", ["sqlite", "postgresql"])
def test_tamper_recovery_via_audited_rebaseline(engine, tmp_path, fake_store, monkeypatch):
    import services.audit_log as audit_log

    events: list[dict[str, Any]] = []
    real_audit = audit_log.append_audit_event

    def _spy(**kw):
        if kw.get("action") == "append_continuity.rebaselined":
            events.append(kw)
            return kw
        return real_audit(**kw)

    monkeypatch.setattr(audit_log, "append_audit_event", _spy)
    dest = _Dest(engine, tmp_path)
    try:
        src = _source(tmp_path, pk=True)
        run = _runner(fake_store, src, dest, pk=True)
        assert run().success
        dest.tamper()
        tampered = dest.snapshot()
        _add_source_rows(src, 11, 16)
        r2 = run()
        assert not r2.success
        assert "/api/v1/transfer/continuity/rebaseline" in r2.error
        assert "continuity/rebaseline" in r2.error_details["recovery"]
        b_old = ac.load_baseline(_key(dest))

        client = _rebaseline_client()
        url = "/api/v1/transfer/continuity/rebaseline"
        resp = client.post(url, json={"destination": _endpoint_body(dest), "reason": "  "})
        assert resp.status_code == 422, resp.text
        assert ac.load_baseline(_key(dest)) == b_old and not events

        resp = client.post(
            url,
            json={"destination": _endpoint_body(dest), "reason": "INC-1: manual fix verified"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["table"] == dest.table
        assert body["previous"]["checksum"] == b_old["checksum"]
        assert body["current"]["rows"] == len(tampered)
        assert len(events) == 1
        ev = events[0]
        assert ev["action"] == "append_continuity.rebaselined"
        assert ev["details"]["reason"] == "INC-1: manual fix verified"
        assert ev["details"]["table"] == dest.table
        assert ev["details"]["rebaseline_id"] == body["rebaseline_id"]
        assert dest.snapshot() == tampered, "re-baseline must not touch the destination"

        r3 = run()
        assert r3.success, r3.error
        assert len(dest.snapshot()) == len(tampered) + 5
    finally:
        dest.drop()


def test_rebaseline_audit_failure_leaves_baseline_in_force(tmp_path, fake_store, monkeypatch):
    import services.audit_log as audit_log

    dest = _Dest("sqlite", tmp_path)
    try:
        src = _source(tmp_path, pk=True)
        assert _runner(fake_store, src, dest, pk=True)().success
        dest.tamper()
        b_old = ac.load_baseline(_key(dest))

        def _audit_down(**kw):
            if kw.get("action") == "append_continuity.rebaselined":
                raise RuntimeError("audit store down")
            return kw

        monkeypatch.setattr(audit_log, "append_audit_event", _audit_down)
        with pytest.raises(RuntimeError):
            ac.rebaseline(dest.endpoint(), actor="ops@x", reason="r")
        assert ac.load_baseline(_key(dest)) == b_old
        with pytest.raises(ac.RebaselineError):
            ac.rebaseline(dest.endpoint(), actor="ops@x", reason="")
    finally:
        dest.drop()


CORRUPTIONS = {
    "garbage": b"\x00\xffnot json",
    "truncated": b'{"sqlite|x": {"rows": 10, "checks',
    "not_a_map": b"[]",
}


@pytest.fixture
def file_store(monkeypatch, tmp_path):
    path = tmp_path / "store" / "append_continuity.json"
    monkeypatch.setattr(ac, "_collection", lambda: None)
    monkeypatch.setattr(ac, "store_path", lambda: path)
    return path


def test_missing_store_reads_as_no_baselines(file_store):
    assert not file_store.exists()
    assert ac.load_baseline("anything") is None


@pytest.mark.parametrize("kind", sorted(CORRUPTIONS))
def test_corrupt_store_is_not_read_as_empty(kind, file_store):
    file_store.parent.mkdir(parents=True)
    file_store.write_bytes(CORRUPTIONS[kind])
    with pytest.raises(ac.ContinuityStoreCorrupt):
        ac.load_baseline("anything")
    with pytest.raises(ac.ContinuityStoreCorrupt):
        ac.save_baseline("anything", {"rows": 1, "checksum": "c"}, "job")
    assert file_store.read_bytes() == CORRUPTIONS[kind], "a corrupt store must not be overwritten"


@pytest.mark.parametrize("kind", sorted(CORRUPTIONS))
def test_corrupt_store_with_tampered_destination_fails_append_loudly(
    kind, tmp_path, fake_store, file_store
):
    dest = _Dest("sqlite", tmp_path)
    src = _source(tmp_path, pk=True)
    run = _runner(fake_store, src, dest, pk=True)
    assert run().success
    assert ac.load_baseline(_key(dest)), "run 1 must record a file-store baseline"
    file_store.write_bytes(CORRUPTIONS[kind])
    dest.tamper_one("cell")
    before = dest.snapshot()
    _add_source_rows(src, 11, 16)
    r2 = run()
    assert not r2.success
    assert r2.error_details.get("mismatch") == "baseline_store_unreadable"
    assert "baseline store could not be read" in r2.error
    assert dest.snapshot() == before
    assert file_store.read_bytes() == CORRUPTIONS[kind]


def test_corrupt_store_rebaseline_audited(tmp_path, fake_store, monkeypatch):
    """X-CS: an unreadable continuity store must be recoverable through the
    audited rebaseline path — not a dead end that only raw file surgery can
    fix. The audit record carries that the previous baseline was unreadable.
    """
    import services.audit_log as audit_log
    import services.append_continuity as ac_mod

    events: list[dict[str, Any]] = []
    real_audit = audit_log.append_audit_event

    def _spy(**kw):
        if kw.get("action") == "append_continuity.rebaselined":
            events.append(kw)
            return kw
        return real_audit(**kw)

    monkeypatch.setattr(audit_log, "append_audit_event", _spy)
    dest = _Dest("sqlite", tmp_path)
    try:
        src = _source(tmp_path, pk=True)
        assert _runner(fake_store, src, dest, pk=True)().success
        dest.tamper()
        tampered = dest.snapshot()

        # Corrupt the store — fail-loud on reads.
        store = ac_mod.store_path()
        store.write_text("{ not json")

        # Run 2 fails closed naming the unreadable store.
        _add_source_rows(src, 11, 13)
        r2 = _runner(fake_store, src, dest, pk=True)()
        assert not r2.success
        assert "could not be read" in (r2.error or "")

        # Audited rebaseline recovers: the new baseline is the tampered table.
        client = _rebaseline_client()
        resp = client.post(
            "/api/v1/transfer/continuity/rebaseline",
            json={
                "destination": _endpoint_body(dest),
                "reason": "INC-2: store file corrupted, reset after verify",
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["current"]["rows"] == len(tampered)
        assert len(events) == 1
        det = events[0]["details"]
        assert det["previous_store_unreadable"] is True
        assert det["store_error"]
        assert det["rebaseline_id"] == body["rebaseline_id"]

        # Store is readable again; next run is green.
        assert ac_mod.load_baseline(_key(dest)) is not None
        r3 = _runner(fake_store, src, dest, pk=True)()
        assert r3.success, r3.error
    finally:
        dest.drop()
