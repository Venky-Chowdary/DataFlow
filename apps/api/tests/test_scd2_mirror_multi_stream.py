"""Multi-stream SCD2/mirror — per-stream staging merges through the sequential
orchestrator. Previously refused outright ("not supported yet"); each selected
stream now gets its own remapped mirror/SCD2 merge with its own identity.
"""
import sqlite3
import tempfile
import uuid
from pathlib import Path

import pytest

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


class _FakeMongo:
    def __init__(self):
        self.jobs = {}

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def update_job_fields(self, job_id: str, fields: dict, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {}).update(fields)
        return True


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)


def _seed(path: Path, table: str, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany(f"INSERT INTO {table} VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def _read(path: Path, table: str) -> list[tuple]:
    conn = sqlite3.connect(str(path))
    rows = conn.execute(f"SELECT id, name FROM {table} ORDER BY id").fetchall()
    conn.close()
    return rows


def _contracts(names: list[str], sync: str) -> list[dict]:
    return [
        {
            "name": n,
            "selected": True,
            "sync_mode": sync,
            "primary_key": "id",
            "mappings": [
                {"source": "id", "target": "id", "confidence": 0.99},
                {"source": "name", "target": "name", "confidence": 0.99},
            ],
        }
        for n in names
    ]


def _run(tmp: str, sync: str, dest_seed: dict | None = None):
    src = Path(tmp) / "src.db"
    dst = Path(tmp) / "dst.db"
    _seed(src, "alpha", [(1, "a1"), (2, "a2")])
    _seed(src, "beta", [(10, "b10"), (20, "b20")])
    for table, rows in (dest_seed or {}).items():
        _seed(dst, table, rows)

    # UI sends the primary stream name on the endpoint fields; per-stream
    # remapping in run_non_cdc_multi_stream_sequential retargets both.
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="alpha"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="alpha"
        ),
        sync_mode=sync,
        stream_contracts=_contracts(["alpha", "beta"], sync),
        skip_preflight=True,
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex)
    return result, dst


def test_mirror_multi_stream_mirrors_each_table():
    """Mirror with 2 selected streams: each dest table becomes an exact copy —
    extra dest rows soft/handled per mirror semantics, missing ones inserted."""
    with tempfile.TemporaryDirectory() as tmp:
        # beta dest already holds a stale row + one that will be updated.
        result, dst = _run(tmp, "mirror", dest_seed={
            "beta": [(10, "stale"), (99, "ghost")],
        })
        assert result.success is True, result.error
        assert _read(dst, "alpha") == [(1, "a1"), (2, "a2")]
        beta = dict(_read(dst, "beta"))
        assert beta.get(10) == "b10"   # updated to source value
        assert beta.get(20) == "b20"   # inserted
        # Mirror soft-deletes missing keys: row 99 stays, flagged _deleted.
        conn = sqlite3.connect(str(dst))
        flags = dict(conn.execute(
            "SELECT id, _deleted FROM beta ORDER BY id"
        ).fetchall())
        conn.close()
        assert flags.get(99) == 1
        assert flags.get(10) in (0, None)
        assert flags.get(20) in (0, None)


def test_scd2_multi_stream_versions_each_table():
    """SCD2 with 2 selected streams: new rows land per stream; dest gets the
    SCD2-stamped current versions for each table independently."""
    with tempfile.TemporaryDirectory() as tmp:
        result, dst = _run(tmp, "scd2")
        assert result.success is True, result.error
        assert _read(dst, "alpha") == [(1, "a1"), (2, "a2")]
        assert _read(dst, "beta") == [(10, "b10"), (20, "b20")]


def test_multi_stream_scd2_with_shape_recipe_refuses(monkeypatch):
    """A recipe SCD2 step is single-stream history — one recipe cannot name N
    streams; the engine must still refuse that combination honestly."""
    import src.transfer.stream_multi as sm

    calls = []

    def _spy(*a, **k):
        calls.append(1)
        raise AssertionError("should not reach per-stream SCD2")

    monkeypatch.setattr(sm, "run_non_cdc_multi_stream_sequential", _spy)
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "src.db"
        dst = Path(tmp) / "dst.db"
        _seed(src, "alpha", [(1, "a1")])
        _seed(src, "beta", [(2, "b2")])
        recipe = {
            "steps": [
                {
                    "kind": "scd2",
                    "keys": ["id"],
                    "tracked": ["name"],
                }
            ]
        }
        request = TransferRequest(
            source=EndpointConfig(
                kind="database", format="sqlite", database=str(src),
                table="alpha",
            ),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(dst),
                table="alpha",
            ),
            sync_mode="scd2",
            stream_contracts=_contracts(["alpha", "beta"], "scd2"),
            shape_recipe=recipe,
            skip_preflight=True,
        )
        result = UniversalTransferEngine().execute_tracked(
            request, uuid.uuid4().hex
        )
        assert result.success is False
        assert "recipe" in (result.error or "").lower()
        assert calls == []
