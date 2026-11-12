"""Audit finding 4 (H2) — a declared ``source_filter`` is applied or refused.

H2a: a filter node without ``column`` (``{"sql": ...}``, ``{"where": ...}``,
``{"region": "EU"}``, ``{"filters": [...]}``) compared an absent column with a
blank value and matched every row: all rows landed and the run was "verified".
H2b: ``mirror`` ignored a valid filter and landed every row.

Both are over-delivery with a green result. The filter is validated once
(``canonical_filter_spec``), Execute refuses before any read or write, the
preflight gate shows the same refusal, and the row matcher itself can never
match-all on a node it cannot evaluate.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services.row_filter import apply_row_filter, canonical_filter_spec  # noqa: E402
from services.source_filter_gate import (  # noqa: E402
    GATE_ID,
    route_request_gate,
    source_filter_refusal,
)
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_UNKNOWN_SHAPES = [
    {"sql": "region = 'EU'"},
    {"where": "region = 'EU'"},
    {"region": "EU"},
    {"filters": [{"column": "region", "op": "eq", "value": "EU"}]},
    {"op": "eq", "value": "EU"},
    {"column": "", "op": "eq", "value": "EU"},
    {"column": "region", "op": "eq", "value": "EU", "sql": "1=1"},
    {"and": []},
    {"and": [{"column": "region", "value": "EU"}, {"sql": "1=1"}]},
    {"and": [{"column": "region", "value": "EU"}], "limit": 5},
    {"or": "region = 'EU'"},
    {"or": [None]},
    {"column": "region", "op": "regex", "value": "(["},
]
_EU = {"column": "region", "op": "eq", "value": "EU"}
_ROWS = [{"id": i, "region": "EU" if i % 2 else "US"} for i in range(1, 11)]


@pytest.mark.parametrize("spec", _UNKNOWN_SHAPES)
def test_source_filter_unknown_shape_is_refused(spec):
    with pytest.raises(ValueError):
        canonical_filter_spec(spec)
    refusal = source_filter_refusal(spec, "full_refresh_overwrite")
    assert refusal.startswith("source_filter is refused") and "keep every row" in refusal


@pytest.mark.parametrize(
    "spec", [s for s in _UNKNOWN_SHAPES if not any(k in s for k in ("and", "or"))][:6]
)
def test_the_row_matcher_never_matches_all_on_a_node_without_a_column(spec):
    """Defence in depth: even an unvalidated spec cannot let every row through."""
    with pytest.raises(ValueError):
        apply_row_filter(list(_ROWS), spec)


@pytest.mark.parametrize(
    "spec",
    [
        _EU,
        {"field": "region", "operator": "=", "value": "EU"},
        {"column": "region", "op": "is_null"},
        {"column": "region", "op": "in", "value": ["EU", "US"]},
        {"and": [_EU, {"or": [{"column": "id", "op": ">", "value": 3}, {"column": "id", "op": "is_null"}]}]},
        {"column": "region", "op": "regex", "value": "^E"},
    ],
)
def test_a_valid_filter_is_still_accepted(spec):
    canonical = canonical_filter_spec(spec)
    assert canonical and source_filter_refusal(spec, "full_refresh_overwrite") == ""
    assert isinstance(apply_row_filter(list(_ROWS), canonical), list)


def test_no_filter_is_no_refusal():
    assert canonical_filter_spec(None) == {} and canonical_filter_spec({}) == {}
    assert source_filter_refusal({}, "mirror") == ""
    assert source_filter_refusal(None, "full_refresh_overwrite") == ""


@pytest.mark.parametrize("sync", ["mirror", "full_refresh_mirror", "scd2"])
def test_history_merging_routes_refuse_a_valid_filter(sync):
    refusal = source_filter_refusal(_EU, sync)
    assert "source_filter is not applied on the" in refusal and "deleted" in refusal
    # ...also when only the selected stream's contract names the mode.
    assert source_filter_refusal(
        _EU, "full_refresh_append", [{"name": "t", "sync_mode": sync, "selected": True}]
    )
    assert not source_filter_refusal(
        _EU, "full_refresh_append", [{"name": "t", "sync_mode": sync, "selected": False}]
    )


def test_preflight_gate_blocks_with_the_refusal_execute_raises():
    gate = route_request_gate(None, "full_refresh_overwrite", {"sql": "region = 'EU'"})
    assert gate and gate["id"] == GATE_ID and gate["status"] == "block"
    assert gate["message"] == source_filter_refusal({"sql": "region = 'EU'"}, "full_refresh_overwrite")
    mirror = route_request_gate(None, "mirror", _EU)
    assert mirror and mirror["id"] == GATE_ID
    assert route_request_gate(None, "full_refresh_overwrite", _EU) is None
    assert route_request_gate(None, "mirror", None) is None


def _preflight(sync_mode: str, source_filter):
    from services.preflight_service import run_file_preflight

    return run_file_preflight(
        columns=["id", "region"],
        column_types={"id": "INTEGER", "region": "TEXT"},
        row_count=2,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99, "target_type": "INTEGER"},
            {"source": "region", "target": "region", "confidence": 0.99, "target_type": "TEXT"},
        ],
        destination_connected=True,
        sample_rows=[{"id": 1, "region": "EU"}, {"id": 2, "region": "US"}],
        destination_column_types={"id": "INTEGER", "region": "TEXT"},
        destination_table_exists=True,
        destination_db_type="sqlite",
        skip_population_fit=True,
        sync_mode=sync_mode,
        source_filter=source_filter,
    )


def _filter_blockers(result):
    return [b for b in result.get("blockers") or [] if b.get("id") == GATE_ID]


def test_run_preflight_blocks_an_unknown_filter_shape_and_a_mirror_filter():
    unknown = _preflight("full_refresh_overwrite", {"where": "region = 'EU'"})
    assert len(_filter_blockers(unknown)) == 1
    assert unknown.get("can_execute") is not True and unknown.get("execute_ready") is not True
    assert any(g.get("id") == GATE_ID and g.get("status") == "block" for g in unknown.get("gates") or [])

    mirror = _preflight("mirror", _EU)
    assert len(_filter_blockers(mirror)) == 1

    valid = _preflight("full_refresh_overwrite", _EU)
    assert _filter_blockers(valid) == []


# ── real SQLite → SQLite runs through the engine ────────────────────────────


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Fake(_FakeMongo):
        def update_job_fields(self, job_id, fields=None, **kw):
            self.jobs.setdefault(job_id, {}).update({**(fields or {}), **kw})
            return True

    fake = _Fake()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _source(tmp_path: Path) -> Path:
    src = tmp_path / "src.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, region TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(r["id"], r["region"]) for r in _ROWS])
    conn.commit()
    conn.close()
    return src


def _run(fake, src: Path, dst: Path, *, mode: str, source_filter, pk: bool = False):
    contract = {"name": "t", "sync_mode": mode, "selected": True}
    if pk:
        contract["primary_key"] = ["id"]
    job_id = "a04" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode=mode,
        stream_contracts=[contract],
        source_filter=source_filter,
        skip_preflight=True,
        validation_mode="strict",
    )
    return UniversalTransferEngine().execute_tracked(request, job_id), job_id


def _landed(dst: Path) -> list[tuple]:
    if not dst.exists():
        return []
    conn = sqlite3.connect(dst)
    try:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='t'").fetchone():
            return []
        return conn.execute("SELECT region, COUNT(*) FROM t GROUP BY region ORDER BY 1").fetchall()
    finally:
        conn.close()


@pytest.mark.parametrize(
    "spec", [{"sql": "region = 'EU'"}, {"where": "region = 'EU'"}, {"region": "EU"}]
)
def test_unknown_filter_shape_never_lands_the_whole_table(spec, tmp_path, isolated):
    src, dst = _source(tmp_path), tmp_path / "dst.db"

    result, job_id = _run(isolated, src, dst, mode="full_refresh_overwrite", source_filter=spec)

    assert result.success is False
    assert "source_filter is refused" in str(result.error)
    assert _landed(dst) == []
    assert result.records_transferred == 0 and job_id


def test_mirror_honours_or_refuses_source_filter(tmp_path, isolated):
    src, dst = _source(tmp_path), tmp_path / "dst.db"

    result, _job = _run(isolated, src, dst, mode="mirror", source_filter=_EU, pk=True)

    # Refused up front, the way mirror refuses a pre-load transform recipe:
    # the US rows the filter keeps out must not land.
    assert result.success is False
    assert "source_filter is not applied on the mirror route" in str(result.error)
    assert ("US", 5) not in _landed(dst) and _landed(dst) == []


def test_a_valid_filter_on_a_filtering_route_still_lands_only_its_rows(tmp_path, isolated):
    src, dst = _source(tmp_path), tmp_path / "dst.db"

    result, _job = _run(isolated, src, dst, mode="full_refresh_overwrite", source_filter=_EU)

    assert _landed(dst) == [("EU", 5)], result.error
