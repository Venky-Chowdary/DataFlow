"""D421 — a no-op incremental re-run reports no drift; real drift names only its keys.

QA r61-r75 (SFTP / S3 CSV -> SQL Server, incremental_upsert key id, transforms
on name, note, payload): the re-run with no source change completed with 0
rows but said "Out-of-window drift: 30 row(s) ... (30 only in the
destination)".

Root causes, both in the keyed no-op poll's out-of-window comparison:

* a file or object source has no population re-read there; its empty
  "re-read" made every destination key "only in the destination";
* a SQL source was compared raw, so every transformed cell (``lower``,
  ``hash_pii``, a recipe step) read as "differ".

Now a source with no re-read makes no drift claim, and a SQL source is
compared as the route writes it (the run's pinned recipe, then the writer's
mapping). A row deleted at the source and a value changed without moving the
cursor are still named, and only those.
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
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

pytestmark = pytest.mark.timeout(600)

_CONTRACT = {
    "sync_mode": "incremental_upsert",
    "cursor_field": "updated_at",
    "cursor_semantics": "modification_timestamp",
    "selected": True,
}


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", "0")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    return fake


def _go(store, request):
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    return result


def _drift(result) -> dict:
    return dict((result.reconciliation or {}).get("out_of_window_drift") or {})


def _message(result) -> str:
    return str((result.reconciliation or {}).get("message") or "")


def test_csv_noop_rerun_claims_no_destination_only_rows(tmp_path, store):
    dst = tmp_path / "dst.sqlite"
    body = "id,name,updated_at\n" + "".join(
        f"{i}, N{i} ,2026-11-01 10:{i:02d}:00\n" for i in range(1, 31)
    )
    maps = [
        {"source": c, "target": c, "confidence": 1, "approved": True}
        for c in ("id", "name", "updated_at")
    ]
    maps[1].update(transform="lower", transform_override=True)

    def request():
        return TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="r61_seed.csv",
            source_content=body.encode(),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="r61"),
            sync_mode="incremental_upsert",
            mappings=maps,
            validation_mode="strict",
            stream_contracts=[{**_CONTRACT, "name": "r61", "primary_key": "id"}],
        )

    assert _go(store, request()).records_transferred == 30
    rerun = _go(store, request())

    assert rerun.records_transferred == 0
    assert _drift(rerun) == {}
    assert "only in the destination" not in _message(rerun)
    # Not measured is said, never left to read as "nothing drifted".
    assert (
        "Out-of-window drift was not measured: this source cannot be re-read for comparison."
        in _message(rerun)
    ), _message(rerun)
    conn = sqlite3.connect(dst)
    assert conn.execute("SELECT count(*) FROM r61").fetchone()[0] == 30
    conn.close()


def test_pg_noop_rerun_with_transforms_names_only_real_drift(store):
    from services.column_transform_chain import compile_column_chains
    from services.shape_models import ShapeRecipe
    from tests.c3_live_helpers import pg_endpoint, pg_sql
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not available")
    src, dst = f"d421s_{uuid.uuid4().hex[:8]}", f"d421d_{uuid.uuid4().hex[:8]}"
    pg_sql(f"CREATE TABLE {src} (id INT PRIMARY KEY, name TEXT, note TEXT, payload TEXT, updated_at TIMESTAMP)")
    for i in range(1, 31):
        pg_sql(
            f"INSERT INTO {src} VALUES (%s, %s, %s, %s, %s)",
            (i, f" N{i} ", f"note {i}", f"p{i}", f"2026-11-01 10:{i:02d}:00"),
        )
    cols = ["id", "name", "note", "payload", "updated_at"]
    recipe, refusal = compile_column_chains({"name": ["trim", "lower"]}, cols)
    assert not refusal
    maps = []
    for c in cols:
        m = {"source": c, "target": c, "confidence": 0.99, "approved": True}
        if c == "note":
            m.update(transform="upper", transform_override=True)
        if c == "payload":
            m.update(transform="hash_pii", transform_override=True)
        maps.append(m)

    def request():
        return TransferRequest(
            source=pg_endpoint(src),
            destination=pg_endpoint(dst),
            sync_mode="incremental_upsert",
            mappings=maps,
            validation_mode="strict",
            shape_recipe=recipe,
            approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
            stream_contracts=[{**_CONTRACT, "name": src, "primary_key": ["id"]}],
        )

    try:
        assert _go(store, request()).records_transferred == 30
        quiet = _go(store, request())
        assert quiet.records_transferred == 0
        assert _drift(quiet) == {}, _message(quiet)
        assert "not measured" not in _message(quiet)  # a SQL source was compared

        # A change made without moving the cursor, and a hard delete.
        pg_sql(f"UPDATE {src} SET note = 'edited' WHERE id = 4")
        pg_sql(f"DELETE FROM {src} WHERE id = 6")
        drifted = _go(store, request())

        drift = _drift(drifted)
        assert drift.get("rows") == 2, drift
        assert drift.get("changed") == 1 and drift.get("destination_only") == 1, drift
        assert sorted(drift.get("sample_keys") or []) == ["4", "6"], drift
        assert [c["key"] for c in drift.get("changed_sample") or []] == ["4"]
        assert drift["changed_sample"][0]["column"] == "note"
        message = _message(drifted)
        assert "1 changed without moving the cursor" in message
        assert "1 only in the destination (deleted at the source" in message
    finally:
        pg_sql(f"DROP TABLE IF EXISTS {src}")
        pg_sql(f"DROP TABLE IF EXISTS {dst}")


def test_pg_noop_rerun_whose_recipe_has_no_row_image_says_so(store):
    """Re-review LOW: a SQL source was re-read, but a recipe that removes a row has
    no row-for-row image - the sentence says that, not "cannot be re-read"."""
    from services.shape_models import ShapeRecipe
    from tests.c3_live_helpers import pg_endpoint, pg_sql
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not available")
    src, dst = f"d421f_{uuid.uuid4().hex[:8]}", f"d421g_{uuid.uuid4().hex[:8]}"
    pg_sql(f"CREATE TABLE {src} (id INT PRIMARY KEY, name TEXT, updated_at TIMESTAMP)")
    for i in range(1, 11):
        pg_sql(f"INSERT INTO {src} VALUES (%s, %s, %s)", (i, f"n{i}", f"2026-11-01 10:{i:02d}:00"))
    recipe = {"steps": [{"op": "filter_rows", "options": {"condition": "[id] <> 3"}}]}
    cols = ["id", "name", "updated_at"]

    def request():
        return TransferRequest(
            source=pg_endpoint(src), destination=pg_endpoint(dst), sync_mode="incremental_upsert",
            mappings=[{"source": c, "target": c, "confidence": 0.99, "approved": True} for c in cols],
            validation_mode="strict", shape_recipe=recipe,
            approved_shape_recipe_hash=ShapeRecipe.parse(recipe, source_columns=cols).recipe_hash,
            stream_contracts=[{**_CONTRACT, "name": src, "primary_key": ["id"]}],
        )

    try:
        assert _go(store, request()).records_transferred == 9
        quiet = _go(store, request())
        message = _message(quiet)
        assert quiet.records_transferred == 0
        assert (
            "Out-of-window drift was not measured: this route's recipe cannot be re-applied "
            "row for row to the re-read source rows." in message
        ), message
        assert "cannot be re-read for comparison" not in message
    finally:
        pg_sql(f"DROP TABLE IF EXISTS {src}")
        pg_sql(f"DROP TABLE IF EXISTS {dst}")
