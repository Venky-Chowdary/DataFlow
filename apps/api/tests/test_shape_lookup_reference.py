"""A1: referenced-table lookup — saved connector rows resolved at run start."""
import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402

import services.lookup_reference as lr  # noqa: E402
from services.connector_store import SavedConnector  # noqa: E402
from services.shape_apply import ShapeRunner  # noqa: E402
from services.shape_models import ShapeError, ShapeRecipe  # noqa: E402


@pytest.fixture()
def ref_db(tmp_path):
    db = tmp_path / "ref.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE countries (code TEXT, name TEXT, risk TEXT)")
    conn.executemany(
        "INSERT INTO countries VALUES (?, ?, ?)",
        [("US", "United States", "low"), ("CN", "China", "high")],
    )
    conn.commit(); conn.close()
    return db


def _connector(db: Path) -> SavedConnector:
    return SavedConnector(
        id="ref-conn-1", name="ref", type="sqlite", role="source",
        database=str(db),
    )


def _recipe(**opt):
    options = {"lookup_key": "code", "map": {"name": "country_name", "risk": "risk"}, **opt}
    return ShapeRecipe.parse(
        {"steps": [{"op": "lookup", "column": "cc", "options": options}]},
        source_columns=["cc"],
    )


def test_reference_resolves_rows_through_saved_connector(ref_db, monkeypatch):
    saved = _connector(ref_db)
    import services.connector_store as cs
    monkeypatch.setattr(cs, "get_connector", lambda cid, workspace_id=None: saved if cid == saved.id else None)
    rows = lr.load_reference_rows(
        {"connector_id": "ref-conn-1", "table": "countries", "columns": ["code", "name", "risk"]}
    )
    assert len(rows) == 2
    assert rows[0]["code"] == "US" and rows[0]["name"] == "United States"


def test_lookup_step_enriches_from_reference(ref_db, monkeypatch):
    saved = _connector(ref_db)
    import services.connector_store as cs
    monkeypatch.setattr(cs, "get_connector", lambda cid, workspace_id=None: saved if cid == saved.id else None)

    recipe = _recipe(
        reference={"connector_id": "ref-conn-1", "table": "countries",
                   "columns": ["code", "name", "risk"]},
        unmatched="divert",
        reason="unknown country",
    )
    runner = ShapeRunner(recipe)
    headers, rows = runner.matrix(["cc"], [["US"], ["CN"], ["XX"]])
    out = [dict(zip(headers, r)) for r in rows]
    assert out[0]["country_name"] == "United States" and out[0]["risk"] == "low"
    assert out[1]["country_name"] == "China" and out[1]["risk"] == "high"
    assert len(out) == 2  # XX diverted
    diverted = runner.take_diverted()
    assert len(diverted) == 1 and "unknown country" in diverted[0].reason


def test_unmatched_default_fills_from_defaults(ref_db, monkeypatch):
    saved = _connector(ref_db)
    import services.connector_store as cs
    monkeypatch.setattr(cs, "get_connector", lambda cid, workspace_id=None: saved if cid == saved.id else None)
    recipe = _recipe(
        reference={"connector_id": "ref-conn-1", "table": "countries",
                   "columns": ["code", "name", "risk"]},
        unmatched="default",
        defaults={"country_name": "UNKNOWN", "risk": "unrated"},
    )
    runner = ShapeRunner(recipe)
    headers, rows = runner.matrix(["cc"], [["XX"]])
    out = dict(zip(headers, rows[0]))
    assert out["country_name"] == "UNKNOWN" and out["risk"] == "unrated"


def test_match_refuse_fails_loud_on_dupes(ref_db, monkeypatch):
    db = ref_db
    conn = sqlite3.connect(str(db)); conn.execute("INSERT INTO countries VALUES ('US', 'Duplicate', 'x')")
    conn.commit(); conn.close()
    saved = _connector(db)
    import services.connector_store as cs
    monkeypatch.setattr(cs, "get_connector", lambda cid, workspace_id=None: saved if cid == saved.id else None)
    recipe = _recipe(
        reference={"connector_id": "ref-conn-1", "table": "countries",
                   "columns": ["code", "name", "risk"]},
        match="refuse",
    )
    runner = ShapeRunner(recipe)
    with pytest.raises(ValueError, match="more than once"):
        runner.matrix(["cc"], [["US"]])


def test_unknown_connector_refuses_named(monkeypatch):
    import services.connector_store as cs
    monkeypatch.setattr(cs, "get_connector", lambda cid, workspace_id=None: None)
    recipe = _recipe(
        reference={"connector_id": "missing", "table": "t", "columns": ["code", "name", "risk"]},
    )
    runner = ShapeRunner(recipe)
    with pytest.raises(lr.LookupReferenceError, match="missing"):
        runner.matrix(["cc"], [["x"]])


@pytest.mark.parametrize("bad,msg", [
    ({"reference": {"table": "t"}, "map": {"a": "b"}, "lookup_key": "k"}, "connector_id"),
    ({"reference": {"connector_id": "c"}, "map": {"a": "b"}, "lookup_key": "k"}, "table"),
    ({"reference": {"connector_id": "c", "table": "t"}, "rows": [], "map": {"a": "b"}, "lookup_key": "k"}, "both"),
    ({"rows": [], "map": {"a": "b"}, "lookup_key": "k"}, "either"),
    ({"rows": [{"k": 1}], "map": {"a": "b"}, "lookup_key": "k", "match": "best"}, "first or refuse"),
    ({"rows": [{"k": 1}], "map": {"a": "b"}, "lookup_key": "k", "unmatched": "default"}, "defaults"),
    ({"reference": {"connector_id": "c", "table": "t", "columns": ["k", "a"]}, "map": {"zzz": "b"}, "lookup_key": "k"}, "not in reference.columns"),
])
def test_lookup_option_validation_refuses(bad, msg):
    with pytest.raises(ShapeError, match=msg):
        ShapeRecipe.parse({"steps": [{"op": "lookup", "column": "k", "options": bad}]})


def test_lookup_reference_end_to_end_destination_readback(ref_db, tmp_path, monkeypatch):
    """Real SQLite->SQLite transfer: looked-up values land; no-match quarantines."""
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    from tests.test_property2_golden_path_never_blocked import _FakeMongo
    import services.connector_store as cs
    from src.transfer.models import EndpointConfig, TransferRequest
    from src.transfer.engine import UniversalTransferEngine
    from services.quarantine_dlq import quarantine_details_from_dlq

    saved = _connector(ref_db)
    monkeypatch.setattr(cs, "get_connector", lambda cid, workspace_id=None: saved if cid == saved.id else None)
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)

    src = tmp_path / "src.db"
    dst = tmp_path / "dst.db"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id TEXT, cc TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [("1", "US"), ("2", "CN"), ("3", "XX")])
    conn.close()

    payload = {"steps": [{
        "op": "lookup",
        "column": "cc",
        "options": {
            "lookup_key": "code",
            "reference": {"connector_id": "ref-conn-1", "table": "countries",
                          "columns": ["code", "name", "risk"]},
            "map": {"name": "country_name", "risk": "risk"},
            "unmatched": "divert",
            "reason": "unknown country",
        },
    }]}
    recipe = ShapeRecipe.parse(payload)
    job_id = "lookup" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
            sync_mode="full_refresh_overwrite",
            shape_recipe=payload,
            approved_shape_recipe_hash=recipe.recipe_hash,
            skip_preflight=True,
            validation_mode="strict",
        ),
        job_id,
    )
    assert result.success, result.error
    conn = sqlite3.connect(str(dst))
    rows = conn.execute("SELECT id, cc, country_name, risk FROM t ORDER BY id").fetchall()
    conn.close()
    assert rows == [
        ("1", "US", "United States", "low"),
        ("2", "CN", "China", "high"),
    ]
    quarantined = quarantine_details_from_dlq(job_id)
    assert any("XX" in str(q.get("row") or "") for q in quarantined)


def test_lookup_reference_rerun_sees_changed_reference_data(ref_db, monkeypatch):
    """Refresh semantics: the reference is read at run start — a rerun sees edits."""
    saved = _connector(ref_db)
    import services.connector_store as cs
    monkeypatch.setattr(cs, "get_connector", lambda cid, workspace_id=None: saved if cid == saved.id else None)
    recipe = _recipe(
        reference={"connector_id": "ref-conn-1", "table": "countries",
                   "columns": ["code", "name", "risk"]},
    )
    headers, rows = ShapeRunner(recipe).matrix(["cc"], [["US"]])
    assert dict(zip(headers, rows[0]))["country_name"] == "United States"

    conn = sqlite3.connect(str(ref_db))
    with conn:
        conn.execute("UPDATE countries SET name='USA' WHERE code='US'")
    conn.close()

    headers, rows = ShapeRunner(recipe).matrix(["cc"], [["US"]])
    assert dict(zip(headers, rows[0]))["country_name"] == "USA"


def test_parse_refuses_lookup_key_outside_reference_columns():
    """A1-D1: lookup_key 'code' with columns [name, risk] used to parse and
    silently NULL every row."""
    with pytest.raises(ShapeError, match="lookup_key 'code' is not in reference.columns"):
        _recipe(
            reference={
                "connector_id": "ref-conn-1",
                "table": "countries",
                "columns": ["name", "risk"],
            }
        )


def test_parse_allows_lookup_key_when_columns_list_is_omitted():
    recipe = _recipe(
        reference={"connector_id": "ref-conn-1", "table": "countries"}
    )
    assert recipe.steps[0].options["reference"]["columns"] is None
