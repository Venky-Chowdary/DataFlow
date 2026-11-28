"""D429 — the capabilities catalog and plan_transfer use one sync-mode vocabulary.

QA rounds 66-75: GET /api/v1/transfer/capabilities lists ``incremental_deduped``
for 36 of 37 destinations, yet plan_transfer refused it in 0.6 s: "Unknown
sync_mode 'incremental_deduped' — I will not guess the load semantics. Valid
modes: cdc_incremental, full_refresh_append, ...". The "Valid modes" list also
omitted mirror, scd2 and cdc, which the planner does plan.

Root cause: the Pilot's strict check knew only its own spellings
(``SYNC_MODES`` and a phrase table); the catalog, the schedule store and the
engine speak ``services.sync_cursor``'s canonical names. The planner now
accepts every published canonical name as the same mode, echoes it as
``sync_mode_canonical``, and a refusal lists every mode it accepts with its
published alias.
"""

from __future__ import annotations

import sqlite3

import pytest

from services import connector_store
from src.ai.copilot import tools as tools_mod


@pytest.mark.parametrize("dest", ["postgresql", "sqlserver", "mysql", "oracle", "snowflake", "s3"])
def test_every_published_mode_is_accepted_by_the_planner(dest):
    from services.destination_sync_modes import sync_modes_for_destination
    from services.sync_cursor import normalize_sync_mode as canonical
    from src.ai.copilot.transfer_tools import normalize_sync_mode

    published = sync_modes_for_destination(dest)
    assert published
    for mode in published:
        pilot = normalize_sync_mode(mode, strict=True)
        assert canonical(pilot, default="") == mode, (dest, mode, pilot)


def test_aliases_resolve_to_one_mode():
    from src.ai.copilot.transfer_tools import normalize_sync_mode

    assert normalize_sync_mode("incremental_deduped", strict=True) == "incremental_upsert"
    assert normalize_sync_mode("incremental_upsert", strict=True) == "incremental_upsert"
    assert normalize_sync_mode("cdc", strict=True) == "cdc_incremental"
    assert normalize_sync_mode("cdc_incremental", strict=True) == "cdc_incremental"
    assert normalize_sync_mode("mirror", strict=True) == "mirror"
    assert normalize_sync_mode("scd2", strict=True) == "scd2"


def test_unknown_mode_refusal_lists_every_accepted_mode_and_alias():
    from src.ai.copilot.transfer_tools import normalize_sync_mode

    with pytest.raises(ValueError) as refused:
        normalize_sync_mode("teleport_mode", strict=True)
    message = str(refused.value)
    assert "Unknown sync_mode 'teleport_mode'" in message
    for name in ("mirror", "scd2", "cdc_incremental (alias cdc)",
                 "incremental_upsert (alias incremental_deduped)", "upsert",
                 "full_refresh_overwrite", "full_refresh_append", "incremental_append"):
        assert name in message, (name, message)


def test_a_destructive_alias_is_not_let_in_by_the_catalog_path():
    """Only canonical names are taken as written; aliases keep the phrase rules."""
    from src.ai.copilot.transfer_tools import _catalog_sync_mode

    assert _catalog_sync_mode("truncate") == ""
    assert _catalog_sync_mode("reverse_etl") == ""


@pytest.fixture()
def sqlite_route(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    connector_store._backend_choice = None
    tools_mod._tools = None
    names = []
    for role in ("src", "dst"):
        db = tmp_path / f"d429_{role}.db"
        conn = sqlite3.connect(db)
        if role == "src":
            conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, sku TEXT, updated_at TEXT)")
            conn.executemany(
                "INSERT INTO orders VALUES (?, ?, ?)",
                [(i, f"s{i}", f"2024-01-01T{i:02d}:00:00") for i in range(1, 6)],
            )
            conn.commit()
        conn.close()
        saved = connector_store.create_connector({
            "name": f"D429{role}", "type": "sqlite", "role": "both",
            "connection_string": f"sqlite:///{db.resolve().as_posix()}", "workspace_id": "",
        })
        names.append(saved.name)
    return names


def test_plan_transfer_accepts_incremental_deduped_and_echoes_the_canonical_name(sqlite_route):
    from src.ai.copilot.transfer_tools import plan_transfer

    res = plan_transfer(
        source_connector_name=sqlite_route[0], source_table="orders",
        dest_connector_name=sqlite_route[1], dest_table="orders_out",
        sync_mode="incremental_deduped", upsert_key="id", cursor_column="updated_at",
    )
    assert res.success, res.error
    assert "Unknown sync_mode" not in str(res.error or "")
    out = res.output
    # One mode, whichever spelling the plan carries forward.
    assert out["sync_mode"] in {"incremental_upsert", "incremental_deduped"}
    assert out["sync_mode_canonical"] == "incremental_deduped"
    assert out["data_rules"]["cursor_column"] == "updated_at"
    assert out["sync_mode_requested"] == "incremental_deduped"


def test_studio_picker_uses_the_canonical_names_the_planner_accepts():
    """AC-3: the web picker's ids are the published canonical names, each planned."""
    import re
    from pathlib import Path

    from services.destination_sync_modes import SYNC_MODE_IDS
    from src.ai.copilot.transfer_tools import normalize_sync_mode

    ts = (Path(__file__).resolve().parents[2] / "web" / "src" / "lib" / "transferConstants.ts")
    text = ts.read_text(encoding="utf-8")
    union = re.search(r"export type SyncModeId =([^;]+);", text)
    assert union, "SyncModeId union not found"
    picker = re.findall(r'"([a-z0-9_]+)"', union.group(1))
    assert tuple(picker) == SYNC_MODE_IDS
    for mode in picker:
        normalize_sync_mode(mode, strict=True)
