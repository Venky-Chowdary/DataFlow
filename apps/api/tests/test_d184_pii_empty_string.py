"""D184 - PII masking must not block on an empty string.

``mask_pii`` and ``hash_pii`` sat outside the identity/text transforms that keep
``''``, so apply_transform answered "Empty value cannot coerce to mask_pii" for a
cell that holds nothing to mask. Validate sampled the cell, blocked on the
failure, and the write quarantined it. An empty string masks to an empty string
(there is no PII in it to protect, and an empty cell must stay distinguishable
from a masked one); SQL NULL stays NULL. ``redact`` already stamped its token on
every non-null cell and is pinned here beside the other two.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

import src.transfer.engine as engine_mod
from services.transform_engine import REDACTED_PLACEHOLDER, apply_transform
from services.value_serializer import SQL_NULL_SENTINEL
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

_TRANSFORMS = ("mask_pii", "hash_pii", "redact")


@pytest.fixture(autouse=True)
def _hash_key(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "d184-test-key")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")


@pytest.mark.parametrize("transform", ["mask_pii", "hash_pii"])
@pytest.mark.parametrize("empty", ["", "   "])
def test_empty_cell_masks_to_empty_not_an_error(transform, empty):
    assert apply_transform(empty, transform) == ("", None)


@pytest.mark.parametrize("transform", _TRANSFORMS)
def test_null_stays_null_and_is_distinct_from_empty(transform):
    assert apply_transform(SQL_NULL_SENTINEL, transform) == (None, None)
    empty, err = apply_transform("", transform)
    assert err is None and empty is not None


@pytest.mark.parametrize("transform", ["mask_pii", "hash_pii"])
def test_non_empty_cells_still_mask(transform):
    out, err = apply_transform("a@b.com", transform)
    assert err is None and out and out != "a@b.com"


@pytest.mark.parametrize("transform", _TRANSFORMS)
def test_preflight_passes_a_sample_with_empty_strings(transform):
    from services.preflight_service import run_file_preflight

    rows = [{"email": "a@b.com"}, {"email": ""}, {"email": None}, {"email": "c@d.org"}]
    result = run_file_preflight(
        columns=["email"],
        column_types={"email": "VARCHAR"},
        row_count=len(rows),
        mappings=[
            {"source": "email", "target": "email", "confidence": 0.99,
             "transform": transform, "target_type": "VARCHAR"}
        ],
        destination_connected=True,
        sample_rows=rows,
        destination_column_types={"email": "VARCHAR"},
        destination_table_exists=True,
        destination_db_type="sqlite",
        skip_population_fit=True,
    )
    blockers = [b.get("message") for b in result.get("blockers") or []]
    assert not blockers, blockers


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.mark.parametrize("transform", _TRANSFORMS)
def test_engine_loads_a_column_with_empty_strings_and_nulls(tmp_path, isolated, transform):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(str(src))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)")
    conn.executemany(
        "INSERT INTO t VALUES (?, ?)",
        [(1, "a@b.com"), (2, ""), (3, None), (4, "c@d.org"), (5, "")],
    )
    conn.commit()
    conn.close()
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "email", "target": "email", "confidence": 0.99, "transform": transform},
        ],
    )
    job_id = "d184" + uuid.uuid4().hex[:12]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error

    out = dict(sqlite3.connect(str(dst)).execute("SELECT id, email FROM t").fetchall())
    assert len(out) == 5
    assert out[3] is None
    if transform == "redact":
        assert out[2] == out[5] == REDACTED_PLACEHOLDER
    else:
        assert out[2] == out[5] == ""
    assert out[1] not in ("a@b.com", None, "")
    assert out[4] not in ("c@d.org", None, "")
