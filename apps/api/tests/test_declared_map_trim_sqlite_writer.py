"""A trim declared on the request's Map is applied by the writer.

SQLite → SQLite through ``execute_tracked`` with ``{"source": "note",
"target": "note", "transform": "trim"}``: the writer's whitespace guard took
every ``trim`` without ``user_override`` for a name-triggered inference and
stored ``'n5  '`` untrimmed, while Gate-8's read-back compare applied the trim
to the source side and failed the job ("row 5 column note→note: source='n5'
dest='n5  '"). The same trim as a shape-recipe step was applied and verified.

The writer and Gate-8 now share one rule (``effective_cell_transform``): a
trim applies when the operator asked for it, which includes declaring it on
the request's mapping contract. An inferred trim is still skipped, and Gate-8
then expects the untrimmed value.
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

_NOTES = {i: (f"n{i}  " if i % 2 else f"n{i}") for i in range(1, 9)}


@pytest.fixture()
def fake_mongo(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _seed(src: Path) -> None:
    conn = sqlite3.connect(str(src))
    try:
        with conn:
            conn.execute(
                "CREATE TABLE pay (id INTEGER PRIMARY KEY, note TEXT, amount NUMERIC(12,4))"
            )
            conn.executemany(
                "INSERT INTO pay VALUES (?, ?, ?)",
                [(i, note, i * 1.25) for i, note in _NOTES.items()],
            )
    finally:
        conn.close()


def _run(fake, src: Path, dst: Path, mappings: list[dict]):
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="pay"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="pay"),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        skip_preflight=True,
        mappings=mappings,
    )
    job_id = "trim" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _notes(dst: Path) -> dict[int, str]:
    conn = sqlite3.connect(str(dst))
    try:
        return dict(conn.execute("SELECT id, note FROM pay ORDER BY id").fetchall())
    finally:
        conn.close()


_TRIM_MAP = [
    {"source": "id", "target": "id"},
    {"source": "note", "target": "note", "transform": "trim"},
    {"source": "amount", "target": "amount"},
]


@pytest.mark.parametrize(
    "mappings",
    [
        _TRIM_MAP,
        # target_type on every mapping did not change the outcome either.
        [
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "note", "target": "note", "transform": "trim", "target_type": "TEXT"},
            {"source": "amount", "target": "amount", "target_type": "NUMERIC(12,4)"},
        ],
    ],
    ids=["bare", "with_target_type"],
)
def test_declared_map_trim_is_written_and_verified(tmp_path: Path, fake_mongo, mappings):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _seed(src)

    result = _run(fake_mongo, src, dst, mappings)

    assert result.success, result.error
    assert result.records_transferred == len(_NOTES)
    assert _notes(dst) == {i: note.strip() for i, note in _NOTES.items()}


def test_untransformed_note_keeps_its_whitespace(tmp_path: Path, fake_mongo):
    """No transform declared: the column is a byte copy and Gate-8 agrees."""
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _seed(src)

    result = _run(
        fake_mongo,
        src,
        dst,
        [
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
            {"source": "amount", "target": "amount"},
        ],
    )

    assert result.success, result.error
    assert _notes(dst) == _NOTES


# --- the shared rule ---------------------------------------------------------


def test_mark_declared_transforms_records_the_declared_id():
    from services.transform_resolver import (
        DECLARED_TRANSFORM_KEY,
        attach_transforms_to_mappings,
        effective_cell_transform,
        mark_declared_transforms,
    )

    marked = mark_declared_transforms(
        [
            {"source": "note", "target": "note", "transform": "trim"},
            {"source": "code", "target": "code", "transform": "none"},
            {"source": "id", "target": "id"},
        ]
    )
    assert marked[0][DECLARED_TRANSFORM_KEY] == "trim"
    assert DECLARED_TRANSFORM_KEY not in marked[1]
    assert DECLARED_TRANSFORM_KEY not in marked[2]

    # The stamp survives enrichment, which overwrites ``transform``.
    enriched = attach_transforms_to_mappings(
        marked, column_types={"note": "TEXT"}, dest_types={"note": "TEXT"}
    )
    assert enriched[0]["transform"] == "trim"
    assert effective_cell_transform("trim", enriched[0]) == "trim"

    # A declared transform other than trim does not license an inferred trim.
    other = {"source": "k", "target": "k", "transform": "trim_id", DECLARED_TRANSFORM_KEY: "upper"}
    assert effective_cell_transform("trim_id", other) is None


# How the engine calls Gate-8 for this route: typed SQLite fingerprints, which
# keep surrounding whitespace (a bare normalize_cell would hide the difference).
_SQLITE = {"sort_key": "id", "dest_db_type": "sqlite", "dest_types": {"id": "INTEGER", "note": "TEXT"}}


def test_writer_and_gate8_agree_on_an_inferred_trim():
    """A trim nobody asked for: the writer keeps the whitespace and Gate-8
    must expect it — not the trimmed value."""
    from connectors.writer_common import _convert_mapped_cell
    from services.reconciliation import sample_compare_rows

    inferred = {"source": "note", "target": "note", "transform": "trim"}
    stored, err = _convert_mapped_cell(
        "n5  ", "trim", inferred, empty_cells_as_null=False, dest_nullability={}, target="note"
    )
    assert err is None and stored == "n5  "

    source = [{"id": 5, "note": "n5  "}]
    mappings = [{"source": "id", "target": "id"}, inferred]
    assert sample_compare_rows(source, [{"id": 5, "note": "n5  "}], mappings, **_SQLITE)["passed"]
    assert not sample_compare_rows(source, [{"id": 5, "note": "n5"}], mappings, **_SQLITE)["passed"]


def test_writer_and_gate8_agree_on_a_declared_trim():
    from connectors.writer_common import _convert_mapped_cell
    from services.reconciliation import sample_compare_rows
    from services.transform_resolver import mark_declared_transforms

    mappings = mark_declared_transforms(
        [{"source": "id", "target": "id"}, {"source": "note", "target": "note", "transform": "trim"}]
    )
    stored, err = _convert_mapped_cell(
        "n5  ", "trim", mappings[1], empty_cells_as_null=False, dest_nullability={}, target="note"
    )
    assert err is None and stored == "n5"

    source = [{"id": 5, "note": "n5  "}]
    assert sample_compare_rows(source, [{"id": 5, "note": "n5"}], mappings, **_SQLITE)["passed"]
    assert not sample_compare_rows(source, [{"id": 5, "note": "n5  "}], mappings, **_SQLITE)["passed"]
