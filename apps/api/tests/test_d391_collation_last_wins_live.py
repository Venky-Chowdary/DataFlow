"""D391 — keys equal under the destination collation: the last source row wins.

QA round 7: a single-batch MySQL upsert of ``a/v1, A/v2, a/v3`` stored
``('a','v2')``. Cause: the shared in-batch dedupe kept the last copy of the
exact key ``a`` but at the position of its *first* copy, so the batch reached
MySQL as ``a/v3, A/v2`` and ``A/v2`` overwrote the last row (rule R4, last
occurrence in source order wins). The transform proof then failed and the
partial batch was removed (R9: never drop a correct write on a proof false
positive), and its local fold missed destination-equal pairs (``Œ``/``OE``,
``Æ``/``AE``, ``Ø``/``O``, a zero-width space), so even a correct write failed.

Rule R5: equality is the destination's. The stored *spelling* of a
collation-equal key is the engine's (MySQL keeps the first inserted text and an
upsert never rewrites key columns); the proof reads that spelling back from the
destination instead of guessing it. The *values* are the last source row's.

Live: a CSV file (in_file_dedupe keep_last), MySQL destination at three collations, through the engine.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_d325_mixed_pipeline_map_transform_proof import _run, fake_jobs  # noqa: E402,F401

pytestmark = pytest.mark.timeout(600)

_ROWS = [
    ("a", "v1"), ("A", "v2"), ("a", "v3"),
    ("Æ", "x1"), ("AE", "x2"),
    ("Ø", "y1"), ("O", "y2"),
    ("Þ", "z1"), ("TH", "z2"),
    ("b​", "w1"), ("b", "w2"),
    ("ss", "s1"), ("ß", "s2"),
    ("Œ", "o1"), ("OE", "o2"),
]

# Groups each collation calls one key (observed on MySQL 8.0), and the value
# the last source row of the group carries.
_GROUPS = {
    "utf8mb4_0900_ai_ci": [
        ({"a", "A"}, "v3"), ({"Æ", "AE"}, "x2"), ({"Ø", "O"}, "y2"), ({"Þ"}, "z1"),
        ({"TH"}, "z2"), ({"b​", "b"}, "w2"), ({"ss", "ß"}, "s2"), ({"Œ", "OE"}, "o2"),
    ],
    "utf8mb4_unicode_ci": [
        ({"a", "A"}, "v3"), ({"Æ"}, "x1"), ({"AE"}, "x2"), ({"Ø"}, "y1"), ({"O"}, "y2"),
        ({"Þ"}, "z1"), ({"TH"}, "z2"), ({"b​", "b"}, "w2"), ({"ss", "ß"}, "s2"),
        ({"Œ", "OE"}, "o2"),
    ],
    "utf8mb4_general_ci": [
        ({"a", "A"}, "v3"), ({"Æ"}, "x1"), ({"AE"}, "x2"), ({"Ø"}, "y1"), ({"O"}, "y2"),
        ({"Þ"}, "z1"), ({"TH"}, "z2"), ({"b​"}, "w1"), ({"b"}, "w2"), ({"ss"}, "s1"),
        ({"ß"}, "s2"), ({"Œ"}, "o1"), ({"OE"}, "o2"),
    ],
}


def _csv() -> bytes:
    import csv
    import io

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\r\n")
    writer.writerow(["k", "v"])
    writer.writerows(_ROWS)
    return buf.getvalue().encode("utf-8")


def _request(dst: str) -> TransferRequest:
    """A CSV upsert with the declared in-file dedupe (keep the last line)."""
    from services.column_transform_chain import compile_column_chains, split_column_transforms
    from services.shape_models import ShapeRecipe
    from tests.typed_fidelity_helpers import mysql_endpoint

    _singles, chains = split_column_transforms({"v": ["trim", "lower"]})
    recipe, refusal = compile_column_chains(chains, ["k", "v"])
    assert not refusal, refusal
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv", extra={"in_file_dedupe": "keep_last", "dedupe_key": "k"}),
        destination=mysql_endpoint(dst),
        source_filename="keys.csv",
        source_content=_csv(),
        mappings=[
            {"source": "k", "target": "k", "approved": True, "confidence": 0.99},
            {"source": "v", "target": "v", "approved": True, "confidence": 0.99},
        ],
        sync_mode="incremental_upsert",
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        stream_contracts=[
            {"name": dst, "primary_key": ["k"], "sync_mode": "incremental_upsert", "selected": True}
        ],
    )


def _sqlite_request(tmp_path, dst: str) -> TransferRequest:
    """The same rows read from a table: the proof can re-read this source."""
    import dataclasses
    import sqlite3

    src = tmp_path / "src.db"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (k TEXT, v TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", _ROWS)
    conn.close()
    return dataclasses.replace(
        _request(dst),
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        source_filename="",
        source_content=b"",
        skip_preflight=True,
    )


def _create(h, dst: str, collation: str) -> None:
    h.execute(
        "mysql",
        f"CREATE TABLE `{dst}` (k varchar(20) CHARACTER SET utf8mb4 COLLATE {collation} "
        "PRIMARY KEY, v varchar(20))",
    )


@pytest.mark.parametrize("collation", sorted(_GROUPS))
def test_validate_refuses_keys_the_destination_calls_equal(
    fake_jobs, collation  # noqa: F811
):
    """D205 / R8: distinct source keys one destination key are named up front."""
    from tests import r4_route_harness as h

    h.dest_config("mysql")  # skips when MySQL is absent
    dst = "d391v_" + uuid.uuid4().hex[:8]
    _create(h, dst, collation)
    try:
        result = _run(fake_jobs, _request(dst))
        stored = h.query("mysql", f"SELECT k FROM `{dst}`")  # nosec B608
    finally:
        h.drop_table("mysql", dst)
    assert not result.success
    assert "Duplicate identity keys" in str(result.error or ""), result.error
    assert stored == []


@pytest.mark.parametrize("collation", sorted(_GROUPS))
def test_collisions_past_the_sample_keep_the_last_row_and_the_proof_agrees(
    tmp_path, fake_jobs, collation, monkeypatch  # noqa: F811
):
    """The write path: collisions Validate did not see (preflight skipped here).

    The last source row of each destination key wins, and the transform proof
    verifies that write (it used to fail it, and the batch was removed).
    """
    import src.transfer.reconcile_step as rs
    from tests import r4_route_harness as h

    h.dest_config("mysql")  # skips when MySQL is absent
    dst = "d391l_" + uuid.uuid4().hex[:8]
    _create(h, dst, collation)
    original = rs._transform_applied_proof
    seen: dict = {}

    def _spy(**kwargs):
        seen["stored"] = [tuple(r) for r in h.query("mysql", f"SELECT k, v FROM `{dst}`")]  # nosec B608
        seen["proof"] = original(**kwargs)
        return seen["proof"]

    monkeypatch.setattr(rs, "_transform_applied_proof", _spy)
    try:
        result = _run(fake_jobs, _sqlite_request(tmp_path, dst))
    finally:
        h.drop_table("mysql", dst)

    by_value = {v: k for k, v in seen.get("stored") or []}
    want = _GROUPS[collation]
    # One row per destination key, holding the last source row's value, under
    # one of the group's own spellings.
    assert sorted(by_value) == sorted(v for _g, v in want), seen.get("stored")
    for spellings, value in want:
        assert by_value[value] in spellings, (value, seen.get("stored"))
    proof = seen.get("proof") or {}
    assert proof.get("status") == "verified", proof
    assert "Transform-applied proof failed" not in str(result.error or "")


def test_the_dedupe_keeps_each_survivor_where_its_last_copy_was() -> None:
    """Unit: the batch order is the order of each key's final image."""
    from connectors.writer_common import dedupe_rows_keeping_numbers

    rows = [("a", "v1"), ("A", "v2"), ("a", "v3"), ("b", "w1")]
    kept, numbers = dedupe_rows_keeping_numbers(rows, ["k"], ["k", "v"], [1, 2, 3, 4])
    assert kept == [("A", "v2"), ("a", "v3"), ("b", "w1")]
    assert numbers == [2, 3, 4]
