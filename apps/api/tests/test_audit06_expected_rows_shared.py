"""Audit finding 6 (H4a) — one expected-rows function for Gate-8, ladder L1 and conservation.

``reconcile()`` subtracted the rows a declared source filter (or a recipe)
removed on the read; verification ladder L1 had no such term. Any DB→DB load
with a ``source_filter`` therefore passed Gate-8's digest and failed L1
("expected 10, target 5") — a correct run reported failed, which made the
filter unusable. The three places now share
``services.expected_rows.expected_destination_rows``.
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
from services.expected_rows import expected_destination_rows  # noqa: E402
from services.reconciliation import reconcile  # noqa: E402
from services.row_conservation import DEST_READBACK, account_population  # noqa: E402
from services.verification_ladder import (  # noqa: E402
    layer_l1_row_balance,
    run_five_layer_verification,
)
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_CASES = [
    # source, rejected, coerced, skipped, shaped_out, filtered, expanded, retained
    (10, 0, 0, 0, 0, 5, 0, 0),
    (10, 2, 1, 1, 0, 3, 0, 0),
    (10, 0, 0, 0, 4, 0, 0, 0),
    (10, 0, 0, 0, 2, 3, 6, 0),
    (10, 3, 3, 0, 0, 0, 0, 0),
    (100, 7, 2, 4, 9, 11, 13, 0),
]


def _terms(case):
    source, rejected, coerced, skipped, shaped, filtered, expanded, retained = case
    return dict(
        rejected_rows=rejected, coerced_null_rows=coerced, rows_skipped=skipped,
        rows_shaped_out=shaped, rows_source_filtered=filtered, rows_expanded=expanded,
        retained_rows=retained,
    )


def test_expected_rows_terms():
    assert expected_destination_rows(10, rows_source_filtered=5) == 5
    # Coerced-to-NULL rows landed; only held-out rows lower the count.
    assert expected_destination_rows(10, rejected_rows=3, coerced_null_rows=3) == 10
    assert expected_destination_rows(10, rejected_rows=3, coerced_null_rows=1) == 8
    assert expected_destination_rows(10, rows_expanded=6, rows_shaped_out=2) == 14
    assert expected_destination_rows(10, retained_rows=2) == 12
    assert expected_destination_rows(2, rows_source_filtered=5) == 0
    assert expected_destination_rows(2, rows_source_filtered=5, clamp=False) == -3


@pytest.mark.parametrize("case", _CASES)
def test_gate8_l1_and_conservation_expect_the_same_row_count(case):
    """A destination holding exactly the expected rows passes all three; one short fails all three."""
    source = case[0]
    terms = _terms(case)
    expected = expected_destination_rows(source, **terms)
    conservation_terms = {k: v for k, v in terms.items() if k != "retained_rows"}

    for target, ok in ((expected, True), (expected + 1, False)):
        gate8 = reconcile(
            source_rows=source, target_rows=target,
            source_checksum="a" * 64, target_checksum="a" * 64, **terms,
        )
        l1 = layer_l1_row_balance(source_rows=source, target_rows=target, **terms)
        ledger = account_population(
            rows_read=source, dest_count=target, dest_count_source=DEST_READBACK,
            dest_count_before=None, writer_ack=None,
            sync_mode="full_refresh_overwrite", **conservation_terms,
        )
        assert l1.details["expected_rows"] == expected
        assert l1.passed is ok, (case, target)
        assert gate8.passed is ok, (case, target, gate8.message)
        assert ledger.balanced is ok, (case, target, ledger.note)


def test_l1_equation_names_the_rows_removed_on_the_read():
    l1 = layer_l1_row_balance(source_rows=10, target_rows=5, rows_source_filtered=5)
    assert l1.passed is True and l1.population_proof is True
    assert l1.details["rows_source_filtered"] == 5
    assert "removed_on_read" in l1.details["equation"]
    # Without removed / expanded rows the identity reads as it always did.
    plain = layer_l1_row_balance(source_rows=10, target_rows=10)
    assert plain.details["equation"] == "source - dropped - skipped == target"


def test_ladder_passes_l1_for_a_correct_filtered_population():
    rows = [{"id": i, "v": f"v{i}"} for i in range(1, 6)]
    ladder = run_five_layer_verification(
        source_rows=rows, target_rows=[dict(r) for r in rows], columns=["id", "v"],
        pk_column="id", source_row_count=10, target_row_count=5, rows_source_filtered=5,
        dest_db_type="sqlite", dest_types={"id": "INTEGER", "v": "TEXT"},
    )
    assert ladder["layers"]["L1"]["passed"] is True
    assert ladder["passed"] is True


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


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "full_refresh_append", "upsert"])
def test_filtered_db_to_db_load_passes_l1(mode, tmp_path, isolated):
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, region TEXT, v TEXT)")
    conn.executemany(
        "INSERT INTO t VALUES (?, ?, ?)",
        [(i, "EU" if i % 2 else "US", f"v{i}") for i in range(1, 11)],
    )
    conn.commit()
    conn.close()
    contract = {"name": "t", "sync_mode": mode, "selected": True}
    if mode == "upsert":
        contract["primary_key"] = ["id"]
    job_id = "a06" + uuid.uuid4().hex[:12]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode=mode,
        stream_contracts=[contract],
        source_filter={"column": "region", "op": "eq", "value": "EU"},
        skip_preflight=True,
        validation_mode="strict",
    )

    result = UniversalTransferEngine().execute_tracked(request, job_id)

    assert result.success, result.error
    recon = result.reconciliation or {}
    assert recon.get("passed") is True
    l1 = ((recon.get("verification_ladder") or {}).get("layers") or {}).get("L1") or {}
    if l1:
        assert l1["passed"] is True
        assert l1["details"]["expected_rows"] == 5 and l1["details"]["target_rows"] == 5
    landed = sqlite3.connect(dst).execute("SELECT region, COUNT(*) FROM t GROUP BY 1").fetchall()
    assert landed == [("EU", 5)]
