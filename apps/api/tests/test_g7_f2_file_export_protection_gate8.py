"""G7-F2: a Studio file_export with "Protect against spreadsheet formulas" on.

New Studio jobs now send ``formula_protection: "prefix"`` by default. This
proves, with real engine jobs, that the protected CSV and TSV carry the ``'``
prefix where a spreadsheet would execute a cell, leave numerals alone, and that
Gate-8 restores the reversible prefix and stays green. A saved job without the
field keeps the API default (``off``, byte-exact).
"""

from __future__ import annotations

import csv
import io
import uuid
from pathlib import Path

import pytest

SOURCE = b'id,note,qty\n1,=1+1,-2\n2,@x,3\n3,-2,-2\n4,\'=q,0\n5,plain,1\n'


def _run(monkeypatch, tmp_path, fmt: str, extra: dict):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig.from_dict("file_export", {"format": fmt, **extra}),
        source_filename="g7f2.csv",
        source_content=SOURCE,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    body = Path(result.destination_summary["path"]).read_bytes().decode("utf-8")
    rows = list(csv.reader(io.StringIO(body, newline=""), delimiter="\t" if fmt == "tsv" else ","))
    return result, rows


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
def test_protected_file_export_is_prefixed_and_gate8_green(monkeypatch, tmp_path, fmt):
    result, rows = _run(monkeypatch, tmp_path, fmt, {"formula_protection": "prefix"})
    by_id = {r[0]: r for r in rows[1:]}
    assert by_id["1"][1:] == ["'=1+1", "-2"]
    assert by_id["2"][1] == "'@x"
    assert by_id["3"][1:] == ["-2", "-2"]  # numerals are never prefixed
    assert by_id["4"][1] == "''=q"  # reversible: one more quote
    assert by_id["5"][1] == "plain"
    recon = result.reconciliation or {}
    assert recon.get("passed") is True, recon
    assert result.destination_summary.get("formula_protection") == "prefix"


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
def test_saved_job_without_the_field_stays_byte_exact(monkeypatch, tmp_path, fmt):
    result, rows = _run(monkeypatch, tmp_path, fmt, {})
    assert [r[1] for r in rows[1:]] == ["=1+1", "@x", "-2", "'=q", "plain"]
    assert (result.reconciliation or {}).get("passed") is True
