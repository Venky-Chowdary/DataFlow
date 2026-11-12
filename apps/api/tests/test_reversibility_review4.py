"""Review 4 of the reversibility proof: what is enforced while writing is enforced on every written row.

H-1: a CSV file into a database took the local CSV COPY fast path, which never
surfaces a value to the per-value check, and the run still read "enforced"
with ``rows_checked: 0``. The fast path now declines when a column is
enforced, and a run whose enforced column was checked on fewer rows than it
wrote fails with a clear reason (the backstop), never "enforced".
"""

from __future__ import annotations

import re

from tests.test_reversibility_review3 import _q, _run, fake  # noqa: F401


def _csv_request(dst, body: bytes, **kw):
    from src.transfer.models import EndpointConfig, TransferRequest

    base = dict(
        source=EndpointConfig(kind="file", format="csv"), source_filename="t.csv", source_content=body,
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite", validation_mode="strict", skip_preflight=False,
        stream_contracts=[{"name": "t", "primary_key": ["id"], "selected": True}],
        require_reversibility=True, compliance_acknowledged=True,
        acknowledgment_actor="owner@example.com", acknowledgment_reason="reviewed run of the reversibility gate")
    base.update(kw)
    return TransferRequest(**base)


def _dates(last: str, n: int = 400) -> bytes:
    lines = ["id,v"] + [f"{i},2024-01-{i % 28 + 1:02d}" for i in range(1, n)] + [f"{n},{last}"]
    return ("\n".join(lines) + "\n").encode()


def test_h1_a_csv_file_past_the_sample_is_checked_on_every_row_not_copied(tmp_path, fake):  # noqa: F811
    dst = tmp_path / "d.sqlite"
    res = _run(fake, _csv_request(dst, _dates("2024-1-5")))
    ds = res.destination_summary or {}
    assert res.success is False, (ds.get("copy_fast_path"), ds.get("reversibility_enforcement"))
    assert "v" in str(res.error) and "reversib" in str(res.error).lower(), res.error
    assert "2024-1-5" not in str(res.error)
    assert ds.get("copy_fast_path") != "used"
    rows = _q(dst, "SELECT name FROM sqlite_master WHERE name='t'") and _q(dst, "SELECT count(*) FROM t")
    assert not rows or rows[0][0] == 0, rows


def test_h1_an_honest_csv_file_is_enforced_on_every_row_and_the_copy_says_why_it_declined(tmp_path, fake):  # noqa: F811
    dst = tmp_path / "d.sqlite"
    res = _run(fake, _csv_request(dst, _dates("2024-01-05")))
    ds = res.destination_summary or {}
    assert res.success is True, res.error
    assert ds.get("copy_fast_path") != "used"
    assert "reversibility is required" in " ".join(ds.get("copy_decline_reasons") or [ds.get("copy_decline_reason")
                                                                                       or ""])
    enforcement = ds.get("reversibility_enforcement") or {}
    assert enforcement.get("enforced") is True
    assert [c["rows_checked"] for c in enforcement["columns"]] == [400]


def test_h1_backstop_a_run_that_wrote_rows_its_enforced_column_never_checked_fails(tmp_path, fake, monkeypatch):  # noqa: F811
    # A bulk path that does not decline (here: the decline forced off) must not read "enforced".
    import services.reversibility_enforce as enforce

    real = enforce.enforced_columns
    monkeypatch.setattr(enforce, "enforced_columns", lambda m: [] if m else real(m))
    dst = tmp_path / "d.sqlite"
    # COPY writes every row; its post-write re-read meets the changed value and stops checking there.
    res = _run(fake, _csv_request(dst, _dates("2024-1-5")))
    ds = res.destination_summary or {}
    assert ds.get("copy_fast_path") == "used", ds.get("copy_decline_reasons")
    assert res.success is False
    assert re.search(r"t\.v was checked on only \d+ of 400 written row\(s\)", str(res.error)), res.error
    assert "2024-1-5" not in str(res.error)
    assert (ds.get("reversibility_enforcement") or {}).get("enforced") is not True
    job = fake.jobs[res.job_id]
    assert job.get("status") == "failed"


def test_h1_record_on_success_never_reports_a_shortfall_as_enforced():
    from services import reversibility_enforce as enforce

    mapping = {"source": "v", "target": "v"}
    assert enforce.stamp(mapping, table="t", column="v", code="normalize", job_id="h1job")
    enforce.note_rows_checked([mapping], 1, 10)
    fields = {"records_processed": 400, "destination_summary": {}}
    note = enforce.record_on_success("h1job", fields)
    found = fields["destination_summary"][enforce.SUMMARY_KEY]
    assert found["enforced"] is False and "t.v" in found["reason"] and "10 of 400" in found["reason"]
    assert note and "10 of 400" in note
    # Every written row checked: enforced.
    assert enforce.stamp(mapping, table="t", column="v", code="normalize", job_id="h1ok")
    enforce.note_rows_checked([mapping], 1, 400)
    ok = {"records_processed": 400, "destination_summary": {}}
    assert enforce.record_on_success("h1ok", ok) is None
    assert ok["destination_summary"][enforce.SUMMARY_KEY]["enforced"] is True
