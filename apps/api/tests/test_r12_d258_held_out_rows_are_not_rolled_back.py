"""D258: rows held out with a reason were reported as a rolled-back unit.

The job summary read the findings from the saved checkpoint, which the stream
never fills, so every rejected row looked like "refused before any row was
judged": rejected_rows 0, rows_rolled_back N, although nothing was written
and every row had a finding on the DLQ.
"""

from __future__ import annotations

from pathlib import Path

import services.quarantine_dlq as dlq
from tests.test_r12_d258_nothing_written_names_quarantine_reasons import (  # noqa: F401
    _run_all_rows_held_out,
    fake_mongo,
)


def test_held_out_rows_are_quarantined_not_rolled_back(
    tmp_path: Path, fake_mongo, monkeypatch  # noqa: F811
) -> None:
    persisted: list[int] = []
    real = dlq.persist_rejected_rows

    def _count(**kw):
        persisted.append(len(kw.get("rejected_details") or []))
        return real(**kw)

    monkeypatch.setattr(dlq, "persist_rejected_rows", _count)
    result = _run_all_rows_held_out(tmp_path, fake_mongo)

    assert result.success is False
    (job,) = fake_mongo.jobs.values()
    summary = job.get("destination_summary") or {}
    assert summary.get("rejected_rows") == 5, summary
    assert not summary.get("rows_rolled_back"), summary
    assert summary.get("rejected_details_total") == 5, summary
    assert sum(persisted) == 5, persisted
