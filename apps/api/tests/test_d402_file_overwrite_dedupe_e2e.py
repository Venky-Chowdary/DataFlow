"""D402 — a file overwrite collapses duplicates only when dedupe_key is declared.

The same SFTP file as the QA check 6 e2e: ids 1-10 repeat, 50 unique keys.
Through ``start_transfer`` (plan, confirm, job) into PostgreSQL:

* ``full_refresh_overwrite`` + ``dedupe_key=id`` lands one row per key, the
  last copy in file order wins, and the plan discloses the collapse (R4).
* ``full_refresh_overwrite`` + ``primary_key=id`` alone still refuses: a key
  names the identity, it does not ask for copies to be dropped. Collapsing
  them anyway is a silent dedupe that R4 forbids (see
  test_upsert_infile_duplicate_start_transfer_e2e).
"""

from __future__ import annotations

from tests.test_upsert_infile_duplicate_start_transfer_e2e import (  # noqa: F401
    _connectors,
    _require_postgres,
    _stage_file,
    _wait_job,
    transfer_client,
)


def test_overwrite_with_dedupe_key_keeps_the_last_copy_and_says_so(
    transfer_client, local_sftp
) -> None:
    _stage_file(local_sftp)
    table = "qa_d402_dedupe_overwrite"
    pg = _require_postgres()
    try:
        with pg.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.qa_d402_dedupe_overwrite")
    finally:
        pg.close()

    from src.ai.copilot.transfer_tools import start_transfer

    source_id, dest_id = _connectors(local_sftp)
    staged = start_transfer(
        source_connector_id=source_id,
        source_table="dups.csv",
        dest_connector_id=dest_id,
        dest_table=table,
        sync_mode="full_refresh_overwrite",
        dedupe_key="id",
        schema_policy="propagate_columns",
        validation_mode="strict",
    )
    assert staged.success, staged.error
    plan = staged.output.get("plan") or {}
    assert plan.get("in_file_dedupe") == "keep_last", plan.keys()
    assert plan.get("dedupe_key") == "id"
    assert "last row" in str(plan.get("in_file_dedupe_note") or "")

    confirmed = transfer_client.post(
        "/api/v1/copilot/confirm",
        json={"ack_id": staged.output["ack_id"], "reason": "d402"},
    )
    assert confirmed.status_code == 200, confirmed.text
    job_id = str(confirmed.json().get("job_id") or "")
    assert job_id, confirmed.json()
    job = _wait_job(job_id)
    assert str(job.get("status") or "") == "completed", {
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
    }

    pg = _require_postgres()
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT id, name FROM public.qa_d402_dedupe_overwrite ORDER BY id")
            rows = [(int(r[0]), str(r[1])) for r in cur.fetchall()]
            cur.execute("DROP TABLE IF EXISTS public.qa_d402_dedupe_overwrite")
    finally:
        pg.close()
    assert [r[0] for r in rows] == list(range(1, 51))
    assert dict(rows[:10]) == {i: f"b{i}" for i in range(1, 11)}
    assert dict(rows[10:]) == {i: f"a{i}" for i in range(11, 51)}
