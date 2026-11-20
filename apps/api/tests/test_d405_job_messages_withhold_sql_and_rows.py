"""D405: a failed job's message never carries SQL parameters, row data or a raw driver dump.

Round 47: a PostgreSQL -> SQL Server incremental_upsert failed with the whole
statement and several full rows on the job (``[SQL: UPDATE ...] [parameters:
[{_k_id: 5121, _v_name: BIG_5121, ...}]]``). Round 49: a resume failure
carried pymssql's DB-Lib text with the host it dialled. eaf74d1e2 classified
one poll path (the keyed no-op re-read); every other writer still stored
``str(exc)``: ``background._run_transfer`` (and its failure e-mail), the
connectors-router replay child, the exit-job guard, the engine's own
``error=str(exc)`` writes, lineage ``run_failed``.

The fix is at the sinks every writer goes through: the job store's
``update_job_status`` / ``update_job_fields`` (memory and Mongo), the
notification payload and the lineage event. The text keeps the classified
cause, the driver error code, column and key names; the statement, its
parameters, a failing-row image and the DB-Lib dump are withheld, and the
original goes to the restricted server log under a reference the message
names (AC-2). Synthetic values only.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from services.job_message_redaction import redact_driver_text, redact_job_update

ROW_VALUE = "BIG_5121"
SQLA_MSSQL = (
    "(pymssql._pymssql.OperationalError) (207, b\"Invalid column name 'long_txt'."
    "DB-Lib error message 20018, severity 16:\\nGeneral SQL Server error: Check messages "
    "from the SQL Server\\n\")\n"
    "[SQL: UPDATE [dbo].[r47_big_ms] SET [name] = %(_v_name)s WHERE [id] = %(_k_id)s]\n"
    "[parameters: [{'_k_id': 5121, '_v_name': 'BIG_5121', '_v_amount': Decimal('12.50'), "
    "'_v_payload': '{\"a\": [1, \"]\"]}'}, {'_k_id': 5122, '_v_name': 'BIG_5122'}]]\n"
    "(Background on this error at: https://sqlalche.me/e/20/e3q8)"
)
DBLIB_CONNECT = (
    "(20009, b'DB-Lib error message 20009, severity 9:\\nUnable to connect: Adaptive Server "
    "is unavailable or does not exist (tunnel-7.internal.example)\\nNet-Lib error during "
    "Connection refused (111)\\n')"
)
PG_NOT_NULL = (
    'null value in column "amount" of relation "orders" violates not-null constraint\n'
    "DETAIL:  Failing row contains (5121, BIG_5121, null, {\"card\": \"4111\"})."
)
MONGO_BULK = (
    "batch op errors occurred, full error: {'writeErrors': [{'index': 0, 'code': 11000, "
    "'errmsg': 'E11000 duplicate key error collection: qa.c index: _id_ dup key: { _id: 5 }', "
    "'keyPattern': {'_id': 1}, 'keyValue': {'_id': 5}, 'op': {'_id': 5, 'name': 'BIG_5121', "
    "'nested': {'x': [1, 2]}}}], 'nInserted': 0}"
)


def _no_rows(text: str) -> None:
    assert ROW_VALUE not in text
    assert "[parameters: [" not in text and "Decimal(" not in text
    assert "UPDATE [dbo]" not in text
    assert "DB-Lib" not in text and "Net-Lib" not in text
    assert "internal.example" not in text
    assert "4111" not in text


def test_sqlalchemy_statement_parameters_and_dblib_dump_are_withheld():
    clean = redact_driver_text(SQLA_MSSQL)
    _no_rows(clean)
    # The cause the operator acts on stays: the driver code and the column.
    assert "SQL Server error 207: Invalid column name 'long_txt'." in clean
    assert "[SQL: withheld]" in clean and "[parameters: withheld]" in clean
    assert "sqlalche.me" not in clean


def test_a_connect_dump_keeps_the_cause_and_drops_the_dialled_host():
    clean = redact_driver_text(DBLIB_CONNECT)
    _no_rows(clean)
    assert clean == "SQL Server error 20009: Unable to connect: Adaptive Server is unavailable or does not exist."


def test_failing_row_images_and_refused_documents_are_withheld():
    pg = redact_driver_text(PG_NOT_NULL)
    _no_rows(pg)
    assert 'null value in column "amount"' in pg and "Failing row contains (withheld)" in pg
    mongo = redact_driver_text(MONGO_BULK)
    _no_rows(mongo)
    # The duplicate key is the key range the operator needs; it stays.
    assert "dup key: { _id: 5 }" in mongo and "'op': {withheld}" in mongo


def test_ordinary_messages_are_unchanged():
    for text in ("Wrote 4,096 rows on orders…", "Duplicate entry '5' for key 'PRIMARY'", ""):
        assert redact_driver_text(text) == text
    fields = {"message": "Writing 30 rows…", "progress_pct": 40}
    assert redact_job_update(fields) is fields


def test_update_carries_a_reference_and_the_server_log_keeps_the_statement(caplog):
    with caplog.at_level(logging.WARNING, logger="datawrap.restricted.job_errors"):
        out = redact_job_update({
            "error": SQLA_MSSQL,
            "message": SQLA_MSSQL,
            "error_details": {"raw": SQLA_MSSQL, "code": "unclassified"},
            "destination_summary.verification.message": PG_NOT_NULL,
            "rejected_details": [{"row": {"name": "kept as quarantined data"}, "error": SQLA_MSSQL}],
        })
    for text in (out["error"], out["message"], out["error_details"]["raw"],
                 out["destination_summary.verification.message"], out["rejected_details"][0]["error"]):
        _no_rows(text)
        assert "Reference " in text
    # A quarantined row is stored data, not a message: it is not touched.
    assert out["rejected_details"][0]["row"] == {"name": "kept as quarantined data"}
    ref = out["error"].rsplit("Reference ", 1)[1].rstrip(".")
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert ref in logged and "UPDATE [dbo].[r47_big_ms]" in logged
    # R11: even the restricted log carries no bound row values.
    assert ROW_VALUE not in logged


@pytest.mark.parametrize("store_name", ["memory"])
def test_the_job_store_sink_withholds_on_status_and_field_writes(store_name):
    from services.mongodb_service import MemoryMongoDBService

    store = MemoryMongoDBService()
    job_id = store.create_transfer_job({"status": "running"})
    job_id = job_id if isinstance(job_id, str) else str(job_id)
    store.update_job_status(job_id, "failed", error=SQLA_MSSQL, message=SQLA_MSSQL,
                            error_details={"raw": SQLA_MSSQL})
    job = store.get_job(job_id)
    for text in (job["error"], job["message"], job["error_details"]["raw"]):
        _no_rows(text)
    store.update_job_fields(job_id, {"verification": {"message": DBLIB_CONNECT}})
    _no_rows(store.get_job(job_id)["verification"]["message"])


def test_background_failure_path_stores_and_mails_no_rows(monkeypatch):
    """The path eaf74d1e2 did not cover: an engine exception reaching the runner."""
    from services import notification_service
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer import background

    store = MemoryMongoDBService()
    job_id = str(store.create_transfer_job({"status": "running"}))
    monkeypatch.setattr(background, "get_mongodb_service", lambda: store)
    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: store)
    monkeypatch.setattr("src.transfer.endpoint_refresh.refresh_endpoints_for_run", lambda *a, **k: None)
    monkeypatch.setattr("services.exit_job_guard.before_run", lambda *a, **k: None)

    class _Engine:
        def execute_tracked(self, *_a, **_k):
            raise RuntimeError(SQLA_MSSQL)

    monkeypatch.setattr(background, "get_transfer_engine", lambda: _Engine())
    sent: list[dict] = []
    monkeypatch.setattr(notification_service, "notify_workspace", lambda ws, p: sent.append(p) or [])
    monkeypatch.setattr(notification_service, "log_job_notifications", lambda *a, **k: True)
    req = SimpleNamespace(source=SimpleNamespace(kind="postgresql"),
                          destination=SimpleNamespace(kind="sqlserver"), workspace_id="ws1")
    background._run_transfer(job_id, req)

    job = store.get_job(job_id)
    assert job["status"] == "failed"
    _no_rows(job["error"])
    _no_rows(job["message"])
    (payload,) = sent
    _no_rows(payload["text"])
    _no_rows(payload["error"])


def test_lineage_run_failed_event_carries_no_rows():
    from services.lineage_telemetry import emit_run_failed

    event = emit_run_failed(run_id="j1", job_id="j1", error=SQLA_MSSQL,
                            error_details={"raw": SQLA_MSSQL})
    blob = repr(event)
    assert ROW_VALUE not in blob and "[parameters: [" not in blob
