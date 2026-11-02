"""QA round 10 follow-up: a guessed CSV encoding is visible and stoppable.

An uploaded CSV with no byte-order mark that is not UTF-8 declares nothing
about its encoding. DataFlow reads it as Windows-1252. That is exact for a
Western export and silently wrong for a Cyrillic one: cp1251 'Привет' reads
back as the printable 'Ïðèâåò'. Contract, through MCP start_dataset_transfer
-> confirm_action -> job on real SQL Server and PostgreSQL:

* cp1252 with '€' and 'é': loads with exact text, the staged preview carries
  one warning naming Windows-1252 and decoded sample values, and the job
  records detected_encoding=cp1252 and encoding_guessed=true;
* cp1251 Cyrillic: refused at preflight, asking for the encoding to be
  declared; nothing is written;
* the same file with source_encoding=cp1251 declared: no warning, exact text,
  run 2 into the created table too.
"""

from __future__ import annotations

import csv
import io

import pytest

from tests import r4_route_harness as h
from tests._r4_temporal_harness import (  # noqa: F401 - fixtures
    connector,
    connector_name,
    mcp,
    tag,
    transfer_client,
    wait_job,
)

pytestmark = pytest.mark.timeout(900)

_WESTERN = [("1", "café", "€ 12,50"), ("2", "Ærø “quoted”", ""), ("3", "  ", "naïve – ok"),
            ("4", "Grüße", "plain")]
_CYRILLIC = [("1", "Привет мир", "Москва"), ("2", "Сергей Иванов", ""),
             ("3", "  ", "Санкт-Петербург"), ("4", "Ёлка", "Чебоксары")]
_CYRILLIC_RUN2 = [*_CYRILLIC, ("5", "Щука", "Жуковка")]


def _bytes(rows, codec: str) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\r\n")
    w.writerow(["id", "name", "city"])
    w.writerows(rows)
    return buf.getvalue().encode(codec)


def _stage(client, dataset: str, dest: str, table: str, **extra):
    return mcp(client, "start_dataset_transfer", {
        "dataset_name": dataset, "dest_connector_name": connector_name(dest),
        "dest_table": table, "sync_mode": "upsert", "primary_key": "id", **extra,
    })


def _confirm(client, staged: dict) -> dict:
    failed, confirmed = mcp(client, "confirm_action", {"ack_id": staged["ack_id"], "reason": "r10c"})
    assert failed is False, confirmed
    job = wait_job(confirmed["job_id"])
    assert str(job.get("status")) == "completed", (job.get("status"), job.get("error"))
    return job


def _assert_exact(kind: str, table: str, rows) -> None:
    got = {int(r[0]): tuple(r[1:]) for r in
           h.query(kind, f"SELECT id, name, city FROM {h.quote(kind, table)}")}
    want = {int(r[0]): tuple(r[1:]) for r in rows}
    assert sorted(got) == sorted(want), got
    for rid, row in want.items():
        for g, w in zip(got[rid], row):
            assert g == w or (w == "" and g is None), (rid, g, w)


def _table_exists(kind: str, table: str) -> bool:
    if kind == "sqlserver":
        sql = f"SELECT COUNT(*) FROM sys.tables WHERE name = '{table}'"
    else:
        sql = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table}'"
    return bool(h.query(kind, sql)[0][0])


@pytest.mark.parametrize("kind", ["sqlserver", "postgresql"])
def test_cp1252_upload_loads_exactly_with_a_visible_guess(transfer_client, kind):
    h.dest_config(kind)
    dataset, table = tag("r10c_w"), tag("r10c_w")
    dest = connector(kind, "destination")
    (transfer_client.uploads / f"{dataset}.csv").write_bytes(_bytes(_WESTERN, "cp1252"))
    try:
        failed, staged = _stage(transfer_client, dataset, dest, table)
        assert failed is False, staged
        preview = staged.get("preview") or {}
        enc = preview.get("source_encoding") or {}
        assert enc.get("detected_encoding") == "cp1252" and enc.get("encoding_guessed") is True, preview
        warnings = preview.get("warnings") or []
        assert len(warnings) == 1 and "Windows-1252" in warnings[0], warnings
        assert "café" in warnings[0] and "€ 12" in warnings[0], warnings
        job = _confirm(transfer_client, staged)
        assert job.get("detected_encoding") == "cp1252", job.get("detected_encoding")
        assert job.get("encoding_guessed") is True, job.get("encoding_guessed")
        _assert_exact(kind, table, _WESTERN)
    finally:
        h.drop_table(kind, table)


@pytest.mark.parametrize("kind", ["sqlserver", "postgresql"])
def test_cp1251_upload_is_refused_until_its_encoding_is_declared(transfer_client, kind):
    h.dest_config(kind)
    dataset, table = tag("r10c_c"), tag("r10c_c")
    dest = connector(kind, "destination")
    path = transfer_client.uploads / f"{dataset}.csv"
    path.write_bytes(_bytes(_CYRILLIC, "cp1251"))
    try:
        failed, staged = _stage(transfer_client, dataset, dest, table)
        assert failed is True, staged
        text = str(staged)
        assert "g1_source_encoding" in text or "Declare the file's encoding" in text, text
        assert not _table_exists(kind, table)

        # Declared: strict, no warning, exact text; run 2 into the created table.
        for run, rows in ((1, _CYRILLIC), (2, _CYRILLIC_RUN2)):
            path.write_bytes(_bytes(rows, "cp1251"))
            failed, staged = _stage(transfer_client, dataset, dest, table, source_encoding="cp1251")
            assert failed is False, staged
            preview = staged.get("preview") or {}
            assert not preview.get("warnings"), preview
            job = _confirm(transfer_client, staged)
            assert job.get("encoding_guessed") is False, (run, job.get("encoding_guessed"))
            assert job.get("detected_encoding") == "cp1251", (run, job.get("detected_encoding"))
            _assert_exact(kind, table, rows)
    finally:
        h.drop_table(kind, table)
