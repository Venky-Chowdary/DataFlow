"""QA round 10 extras: text keys under the destination's collation; legacy-encoded CSV uploads.

1. Text-key upsert into SQL Server (and the MySQL sibling). An S3 CSV keyed on
   ``city`` into a NEW SQL Server table died on run 1 with "Violation of
   PRIMARY KEY ... duplicate key value is (  )". Cause: the key column was
   created with no COLLATE, so it took the server default
   ``SQL_Latin1_General_CP1_CI_AS``. That collation folds case, expands
   ``ß`` and gives supplementary characters no weight at all, so
   ``'𝔘𝔫𝔦𝔠𝔬𝔡𝔢' = '  '`` and ``'😀' = '🚀'``. Map had stamped
   ``COLLATE Latin1_General_BIN`` and the D205 key walk judged collisions
   under that stamp; CREATE dropped it. MySQL took ``utf8mb4_0900_ai_ci``
   (case and accents folded) and failed its row-count proof.
   Now: a key column DataFlow creates carries the stamped (binary) equality,
   so distinct source keys stay distinct; the only rule left, PAD SPACE, is
   refused at preflight by name. For an existing key column under a
   non-binary collation the destination itself is asked (GROUP BY under the
   column's collation), so a collision the Python model cannot know
   (zero-weight characters) is refused before a row is written.
2. A Windows-1252 or UTF-16 CSV uploaded through ``start_dataset_transfer``
   failed with "Uploaded dataset ... was not found": the parser refused every
   non-UTF-8 byte stream and the dataset index dropped the file silently.
   Now the profile read and the streaming writer share one sniff (BOM, UTF-16
   without a BOM, UTF-8, else Windows-1252), and the text lands exactly.

Real engines: SQL Server 2022, MySQL 8, PostgreSQL; S3 via moto's server and
the real boto3 client.
"""

from __future__ import annotations

import csv
import io
import uuid

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
from tests.test_r10_fidelity_run2_e2e import _s3_csv, client  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(900)


# --------------------------------------------------------------------------- #
# 1. Text keys under the destination's comparison
# --------------------------------------------------------------------------- #

#: QA's file: distinct cities, one of them two spaces, one in math letters.
_QA_ROWS = [("1", "café", "Straße", "plain"), ("2", "中文", "naïve ✓", "ascii only"),
            ("3", "emoji 😀", "𝔘𝔫𝔦𝔠𝔬𝔡𝔢", "x"), ("4", "", "  ", "y"),
            ("5", "東京", "end", "z")]
_QA_RUN2 = [("1", "café ✓", "Straße", "plain"), *_QA_ROWS[1:],
            ("6", "new", "STRASSE", "case and ß differ"), ("7", "rocket", "🚀", "astral")]

#: Keys distinct in the source that a non-binary collation can merge.
_KEYSETS = {
    "case": ["abc", "ABC", "x"],
    "accent": ["café", "cafe", "y"],
    "sharp_s": ["Straße", "Strasse", "z"],
    "astral": ["😀", "🚀", "w"],
    "astral_vs_spaces": ["𝔘𝔫𝔦𝔠𝔬𝔡𝔢", "  ", "v"],
}


def _csv(header: list[str], rows) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _dest(kind: str):
    from services.connector_store import create_connector

    return create_connector({"name": f"r10b-{kind}-" + uuid.uuid4().hex[:6],
                             "role": "destination", "workspace_id": "", "ssl": False,
                             **h.dest_config(kind)})


def _stage(client, src, dst, table: str, key: str):
    return h.mcp_call(client, "start_transfer", {
        "source_connector_id": src.id, "source_table": "k/data.csv",
        "dest_connector_id": dst.id, "dest_table": table,
        "sync_mode": "incremental_upsert", "primary_key": key,
    })


def _image(kind: str, table: str, cols: str) -> list[tuple]:
    return sorted(tuple(r) for r in h.query(kind, f"SELECT {cols} FROM {h.quote(kind, table)}"))


def test_qa_text_key_upsert_into_new_sqlserver_table_run1_run2(client):
    dst = _dest("sqlserver")
    table = "r10b_qa_" + uuid.uuid4().hex[:6]
    src, stop = _s3_csv("k/data.csv", _csv(["id", "name", "city", "note"], _QA_ROWS))
    try:
        ok, staged = _stage(client, src, dst, table, "city")
        assert ok and not h.blockers_of(staged), staged
        job = h.confirm(client, staged["ack_id"], reason="r10b")
        assert job.get("status") == "completed", job.get("error")
        want = sorted((int(i), n or None, c, t) for i, n, c, t in _QA_ROWS)
        assert _image("sqlserver", table, "id, name, city, note") == want
    finally:
        stop()
    # Run 2 into the table run 1 created: case, ß and astral keys are new rows.
    src2, stop2 = _s3_csv("k/data.csv", _csv(["id", "name", "city", "note"], _QA_RUN2))
    try:
        ok, staged = _stage(client, src2, dst, table, "city")
        assert ok and not h.blockers_of(staged), staged
        job = h.confirm(client, staged["ack_id"], reason="r10b-2")
        assert job.get("status") == "completed", job.get("error")
        want = sorted((int(i), n or None, c, t) for i, n, c, t in _QA_RUN2)
        assert _image("sqlserver", table, "id, name, city, note") == want
    finally:
        stop2()
        h.drop_table("sqlserver", table)


@pytest.mark.parametrize("keyset", list(_KEYSETS))
@pytest.mark.parametrize("kind", ["sqlserver", "mysql"])
def test_distinct_text_keys_stay_distinct_in_a_table_dataflow_creates(client, kind, keyset):
    dst = _dest(kind)
    table = f"r10b_{keyset}_" + uuid.uuid4().hex[:6]
    rows = [(k, f"r{i}") for i, k in enumerate(_KEYSETS[keyset])]
    src, stop = _s3_csv("k/data.csv", _csv(["k", "n"], rows))
    try:
        for run in (1, 2):  # run 2 upserts into the table run 1 created
            ok, staged = _stage(client, src, dst, table, "k")
            assert ok and not h.blockers_of(staged), staged
            job = h.confirm(client, staged["ack_id"], reason=f"r10b-{run}")
            assert job.get("status") == "completed", job.get("error")
            got = _image(kind, table, "k, n")
            assert got == sorted(rows), {"run": run, "rows": got}
    finally:
        stop()
        h.drop_table(kind, table)


@pytest.mark.parametrize("kind", ["sqlserver", "mysql"])
def test_trailing_space_keys_are_refused_at_preflight_by_name(client, kind):
    """PAD SPACE is the one rule a created key keeps: refused, never a mid-run error."""
    dst = _dest(kind)
    table = "r10b_pad_" + uuid.uuid4().hex[:6]
    src, stop = _s3_csv("k/data.csv", _csv(["k", "n"], [("a", "r0"), ("a  ", "r1"), ("b", "r2")]))
    try:
        ok, staged = _stage(client, src, dst, table, "k")
        assert not ok, staged
        assert "distinct in the source but equal" in str(staged), staged
        assert "'a'" in str(staged) and "'a  '" in str(staged), staged
    finally:
        stop()
        h.drop_table(kind, table)


@pytest.mark.parametrize("keyset", ["astral", "astral_vs_spaces", "case", "sharp_s"])
def test_existing_sqlserver_key_under_default_collation_refuses_what_it_would_merge(
    client, keyset
):
    """An operator's table keyed under the server default collation: the
    destination says which keys it merges, before a row is written."""
    dst = _dest("sqlserver")
    table = f"r10b_ex_{keyset}_" + uuid.uuid4().hex[:6]
    h.execute("sqlserver", f"CREATE TABLE [{table}] (k NVARCHAR(100) NOT NULL PRIMARY KEY, "
              "n NVARCHAR(10))")
    keys = _KEYSETS[keyset]
    src, stop = _s3_csv("k/data.csv", _csv(["k", "n"], [(k, f"r{i}") for i, k in enumerate(keys)]))
    try:
        ok, staged = _stage(client, src, dst, table, "k")
        assert not ok, staged
        text = str(staged)
        assert "distinct in the source but equal" in text, text
        assert repr(keys[0]) in text and repr(keys[1]) in text, text
        assert h.query("sqlserver", f"SELECT COUNT(*) FROM [{table}]")[0][0] == 0
    finally:
        stop()
        h.drop_table("sqlserver", table)


@pytest.mark.parametrize("keyset", ["case", "accent", "sharp_s"])
def test_existing_mysql_key_under_default_collation_refuses_what_it_would_merge(client, keyset):
    dst = _dest("mysql")
    table = f"r10b_mex_{keyset}_" + uuid.uuid4().hex[:6]
    h.execute("mysql", f"CREATE TABLE `{table}` (k VARCHAR(100) NOT NULL PRIMARY KEY, "
              "n VARCHAR(10)) DEFAULT CHARSET=utf8mb4")
    keys = _KEYSETS[keyset]
    src, stop = _s3_csv("k/data.csv", _csv(["k", "n"], [(k, f"r{i}") for i, k in enumerate(keys)]))
    try:
        ok, staged = _stage(client, src, dst, table, "k")
        assert not ok, staged
        assert "distinct in the source but equal" in str(staged), staged
        assert h.query("mysql", f"SELECT COUNT(*) FROM `{table}`")[0][0] == 0
    finally:
        stop()
        h.drop_table("mysql", table)


# --------------------------------------------------------------------------- #
# 2. Windows-1252 / UTF-16 CSV uploads
# --------------------------------------------------------------------------- #

#: Representable in Windows-1252: the 0x80-0x9F row (€ “ ” – …) and latin-1.
_CP1252_ROWS = [("1", "café", "Straße", "€ 12,50"), ("2", "Ærø", "“quoted”", "plain"),
                ("3", "", "  ", "en–dash … ellipsis"), ("4", "Grüße", "naïve", "")]
#: UTF-16 holds every scalar.
_UTF16_ROWS = [*_CP1252_ROWS, ("5", "東京", "emoji 😀", "𝔘𝔫𝔦𝔠𝔬𝔡𝔢")]
_RUN2_EXTRA = ("6", "Ñandú", "Øresund", "run 2 ± §")

_ENCODINGS = {
    "cp1252": ("cp1252", _CP1252_ROWS),
    "utf16_bom": ("utf-16", _UTF16_ROWS),
    "utf16le_no_bom": ("utf-16-le", _UTF16_ROWS),
}


def _encoded(rows, codec: str) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\r\n")
    w.writerow(["id", "name", "city", "note"])
    w.writerows(rows)
    return buf.getvalue().encode(codec)


def _upload_run(client, dataset: str, dest: str, table: str, mode: str, key: str = "") -> dict:
    args = {"dataset_name": dataset, "dest_connector_name": connector_name(dest),
            "dest_table": table, "sync_mode": mode}
    if key:
        args["primary_key"] = key
    failed, staged = mcp(client, "start_dataset_transfer", args)
    assert failed is False, staged
    failed, confirmed = mcp(client, "confirm_action", {"ack_id": staged["ack_id"], "reason": "r10b"})
    assert failed is False, confirmed
    job = wait_job(confirmed["job_id"])
    assert str(job.get("status")) == "completed", (job.get("status"), job.get("error"))
    return job


def _assert_exact(kind: str, table: str, rows) -> None:
    got = {int(r[0]): tuple(r[1:]) for r in
           h.query(kind, f"SELECT id, name, city, note FROM {h.quote(kind, table)}")}
    want = {int(r[0]): tuple(r[1:]) for r in rows}
    assert sorted(got) == sorted(want), got
    for rid, row in want.items():
        for g, w in zip(got[rid], row):
            # A CSV empty field: the file policy decides NULL or ''.
            assert g == w or (w == "" and g is None), (rid, g, w)


@pytest.mark.parametrize("kind", ["sqlserver", "postgresql"])
@pytest.mark.parametrize("enc", list(_ENCODINGS))
def test_legacy_encoded_csv_upload_loads_exactly_run1_run2(transfer_client, enc, kind):
    h.dest_config(kind)  # skips when the engine is absent
    codec, rows = _ENCODINGS[enc]
    dataset, table = tag(f"r10b_{enc}"), tag(f"r10b_{enc}")
    dest = connector(kind, "destination")
    path = transfer_client.uploads / f"{dataset}.csv"
    try:
        path.write_bytes(_encoded(rows, codec))
        _upload_run(transfer_client, dataset, dest, table, "upsert", key="id")
        _assert_exact(kind, table, rows)

        run2 = [*rows, _RUN2_EXTRA]
        path.write_bytes(_encoded(run2, codec))
        _upload_run(transfer_client, dataset, dest, table, "upsert", key="id")
        _assert_exact(kind, table, run2)
    finally:
        h.drop_table(kind, table)
