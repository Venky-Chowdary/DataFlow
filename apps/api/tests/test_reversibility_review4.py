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
    checked = {c["column"]: c["rows_checked"] for c in enforcement["columns"]}
    assert checked.get("v") == 400 and set(checked.values()) == {400}, checked  # id too, once M-2 casts it from text


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


# ------------------------------------------------- M-1: nothing written when a violation exists

def _late_violation(tmp_path, n: int):
    from tests.test_reversibility_review3 import _exec

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _exec(src, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
    rows = [(i, f"U{i}@EXAMPLE.COM") for i in range(1, n + 1)]
    rows[-1] = (n, "late@Example.com")  # the one value the transform would change, after every batch
    _exec(src, "INSERT INTO people VALUES (?, ?)", rows)
    _exec(dst, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
    _exec(dst, "INSERT INTO people VALUES (?, ?)", [(i, f"OLD{i}") for i in range(n + 1, n + 6)])
    return src, dst


import pytest  # noqa: E402


@pytest.mark.parametrize("mode", ["full_refresh_append", "upsert"])
def test_m1_append_and_upsert_write_nothing_when_a_value_past_every_batch_would_change(tmp_path, fake, mode):  # noqa: F811
    from tests.test_reversibility_review3 import _UP, _sqlite_request

    n = 25_000
    src, dst = _late_violation(tmp_path, n)
    result = _run(fake, _sqlite_request(src, dst, mappings=[dict(m) for m in _UP], sync_mode=mode))
    text = str(result.error)
    assert result.success is False
    assert _q(dst, "SELECT count(*), sum(email LIKE 'OLD%') FROM people") == [(5, 5)], "a partial write was kept"
    assert "people.email would change under its transform (upper)" in text, text
    assert "Execute would commit nothing" in text, text
    assert "late@" not in text and "Example.com" not in text


def test_m1_the_violation_never_claims_it_blocked_a_partial_write_when_rows_were_kept():
    from src.transfer.job_failure import account_blocked_write
    from services.reversibility_enforce import ReversibilityViolation

    class _Mongo:
        def get_job(self, _job_id):
            return {"records_processed": 20_000}

    class _Blocked(Exception):
        rows_written = 0

    violation = ReversibilityViolation(
        "Reversibility is required and people.email would change under its transform (upper): a value in the batch "
        "after the first 20,000 checked row(s) does not round-trip (normalize). That batch was not written; "
        "reversibility blocks partial write. Fix the value at the source, remove the transform, or turn off "
        "require reversibility.")
    blocked = _Blocked(str(violation))
    kept, message = account_blocked_write(_Mongo(), "j", blocked)
    assert kept == 20_000
    assert "blocks partial write" not in message, message
    assert "20000 row(s) were already written and were not removed" in message


# ------------------------------------- M-2: a file source's inferred type is a cast from text

@pytest.mark.parametrize("target, raw, converted, dest_db, changed", [
    ("INTEGER", "007", 7, "sqlite", True),
    ("INTEGER", "1e3", None, "sqlite", True),
    ("INTEGER", "7", 7, "sqlite", False),
    ("DATE", "2024-1-5", "2024-1-5", "postgresql", True),  # the writer left the text as it was
    ("DATE", "2024-01-05", __import__("datetime").date(2024, 1, 5), "postgresql", False),
    ("REAL", "0.1000", __import__("decimal").Decimal("0.1000"), "sqlite", True),
    ("REAL", "0.1", 0.1, "sqlite", False),
    ("TIMESTAMP", "2024-01-05 10:00:00", __import__("datetime").datetime(2024, 1, 5, 10), "postgresql", False),
])
def test_m2_the_writer_cast_check_compares_the_destination_rendering(target, raw, converted, dest_db, changed):
    from services.reversibility_enforce import changes

    rule = {"code": "cast", "target_type": target, "dest_db": dest_db}
    assert changes(rule, raw, converted) is changed


@pytest.mark.parametrize("target, values, dest_db, status", [
    ("INTEGER", ["7", "007"], "sqlite", "does_not_fit"),
    ("REAL", ["0.1", "0.1000"], "sqlite", "does_not_fit"),
    ("DATE", ["2024-01-05", "2024-1-5"], "postgresql", "does_not_fit"),
    ("TIMESTAMP", ["2024-01-05 10:00:00", "2024-01-05 10:00:00.5"], "postgresql", "fits"),
    ("INTEGER", ["7", "-3", "0"], "sqlite", "fits"),
])
def test_m2_the_gate_and_the_writer_agree_on_a_cast_from_text(target, values, dest_db, status):
    from services.reversibility import classify_mapping
    from services.reversibility_enforce import changes

    entry = classify_mapping({"source": "v", "target": "v"}, source_type="TEXT", target_type=target,
                             dest_db=dest_db, observed=values)
    assert entry["reason_code"] == "cast" and entry["check"]["status"] == status, entry
    rule = {"code": "cast", "target_type": target, "dest_db": dest_db}
    assert any(changes(rule, v, v) for v in values) is (status == "does_not_fit")


def _ints(last: str, n: int = 400) -> bytes:
    return ("\n".join(["id,v"] + [f"{i},{i}" for i in range(1, n)] + [f"{n},{last}"]) + "\n").encode()


@pytest.mark.parametrize("last", ["007", "1e3"])
def test_m2_a_csv_value_past_the_sample_the_inferred_integer_would_change_fails_the_run(tmp_path, fake, last):  # noqa: F811
    dst = tmp_path / "d.sqlite"
    res = _run(fake, _csv_request(dst, _ints(last)))
    assert res.success is False, (res.destination_summary or {}).get("reversibility_enforcement")
    assert "t.v would change under" in str(res.error), res.error
    assert last not in str(res.error).split("would change under")[1]
    rows = _q(dst, "SELECT name FROM sqlite_master WHERE name='t'") and _q(dst, "SELECT count(*) FROM t")
    assert not rows or rows[0][0] == 0, rows


def test_m2_an_honest_csv_is_enforced_as_a_cast_from_text_on_every_row(tmp_path, fake):  # noqa: F811
    dst = tmp_path / "d.sqlite"
    res = _run(fake, _csv_request(dst, _ints("400")))
    assert res.success is True, res.error
    columns = {c["column"]: c["rows_checked"] for c in res.destination_summary["reversibility_enforcement"]["columns"]}
    assert columns.get("v") == 400, columns
    assert _q(dst, "SELECT v FROM t WHERE id = 400") == [(400,)]


@pytest.mark.parametrize("transform, target, value, dest_db, status", [
    ("datetime", "TIMESTAMP", "2024-01-05 10:00:00", "postgresql", "fits"),
    ("datetime", "TIMESTAMP", "2024-01-05T10:00:00", "postgresql", "does_not_fit"),
    ("date", "DATE", "2024-01-05", "sqlite", "fits"),
    ("integer", "INTEGER", "007", "sqlite", "does_not_fit"),
])
def test_m2_a_type_transform_from_text_is_checked_by_the_destination_rendering(transform, target, value, dest_db,
                                                                                 status):
    from services.reversibility import classify_mapping
    from services.reversibility_enforce import changes

    entry = classify_mapping({"source": "v", "target": "v", "transform": transform}, source_type="TEXT",
                             target_type=target, dest_db=dest_db, observed=[value])
    assert entry["check"]["status"] == status, entry
    rule = {"code": entry["reason_code"], "transform": transform, "target_type": target, "dest_db": dest_db}
    from services.transform_engine import apply_transform

    converted, _err = apply_transform(value, transform)
    assert changes(rule, value, converted) is (status == "does_not_fit")


# ------------------------------------------------------------------------- Lows

def test_low_g24_selects_streams_exactly_as_the_writer_does():
    from services.reversibility_streams import selected_stream_names
    from services.sync_cursor import resolve_selected_sync_contracts

    contracts = [{"name": "people"}, {"stream": "orders"}, {"name": "off", "selected": None},
                 {"name": "zero", "selected": 0}, {"name": "yes", "selected": "yes"}, {"name": "no", "selected": False}]
    assert selected_stream_names(contracts) == [c.name for c in resolve_selected_sync_contracts(contracts)]
    assert selected_stream_names(contracts) == ["people", "orders", "yes"]


@pytest.mark.parametrize("second", [
    {"stream": "orders", "primary_key": ["id"], "selected": True},
    {"name": "orders", "primary_key": ["id"], "selected": "yes"},
])
def test_low_a_stream_named_by_its_stream_key_or_selected_truthy_is_analysed_by_g24(tmp_path, fake, second):  # noqa: F811
    from tests.test_reversibility_review3 import _ID, _g24, _note, _sqlite_request, _tables, _two_tables

    src, dst = _two_tables(tmp_path)
    contracts = [{"name": "people", "primary_key": ["id"], "selected": True},
                 {**second, "mappings": [dict(_ID), _note("hash_pii")]}]
    result = _run(fake, _sqlite_request(src, dst, mappings=[], stream_contracts=contracts))
    assert not result.success and _g24(result), result.error
    assert "orders.note" in str(result.error), result.error
    assert not _tables(dst) & {"people", "orders"}


def test_low_a_column_the_writer_cannot_be_told_about_is_unavailable_not_enforced():
    from services.reversibility import UNAVAILABLE, WHILE_WRITING, analyze_table, build_report
    from services.reversibility_sources import _stamp_enforced, preflight_gate

    maps = [{"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
            {"source": "email", "target": "email", "transform": "uppercase", "source_type": "TEXT",
             "target_type": "TEXT"}]
    table = analyze_table(table="people", mappings=maps, source_columns=["id", "email"],
                          source_types={"id": "INTEGER", "email": "TEXT"}, target_types={}, key_columns=["id"],
                          sample_rows=[{"id": 1, "email": "A@B.C"}], population_rows=None, enforce=WHILE_WRITING)
    assert table["verdict"] == "reversible"
    _stamp_enforced(table, [{"source": "id", "target": "id"}])  # the writer's dicts lack email: stamp() fails
    email = next(c for c in table["columns"] if c["source"] == "email")
    assert email["check"]["enforcement"] == UNAVAILABLE, email["check"]
    assert table["verdict"] != "reversible"
    gate = preflight_gate(build_report([table], scope="job", subject_id="j", require=True, data_rows=1,
                                       data_source="preflight_sample", population_rows=None))
    assert gate["status"] == "block" and "people.email" in gate["message"], gate


@pytest.mark.parametrize("transform", ["default:Sekrit-Literal", "replace:Sekrit-Literal:x"])
def test_low_the_violation_names_the_transform_never_its_literal(transform):
    from services.reversibility_enforce import ReversibilityViolation, refuse_changed_value, summary

    mapping = {"source": "c", "target": "c", "reversibility_enforce": {
        "job_id": "low5", "table": "t", "column": "c", "code": "default", "transform": transform}}
    with pytest.raises(ReversibilityViolation) as caught:
        refuse_changed_value(mapping, None, "Sekrit-Literal")
    text = str(caught.value)
    assert f"t.c would change under its transform ({transform.split(':')[0]})" in text, text
    assert "Sekrit" not in text
    summary("low5")


def test_low_a_quarantine_replay_reports_its_own_enforcement_tally(tmp_path, fake, monkeypatch):  # noqa: F811
    """The replay's g24 runs before its child job exists: the tally must be keyed to that child, not to ''."""
    import asyncio

    import services.reversibility_sources as sources
    from services.reversibility_enforce import stamp, summary
    from src.routers.connectors_router import QuarantineReplayRequest, replay_job_quarantine
    from tests.test_reversibility_review3 import _http, _replay_parent

    def gate(_request, planned):  # g24 as Execute runs it on a replay: it stamps with no job in context
        for mapping in planned[0]:
            if isinstance(mapping, dict) and mapping.get("source") == "v":
                stamp(mapping, table="t", column="v", code="normalize", transform="uppercase", job_id="")
        return {"id": "g24_reversibility", "status": "pass", "message": "Every table is reversible."}

    monkeypatch.setattr(sources, "execute_gate", gate)
    summary("")  # another test's stamp made outside a job: not this replay's
    job_id, _dst = _replay_parent(tmp_path, monkeypatch, fake, require=True)
    before = set(fake.jobs)
    resp = asyncio.run(replay_job_quarantine(job_id, QuarantineReplayRequest(
        rows=[{"id": "2", "v": "42"}], transform_overrides={"v": "uppercase"}), _http()))
    assert resp.get("rows_written", 0) >= 1, resp
    child = next(fake.jobs[j] for j in set(fake.jobs) - before)
    found = (child.get("destination_summary") or {}).get("reversibility_enforcement") or {}
    assert found.get("enforced") is True and [c["rows_checked"] for c in found["columns"]] == [1], found
    assert summary("") is None, "nothing is left keyed to an empty job id"
