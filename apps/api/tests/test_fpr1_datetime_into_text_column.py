"""PR 411 review 1, H2: a typed datetime into an existing TEXT column.

The writer renders the instant in its own spelling: the Python path writes
``'2024-01-01T00:00:00'``, the MySQL engine copy ``'2024-01-01 00:00:00'`` (and
``'2024-01-01 00:00:00.000000'`` from ``DATETIME(6)`` into MySQL text). Gate-8
re-read the source as the ISO ``T`` spelling and failed every correct
engine-copy load into a text column. A text column whose source column is a
typed datetime now compares as an instant (either separator, the fraction the
writer rendered); text that was text stays exact, and a changed instant fails.
"""

from __future__ import annotations

import datetime as dt
import uuid

import pytest

from services.fingerprint_rules import mark_typed_text_carriers
from services.verification_ladder import run_five_layer_verification
from tests._r4_temporal_harness import transfer_client  # noqa: F401  (fixture)

_MAPPINGS = [
    {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INT4"},
    {"source": "c", "target": "c", "source_type": "DATETIME", "target_type": "TEXT"},
]


def _ladder(source_c, target_c, *, catalog_c="datetime(6)", engine="postgresql"):
    types = mark_typed_text_carriers(
        {"id": "INT4", "c": "TEXT"}, _MAPPINGS, {"id": "int", "c": catalog_c}
    )
    report = run_five_layer_verification(
        source_rows=[{"id": i, "c": v} for i, v in enumerate(source_c)],
        target_rows=[{"id": str(i), "c": v} for i, v in enumerate(target_c)],
        columns=["id", "c"],
        pk_column="id",
        source_row_count=len(source_c),
        target_row_count=len(target_c),
        dest_db_type=engine,
        dest_types=types,
    )
    return report


def _passed(report) -> bool:
    return bool(report.get("passed")) if isinstance(report, dict) else bool(report.passed)


_SOURCE = ["2024-01-01T00:00:00", "2024-01-02T03:04:05.500000", None, "1999-12-31T23:59:59"]


@pytest.mark.parametrize("engine", ["postgresql", "mysql"])
@pytest.mark.parametrize(
    "stored",
    [
        ["2024-01-01 00:00:00", "2024-01-02 03:04:05.500000", None, "1999-12-31 23:59:59"],
        [
            "2024-01-01 00:00:00.000000",
            "2024-01-02 03:04:05.500000",
            None,
            "1999-12-31 23:59:59.000000",
        ],
        list(_SOURCE),
    ],
    ids=["space", "space-micro", "iso"],
)
def test_typed_datetime_matches_either_spelling_the_writer_stored(engine, stored):
    assert _passed(_ladder(_SOURCE, stored, engine=engine))


def test_typed_datetime_values_compare_as_the_instant():
    source = [dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2, 3, 4, 5, 500000)]  # noqa: DTZ001
    assert _passed(_ladder(source, ["2024-01-01 00:00:00", "2024-01-02 03:04:05.5"]))


@pytest.mark.parametrize(
    "changed",
    [
        "2024-01-01 00:00:01",
        "2024-01-02 03:04:05.400000",
        "2024-01-01",
        " 2024-01-01 00:00:00",
        "x2024-01-01 00:00:00",
    ],
)
def test_a_changed_instant_or_other_text_still_fails(changed):
    stored = [changed, "2024-01-02 03:04:05.5", None, "1999-12-31 23:59:59"]
    assert not _passed(_ladder(_SOURCE, stored))


def test_text_into_text_stays_exact():
    """A text source column of date-like text (sampling calls it DATETIME)."""
    stored = ["2024-01-01 00:00:00", "2024-01-02T03:04:05.500000", None, "1999-12-31T23:59:59"]
    assert not _passed(_ladder(_SOURCE, stored, catalog_c="text"))


# ---------------------------------------------------------------- live


def _live(client, h, *, styp, dest, trigger_shift=False):
    t = f"fpr1_d_{uuid.uuid4().hex[:6]}"
    out = f"{t}_o"
    h.mysql_exec(f"CREATE TABLE `{t}` (id int primary key, c {styp})")
    try:
        for i, v in enumerate(
            ["2024-01-01 00:00:00", "2024-01-02 03:04:05.5", None, "1999-12-31 23:59:59"]
        ):
            h.mysql_exec(f"INSERT INTO `{t}` VALUES (%s, %s)", (i, v))
        if dest == "postgresql":
            h.pg_exec(f'CREATE TABLE public."{out}" (id int primary key, c text)')
            if trigger_shift:
                h.pg_exec(
                    f'CREATE FUNCTION public."{out}_fn"() RETURNS trigger AS $$ BEGIN '
                    "IF NEW.id = 0 THEN NEW.c := '2024-01-01 00:00:01'; END IF; "
                    "RETURN NEW; END $$ LANGUAGE plpgsql"
                )
                h.pg_exec(
                    f'CREATE TRIGGER "{out}_tg" BEFORE INSERT OR UPDATE ON public."{out}" '
                    f'FOR EACH ROW EXECUTE FUNCTION public."{out}_fn"()'
                )
        else:
            h.mysql_exec(f"CREATE TABLE `{out}` (id int primary key, c text)")
        staged = h.stage(
            source_connector_id=h.connector("mysql", "source"),
            source_table=t,
            dest_connector_id=h.connector(dest, "destination"),
            dest_table=out,
            primary_key="id",
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
            schema_drift_acknowledged=True,
            risk_acceptance={
                "approved_by": "qa@example.com",
                "reason": "existing destination",
                "execution_policy": "CAST_AND_CONTINUE",
            },
        )
        assert staged.success, h.staged_summary(staged)
        job = h.confirm(client, staged)
        stored = (
            h.pg_exec(f'SELECT c FROM public."{out}" ORDER BY id')
            if dest == "postgresql"
            else h.mysql_exec(f"SELECT c FROM `{out}` ORDER BY id")
        )
        return job, [r[0] for r in stored]
    finally:
        h.mysql_exec(f"DROP TABLE IF EXISTS `{t}`")
        if dest == "postgresql":
            h.pg_exec(f'DROP TABLE IF EXISTS public."{out}"')
            h.pg_exec(f'DROP FUNCTION IF EXISTS public."{out}_fn"() CASCADE')
        else:
            h.mysql_exec(f"DROP TABLE IF EXISTS `{out}`")


@pytest.mark.parametrize("dest", ["postgresql", "mysql"])
@pytest.mark.parametrize("styp", ["datetime", "datetime(6)"])
def test_live_mysql_datetime_table_mode_into_existing_text_passes(transfer_client, styp, dest):
    from tests import _r4_temporal_harness as h

    job, stored = _live(transfer_client, h, styp=styp, dest=dest)
    assert stored[2] is None and str(stored[0]).startswith("2024-01-01")
    assert str(job.get("status") or "").startswith("completed"), (
        job.get("error") or job.get("message")
    )


def test_live_changed_instant_still_fails(transfer_client):
    from tests import _r4_temporal_harness as h

    job, stored = _live(transfer_client, h, styp="datetime", dest="postgresql", trigger_shift=True)
    assert job.get("status") == "failed", job.get("error") or job.get("message")
