"""A source column the destination table lacks is added or blocks. It is never dropped.

The defect (reviewer repro, PostgreSQL 16): ``full_refresh_overwrite`` from a
table with a column ``doc`` into an existing table without it copied the other
columns, returned ``success=True`` with reconciliation passed, and left no
``doc`` column. The run had skipped Validate, so gate G13 (every source column
is written, declared omitted, or blocks) never ran: overwrite's auto-map keeps
only the destination's own columns and leaves the rest "so G13 can block".
Gate-8 then compared the mapped columns only and reported the rows verified.
The same hole was open in every sync mode for a run that skips Validate with
stored mappings that predate the column, and, with Validate on, for the second
table of a multi-table job (copied with the first table's column map).

Rule (``docs/migration-rules.md`` 4, 7, 12; R8 and R9 of the defect brief): the
column is added when the schema policy approves it, or the run stops before
the first write. Execute checks the predicate Validate checks, and a run that
leaves a column out by decision says so on every trust surface.

The table below runs every sync mode on the default route (COPY-eligible) and
on the row path (forced with a source filter) through
``UniversalTransferEngine.execute_tracked``. The ``doc`` column carries the
canonical seed values, so an added column must read back exactly.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path
from typing import Any

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.c3_live_helpers import (  # noqa: E402
    mysql_endpoint,
    mysql_sql,
    pg_endpoint,
    pg_sql,
    token,
)
from tests.test_commit_safe_review_round2 import isolated  # noqa: E402,F401 - fixture

ROWS = 30
EXISTING_ID = 1001  # the row the destination held before the run
KEPT = ("id", "name", "updated_at")

#: Canonical seed cells for ``doc`` (the brief's 30-row seed): '' is a value,
#: NULL is not, and the text 'none' / 'NULL' are strings.
_SEED_DOC = {
    2: "",
    3: None,
    5: "none",
    6: "NULL",
    7: 'q"uote\\back,comma\ttab',
    8: "line1\nline2",
    11: "漢字 ß \U0001f600",
    30: None,
}


def seed_doc(i: int) -> str | None:
    return _SEED_DOC.get(i, f"d{i}")


EXPECTED_DOC = [(i, seed_doc(i)) for i in range(1, ROWS + 1)]

#: The decision context of a run, and what must happen to ``doc`` in it. The
#: same in every sync mode: under ``manual_review`` Execute never adds a column
#: ("Execute does not ADD COLUMN"); propagate, backfill or an acknowledged
#: drift approve it.
VARIANTS: dict[str, dict[str, Any]] = {
    # Validate on, default policy: the operator has not approved a new column.
    "validate_manual_review": {"expect": "blocked"},
    # Validate on, the policy approves additive columns.
    "validate_propagate": {"schema_policy": "propagate_columns", "expect": "added"},
    # No Validate, nothing mapped: the engine maps by itself (it proposes the
    # column as an ADD COLUMN, which manual_review does not approve).
    "no_validate_automap": {"skip_preflight": True, "expect": "blocked"},
    # No Validate, auto-map, and the operator turned on backfill new fields.
    "no_validate_automap_backfill": {
        "skip_preflight": True, "backfill_new_fields": True, "expect": "added",
    },
    # No Validate, mappings stored before the column existed (a schedule replay).
    "no_validate_stored_mappings": {"skip_preflight": True, "stored": True, "expect": "blocked"},
    # The same stored mappings, and the policy approves additive columns.
    "no_validate_stored_propagate": {
        "skip_preflight": True, "stored": True, "schema_policy": "propagate_columns",
        "expect": "added",
    },
    # No Validate, the operator's map proposes doc as a new column.
    "no_validate_create_new": {"skip_preflight": True, "create_new": True, "expect": "blocked"},
    # The same map, and the drift is acknowledged for this run.
    "no_validate_create_new_acknowledged": {
        "skip_preflight": True, "create_new": True, "schema_drift_acknowledged": True,
        "expect": "added",
    },
}

SQL_MODES = (
    "full_refresh_overwrite",
    "full_refresh_append",
    "incremental_append",
    "incremental_deduped",
    "upsert",
    "scd2",
    "mirror",
)
#: SCD2 and mirror stage the whole source and refuse a source filter, so they
#: have no forced row path.
WHOLE_SOURCE_MODES = frozenset({"scd2", "mirror"})


def _expected(variant: str, mode: str) -> str:
    del mode  # one rule for every sync mode
    return VARIANTS[variant]["expect"]


def _stored_mappings() -> list[dict[str, Any]]:
    return [{"source": c, "target": c, "confidence": 0.99} for c in KEPT]


def _create_new_mappings() -> list[dict[str, Any]]:
    """What Map proposes for a source column the destination lacks."""
    return [
        *_stored_mappings(),
        {
            "source": "doc", "target": "doc", "confidence": 0.99, "create_new": True,
            "assignment_strategy": "create_compatible_new", "target_type": "TEXT",
        },
    ]


def _contract(table: str, mode: str) -> dict[str, Any]:
    contract: dict[str, Any] = {
        "name": table, "sync_mode": mode, "primary_key": "id", "selected": True,
    }
    if mode.startswith("incremental"):
        contract.update(cursor_field="id", cursor_semantics="monotonic_sequence")
    return contract


def _request(source: Any, destination: Any, table: str, mode: str, path: str, variant: str):
    spec = VARIANTS[variant]
    extra: dict[str, Any] = {}
    if spec.get("skip_preflight"):
        extra["skip_preflight"] = True
    if spec.get("schema_policy"):
        extra.update(schema_policy=spec["schema_policy"], schema_policy_declared=True)
    if spec.get("stored"):
        extra["mappings"] = _stored_mappings()
    if spec.get("create_new"):
        extra["mappings"] = _create_new_mappings()
    for flag in ("backfill_new_fields", "schema_drift_acknowledged"):
        if spec.get(flag):
            extra[flag] = True
    if path == "row":
        # A source filter keeps the run off every COPY fast path (copy_route).
        extra["source_filter"] = {"column": "id", "op": "gte", "value": 0}
    return TransferRequest(
        source=source, destination=destination, sync_mode=mode, validation_mode="strict",
        stream_contracts=[_contract(table, mode)], **extra,
    )


def _run(store: Any, request: TransferRequest):
    job_id = "owc" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    return result, store.jobs.get(job_id) or {}


def _assert_blocked(result: Any, job: dict[str, Any], columns: list[str], rows: list[tuple]) -> None:
    assert not result.success, "the run wrote without a decision on the source column doc"
    assert "doc" in str(result.error), result.error
    assert "doc" not in columns, columns
    # Nothing was written: an overwrite did not empty the table either.
    assert rows == [(EXISTING_ID, "old")], rows
    assert job.get("status") == "failed", job.get("status")


def _pg_many(statement: str, rows: list[tuple]) -> None:
    import psycopg2

    from tests.helpers.live_env import pg_creds

    c = pg_creds()
    conn = psycopg2.connect(
        host=c["host"], port=c["port"], dbname=c["database"], user=c["username"],
        password=c["password"],
    )
    try:
        with conn, conn.cursor() as cur:
            cur.executemany(statement, rows)
    finally:
        conn.close()


def _seed_rows() -> list[tuple]:
    return [
        (i, f"n{i}", f"2026-01-01 00:{i:02d}:00", seed_doc(i)) for i in range(1, ROWS + 1)
    ]


# -- PostgreSQL to PostgreSQL: every mode, default route and row path ---------


@pytest.fixture()
def pg_pair():
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    made: list[str] = []

    def make(doc_type: str = "text") -> tuple[str, str]:
        src, dst = f"owc_s_{token()}", f"owc_d_{token()}"
        made.extend([src, dst])
        pg_sql(
            f'CREATE TABLE public."{src}" (id integer PRIMARY KEY, name text, '
            f"updated_at timestamp, doc {doc_type})"
        )
        if doc_type == "text":
            _pg_many(f'INSERT INTO public."{src}" VALUES (%s, %s, %s, %s)', _seed_rows())
        else:
            pg_sql(
                f'INSERT INTO public."{src}" SELECT g, \'n\' || g, '
                f"timestamp '2026-01-01' + g * interval '1 minute', '{{\"k\": 1}}' "
                f"FROM generate_series(1, {ROWS}) g"
            )
        pg_sql(
            f'CREATE TABLE public."{dst}" (id integer PRIMARY KEY, name text, updated_at timestamp)'
        )
        pg_sql(f"INSERT INTO public.\"{dst}\" VALUES ({EXISTING_ID}, 'old', now())")
        return src, dst

    try:
        yield make
    finally:
        for name in made:
            leftovers = pg_sql(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                "AND tablename LIKE %s",
                (name + "%",), fetch=True,
            )
            for (table,) in leftovers or []:
                pg_sql(f'DROP TABLE IF EXISTS public."{table}" CASCADE')


def _pg_columns(table: str, schema: str = "public") -> list[str]:
    return [
        r[0]
        for r in pg_sql(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = %s "
            "AND table_name = %s ORDER BY ordinal_position",
            (schema, table), fetch=True,
        )
    ]


def _pg_cells() -> list[tuple[str, str, str]]:
    cells = []
    for mode in SQL_MODES:
        for path in ("default", "row"):
            if path == "row" and mode in WHOLE_SOURCE_MODES:
                continue
            for variant in VARIANTS:
                cells.append((mode, path, variant))
    return cells


@pytest.mark.parametrize(("mode", "path", "variant"), _pg_cells())
def test_pg_to_pg_source_column_is_added_or_blocks(isolated, pg_pair, mode, path, variant):  # noqa: F811
    src, dst = pg_pair()
    result, job = _run(
        isolated, _request(pg_endpoint(src), pg_endpoint(dst), src, mode, path, variant)
    )
    columns = _pg_columns(dst)

    if _expected(variant, mode) == "added":
        assert result.success, result.error
        assert "doc" in columns, f"{mode}/{path}/{variant}: succeeded without the column doc"
        landed = pg_sql(f'SELECT id, doc FROM public."{dst}" WHERE id <= {ROWS} ORDER BY id', fetch=True)
        assert [tuple(r) for r in landed] == EXPECTED_DOC
        assert (job.get("schema_propagation") or {}).get("added_columns") == ["doc"]
        return
    rows = pg_sql(f'SELECT id, name FROM public."{dst}" ORDER BY id', fetch=True)
    _assert_blocked(result, job, columns, [tuple(r) for r in rows])


@pytest.mark.parametrize("doc_type", ["jsonb", "text"])
@pytest.mark.parametrize("path", ["default", "row"])
def test_reviewer_repro_overwrite_without_validate_no_longer_reports_verified(
    isolated, pg_pair, doc_type, path  # noqa: F811
):
    """The reported run: before, success=True, 'Row fidelity verified', no doc column."""
    src, dst = pg_pair(doc_type)
    result, job = _run(
        isolated,
        _request(
            pg_endpoint(src), pg_endpoint(dst), src, "full_refresh_overwrite", path,
            "no_validate_automap",
        ),
    )

    assert not result.success
    assert not (result.reconciliation or {}).get("passed")
    assert "neither mapped nor declared omitted: doc" in result.error
    # The refusal names what the operator can do, in the product's own words.
    assert "propagate columns" in result.error and "declare it omitted" in result.error
    details = job.get("error_details") or {}
    assert details.get("unaccounted_sources") == ["doc"]
    assert [g.get("gate") for g in details.get("guidance") or []] == ["g13_source_coverage"]
    assert pg_sql(f'SELECT id, name FROM public."{dst}"', fetch=True) == [(EXISTING_ID, "old")]


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "full_refresh_append"])
@pytest.mark.parametrize("path", ["default", "row"])
def test_declared_omission_is_stated_by_gate8_and_the_trust_report(isolated, pg_pair, mode, path):  # noqa: F811
    """Leaving a column out is the operator's choice; the run then says so (rules 4, 12; R9)."""
    from services.job_trust import compute_job_trust
    from services.parity_report import build_parity_report, public_report

    src, dst = pg_pair()
    request = _request(pg_endpoint(src), pg_endpoint(dst), src, mode, path, "validate_manual_review")
    request.mappings = [
        *_stored_mappings(),
        {"source": "doc", "target": "", "intentional_omit": True, "transform": "omit"},
    ]
    result, job = _run(isolated, request)

    assert result.success, result.error
    assert "doc" not in _pg_columns(dst)
    coverage = result.reconciliation["column_coverage"]
    assert coverage["not_migrated"] == [{"column": "doc", "reason": "declared_omitted"}]
    assert (coverage["source_columns"], coverage["columns_written"]) == (4, 3)
    sentence = "1 source column(s) were not migrated: doc (declared omitted)."
    assert "The check covers the 3 of 4 source columns this run wrote." in result.reconciliation["message"]
    assert sentence in result.reconciliation["message"]
    assert sentence in job["message"]

    completeness = next(f for f in compute_job_trust(job)["factors"] if f["id"] == "completeness")
    assert sentence in completeness["note"]

    report = public_report(build_parity_report(job, reread=False, job_id="j"))
    assert report["column_coverage"]["not_migrated"] == coverage["not_migrated"]
    assert sentence in report["confidence"]["statement"]
    assert "the destination matches the source" not in report["verdict_reason"]


# -- PostgreSQL to MySQL (MariaDB locally): row path --------------------------
#
# The row path is forced on purpose: the PG->MySQL engine COPY uses LOAD DATA,
# which must not be exercised against a shared MariaDB (bulk-insert freeze).

MYSQL_MODES = ("full_refresh_overwrite", "full_refresh_append", "incremental_deduped")


@pytest.fixture()
def pg_mysql_pair():
    from tests.helpers.live_env import mysql_up, pg_up

    if not (pg_up() and mysql_up()):
        pytest.skip("live PostgreSQL and MySQL not reachable")
    src, dst = f"owc_s_{token()}", f"owc_d_{token()}"
    pg_sql(
        f'CREATE TABLE public."{src}" (id integer PRIMARY KEY, name text, '
        "updated_at timestamp, doc text)"
    )
    _pg_many(f'INSERT INTO public."{src}" VALUES (%s, %s, %s, %s)', _seed_rows())
    mysql_sql(
        f"CREATE TABLE `{dst}` (id INT PRIMARY KEY, name TEXT, updated_at DATETIME(6)) "
        "DEFAULT CHARSET=utf8mb4"
    )
    mysql_sql(f"INSERT INTO `{dst}` VALUES ({EXISTING_ID}, 'old', NOW())")
    try:
        yield src, dst
    finally:
        pg_sql(f'DROP TABLE IF EXISTS public."{src}" CASCADE')
        for (table,) in mysql_sql(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() "
            "AND table_name LIKE %s",
            (dst + "%",), fetch=True,
        ) or []:
            mysql_sql(f"DROP TABLE IF EXISTS `{table}`")


@pytest.mark.parametrize("variant", sorted(VARIANTS))
@pytest.mark.parametrize("mode", MYSQL_MODES)
def test_pg_to_mysql_row_path_source_column_is_added_or_blocks(
    isolated, pg_mysql_pair, mode, variant  # noqa: F811
):
    src, dst = pg_mysql_pair
    result, job = _run(
        isolated, _request(pg_endpoint(src), mysql_endpoint(dst), src, mode, "row", variant)
    )
    columns = [
        r[0]
        for r in mysql_sql(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() "
            "AND table_name = %s ORDER BY ordinal_position",
            (dst,), fetch=True,
        )
    ]

    if _expected(variant, mode) == "added":
        assert result.success, result.error
        assert "doc" in columns, f"{mode}/{variant}: succeeded without the column doc"
        landed = mysql_sql(f"SELECT id, doc FROM `{dst}` WHERE id <= {ROWS} ORDER BY id", fetch=True)
        assert [tuple(r) for r in landed] == EXPECTED_DOC
        return
    rows = mysql_sql(f"SELECT id, name FROM `{dst}` ORDER BY id", fetch=True)
    _assert_blocked(result, job, columns, [tuple(r) for r in rows])


# -- SQLite and a file source: the same rule with no live engine --------------

LOCAL_MODES = ("full_refresh_overwrite", "full_refresh_append", "incremental_append", "incremental_deduped")
LOCAL_VARIANTS = tuple(v for v in VARIANTS if v.startswith("no_validate"))


def _sqlite_dest(path: Path) -> EndpointConfig:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT)")
    conn.execute("INSERT INTO t VALUES (?, 'old', '2025-01-01')", (EXISTING_ID,))
    conn.commit()
    conn.close()
    return EndpointConfig(kind="database", format="sqlite", database=str(path), table="t")


def _sqlite_state(path: Path) -> tuple[list[str], list[tuple]]:
    conn = sqlite3.connect(path)
    try:
        columns = [r[1] for r in conn.execute("PRAGMA table_info(t)")]
        landed = conn.execute(
            "SELECT id, name FROM t ORDER BY id" if "doc" not in columns
            else f"SELECT id, doc FROM t WHERE id <= {ROWS} ORDER BY id"
        ).fetchall()
        return columns, landed
    finally:
        conn.close()


def _assert_local(
    result: Any, job: dict[str, Any], dst: Path, variant: str, mode: str, expected_doc: list
) -> None:
    columns, rows = _sqlite_state(dst)
    if _expected(variant, mode) == "added":
        assert result.success, result.error
        assert "doc" in columns, f"{mode}/{variant}: succeeded without the column doc"
        assert rows == expected_doc, rows
        return
    _assert_blocked(result, job, columns, rows)


@pytest.mark.parametrize("variant", LOCAL_VARIANTS)
@pytest.mark.parametrize("mode", LOCAL_MODES)
def test_sqlite_source_column_is_added_or_blocks(tmp_path, isolated, mode, variant):  # noqa: F811
    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT, doc TEXT)")
    conn.executemany("INSERT INTO t VALUES (?,?,?,?)", _seed_rows())
    conn.commit()
    conn.close()
    dst = tmp_path / "dst.sqlite"
    destination = _sqlite_dest(dst)
    source = EndpointConfig(kind="database", format="sqlite", database=str(src), table="t")

    result, job = _run(isolated, _request(source, destination, "t", mode, "default", variant))

    _assert_local(result, job, dst, variant, mode, EXPECTED_DOC)


@pytest.mark.parametrize("variant", LOCAL_VARIANTS)
@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "full_refresh_append"])
def test_csv_file_source_column_is_added_or_blocks(tmp_path, isolated, mode, variant):  # noqa: F811
    # The CSV dialect (R12) is not the subject here: plain cells only.
    csv_path = tmp_path / "t.csv"
    csv_path.write_text(
        "id,name,updated_at,doc\n"
        + "".join(f"{i},n{i},2026-01-{(i % 28) + 1:02d},d{i}\n" for i in range(1, ROWS + 1))
    )
    dst = tmp_path / "dst.sqlite"
    destination = _sqlite_dest(dst)
    request = _request(
        EndpointConfig(kind="file", format="csv"), destination, "t", mode, "default", variant
    )
    request.source_path = str(csv_path)
    request.source_filename = "t.csv"

    result, job = _run(isolated, request)

    _assert_local(
        result, job, dst, variant, mode, [(i, f"d{i}") for i in range(1, ROWS + 1)]
    )


# -- Several tables in one job: a wider second table keeps its column ---------
#
# A multi-table run hands every stream the first table's column map. The row
# path swaps a map that does not fit for the stream's own columns, but the COPY
# decision came first and copied with the borrowed map: a second table with one
# more column than the first landed without it and the job read "Checksum
# matches". This is the product's normal flow (Validate on).

MULTI_MODES = ("full_refresh_overwrite", "full_refresh_append", "upsert")


def _multi_contracts(mode: str, names: tuple[str, ...] = ("m1", "m2")) -> list[dict[str, Any]]:
    return [
        {"name": name, "sync_mode": mode, "primary_key": "id", "selected": True}
        for name in names
    ]


def _multi_sqlite_source(tmp_path: Path, second: str = "name TEXT, doc TEXT") -> Path:
    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE m1 (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute(f"CREATE TABLE m2 (id INTEGER PRIMARY KEY, {second})")
    conn.executemany("INSERT INTO m1 VALUES (?,?)", [(i, f"n{i}") for i in range(1, ROWS + 1)])
    conn.executemany(
        "INSERT INTO m2 VALUES (?,?,?)", [(i, f"n{i}", seed_doc(i)) for i in range(1, ROWS + 1)]
    )
    conn.commit()
    conn.close()
    return src


def _multi_sqlite_request(src: Path, dst: Path, mode: str, **extra: Any) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="m1"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="m1"),
        sync_mode=mode, validation_mode="strict", stream_contracts=_multi_contracts(mode), **extra,
    )


@pytest.mark.parametrize("skip_preflight", [False, True], ids=["validate", "no_validate"])
@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "full_refresh_append"])
def test_sqlite_multi_table_wider_second_table_lands_every_column(
    tmp_path, isolated, mode, skip_preflight  # noqa: F811
):
    src = _multi_sqlite_source(tmp_path)
    dst = tmp_path / "dst.sqlite"

    result, _job = _run(
        isolated, _multi_sqlite_request(src, dst, mode, skip_preflight=skip_preflight)
    )

    assert result.success, result.error
    conn = sqlite3.connect(dst)
    try:
        assert [r[1] for r in conn.execute("PRAGMA table_info(m2)")] == ["id", "name", "doc"]
        assert conn.execute("SELECT id, doc FROM m2 ORDER BY id").fetchall() == EXPECTED_DOC
    finally:
        conn.close()


def test_sqlite_multi_table_with_different_columns_is_verified_under_its_own_names(
    tmp_path, isolated  # noqa: F811
):
    """The second table shares only the key: Gate-8 must not read it by the first table's map."""
    src = _multi_sqlite_source(tmp_path, second="label TEXT, doc TEXT")
    dst = tmp_path / "dst.sqlite"

    result, _job = _run(isolated, _multi_sqlite_request(src, dst, "full_refresh_overwrite"))

    assert result.success, result.error
    conn = sqlite3.connect(dst)
    try:
        assert conn.execute("SELECT id, label, doc FROM m2 ORDER BY id").fetchall() == [
            (i, f"n{i}", seed_doc(i)) for i in range(1, ROWS + 1)
        ]
    finally:
        conn.close()


@pytest.fixture()
def pg_multi():
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    tag = token()
    dest_schema = f"owc_dst_{tag}"
    names = (f"owc_m1_{tag}", f"owc_m2_{tag}")
    pg_sql(f'CREATE SCHEMA "{dest_schema}"')
    pg_sql(f'CREATE TABLE public."{names[0]}" (id integer PRIMARY KEY, name text)')
    pg_sql(f'CREATE TABLE public."{names[1]}" (id integer PRIMARY KEY, name text, doc text)')
    _pg_many(
        f'INSERT INTO public."{names[0]}" VALUES (%s, %s)',
        [(i, f"n{i}") for i in range(1, ROWS + 1)],
    )
    _pg_many(
        f'INSERT INTO public."{names[1]}" VALUES (%s, %s, %s)',
        [(i, f"n{i}", seed_doc(i)) for i in range(1, ROWS + 1)],
    )
    try:
        yield dest_schema, names
    finally:
        pg_sql(f'DROP SCHEMA IF EXISTS "{dest_schema}" CASCADE')
        for name in names:
            pg_sql(f'DROP TABLE IF EXISTS public."{name}" CASCADE')


@pytest.mark.parametrize("dest_state", ["absent", "has_the_column", "lacks_the_column"])
@pytest.mark.parametrize("mode", MULTI_MODES)
def test_pg_multi_table_wider_second_table_is_copied_whole_or_blocks(
    isolated, pg_multi, mode, dest_state  # noqa: F811
):
    dest_schema, names = pg_multi
    wide = names[1]
    if dest_state != "absent":
        for name in names:
            extra = ", doc text" if (name == wide and dest_state == "has_the_column") else ""
            pg_sql(f'CREATE TABLE "{dest_schema}"."{name}" (id integer PRIMARY KEY, name text{extra})')
    destination = pg_endpoint(names[0])
    destination.schema = dest_schema
    request = TransferRequest(
        source=pg_endpoint(names[0]), destination=destination, sync_mode=mode,
        validation_mode="strict", stream_contracts=_multi_contracts(mode, names),
    )

    result, _job = _run(isolated, request)

    columns = _pg_columns(wide, dest_schema)
    if dest_state == "lacks_the_column":
        # The existing table has no carrier and no policy approved one: the
        # stream stops at the writer and says which column.
        assert not result.success
        assert "doc" in str(result.error), result.error
        assert "doc" not in columns
        assert pg_sql(f'SELECT count(*) FROM "{dest_schema}"."{wide}"', fetch=True)[0][0] == 0
        return
    assert result.success, result.error
    assert columns == ["id", "name", "doc"], columns
    landed = pg_sql(f'SELECT id, doc FROM "{dest_schema}"."{wide}" ORDER BY id', fetch=True)
    assert [tuple(r) for r in landed] == EXPECTED_DOC


def test_copy_is_declined_for_a_borrowed_map_that_does_not_fit_this_table(monkeypatch):
    from types import SimpleNamespace

    import src.transfer.adapters_introspect as introspect
    from services.copy_fast_path import begin_copy_decline_capture, reset_copy_decline_capture
    from src.transfer.stream_dest_procedure import copy_declined_for_borrowed_map

    catalog = {"cols": {"id": "integer", "name": "text", "doc": "text"}}
    monkeypatch.setattr(
        introspect, "_introspect_table_schema", lambda *_a, **_k: dict(catalog["cols"])
    )
    borrowed = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]

    def cfg(mappings):
        return SimpleNamespace(
            mappings=mappings, source=SimpleNamespace(table="m2", collection=""),
            src_read_type="postgresql", src_cfg={},
        )

    capture, reasons = begin_copy_decline_capture()
    try:
        # The stream's own map, or no map at all: nothing to check.
        assert copy_declined_for_borrowed_map(cfg(borrowed), inherited=False) is False
        assert copy_declined_for_borrowed_map(cfg([]), inherited=True) is False
        # The borrowed map leaves out this table's column doc.
        assert copy_declined_for_borrowed_map(cfg(borrowed), inherited=True) is True
        # A map that names exactly this table's columns may be copied with.
        exact = [*borrowed, {"source": "doc", "target": "doc"}]
        assert copy_declined_for_borrowed_map(cfg(exact), inherited=True) is False
        # Columns that cannot be listed prove nothing: no copy.
        catalog["cols"] = {}
        assert copy_declined_for_borrowed_map(cfg(exact), inherited=True) is True
    finally:
        reset_copy_decline_capture(capture)
    assert any("belongs to another table" in reason for reason in reasons), reasons


# -- The predicate and the report, without an engine --------------------------


def test_execute_refuses_only_when_validate_did_not_run():
    from types import SimpleNamespace

    from src.transfer.engine_schema_propagation import SchemaPropagationRefused
    from src.transfer.source_column_coverage import (
        SourceColumnsUnaccounted,
        refuse_unaccounted_without_validate,
    )

    columns = [*KEPT, "doc"]
    stored = _stored_mappings()
    # Validate on: G13 owns the block, the approval request and the acknowledgement.
    refuse_unaccounted_without_validate(SimpleNamespace(skip_preflight=False), columns, stored)
    # Every column decided: written, or declared omitted.
    declared = [*stored, {"source": "doc", "target": "", "intentional_omit": True}]
    refuse_unaccounted_without_validate(SimpleNamespace(skip_preflight=True), columns, declared)
    added = [*stored, {"source": "doc", "target": "doc", "create_new": True}]
    refuse_unaccounted_without_validate(SimpleNamespace(skip_preflight=True), columns, added)

    with pytest.raises(SourceColumnsUnaccounted) as refused:
        refuse_unaccounted_without_validate(SimpleNamespace(skip_preflight=True), columns, stored)
    assert isinstance(refused.value, SchemaPropagationRefused)
    assert refused.value.columns == ["doc"]
    assert refused.value.failure_details["retriable"] is False


def test_execute_and_validate_use_the_same_g13_sentence():
    """R8: one predicate. Execute's refusal starts with Validate's G13 message."""
    from types import SimpleNamespace

    from services.source_coverage_gate import build_source_coverage_gate
    from src.transfer.source_column_coverage import refuse_unaccounted_without_validate

    columns = [*KEPT, "doc"]
    _coverage, gate = build_source_coverage_gate(source_columns=columns, mappings=_stored_mappings())
    with pytest.raises(ValueError) as refused:
        refuse_unaccounted_without_validate(
            SimpleNamespace(skip_preflight=True), columns, _stored_mappings()
        )
    assert gate["status"] == "block"
    assert str(refused.value).startswith(gate["message"])


def test_the_refusal_keeps_its_words_and_is_classified_as_source_drift():
    from types import SimpleNamespace

    from services.drift_approval import gate_codes_from_job, is_source_drift_code
    from services.error_handling import humanize_transfer_failure
    from src.transfer.job_failure import _job_failure_fields
    from src.transfer.source_column_coverage import refuse_unaccounted_without_validate

    try:
        refuse_unaccounted_without_validate(
            SimpleNamespace(skip_preflight=True), [*KEPT, "doc"], _stored_mappings()
        )
    except ValueError as exc:
        human = humanize_transfer_failure(exc)
        details, _extras = _job_failure_fields(exc)
    assert human["code"] == "run_refused"
    assert "neither mapped nor declared omitted: doc" in human["message"]
    assert details["unaccounted_sources"] == ["doc"]
    codes = gate_codes_from_job({"error_details": details})
    assert any(is_source_drift_code(code) for code in codes), codes


def test_a_column_left_out_under_an_acknowledged_drift_is_named_with_that_reason():
    from services.column_coverage_report import column_coverage_of
    from services.preflight_policy_gates import finalize_acknowledged_schema_drift

    preflight = {
        "gates": [
            {
                "id": "g13_source_coverage", "status": "block",
                "message": "1 source column(s) are neither mapped nor declared omitted: doc",
                "details": {"unaccounted_sources": ["doc"]},
            }
        ],
        "blockers": [],
    }
    finalize_acknowledged_schema_drift(
        preflight, evolution={"unmapped_sources": ["doc"]}, acknowledgment={"actor": "op"}
    )
    job = {
        "status": "completed",
        "preflight": preflight,
        "schema_propagation": {
            "added_columns": [], "skipped_columns": ["doc"], "omitted_columns": [],
            "source_column_count": 4,
        },
        "reconciliation": {"passed": True},
    }

    coverage = column_coverage_of(job)

    assert coverage["not_migrated"] == [{"column": "doc", "reason": "schema_drift_acknowledged"}]
    assert "doc (left out under the acknowledged schema drift)" in coverage["statement"]


def test_a_stored_job_that_dropped_a_column_with_no_decision_is_not_reported_proven():
    """A job finished before this fix: its record names the column, so the report must."""
    from services.column_coverage_report import column_coverage_of, undecided_columns
    from services.job_trust import compute_job_trust

    job = {
        "status": "completed",
        "records_processed": 6,
        "schema_propagation": {"added_columns": [], "skipped_columns": ["doc"]},
        "reconciliation": {
            "passed": True, "assurance_level": "full_checksum", "checksum_match": True,
            "source_rows": 6, "target_rows": 6, "source_checksum": "a", "target_checksum": "a",
        },
    }

    coverage = column_coverage_of(job)
    assert undecided_columns(coverage) == ["doc"]
    assert "doc (no mapping and no recorded decision)" in coverage["statement"]

    trust = compute_job_trust(job)
    completeness = next(f for f in trust["factors"] if f["id"] == "completeness")
    assert completeness["score"] <= 78
    assert "doc (no mapping and no recorded decision)" in completeness["note"]
    assert trust["grade"] != "A"


def test_a_run_that_moved_every_column_reports_nothing_extra():
    from services.column_coverage_report import column_coverage_of, stamp_column_coverage

    class _Store:
        def get_job(self, _job_id):
            return {"schema_propagation": {"added_columns": ["doc"], "skipped_columns": []}}

    fields = {"message": "Row fidelity verified", "reconciliation": {"message": "Row fidelity verified"}}
    stamp_column_coverage(_Store(), "j", fields)

    assert fields == {
        "message": "Row fidelity verified", "reconciliation": {"message": "Row fidelity verified"}
    }
    assert column_coverage_of({"reconciliation": {"passed": True}}) is None


# -- SQLite overwrite publish: ADD COLUMN on the live table, rolled back on failure


def _publish_fixture(tmp_path: Path, *, live_unique_name: bool = False) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "pub.sqlite")
    unique = " UNIQUE" if live_unique_name else ""
    conn.execute(f"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT{unique})")
    conn.execute("INSERT INTO t VALUES (1001, 'old')")
    conn.execute("CREATE TABLE t__df_stage (id INTEGER PRIMARY KEY, name TEXT, doc TEXT)")
    conn.executemany(
        "INSERT INTO t__df_stage VALUES (?,?,?)",
        [(1, "n", seed_doc(2)), (2, "n" if live_unique_name else "m", seed_doc(8))],
    )
    conn.commit()
    return conn


def _live(conn: sqlite3.Connection) -> tuple[list[str], list[tuple]]:
    return (
        [r[1] for r in conn.execute("PRAGMA table_info(t)")],
        conn.execute("SELECT * FROM t ORDER BY id").fetchall(),
    )


def _publish(conn: sqlite3.Connection, *, add_missing: bool) -> None:
    from connectors.sqlite_writer import _publish_sqlite_stage

    _publish_sqlite_stage(
        conn, stage_quoted='"t__df_stage"', stage_name="t__df_stage", live_name="t",
        columns=["id", "name", "doc"], add_missing_columns=add_missing,
    )


def test_sqlite_publish_adds_an_approved_column_to_the_live_table(tmp_path):
    conn = _publish_fixture(tmp_path)
    try:
        _publish(conn, add_missing=True)
        assert _live(conn) == (["id", "name", "doc"], [(1, "n", ""), (2, "m", "line1\nline2")])
    finally:
        conn.close()


def test_sqlite_publish_without_approval_fails_closed_and_keeps_the_live_table(tmp_path):
    conn = _publish_fixture(tmp_path)
    try:
        with pytest.raises(sqlite3.Error):
            _publish(conn, add_missing=False)
        assert _live(conn) == (["id", "name"], [(1001, "old")])
    finally:
        conn.close()


def test_sqlite_publish_that_fails_after_the_add_rolls_the_add_back(tmp_path):
    """R7: DDL applied by a failed run is rolled back. The live table is as before."""
    conn = _publish_fixture(tmp_path, live_unique_name=True)  # the stage repeats name 'n'
    try:
        with pytest.raises(sqlite3.Error):
            _publish(conn, add_missing=True)
        assert _live(conn) == (["id", "name"], [(1001, "old")])
    finally:
        conn.close()


# -- A field first seen mid-run (schemaless and record-batch sources) ---------
#
# Under manual_review the writer must not add a field a later page introduces:
# the run stops before writing that page, resumable from the batch before it.
# Under propagate / backfill it is added. MongoDB is driven here by a fake page
# reader under the real stream (src.transfer.stream); live MongoDB is CI-only.

_MONGO_DOCS = [
    *({"id": i, "name": f"n{i}"} for i in range(1, 4)),
    *({"id": i, "name": f"n{i}", "doc": seed_doc(i)} for i in range(4, 7)),
]
_PAGE = 3


class _FakePage:
    def __init__(self, docs: list[dict[str, Any]], more: bool) -> None:
        headers: list[str] = []
        for doc in docs:
            headers += [k for k in doc if k not in headers]
        self.headers = headers
        self.rows = [[doc.get(h) for h in headers] for doc in docs]
        self.total_rows = len(_MONGO_DOCS)
        self.meta: dict[str, Any] = {}
        self.has_more = more


@pytest.fixture()
def fake_mongo_pages(monkeypatch):
    import src.transfer.batch_readers as batch_readers
    import src.transfer.stream as stream_mod

    def read(_src_type, _cfg, _table, _columns, offset, _limit, *_a, **_kw):
        docs = _MONGO_DOCS[offset: offset + _PAGE]
        return _FakePage(docs, offset + _PAGE < len(_MONGO_DOCS))

    monkeypatch.setattr(batch_readers, "_read_batch_impl", read)
    monkeypatch.setattr(stream_mod, "_read_batch_impl", read)


def _mongo_stream(dst: Path, policy: str, *, checkpoint=None, service=None, mappings=None):
    from types import SimpleNamespace

    from src.transfer.source_column_coverage import stamp_mid_run_column_policy
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig(
        kind="database", format="mongodb", connection_string="mongodb://127.0.0.1:1",
        database="db", table="docs",
    )
    destination = EndpointConfig(kind="database", format="sqlite", database=str(dst), table="docs")
    conn = sqlite3.connect(dst)
    live = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(docs)")}
    conn.close()
    # What the engine stamps before the first write (_record_schema_propagation).
    stamp_mid_run_column_policy(
        SimpleNamespace(destination=destination, schema_policy=policy, backfill_new_fields=False),
        live,
    )
    contracts = [{"name": "docs", "sync_mode": "full_refresh_append", "primary_key": "id", "selected": True}]
    return stream_database_transfer(
        source, destination, list(mappings or []), {}, None, sync_mode="full_refresh_append",
        stream_contracts=contracts, job_id="mid-run", checkpoint=checkpoint,
        checkpoint_service=service,
    )


def _docs_dest(tmp_path: Path) -> Path:
    dst = tmp_path / "dst.sqlite"
    conn = sqlite3.connect(dst)
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute(f"INSERT INTO docs VALUES ({EXISTING_ID}, 'old')")
    conn.commit()
    conn.close()
    return dst


def _docs_state(dst: Path) -> tuple[list[str], list[tuple]]:
    conn = sqlite3.connect(dst)
    try:
        return (
            [r[1] for r in conn.execute("PRAGMA table_info(docs)")],
            conn.execute("SELECT * FROM docs ORDER BY id").fetchall(),
        )
    finally:
        conn.close()


def test_mongo_mid_run_field_stops_before_its_batch_and_resumes_under_propagate(
    tmp_path, isolated, fake_mongo_pages  # noqa: F811
):
    from services.checkpoint_service import Checkpoint, CheckpointService
    from src.transfer.source_column_coverage import SourceColumnsUnaccounted

    dst = _docs_dest(tmp_path)
    checkpoint, service = Checkpoint(job_id="mid-run"), CheckpointService(mongo=isolated)

    with pytest.raises(SourceColumnsUnaccounted) as refused:
        _mongo_stream(dst, "manual_review", checkpoint=checkpoint, service=service)

    message = str(refused.value)
    assert "neither mapped nor declared omitted: doc" in message
    assert "First seen in a later batch" in message and "propagate columns" in message
    assert refused.value.failure_details["guidance"][0]["gate"] == "g13_source_coverage"
    # The first page landed and was checkpointed; nothing of the page with doc did.
    assert (checkpoint.offset, checkpoint.rows_processed) == (_PAGE, _PAGE)
    assert _docs_state(dst) == (
        ["id", "name"], [(1, "n1"), (2, "n2"), (3, "n3"), (EXISTING_ID, "old")]
    )

    # The operator approves the column and resumes from the same checkpoint.
    _mongo_stream(dst, "propagate_columns", checkpoint=checkpoint, service=service)

    columns, rows = _docs_state(dst)
    assert columns == ["id", "name", "doc"]
    assert rows == [
        *((i, f"n{i}", None) for i in range(1, 4)),
        *((i, f"n{i}", seed_doc(i)) for i in range(4, 7)),
        (EXISTING_ID, "old", None),
    ]


def test_mongo_mid_run_field_is_added_under_propagate(tmp_path, isolated, fake_mongo_pages):  # noqa: F811
    dst = _docs_dest(tmp_path)

    _mongo_stream(dst, "propagate_columns")

    columns, rows = _docs_state(dst)
    assert columns == ["id", "name", "doc"]
    assert [r[2] for r in rows if r[0] in (4, 5, 6)] == [seed_doc(4), seed_doc(5), seed_doc(6)]


@pytest.mark.parametrize("policy", ["manual_review", "propagate_columns"])
def test_a_field_declared_omitted_is_never_written_when_it_appears_mid_run(
    tmp_path, isolated, fake_mongo_pages, policy  # noqa: F811
):
    """Omits leave the write map before the read; a late omitted field was adopted as new."""
    dst = _docs_dest(tmp_path)
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "name", "target": "name", "confidence": 0.99},
        {"source": "doc", "target": "", "intentional_omit": True, "transform": "omit"},
    ]

    _mongo_stream(dst, policy, mappings=mappings)

    columns, rows = _docs_state(dst)
    assert columns == ["id", "name"]
    assert len(rows) == len(_MONGO_DOCS) + 1


@pytest.mark.parametrize(
    ("policy", "backfill", "acknowledged", "adds"),
    [
        ("manual_review", False, False, False),
        ("manual_review", False, True, False),  # an ack covers only what Validate disclosed
        ("manual_review", True, False, True),
        ("propagate_columns", False, False, True),
        ("type_locked", False, False, False),
    ],
)
def test_the_engine_stamps_whether_a_mid_run_field_may_be_added(policy, backfill, acknowledged, adds):
    from src.transfer.engine_schema_propagation import _record_schema_propagation
    from src.transfer.source_column_coverage import MID_RUN_POLICY_KEY

    class _Store:
        def update_job_status(self, *_a, **_k):
            return True

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="mongodb", table="docs"),
        destination=EndpointConfig(kind="database", format="sqlite", database="x", table="docs"),
        schema_policy=policy, backfill_new_fields=backfill, schema_drift_acknowledged=acknowledged,
    )
    mappings = [{"source": c, "target": c} for c in ("id", "name")]

    _record_schema_propagation(_Store(), "j", ["id", "name"], mappings, {"id": "INTEGER", "name": "TEXT"}, request)

    stamp = request.destination.extra[MID_RUN_POLICY_KEY]
    assert stamp["adds_columns"] is adds
    assert stamp["dest_columns"] == ["id", "name"]


@pytest.mark.parametrize("fmt", ["jsonl", "json"])
@pytest.mark.parametrize(
    ("variant", "expect"),
    [("validate_manual_review", "blocked"), ("no_validate_automap", "blocked"),
     ("no_validate_automap_backfill", "added")],
)
def test_json_file_whose_last_record_adds_a_field(tmp_path, isolated, monkeypatch, fmt, variant, expect):  # noqa: F811
    """A later record introduces doc; small batches put it past the first write."""
    import json

    import src.transfer.file_stream as file_stream

    monkeypatch.setattr(file_stream, "CHUNK_SIZE", 5)
    records = [{"id": i, "name": f"n{i}", "updated_at": f"2026-01-{i:02d}"} for i in range(1, ROWS)]
    records.append({"id": ROWS, "name": f"n{ROWS}", "updated_at": "2026-01-30", "doc": "late"})
    path = tmp_path / f"t.{fmt}"
    path.write_text(
        "".join(json.dumps(r) + "\n" for r in records) if fmt == "jsonl" else json.dumps(records)
    )
    dst = tmp_path / "dst.sqlite"
    request = _request(
        EndpointConfig(kind="file", format=fmt), _sqlite_dest(dst), "t", "full_refresh_append",
        "default", variant,
    )
    request.source_path, request.source_filename = str(path), path.name

    result, job = _run(isolated, request)

    columns, rows = _sqlite_state(dst)
    if expect == "added":
        assert result.success, result.error
        assert "doc" in columns
        assert rows[-1] == (ROWS, "late") and rows[0] == (1, None)
        return
    _assert_blocked(result, job, columns, rows)


# -- A flattened child is carried inside a parent written whole (G13) ---------
#
# Lead decision (27 Nov): a column the reader flattened out of a document field
# (nested_city from nested) is covered when its parent object is mapped and
# written whole (pass-through or JSON carry). Disclosed as "carried inside
# <parent>". A transformed, omitted or unmapped parent leaves the child to
# decide. One rule at Validate and Execute (classify_source_coverage).

_LINEAGE = {"nested_city": "nested", "nested_geo": "nested", "nested_geo_lat": "nested_geo"}
_FLAT_COLUMNS = ["pk", "name", "nested", "nested_city", "nested_geo", "nested_geo_lat"]


def _flat_maps(parent: dict[str, Any] | None) -> list[dict[str, Any]]:
    maps = [{"source": "pk", "target": "pk"}, {"source": "name", "target": "name"}]
    return maps + ([parent] if parent else [])


@pytest.mark.parametrize(
    ("parent", "carried"),
    [
        ({"source": "nested", "target": "nested"}, True),
        ({"source": "nested", "target": "nested", "transform": "none"}, True),
        ({"source": "nested", "target": "nested", "transform": "json"}, True),
        ({"source": "nested", "target": "nested", "transform": "lower"}, False),
        ({"source": "nested", "target": "nested", "code_crosswalk": {"a": "b"}}, False),
        ({"source": "nested", "target": "", "intentional_omit": True}, False),
        (None, False),
    ],
    ids=["no_transform", "none", "json_carry", "transformed", "crosswalk", "omitted", "unmapped"],
)
def test_g13_counts_a_flattened_child_as_carried_only_inside_a_whole_parent(parent, carried):
    from services.source_coverage_gate import build_source_coverage_gate

    coverage, gate = build_source_coverage_gate(
        source_columns=_FLAT_COLUMNS, mappings=_flat_maps(parent), lineage=_LINEAGE
    )

    children = ["nested_city", "nested_geo", "nested_geo_lat"]
    if carried:
        assert gate["status"] == "pass", gate["message"]
        # A grandchild is carried by the ancestor written whole.
        assert coverage["carried"] == [{"column": c, "parent": "nested"} for c in children]
        assert "nested_city (carried inside nested)" in gate["message"]
        assert gate["details"]["carried_sources"] == coverage["carried"]
        return
    assert gate["status"] == "block"
    assert set(children) <= set(coverage["unaccounted"])
    assert coverage["carried"] == []


def test_no_lineage_means_no_carried_children():
    """A JSON column and a real payload_k column are unrelated unless the reader flattens."""
    from services.flatten_protection import coverage_lineage
    from services.mapping_constraints import classify_source_coverage

    rows = [{"payload": {"k": 1}, "payload_k": "a real column"}]
    assert coverage_lineage("postgresql", {}, rows) == {}
    assert coverage_lineage("mongodb", {"flatten_nested": False}, rows) == {}
    assert coverage_lineage("mongodb", {}, rows) == {"payload_k": "payload"}
    coverage = classify_source_coverage(
        ["payload", "payload_k"], [{"source": "payload", "target": "payload"}], lineage={}
    )
    assert coverage["unaccounted"] == ["payload_k"]


@pytest.mark.parametrize("transformed", [False, True], ids=["whole", "transformed"])
def test_g16_ledger_agrees_with_g13_on_a_carried_child(transformed):
    from services.field_reduction_ledger import build_field_reduction_evidence

    parent = {"source": "nested", "target": "nested", **({"transform": "lower"} if transformed else {})}
    ledger, gate = build_field_reduction_evidence(
        source_columns=_FLAT_COLUMNS, mappings=_flat_maps(parent), lineage=_LINEAGE
    )

    child = next(e for e in ledger["entries"] if e["source"] == "nested_city")
    if transformed:
        # Undecided: G16 records it and leaves the block to G13 (no double count).
        assert child["disposition"] == "unaccounted" and child["carried"] is False
        return
    assert child["carried"] is True and child["disposition_label"] == "carried inside nested"
    assert child["targets"] == ["nested"]
    assert gate["status"] != "block"


def test_execute_refusal_uses_the_same_carried_rule():
    from types import SimpleNamespace

    from src.transfer.source_column_coverage import (
        SourceColumnsUnaccounted,
        refuse_unaccounted_without_validate,
    )

    request = SimpleNamespace(skip_preflight=True)
    whole = _flat_maps({"source": "nested", "target": "nested"})
    refuse_unaccounted_without_validate(request, _FLAT_COLUMNS, whole, _LINEAGE)
    with pytest.raises(SourceColumnsUnaccounted) as refused:
        refuse_unaccounted_without_validate(
            request, _FLAT_COLUMNS, _flat_maps({"source": "nested", "target": "nested", "transform": "lower"}),
            _LINEAGE,
        )
    assert {"nested_city", "nested_geo", "nested_geo_lat"} <= set(refused.value.columns)


# Real reader and engine: DynamoDB (moto) flattens ``nested`` into children.

_DDB_TABLE = "owc_ddb_nested"


@pytest.fixture()
def ddb_nested():
    boto3 = pytest.importorskip("boto3")
    moto = pytest.importorskip("moto")
    with moto.mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName=_DDB_TABLE,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        for pk, city in (("U1", "austin"), ("U2", "oslo")):
            client.put_item(
                TableName=_DDB_TABLE,
                Item={
                    "pk": {"S": pk}, "name": {"S": f"n-{pk}"},
                    "nested": {"M": {"city": {"S": city}, "geo": {"L": [{"N": "1"}, {"N": "2"}]}}},
                },
            )
        yield


@pytest.mark.parametrize("validate", [True, False], ids=["validate", "no_validate"])
@pytest.mark.parametrize("transformed", [False, True], ids=["parent_whole", "parent_crosswalk"])
def test_dynamodb_flattened_children_carried_inside_the_parent(
    tmp_path, isolated, ddb_nested, validate, transformed  # noqa: F811
):
    # A code crosswalk rewrites the parent's value. (A cast the carrier cannot
    # take, such as lower on an object, is replaced by the JSON carry at
    # Validate and Execute alike, so the parent is then written whole.)
    parent = {
        "source": "nested", "target": "nested", "confidence": 0.99,
        **({"code_crosswalk": {"austin": "AUS"}} if transformed else {}),
    }
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="dynamodb", host="us-east-1", port=443, table=_DDB_TABLE),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(tmp_path / "d.sqlite"), table="people"
        ),
        sync_mode="full_refresh_overwrite", validation_mode="warn", skip_preflight=not validate,
        stream_contracts=[{"name": _DDB_TABLE, "sync_mode": "full_refresh_overwrite", "primary_key": "pk", "selected": True}],
        mappings=[
            {"source": "pk", "target": "pk", "confidence": 0.99},
            {"source": "name", "target": "name", "confidence": 0.99},
            parent,
        ],
    )

    result, job = _run(isolated, request)

    if transformed:
        assert not result.success
        assert "neither mapped nor declared omitted" in str(result.error)
        assert "nested_city" in str(result.error)
        return
    assert result.success, result.error
    assert result.records_transferred == 2
    carried = (job.get("schema_propagation") or {}).get("carried_columns") or []
    assert {"column": "nested_city", "parent": "nested"} in carried
    statement = (result.reconciliation.get("column_coverage") or {}).get("statement", "")
    assert "nested_city (carried inside nested)" in statement
    conn = sqlite3.connect(tmp_path / "d.sqlite")
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(people)")]
        nested = [r[0] for r in conn.execute("SELECT nested FROM people ORDER BY pk")]
    finally:
        conn.close()
    assert "nested_city" not in cols
    assert "austin" in str(nested[0]) and "oslo" in str(nested[1])
