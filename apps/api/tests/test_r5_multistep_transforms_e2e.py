"""Transforms with more than one step must run — on every route a user drives.

Owner's report (round 5, provisional D203): "Transforms with more than one step
can't run." Reproduced on the base through the real routes:

* UI REST Validate (``/api/v1/preflight/run`` with the uploaded file id) answered
  HTTP 500 for any recipe that changes the column set (rename / derive): the
  stored-file population was re-shaped against the *shaped* column list, so
  step 1 could not find the column it renames.
* Execute from a database source wrote the rows, then Gate-8 failed the job and
  dropped the table it created for ``trim -> default_if_null -> upper``: its SQL
  oracle treated ``''`` / whitespace as NULL while the writer (correctly, for a
  typed source) kept them as values. The failure named every step as ``case``.
* Gate-8 re-parsed a rename / derive recipe against the shaped columns and
  never verified it ("unverifiable: column 'name' is not available").
* ``cast_column`` to ``timestamp`` (and ``datetime`` in a chain) dropped the
  time of day, so ``cast -> assume_timezone`` landed midnight.
* MCP ``start_transfer`` could not express more than one transform per column
  and Confirm dropped any recipe.

Each test drives the route the user drives against live PostgreSQL / MySQL, with
NULL, '', whitespace and unicode cells, and a second run into the table the
first run created.
"""

from __future__ import annotations

import io
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

# id, name, amount, ts — text columns so every step does real work.
_SEED: list[tuple[Any, ...]] = [
    (1, "  ada  ", " 10.50 ", "2024-01-02 03:04:05"),
    (2, None, None, None),
    (3, "", "", None),
    (4, "   ", "  7 ", "2024-06-01 23:59:58"),
    (5, "zoë ünï 山田", "3.25", "2024-12-31 12:00:00"),
]


def _seed_pg_source(rows: list[tuple[Any, ...]] | None = None) -> str:
    table = f"r5ms_src_{uuid.uuid4().hex[:8]}"
    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f'CREATE TABLE public."{table}" (id integer PRIMARY KEY, name text, '  # nosec B608
            "amount text, ts text)"
        )
        cur.executemany(
            f'INSERT INTO public."{table}" VALUES (%s, %s, %s, %s)',  # nosec B608
            rows or _SEED,
        )
    finally:
        conn.close()
    return table


def _norm(v: Any) -> Any:
    if isinstance(v, Decimal):
        return v.normalize()
    if isinstance(v, datetime) and v.tzinfo is not None:
        return v.astimezone(timezone.utc).replace(tzinfo=None)
    return v


def _read(kind: str, table: str, cols: list[str]) -> list[tuple]:
    q = h.quote
    sel = ", ".join(q(kind, c) for c in cols)
    rows = h.query(kind, f"SELECT {sel} FROM {q(kind, table)} ORDER BY {q(kind, 'id')}")  # nosec B608
    return [tuple(_norm(v) for v in r) for r in rows]


# --------------------------------------------------------------------------- #
# Execute (engine) — database source, Gate-8 must verify the chain
# --------------------------------------------------------------------------- #


def _endpoint(kind: str, table: str):
    from tests.typed_fidelity_helpers import mysql_endpoint, pg_endpoint

    return pg_endpoint(table) if kind == "postgresql" else mysql_endpoint(table)


def _execute(source, dest, recipe: dict, out_cols: list[str], types: dict | None = None):
    from services.shape_models import ShapeRecipe
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    parsed = ShapeRecipe.parse(recipe)
    request = TransferRequest(
        source=source,
        destination=dest,
        mappings=[
            {"source": c, "target": c, "approved": True, "confidence": 0.99,
             **({"target_type": (types or {})[c]} if c in (types or {}) else {})}
            for c in out_cols
        ],
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=parsed.recipe_hash,
    )
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def _assert_verified(result) -> None:
    assert result.success, result.error
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof


TRIM_DEFAULT_UPPER = {
    "steps": [
        {"op": "trim", "column": "name"},
        {"op": "default_if_null", "column": "name", "options": {"value": "none"}},
        {"op": "case", "column": "name", "options": {"mode": "upper"}},
    ]
}


@pytest.mark.parametrize("dest_kind", ["postgresql", "mysql"])
def test_db_source_same_column_chain_runs_twice_and_gate8_verifies(dest_kind):
    """trim -> default_if_null -> upper on one column, PG source.

    A typed source keeps '' and whitespace as values: '   ' trims to '' and
    stays '' (only SQL NULL takes the default). The base wrote these rows, then
    Gate-8's SQL oracle defaulted '' too, failed the job and dropped the table.
    """
    h.dest_config(dest_kind)  # skips when the engine is absent
    src = _seed_pg_source()
    dst = f"r5ms_d_{uuid.uuid4().hex[:8]}"
    try:
        for _run in (1, 2):
            result = _execute(
                _endpoint("postgresql", src),
                _endpoint(dest_kind, dst),
                TRIM_DEFAULT_UPPER,
                ["id", "name", "amount", "ts"],
            )
            _assert_verified(result)
            assert result.records_transferred == 5
            got = [r[1] for r in _read(dest_kind, dst, ["id", "name"])]
            assert got == ["ADA", "NONE", "", "", "ZOË ÜNÏ 山田"]
    finally:
        h.drop_table("postgresql", src)
        h.drop_table(dest_kind, dst)


@pytest.mark.parametrize(
    "recipe,out_cols,want",
    [
        (
            # rename, then two steps on the NEW name.
            {
                "steps": [
                    {"op": "rename_column", "column": "name", "options": {"to": "nm"}},
                    {"op": "trim", "column": "nm"},
                    {"op": "case", "column": "nm", "options": {"mode": "upper"}},
                ]
            },
            ["id", "nm", "amount", "ts"],
            [(1, "ADA"), (2, None), (3, ""), (4, ""), (5, "ZOË ÜNÏ 山田")],
        ),
        (
            # derive from a column a later step rewrites, steps across columns.
            {
                "steps": [
                    {
                        "op": "derive_column",
                        "options": {"to": "label", "expression": "upper(trim([name]))"},
                    },
                    {"op": "trim", "column": "name"},
                    {"op": "trim", "column": "amount"},
                ]
            },
            ["id", "name", "amount", "ts", "label"],
            [
                (1, "ada", "10.50", "ADA"),
                (2, None, None, None),
                (3, "", "", ""),
                (4, "", "7", ""),
                (5, "zoë ünï 山田", "3.25", "ZOË ÜNÏ 山田"),
            ],
        ),
    ],
    ids=["rename_then_steps", "derive_across_columns"],
)
def test_column_changing_chain_is_verified_not_skipped(recipe, out_cols, want):
    """Gate-8 must re-apply the recipe against the columns it reads.

    The base parsed it against the shaped columns, so a renamed or derived
    column read as missing and the proof was "unverifiable" on every run.
    """
    h.dest_config("postgresql")
    src = _seed_pg_source()
    dst = f"r5ms_d_{uuid.uuid4().hex[:8]}"
    try:
        for _run in (1, 2):
            result = _execute(
                _endpoint("postgresql", src),
                _endpoint("postgresql", dst),
                recipe,
                out_cols,
            )
            _assert_verified(result)
            # The written column carries the step's output type: a trimmed
            # text column still holding '' is text, not DECIMAL (base inferred
            # DECIMAL from the shaped values and refused the '' cell).
            read_cols = [c for c in out_cols if c != "ts"][: len(want[0])]
            assert _read("postgresql", dst, read_cols) == want
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)


_CSV_PLAIN = (
    "id,name,amount,ts\n"
    "1,  ada  , 10.50 ,2024-01-02 03:04:05\n"
    "2,,,\n"
    "3,   ,  7 ,2024-06-01 23:59:58\n"
    "4,zoë ünï 山田,3.25,2024-12-31 12:00:00\n"
).encode("utf-8")

_CSV = (
    "id,name,amount,ts\n"
    "1,  ada  , 10.50 ,2024-01-02 03:04:05\n"
    "2,,,\n"
    "3,   ,  7 ,2024-06-01 23:59:58\n"
    "4,zoë ünï 山田,3.25,2024-12-31T12:00:00+05:30\n"
).encode("utf-8")


def test_csv_cast_then_assume_timezone_keeps_the_clock():
    """cast -> assume_timezone: the time of day survives, the zone is attached.

    The base cast parsed the calendar date only, so every row landed at
    midnight; an explicit offset was dropped as well.
    """
    from src.transfer.models import EndpointConfig

    h.dest_config("postgresql")
    dst = f"r5ms_d_{uuid.uuid4().hex[:8]}"
    recipe = {
        "steps": [
            {"op": "trim", "column": "ts"},
            {"op": "cast_column", "column": "ts", "options": {"to_type": "timestamp"}},
            {
                "op": "cast_column",
                "column": "ts",
                "options": {"to_type": "timestamp", "timezone": "America/New_York"},
            },
        ]
    }
    try:
        for _run in (1, 2):
            source = EndpointConfig(kind="file", format="csv")
            from services.shape_models import ShapeRecipe
            from src.transfer.engine import UniversalTransferEngine
            from src.transfer.models import TransferRequest

            request = TransferRequest(
                source=source,
                destination=_endpoint("postgresql", dst),
                source_content=_CSV,
                source_filename="ts.csv",
                mappings=[
                    {"source": c, "target": c, "approved": True, "confidence": 0.99}
                    for c in ("id", "name", "amount", "ts")
                ],
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
                shape_recipe=recipe,
                approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
            )
            result = UniversalTransferEngine().execute_tracked(
                request, uuid.uuid4().hex[:24]
            )
            assert result.success, result.error
            got = dict(_read("postgresql", dst, ["id", "ts"]))
            assert got == {
                1: datetime(2024, 1, 2, 8, 4, 5),  # 03:04:05 New York = 08:04:05Z
                2: None,
                3: datetime(2024, 6, 2, 3, 59, 58),  # EDT, crosses midnight UTC
                4: datetime(2024, 12, 31, 6, 30, 0),  # stated offset kept
            }
    finally:
        h.drop_table("postgresql", dst)


# --------------------------------------------------------------------------- #
# UI REST path: upload -> Transform preview -> Validate -> Run
# --------------------------------------------------------------------------- #


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(tmp_path / "uploads"))
    # Settings() parses this as a bool; "" made /connectors/upload drop the file id.
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


RENAME_FILTER_DERIVE = {
    "steps": [
        {"op": "rename_column", "column": "name", "options": {"to": "nm"}},
        {"op": "trim", "column": "nm"},
        {"op": "filter_rows", "options": {"condition": "[id] <> 3"}},
        {"op": "derive_column", "options": {"to": "label", "expression": "concat(upper([nm]), '!')"}},
    ]
}


def test_rest_validate_and_run_with_a_column_changing_recipe(client):
    """The Studio's own calls, in order, for a 4-step recipe on an uploaded CSV."""
    c = client
    cfg = h.dest_config("postgresql")
    dst = f"r5ms_d_{uuid.uuid4().hex[:8]}"
    up = c.post("/api/v1/connectors/upload", files={"file": ("people.csv", _CSV_PLAIN, "text/csv")})
    assert up.status_code == 200, up.text
    body = up.json()
    assert body.get("file_id"), body
    columns = [x["name"] if isinstance(x, dict) else x for x in body["columns"]]
    sample = body.get("data") or body.get("sample_data")

    pv = c.post(
        "/api/v1/shape/preview",
        json={"recipe": RENAME_FILTER_DERIVE, "sample_rows": sample, "source_columns": columns},
    )
    assert pv.status_code == 200, pv.text
    recipe_info = pv.json()["recipe"]
    out_cols = recipe_info["output_columns"]
    assert out_cols == ["id", "nm", "amount", "ts", "label"]
    mappings = [{"source": x, "target": x, "confidence": 0.99} for x in out_cols]

    pf = c.post(
        "/api/v1/preflight/run",
        json={
            "columns": columns,
            "column_types": {},
            "row_count": 4,
            "sample_rows": sample,
            "source_file_id": body["file_id"],
            "source_kind": "file",
            "dest_kind": "database",
            "dest_type": "postgresql",
            "dest_host": cfg["host"],
            "dest_port": cfg["port"],
            "dest_database": cfg["database"],
            "dest_username": cfg["username"],
            "dest_password": cfg["password"],
            "dest_schema": "public",
            "dest_table": dst,
            "mappings": mappings,
            "shape_recipe": RENAME_FILTER_DERIVE,
        },
    )
    # Base: HTTP 500 (ShapeError: step 1: column 'name' is not available).
    assert pf.status_code == 200, pf.text
    image = (pf.json().get("transform_image") or {})
    assert image.get("columns", out_cols) == out_cols

    try:
        for _run in (1, 2):
            run = c.post(
                "/api/v1/transfer/run",
                files={"file": ("people.csv", io.BytesIO(_CSV_PLAIN), "text/csv")},
                data={
                    "source_kind": "file",
                    "dest_kind": "database",
                    "dest_format": "postgresql",
                    "dest_host": cfg["host"],
                    "dest_port": str(cfg["port"]),
                    "dest_database": cfg["database"],
                    "dest_username": cfg["username"],
                    "dest_password": cfg["password"],
                    "dest_schema": "public",
                    "dest_table": dst,
                    "async_mode": "false",
                    "sync_mode": "full_refresh_overwrite",
                    "validation_mode": "strict",
                    "mappings_json": __import__("json").dumps(mappings),
                    "shape_recipe_json": __import__("json").dumps(RENAME_FILTER_DERIVE),
                    "approved_shape_recipe_hash": recipe_info["recipe_hash"],
                },
            )
            assert run.status_code == 200, run.text
            job = run.json()
            job_id = job.get("job_id") or (job.get("job") or {}).get("id")
            if job.get("status") not in {"completed", "failed", "partial"} and job_id:
                job = h.wait_job(str(job_id))
            assert str(job.get("status") or ("completed" if job.get("success") else "")) == "completed", job
            got = _read("postgresql", dst, ["id", "nm", "label"])
            # CSV: an empty cell is NULL; trimmed-away whitespace is too (file
            # semantics); row 3 is filtered; unicode upper-cases.
            assert got == [
                (1, "ada", "ADA!"),
                (2, None, "!"),
                (4, "zoë ünï 山田", "ZOË ÜNÏ 山田!"),
            ]
    finally:
        h.drop_table("postgresql", dst)


# --------------------------------------------------------------------------- #
# MCP: start_transfer -> /api/v1/copilot/confirm -> job
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("dest_kind", ["postgresql", "mysql"])
def test_mcp_start_transfer_runs_multi_step_column_transforms(client, dest_kind):
    """Per-column pipelines through QA's route, twice into the table run 1 made.

    ``trim -> upper`` (list), ``trim, decimal`` (comma chain) and
    ``datetime -> assume_timezone`` — the base refused every one of them as
    "not a transform the engine applies".
    """
    from services.connector_store import create_connector

    c = client
    token = uuid.uuid4().hex[:8]
    src_cfg = h.dest_config("postgresql")
    dst_cfg = h.dest_config(dest_kind)
    # A typed source's '' is not a number: casting it is refused exactly as the
    # single-op Map ``decimal`` refuses it, so the amount column carries NULL
    # there; '' and whitespace stay on the text column.
    src_table = _seed_pg_source([r if r[0] != 3 else (3, "", None, None) for r in _SEED])
    dst_table = f"r5ms_m_{token}"
    src = create_connector(
        {"name": f"r5ms-src-{token}", "role": "source", "workspace_id": "", "ssl": False, **src_cfg}
    )
    dst = create_connector(
        {"name": f"r5ms-dst-{token}", "role": "destination", "workspace_id": "", "ssl": False, **dst_cfg}
    )
    args = {
        "source_connector_id": src.id,
        "source_table": src_table,
        "dest_connector_id": dst.id,
        "dest_table": dst_table,
        "sync_mode": "full_refresh_overwrite",
        "column_transforms": {
            "name": ["trim", "upper"],
            "amount": "trim, decimal",
            "ts": ["datetime", "assume_timezone:UTC"],
        },
    }
    try:
        for _run in (1, 2):
            h.stage_and_run(c, "start_transfer", args)
            got = _read(dest_kind, dst_table, ["id", "name", "amount", "ts"])
            assert got == [
                (1, "ADA", Decimal("10.5"), datetime(2024, 1, 2, 3, 4, 5)),
                (2, None, None, None),
                (3, "", None, None),
                (4, "", Decimal("7"), datetime(2024, 6, 1, 23, 59, 58)),
                (5, "ZOË ÜNÏ 山田", Decimal("3.25"), datetime(2024, 12, 31, 12, 0, 0)),
            ], got
    finally:
        h.drop_table("postgresql", src_table)
        h.drop_table(dest_kind, dst_table)
