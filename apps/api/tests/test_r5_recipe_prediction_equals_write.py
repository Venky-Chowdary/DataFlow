"""Property: what design time predicts for a recipe is what Execute writes.

For a matrix of 2-4-step recipes over a CSV source and a typed PostgreSQL
source, the Studio's prediction (``/api/v1/shape/preview`` — the after-rows and
column types Map is built from — and Validate's ``transform_image`` from
``/api/v1/preflight/run``) must equal, column for column and cell for cell, the
table ``UniversalTransferEngine.execute_tracked`` writes into live PostgreSQL.

On the base, a typed source previewed and validated under spreadsheet
semantics ('' and whitespace read as NULL) while Execute kept them as values,
a timestamp cast predicted and wrote midnight, and Validate refused any recipe
that renames or derives a column (HTTP 500).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)

_COLS = ["id", "name", "amount", "ts"]
_ROWS: list[dict[str, Any]] = [
    {"id": 1, "name": "  Ada  Lovelace ", "amount": " 10.50 ", "ts": "2024-01-02 03:04:05"},
    {"id": 2, "name": None, "amount": None, "ts": None},
    {"id": 3, "name": "", "amount": "n/a", "ts": "2024-06-01 23:59:58"},
    {"id": 4, "name": "   ", "amount": "  7 ", "ts": "2024-06-01 00:00:00"},
    {"id": 5, "name": "zoë ünï 山田", "amount": "3.25", "ts": "2024-12-31 12:00:00"},
    {"id": 6, "name": "n/a", "amount": "-0.5", "ts": "2025-03-09 01:30:00"},
]

RECIPES: dict[str, dict[str, Any]] = {
    "trim_upper": {
        "steps": [
            {"op": "trim", "column": "name"},
            {"op": "case", "column": "name", "options": {"mode": "upper"}},
        ]
    },
    "trim_default_upper": {
        "steps": [
            {"op": "trim", "column": "name"},
            {"op": "default_if_null", "column": "name", "options": {"value": "none"}},
            {"op": "case", "column": "name", "options": {"mode": "upper"}},
        ]
    },
    "trim_nullif_cast_decimal": {
        "steps": [
            {"op": "trim", "column": "amount"},
            {"op": "null_if", "column": "amount", "options": {"values": ["n/a", ""]}},
            {"op": "cast_column", "column": "amount", "options": {"to_type": "decimal"}},
        ]
    },
    "rename_trim_derive": {
        "steps": [
            {"op": "rename_column", "column": "name", "options": {"to": "nm"}},
            {"op": "trim", "column": "nm"},
            {"op": "collapse_whitespace", "column": "nm"},
            {
                "op": "derive_column",
                "options": {"to": "label", "expression": "concat(upper([nm]), '#')"},
            },
        ]
    },
    "cast_ts_assume_tz": {
        "steps": [
            {"op": "cast_column", "column": "ts", "options": {"to_type": "timestamp"}},
            {
                "op": "cast_column",
                "column": "ts",
                "options": {"to_type": "timestamp", "timezone": "UTC"},
            },
        ]
    },
    "filter_trim_lower_across": {
        "steps": [
            {"op": "filter_rows", "options": {"condition": "[id] <> 2"}},
            {"op": "trim", "column": "name"},
            {"op": "case", "column": "name", "options": {"mode": "lower"}},
            {"op": "replace", "column": "amount", "options": {"search": ".", "replacement": ","}},
        ]
    },
}


def _csv_bytes() -> bytes:
    import csv
    import io

    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(_COLS)
    for r in _ROWS:
        w.writerow(["" if r[c] is None else r[c] for c in _COLS])
    return buf.getvalue().encode("utf-8")


def _seed_pg() -> str:
    table = f"r5pp_src_{uuid.uuid4().hex[:8]}"
    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f'CREATE TABLE public."{table}" (id integer PRIMARY KEY, name text, '  # nosec B608
            "amount text, ts text)"
        )
        cur.executemany(
            f'INSERT INTO public."{table}" VALUES (%s, %s, %s, %s)',  # nosec B608
            [tuple(r[c] for c in _COLS) for r in _ROWS],
        )
    finally:
        conn.close()
    return table


def _family(carrier: str) -> str:
    t = str(carrier or "").strip().lower()
    if "timestamp with time zone" in t or t.startswith("timestamptz"):
        return "timestamptz"
    if t.startswith("timestamp") or t.startswith("datetime"):
        return "timestamp"
    if t.startswith("date"):
        return "date"
    if t in {"integer", "int", "bigint", "smallint", "int4", "int8"} or t.startswith("integer"):
        return "integer"
    if t.startswith(("numeric", "decimal", "number")):
        return "decimal"
    if t.startswith("bool"):
        return "boolean"
    return "text"


def _canon(value: Any, family: str, *, file_source: bool) -> Any:
    """One spelling per value, so JSON preview cells and driver cells compare."""
    if value is None:
        return None
    if file_source and isinstance(value, str) and value == "":
        # A file's empty cell is its NULL (the CSV reader writes NULL for it).
        return None
    if family in {"timestamp", "timestamptz"}:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, datetime) and value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat()
    if family == "date":
        return value.isoformat() if isinstance(value, (date, datetime)) else str(value)
    if family in {"integer", "decimal"}:
        try:
            return str(Decimal(str(value)).normalize())
        except InvalidOperation:
            return str(value)
    return str(value)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _source_sample(kind: str) -> list[dict[str, Any]]:
    if kind == "csv":
        # What the Studio holds after an upload: text cells, '' for empty.
        return [
            {c: ("" if r[c] is None else str(r[c])) for c in _COLS} for r in _ROWS
        ]
    # What a database sample carries: driver values, NULL as null.
    return [dict(r) for r in _ROWS]


@pytest.mark.parametrize("source_kind", ["csv", "postgresql"])
@pytest.mark.parametrize("recipe_name", sorted(RECIPES))
def test_predicted_schema_and_values_equal_what_execute_writes(
    client, source_kind, recipe_name
):
    from services.shape_models import ShapeRecipe
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.typed_fidelity_helpers import pg_endpoint

    cfg = h.dest_config("postgresql")
    recipe = RECIPES[recipe_name]
    src_type = "csv" if source_kind == "csv" else "postgresql"
    sample = _source_sample(source_kind)
    if source_kind == "csv":
        # The Studio sends the upload's own inferred schema.
        up = client.post(
            "/api/v1/connectors/upload", files={"file": ("people.csv", _csv_bytes(), "text/csv")}
        )
        assert up.status_code == 200, up.text
        declared = {str(k): str(v) for k, v in (up.json().get("schema") or {}).items()}
        assert sorted(declared) == sorted(_COLS), up.json().get("schema")
    else:
        declared = {"id": "INTEGER", "name": "TEXT", "amount": "TEXT", "ts": "TEXT"}

    # --- design time: the Studio's Transform preview -----------------------
    pv = client.post(
        "/api/v1/shape/preview",
        json={
            "recipe": recipe,
            "sample_rows": sample,
            "source_columns": _COLS,
            "column_types": declared,
            "source_type": src_type,
        },
    )
    assert pv.status_code == 200, pv.text
    preview = pv.json()
    assert not preview.get("refusal"), preview.get("refusal")
    out_cols = list(preview["recipe"]["output_columns"])
    predicted_types = dict(preview.get("column_types") or {})
    predicted_rows = list(preview.get("after") or [])

    # --- Validate: the image the gates judge -------------------------------
    dst = f"r5pp_d_{uuid.uuid4().hex[:8]}"
    mappings = [{"source": c, "target": c, "confidence": 0.99} for c in out_cols]
    pf = client.post(
        "/api/v1/preflight/run",
        json={
            "columns": _COLS,
            "column_types": declared,
            "row_count": len(_ROWS),
            "sample_rows": sample,
            "source_kind": "file" if source_kind == "csv" else "database",
            "source_type": src_type,
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
            "shape_recipe": recipe,
        },
    )
    assert pf.status_code == 200, pf.text
    image = pf.json().get("transform_image") or {}
    assert image.get("columns") == out_cols, image
    # Validate and the preview are one model: same column carriers.
    assert {
        c: _family(t) for c, t in (image.get("column_types") or {}).items()
    } == {c: _family(predicted_types[c]) for c in out_cols}, image

    # --- Execute -----------------------------------------------------------
    src_table = _seed_pg() if source_kind == "postgresql" else ""
    try:
        if source_kind == "csv":
            source = EndpointConfig(kind="file", format="csv")
            extra = {"source_content": _csv_bytes(), "source_filename": "people.csv"}
        else:
            source = pg_endpoint(src_table)
            extra = {}
        request = TransferRequest(
            source=source,
            destination=pg_endpoint(dst),
            mappings=[
                {"source": c, "target": c, "approved": True, "confidence": 0.99}
                for c in out_cols
            ],
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
            shape_recipe=recipe,
            approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
            **extra,
        )
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        assert result.success, result.error
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        if source_kind == "csv":
            # Gate-8's independent re-read needs a database source.
            assert proof.get("status") in {"verified", "skipped", "unverifiable"}, proof
        else:
            assert proof.get("status") in {"verified", "skipped"}, proof

        written_types = {
            r[0]: r[1]
            for r in h.query(
                "postgresql",
                "SELECT column_name, data_type FROM information_schema.columns "  # nosec B608
                f"WHERE table_schema = 'public' AND table_name = '{dst}'",
            )
        }
        assert sorted(written_types) == sorted(out_cols)
        families = {c: _family(written_types[c]) for c in out_cols}
        # The carrier a step writes (or creates) is decided from the shaped
        # values; prediction and Execute must decide it the same way. Columns no
        # step touches keep the ordinary Map/automap decision.
        parsed = ShapeRecipe.parse(recipe, source_columns=_COLS)
        written_by_recipe = [
            c for c in out_cols if c in parsed.touched_columns or c not in _COLS
        ]
        assert {c: _family(predicted_types[c]) for c in written_by_recipe} == {
            c: families[c] for c in written_by_recipe
        }

        sel = ", ".join(f'"{c}"' for c in out_cols)
        written = h.query("postgresql", f'SELECT {sel} FROM public."{dst}" ORDER BY "id"')  # nosec B608
        file_src = source_kind == "csv"
        got = [
            tuple(_canon(v, families[c], file_source=file_src) for c, v in zip(out_cols, row))
            for row in written
        ]
        want = sorted(
            (
                tuple(
                    _canon(r.get(c), families[c], file_source=file_src) for c in out_cols
                )
                for r in predicted_rows
            ),
            key=lambda t: int(t[0]),
        )
        assert got == want
    finally:
        h.drop_table("postgresql", dst)
        if src_table:
            h.drop_table("postgresql", src_table)
