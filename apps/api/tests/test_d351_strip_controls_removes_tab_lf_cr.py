"""D351: ``strip_controls`` removes every control character — TAB, LF and CR
included — in a column_transforms pipeline and as a single step, as the
catalog says ("remove control characters").

QA (Mongo -> PostgreSQL ``name: ["strip_controls", "trim", "upper"]`` and SQL
Server -> MariaDB ``name: ["strip_controls", "trim", "lower"]``): the tab in
row 7 and the newline in row 8 were written. The pipeline compiled
``strip_controls`` to the shape class ``controls``, which keeps tab/LF/CR; the
single-step Map transform kept them too and then collapsed every whitespace run
to one space, so the two paths disagreed.

Decision: removal, nothing put in their place (``"a<TAB>b"`` -> ``"ab"``), and
no other whitespace change (no collapse, no trim) — ``normalize_unicode`` is the
transform that turns tab/newline into a space. The Transform Library class
``strip_characters {characters: "controls"}`` keeps tab/LF/CR as before.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from services.column_transform_chain import compile_column_chains, split_column_transforms
from services.shape_engine import ShapeEngine
from services.shape_expr import _BLANK_AS_NULL
from services.shape_models import ShapeRecipe
from services.transform_engine import TRANSFORM_SEMANTICS, apply_transform
from src.transfer.models import EndpointConfig, TransferRequest
from tests.test_d350_pipeline_default_on_empty import (  # noqa: F401 - fixture
    _dest,
    _drop,
    _mariadb_conn,
    _run,
    _sqlite_ep,
    fake_jobs,
)

pytestmark = pytest.mark.timeout(600)

#: The QA seed rows 7 and 8, plus CR, real controls and non-control text.
VALUES = [
    "O'Brien \\ back\\slash, comma \"q\" \tTAB",
    "line1\nline2",
    "cr\rhere",
    "crlf\r\nend",
    "nul\x00esc\x1bdel\x7fnel\x85",
    "zero​width﻿",
    "ßtraße é ü 山田",
    "  spaced  out  ",
]
REMOVED = [
    "O'Brien \\ back\\slash, comma \"q\" TAB",
    "line1line2",
    "crhere",
    "crlfend",
    "nulescdelnel",
    "zerowidth",
    "ßtraße é ü 山田",
    "  spaced  out  ",
]


def _shape(chains, columns, rows, *, typed: bool):
    payload, refusal = compile_column_chains(chains, columns)
    assert refusal == ""
    engine = ShapeEngine(ShapeRecipe.parse(payload, source_columns=columns))
    token = _BLANK_AS_NULL.set(not typed)
    try:
        return engine.apply_batch(rows)
    finally:
        _BLANK_AS_NULL.reset(token)


def _single_chain(value, ops):
    for op in ops:
        value, err = apply_transform(value, op)
        assert err is None
    return value


def test_single_step_removes_every_control_and_changes_nothing_else():
    assert [apply_transform(v, "strip_controls")[0] for v in VALUES] == REMOVED


@pytest.mark.parametrize("typed", [True, False])
def test_pipeline_alone_removes_the_same_characters(typed):
    out = _shape({"name": ["strip_controls"]}, ["name"], [{"name": v} for v in VALUES], typed=typed)
    assert [r["name"] for r in out] == REMOVED


@pytest.mark.parametrize("case_op", ["upper", "lower"])
@pytest.mark.parametrize("typed", [True, False])
def test_pipeline_then_trim_and_case_equals_the_single_steps(typed, case_op):
    ops = ["strip_controls", "trim", case_op]
    out = _shape({"name": ops}, ["name"], [{"name": v} for v in VALUES], typed=typed)
    got = [r["name"] for r in out]
    assert got == [_single_chain(v, ops) for v in VALUES]
    assert not any(c in g for g in got for c in "\t\n\r\x00\x1b")
    assert got[0].endswith('"Q" TAB') or got[0].endswith('"q" tab')


@pytest.mark.parametrize("typed", [True, False])
def test_null_and_empty_follow_the_source_value_model(typed):
    out = _shape(
        {"name": ["strip_controls", "upper"]}, ["name"], [{"name": None}, {"name": ""}], typed=typed
    )
    assert out[0]["name"] is None
    assert out[1]["name"] == ("" if typed else None)


def test_pipeline_compiles_strip_controls_to_the_all_controls_class():
    payload, refusal = compile_column_chains({"name": ["strip_controls", "trim"]}, ["name"])
    assert refusal == ""
    assert payload["steps"][0] == {
        "op": "strip_characters",
        "options": {"characters": "all_controls"},
        "column": "name",
    }


def test_library_controls_class_still_keeps_tab_newline_cr():
    """A saved recipe's strip_characters {controls} keeps its meaning."""
    recipe = ShapeRecipe.parse(
        {"steps": [{"op": "strip_characters", "column": "v", "options": {"characters": "controls"}}]},
        source_columns=["v"],
    )
    out = ShapeEngine(recipe).apply_batch([{"v": "a\x00b\tc\nd\re"}])
    assert out[0]["v"] == "ab\tc\nd\re"


def test_normalize_unicode_still_turns_tab_newline_into_a_space():
    """D332's contract is separate: NFKC + whitespace collapse."""
    assert apply_transform("a\tb\nc", "normalize_unicode")[0] == "a b c"
    out = _shape({"n": ["normalize_unicode", "lower"]}, ["n"], [{"n": "A\tB\nC"}], typed=True)
    assert out[0]["n"] == "a b c"


def test_catalog_says_tab_newline_cr_are_removed():
    text = TRANSFORM_SEMANTICS["strip_controls"]["semantics"]
    assert "remove" in text and "tab" in text and "newline" in text


# ---------------------------------------------------------------------------
# The transform proof agrees with the writer (SQLite in-process; live typed
# pairs when the engines are configured).
# ---------------------------------------------------------------------------

_TRANSFORMS = {"name": ["strip_controls", "trim", "upper"]}
_ROWS = [(i + 1, v) for i, v in enumerate(VALUES)] + [(len(VALUES) + 1, None)]
_WANT = [(i, None if v is None else _single_chain(v, _TRANSFORMS["name"])) for i, v in _ROWS]


def _request(source, dest) -> TransferRequest:
    _singles, chains = split_column_transforms(_TRANSFORMS)
    recipe, refusal = compile_column_chains(chains, ["id", "name"])
    assert not refusal, refusal
    return TransferRequest(
        source=source,
        destination=dest,
        mappings=[
            {"source": c, "target": c, "approved": True, "confidence": 0.99}
            for c in ("id", "name")
        ],
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        skip_preflight=True,
    )


def _sqlite_pair(tmp_path: Path) -> tuple[Path, Path]:
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", _ROWS)
    conn.close()
    return src, dst


def _sqlite_rows(path: Path) -> list[tuple]:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("SELECT id, name FROM t ORDER BY id").fetchall()
    finally:
        conn.close()


def test_sqlite_run_removes_tab_lf_cr_and_the_proof_verifies(tmp_path, fake_jobs):
    src, dst = _sqlite_pair(tmp_path)
    result = _run(fake_jobs, _request(_sqlite_ep(src), _sqlite_ep(dst)))

    assert result.success, result.error
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    assert _sqlite_rows(dst) == _WANT


def test_proof_fails_when_the_destination_kept_the_tab(tmp_path, fake_jobs, monkeypatch):
    import src.transfer.reconcile_step as rs

    src, dst = _sqlite_pair(tmp_path)
    original = rs._transform_applied_proof

    def _keep_tab_then_prove(**kwargs):
        conn = sqlite3.connect(str(dst))
        with conn:
            conn.execute("UPDATE t SET name = ? WHERE id = 1", (VALUES[0].upper(),))
        conn.close()
        return original(**kwargs)

    monkeypatch.setattr(rs, "_transform_applied_proof", _keep_tab_then_prove)
    result = _run(fake_jobs, _request(_sqlite_ep(src), _sqlite_ep(dst)))

    assert not result.success
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "failed", proof
    assert proof.get("culprit_columns") == ["name"], proof


def _seed(kind: str, table: str) -> EndpointConfig:
    from tests import r4_route_harness as h
    from tests import typed_fidelity_helpers as tf

    if kind == "postgresql":
        conn = h.pg_conn()
        q, ph, ep = f'public."{table}"', "%s", tf.pg_endpoint(table)
    else:
        tf.require_sqlserver_drivers()
        conn = h.mssql_conn()
        q, ph, ep = f"[{table}]", "?", tf.sqlserver_endpoint(table)
    try:
        cur = conn.cursor()
        ntext = "nvarchar(200)" if kind == "sqlserver" else "varchar(200)"
        cur.execute(f"CREATE TABLE {q} (id integer PRIMARY KEY, name {ntext})")  # nosec B608
        for row in _ROWS:
            # PostgreSQL text cannot hold NUL; that row is checked in-process.
            if kind == "postgresql" and row[1] and "\x00" in row[1]:
                continue
            cur.execute(f"INSERT INTO {q} VALUES ({ph}, {ph})", row)  # nosec B608
    finally:
        conn.close()
    return ep


def _read(kind: str, table: str) -> list[tuple]:
    from tests import r4_route_harness as h

    sql = "SELECT id, name FROM {} ORDER BY id"
    if kind == "mariadb":
        conn = _mariadb_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql.format(f"`{table}`"))  # nosec B608
            return [tuple(r) for r in cur.fetchall()]
        finally:
            conn.close()
    return [tuple(r) for r in h.query(kind, sql.format(h.quote(kind, table)))]  # nosec B608


@pytest.mark.parametrize(
    ("src_kind", "dst_kind"), [("sqlserver", "mariadb"), ("postgresql", "mysql")]
)
def test_live_typed_pair_writes_no_tab_lf_cr_and_verifies(fake_jobs, src_kind, dst_kind):
    src_t = f"d351_src_{uuid.uuid4().hex[:8]}"
    dst_t = f"d351_dst_{uuid.uuid4().hex[:8]}"
    dest = _dest(dst_kind, dst_t)
    try:
        source = _seed(src_kind, src_t)
        result = _run(fake_jobs, _request(source, dest))
        assert result.success, result.error
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert proof.get("status") == "verified", proof
        got = _read(dst_kind, dst_t)
        assert got == [r for r in _WANT if r[0] in {g[0] for g in got}]
        assert len(got) >= len(_WANT) - 1
    finally:
        _drop(src_kind, src_t)
        _drop(dst_kind, dst_t)
