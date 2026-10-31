"""One on_error vocabulary, and a preview that shows refused rows (QA round 11).

T9 — the built-in template "Safe Cast + Null-Safe Numerics" (and every
library recipe) speaks ``on_error: fail``, while ``/shape`` accepted only
``refuse | divert | null`` and answered 400 for the template's own word.
``fail`` is now the recipe spelling of ``refuse`` everywhere; ``keep_original``
(a recipe preview notion with no engine equivalent) is refused by name.

T2 — previewing a cast to integer on ``1.37`` stopped at the first refused
row and returned ``after: []`` with an empty ``preview_note``. The preview now
evaluates every sampled row, shows the rows that shape, counts the refused
ones and says so.

Through the real routes (``/api/v1/transform-library``, ``/api/v1/shape``) and
``UniversalTransferEngine().execute_tracked`` from a live PostgreSQL source into
the live PostgreSQL, twice into the table the first run created.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(300)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "library.json"))
    from services import transform_library

    transform_library.reset_transform_library()
    from fastapi.testclient import TestClient
    from src.main import app

    return TestClient(app)


_ROWS = [
    {"id": "1", "qty": "1"},
    {"id": "2", "qty": "1.37"},
    {"id": "3", "qty": None},
    {"id": "4", "qty": ""},
    {"id": "5", "qty": "   "},
    {"id": "6", "qty": "x中文"},
    {"id": "7", "qty": "42"},
]


def _cast(on_error):
    step = {"op": "cast_column", "column": "qty", "options": {"to_type": "integer"}}
    if on_error is not None:
        step["on_error"] = on_error
    return {"steps": [step]}


def test_t9_template_vocabulary_is_accepted_by_shape(client):
    cloned = client.post("/api/v1/transform-library/templates/safe-cast-numerics/clone")
    assert cloned.status_code == 201, cloned.text
    words = {s["on_error"] for s in cloned.json()["steps"]}
    assert words == {"fail"}, words

    for spelling in ("fail", "FAIL", "  fail  ", "refuse", "", None):
        res = client.post(
            "/api/v1/shape/validate",
            json={"recipe": _cast(spelling), "source_columns": ["id", "qty"]},
        )
        assert res.status_code == 200, (spelling, res.text)
    # ``fail`` means refuse: the same recipe identity as the engine word.
    as_fail = client.post("/api/v1/shape/validate", json={"recipe": _cast("fail"),
                                                          "source_columns": ["id", "qty"]})
    as_refuse = client.post("/api/v1/shape/validate", json={"recipe": _cast("refuse"),
                                                            "source_columns": ["id", "qty"]})
    assert as_fail.json()["recipe_hash"] == as_refuse.json()["recipe_hash"]

    keep = client.post("/api/v1/shape/validate",
                       json={"recipe": _cast("keep_original"), "source_columns": ["id", "qty"]})
    assert keep.status_code == 400, keep.text
    assert "keep_original" in keep.text and "divert" in keep.text
    bad = client.post("/api/v1/shape/validate",
                      json={"recipe": _cast("ñope"), "source_columns": ["id", "qty"]})
    assert bad.status_code == 400 and "fail" in bad.text, bad.text

    # A library recipe written in the engine's words saves and compiles too.
    made = client.post("/api/v1/transform-library/", json={
        "name": f"r11 refuse {uuid.uuid4().hex[:6]}",
        "steps": [{"column": "qty", "op": "integer", "on_error": "refuse"}],
    })
    assert made.status_code == 201, made.text
    assert made.json()["steps"][0]["on_error"] == "fail"


def test_t2_preview_of_a_refused_cast_shows_the_good_rows_and_says_why(client):
    res = client.post(
        "/api/v1/shape/preview",
        json={"recipe": _cast(None), "sample_rows": _ROWS, "source_columns": ["id", "qty"]},
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["refused"] is True
    assert body["refused_rows"] == 2, body["refusals"]
    assert [r["row"] for r in body["refusals"]] == [2, 6]
    assert body["refusal"]["row"] == 2  # the first one, as before
    shaped = {r["id"]: r["qty"] for r in body["after"]}
    assert set(shaped) == {"1", "3", "4", "5", "7"}, body["after"]
    assert shaped["1"] in (1, "1") and shaped["7"] in (42, "42")
    note = body["preview_note"]
    assert "2 of 7" in note and "refused" in note.lower(), note
    assert "1.37" in note or "row 2" in note, note

    clean = client.post(
        "/api/v1/shape/preview",
        json={"recipe": _cast("refuse"), "sample_rows": [r for r in _ROWS if r["id"] in "1347"],
              "source_columns": ["id", "qty"]},
    )
    assert clean.status_code == 200, clean.text
    assert clean.json()["refused"] is False and clean.json()["refused_rows"] == 0
    assert clean.json()["preview_note"] == ""


def _seed(values: list[tuple]) -> str:
    table = f"r11sh_{uuid.uuid4().hex[:8]}"
    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE public."{table}" (id integer PRIMARY KEY, qty text)')  # nosec B608
        cur.executemany(f'INSERT INTO public."{table}" VALUES (%s, %s)', values)  # nosec B608
    finally:
        conn.close()
    return table


def _run(src: str, dst: str, recipe: dict):
    from services.shape_models import ShapeRecipe
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest
    from tests.typed_fidelity_helpers import pg_endpoint

    parsed = ShapeRecipe.parse(recipe)
    request = TransferRequest(
        source=pg_endpoint(src),
        destination=pg_endpoint(dst),
        mappings=[{"source": c, "target": c, "approved": True, "confidence": 0.99}
                  for c in ("id", "qty")],
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=parsed.recipe_hash,
    )
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def test_t9_fail_runs_as_refuse_on_the_transfer_engine(client):
    good = _seed([(1, "1"), (2, None), (3, "42"), (4, "-7")])
    bad = _seed([(1, "1"), (2, "1.37"), (3, "x中文")])
    dst = f"r11sh_d_{uuid.uuid4().hex[:8]}"
    try:
        for _ in range(2):  # second run into the table the first created
            ok = _run(good, dst, _cast("fail"))
            assert ok.success, ok.error
            rows = h.query("postgresql", f'SELECT id, qty FROM "{dst}" ORDER BY id')  # nosec B608
            assert [(int(i), None if q is None else int(q)) for i, q in rows] == [
                (1, 1), (2, None), (3, 42), (4, -7)
            ]
        refused = _run(bad, f"{dst}_bad", _cast("fail"))
        assert not refused.success
        assert "transform step 1 (cast_column)" in (refused.error or ""), refused.error
    finally:
        for t in (good, bad, dst, f"{dst}_bad"):
            h.drop_table("postgresql", t)
