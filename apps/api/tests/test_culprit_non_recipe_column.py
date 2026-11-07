"""Culprit attribution: destination drift on a column no recipe step touches
must name that column — not blame the recipe's declared columns/steps.

Regression: a tampered non-recipe dest column used to come back as
``culprit_columns = [every declared column]`` and ``culprit_steps = [every
step]``, because per-column attribution only compared recipe-declared
columns and fell back to blaming all of them.
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


class _FakeMongo:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True

    def list_jobs(self, limit: int = 50) -> list[dict]:
        return list(self.jobs.values())

    def create_transfer_job(self, job_data: dict) -> str:
        job_id = "0" * 24
        self.jobs[job_id] = job_data
        return job_id

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        self.jobs.setdefault(job_id, {}).update(fields)
        return True


COLS = ["id", "amount", "note"]


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    fake_mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake_mongo)


def _make_source(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount TEXT, note TEXT)"
    )
    for i in range(1, 6):
        conn.execute(
            "INSERT INTO orders VALUES (?, ?, ?)", (i, f"{i}.00", f"n{i}")
        )
    conn.commit()
    conn.close()


def _request(src: Path, dst: Path) -> TransferRequest:
    recipe = {
        "steps": [
            {"op": "case", "column": "note", "options": {"mode": "upper"}},
        ]
    }
    from services.shape_models import ShapeRecipe

    ep = lambda path, tbl: EndpointConfig(  # noqa: E731
        kind="database", format="sqlite", database=str(path), table=tbl
    )
    return TransferRequest(
        source=ep(src, "orders"),
        destination=ep(dst, "orders_out"),
        sync_mode="full_refresh_overwrite",
        mappings=[
            {"source": c, "target": c, "confidence": 1.0,
             "user_override": True, "transform": "none"}
            for c in COLS
        ],
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(
            recipe, source_columns=COLS
        ).recipe_hash,
        stream_contracts=[
            {"name": "orders", "primary_key": "id",
             "sync_mode": "full_refresh_overwrite"}
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


def test_tampered_non_recipe_column_names_itself(monkeypatch):
    """Corrupt only ``amount`` (untouched by the recipe) on its way to the
    destination — the proof must name ``amount`` and no recipe step."""
    from services import shape_apply as shape_apply_mod

    real_matrix = shape_apply_mod.ShapeRunner.matrix

    def corrupt_amount(self, headers, rows):
        out_headers, out_rows = real_matrix(self, headers, rows)
        idx = out_headers.index("amount")
        return out_headers, [
            [("TAMPERED" if i == idx else v) for i, v in enumerate(r)]
            for r in out_rows
        ]

    monkeypatch.setattr(shape_apply_mod.ShapeRunner, "matrix", corrupt_amount)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src, dst = tmp_path / "src.db", tmp_path / "dst.db"
        _make_source(src)

        engine = UniversalTransferEngine()
        res = engine.execute_tracked(_request(src, dst), "0" * 24)
        assert not res.success, "tampered write must fail the proof"
        recon = res.reconciliation or {}
        proof = recon.get("transform_applied") or {}
        assert proof.get("status") == "failed", proof
        assert proof.get("culprit_columns") == ["amount"], proof
        steps = proof.get("culprit_steps") or []
        assert not any("case" in s for s in steps), steps
        assert any("not explained by a recipe step" in s for s in steps), steps
        # X-32: the rendered message must be one clean sentence — not the two
        # templates spliced ("…not explained by a recipe step did not apply as
        # declared").
        assert proof.get("non_recipe_drift_columns") == ["amount"], proof
        msg = str(recon.get("message") or "")
        assert "is not explained by any recipe step" in msg, msg
        assert "did not apply as declared" not in msg, msg


def test_tampered_recipe_column_still_names_the_step(monkeypatch):
    """Counterpart: corrupt the recipe's own column — the step is blamed."""
    from services import shape_apply as shape_apply_mod

    real_matrix = shape_apply_mod.ShapeRunner.matrix

    def corrupt_note(self, headers, rows):
        out_headers, out_rows = real_matrix(self, headers, rows)
        idx = out_headers.index("note")
        return out_headers, [
            [("TAMPERED" if i == idx else v) for i, v in enumerate(r)]
            for r in out_rows
        ]

    monkeypatch.setattr(shape_apply_mod.ShapeRunner, "matrix", corrupt_note)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src, dst = tmp_path / "src.db", tmp_path / "dst.db"
        _make_source(src)

        engine = UniversalTransferEngine()
        res = engine.execute_tracked(_request(src, dst), "0" * 24)
        assert not res.success, "tampered write must fail the proof"
        recon = res.reconciliation or {}
        proof = recon.get("transform_applied") or {}
        assert proof.get("status") == "failed", proof
        assert proof.get("culprit_columns") == ["note"], proof
        assert any("case" in s for s in (proof.get("culprit_steps") or [])), proof
