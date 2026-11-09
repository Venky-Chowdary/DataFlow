"""Non-key schema drift between runs, for every sync mode, on every live SQL
destination, read back with the destination's own driver.

* ``widen_text``: run 2's ``name`` grows from VARCHAR(32) to TEXT with a
  100-char value. A server destination keeps VARCHAR(32): Validate refuses,
  run 2 writes 0 rows and the destination is unchanged (never truncated).
  SQLite does not enforce VARCHAR(n): the full 100 chars land.
* ``name_to_int``: run 2's ``name`` becomes BIGINT. BIGINT renders in at most
  20 chars, so it lands in the existing VARCHAR(32) as text for every mode.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

import src.transfer.engine as engine_mod
from tests.route_cert_readback import read_back
from tests.test_execute_tracked_universal_matrix import _build_db_endpoint, _endpoint_reachable
from tests.test_identity_sync_mode_matrix import COLS, _run, _seed
from tests.test_property2_golden_path_never_blocked import _FakeMongo

ENGINES = ["sqlite", "postgresql", "mysql", "sqlserver", "oracle"]
LONG = "x" * 100
FIRST = [(1, "eu", "a", 1), (2, "eu", "b", 1)]
MODES = [
    ("full_refresh_overwrite", "", ""),
    ("full_refresh_append", "", ""),
    ("upsert", "id", ""),
    ("incremental_deduped", "id", "updated_at"),
    ("mirror", "id", ""),
    ("scd2", "id", ""),
]
DRIFTS = {
    "widen_text": (("VARCHAR(8)", "TEXT"), [(2, "eu", LONG, 2), (3, "eu", "c", 2)]),
    "name_to_int": (("VARCHAR(8)", "INTEGER"), [(2, "eu", 7, 2), (3, "eu", 8, 2)]),
}


def _expected(mode: str, second: list[tuple]) -> list[tuple]:
    """Rows (id, name) each mode leaves; mirror keeps id 1 as a soft delete."""
    run2 = [(str(i), str(n)) for i, _, n, _ in second]
    first = [(str(i), str(n)) for i, _, n, _ in FIRST]
    if mode == "full_refresh_overwrite":
        return sorted(run2)
    if mode in {"full_refresh_append", "scd2"}:
        return sorted(first + run2)
    return sorted([first[0], *run2])


def _population(dest) -> list[tuple]:
    return sorted((str(r["id"]).removesuffix(".0"), str(r["name"])) for r in read_back(dest, COLS))


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("mode", MODES, ids=[m[0] for m in MODES])
@pytest.mark.parametrize("drift", list(DRIFTS))
def test_non_key_drift(engine: str, mode: tuple, drift: str, tmp_path: Path, monkeypatch) -> None:
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    dest = _build_db_endpoint(engine, tmp_path, "dst", uuid.uuid4().hex[:8])
    if engine != "sqlite" and not _endpoint_reachable(dest):
        pytest.skip(f"{engine} not reachable on its configured host/port")
    types2, second = DRIFTS[drift]
    src = tmp_path / "src.db"
    cell = (drift, *mode, "")
    _seed(src, FIRST, text_types=("VARCHAR(8)", "VARCHAR(32)"))
    r1 = _run(fake, src, dest, cell)
    assert r1.success, r1.error
    before = _population(dest)

    _seed(src, second, text_types=types2)
    r2 = _run(fake, src, dest, cell)
    if drift == "widen_text" and engine != "sqlite":
        assert not r2.success
        assert int(r2.records_transferred or 0) == 0
        assert "width overflow" in (r2.error or "").lower(), r2.error
        assert _population(dest) == before
        return
    assert r2.success, r2.error
    assert (r2.reconciliation or {}).get("passed") is True
    assert _population(dest) == _expected(mode[0], second)
