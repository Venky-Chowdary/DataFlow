"""Parity at Scale driver on SQLite, the reference engine (design §5.1).

Mode R and mode H, composite keys, the leaf merge, duplicates, empty and
single-row tables, keyless multisets, the systematic stop, statement timeouts
(``not_compared``, never a pass), the streamed fingerprint pass, asynchronous
runs and resume from every checkpoint.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from services.parity_scale import ParityScaleDisabled
from services.parity_scale.dialects.base import UnitTimeout
from services.parity_scale.dialects.sqlite import SqliteSide
from services.parity_scale.driver import ParityScan, ScanConfig, ScanStateError
from services.parity_scale.plan import build_plan
from services.parity_scale.store import FileScanStore, MemoryScanStore

TYPES = {"id": "INTEGER", "name": "VARCHAR(20)", "amt": "NUMERIC(10,2)", "note": "TEXT"}
SMALL = ScanConfig(segment_rows=200, leaf_threshold=50, bisection_factor=4, systematic_min_units=10)


@pytest.fixture(autouse=True)
def _flag_on(monkeypatch):
    monkeypatch.setenv("DATAWRAP_PARITY_SCALE", "1")


def make_db(tmp_path: Path, rows: int = 2000, *, keyless: bool = False, composite: bool = False,
            types: dict[str, str] | None = None) -> Path:
    path = tmp_path / "parity.db"
    conn = sqlite3.connect(path)
    types = types or TYPES
    for table in ("src", "dst"):
        cols = ", ".join(f"{c} {t}" for c, t in types.items())
        if composite:
            cols = "region INTEGER, " + cols + ", PRIMARY KEY (region, id)"
        elif not keyless:
            cols = cols.replace("id INTEGER", "id INTEGER PRIMARY KEY")
        conn.execute(f"CREATE TABLE {table} ({cols})")
        data = []
        for i in range(1, rows + 1):
            row = (i, f"n{i}", round(i * 1.25, 2), "x" * (i % 7))
            data.append(((i % 3,) + row) if composite else row)
        marks = ", ".join("?" for _ in data[0]) if data else ""
        if data:
            conn.executemany(f"INSERT INTO {table} VALUES ({marks})", data)
    conn.commit()
    conn.close()
    return path


def plan_for(*, keyless: bool = False, composite: bool = False, types: dict[str, str] | None = None,
             mappings: list[dict[str, Any]] | None = None, key: list[str] | None = None):
    types = dict(types or TYPES)
    if composite:
        types = {"region": "INTEGER", **types}
    maps = mappings or [{"source": c, "target": c} for c in types]
    if key is None:
        key = [] if keyless else (["region", "id"] if composite else ["id"])
    return build_plan(maps, source_types=types, dest_types=types, source_engine="sqlite",
                      dest_engine="sqlite", key_columns=key)


def opener(path: Path):
    def open_side(role: str, plan):
        return SqliteSide(role=role, plan=plan, table="src" if role == "source" else "dst", path=str(path))

    return open_side


def scan(path: Path, plan, *, mode: str = "auto", config: ScanConfig = SMALL, store=None, fault=None):
    store = store or MemoryScanStore()
    cfg = ScanConfig(**{**config.__dict__, "mode": mode})
    run = ParityScan.start(plan, store=store, open_side=opener(path), config=cfg, fault=fault)
    run.run()
    assert run.done
    return run.result()


def execute(path: Path, *sql: str) -> None:
    conn = sqlite3.connect(path)
    for stmt in sql:
        conn.execute(stmt)
    conn.commit()
    conn.close()


MODES = ["R", "H"]


def test_flag_off_refuses_to_start(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("DATAWRAP_PARITY_SCALE", raising=False)
    monkeypatch.delenv("DATAFLOW_PARITY_SCALE", raising=False)
    path = make_db(tmp_path, 10)
    with pytest.raises(ParityScaleDisabled):
        ParityScan.start(plan_for(), store=MemoryScanStore(), open_side=opener(path))


@pytest.mark.parametrize("mode", MODES)
def test_identical_tables_match_with_full_coverage(tmp_path, mode) -> None:
    path = make_db(tmp_path)
    ev = scan(path, plan_for(), mode=mode)
    assert ev["outcome"] == "match"
    assert ev["coverage"]["complete"] is True
    assert ev["population"]["rows_source"] == ev["population"]["rows_destination"] == 2000
    assert ev["check_kind"] == "segment_diff" and ev["mode"] == mode
    assert ev["differences"]["key_attribution"] == "per_key"
    assert ev["experimental"] is True


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("rows", [0, 1])
def test_empty_and_single_row_tables(tmp_path, mode, rows) -> None:
    path = make_db(tmp_path, rows)
    ev = scan(path, plan_for(), mode=mode)
    assert ev["outcome"] == "match"
    assert ev["population"]["rows_source"] == rows


@pytest.mark.parametrize("mode", MODES)
def test_faults_are_found_with_exact_keys_and_causes(tmp_path, mode) -> None:
    path = make_db(tmp_path)
    execute(
        path,
        "UPDATE dst SET name = 'n1500x' WHERE id = 1500",          # one cell changed
        "DELETE FROM dst WHERE id = 10",                            # missing row
        "INSERT INTO dst VALUES (5000, 'extra', 1, 'e')",           # extra row
        "UPDATE src SET note = NULL WHERE id = 700",                # NULL vs ''
        "UPDATE dst SET note = '' WHERE id = 700",
        "UPDATE src SET note = '' WHERE id = 701",                  # '' vs NULL
        "UPDATE dst SET note = NULL WHERE id = 701",
        "UPDATE dst SET name = 'n900 ' WHERE id = 900",             # trailing space
    )
    ev = scan(path, plan_for(), mode=mode)
    diff = ev["differences"]
    assert ev["outcome"] == "differences_found"
    assert ev["coverage"]["complete"] is True and diff["at_least"] is False
    assert diff["missing_in_destination"] == 1 and diff["missing_keys"] == [[{"$i": "10"}]]
    assert diff["extra_in_destination"] == 1 and diff["extra_keys"] == [[{"$i": "5000"}]]
    assert diff["changed_rows"] == 4
    assert sorted(k[0]["$i"] for k in diff["changed_keys"]) == ["1500", "700", "701", "900"]
    assert diff["changed_cells_by_column"] == {"name": 2, "note": 2}
    assert diff["causes"].get("null_vs_empty") == 2
    assert diff["causes"].get("whitespace") == 1
    assert diff["unexplained_rows"] >= 1


@pytest.mark.parametrize("mode", MODES)
def test_composite_keys(tmp_path, mode) -> None:
    path = make_db(tmp_path, composite=True)
    execute(path, "UPDATE dst SET amt = amt + 1 WHERE region = 1 AND id = 1000",
            "DELETE FROM dst WHERE region = 2 AND id = 1997")
    ev = scan(path, plan_for(composite=True), mode=mode)
    diff = ev["differences"]
    assert diff["changed_keys"] == [[{"$i": "1"}, {"$i": "1000"}]]
    assert diff["missing_keys"] == [[{"$i": "2"}, {"$i": "1997"}]]
    assert diff["changed_cells_by_column"] == {"amt": 1}


@pytest.mark.parametrize("mode", MODES)
def test_duplicate_key_in_a_destination_without_a_key(tmp_path, mode) -> None:
    path = tmp_path / "dup.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("CREATE TABLE dst (id INTEGER, name TEXT)")  # no enforced key
    rows = [(i, f"n{i}") for i in range(1, 501)]
    conn.executemany("INSERT INTO src VALUES (?, ?)", rows)
    conn.executemany("INSERT INTO dst VALUES (?, ?)", rows + [(250, "n250")])
    conn.commit()
    conn.close()
    types = {"id": "INTEGER", "name": "TEXT"}
    ev = scan(path, plan_for(types=types), mode=mode)
    assert ev["differences"]["duplicate_keys"] == 1
    assert ev["differences"]["duplicate_key_values"] == [[{"$i": "250"}]]
    assert ev["population"]["rows_destination"] == 501
    assert ev["outcome"] == "differences_found"


def test_keyless_multiset_counts_without_attributing_keys(tmp_path) -> None:
    path = make_db(tmp_path, 1500, keyless=True)
    execute(path, "INSERT INTO dst SELECT * FROM dst WHERE id = 42",  # one row duplicated
            "DELETE FROM dst WHERE id = 43")                          # one row removed
    ev = scan(path, plan_for(keyless=True))
    diff = ev["differences"]
    assert ev["check_kind"] == "multiset_digest"
    assert ev["check_label"].startswith("Content match (no key)")
    assert diff["key_attribution"] == "cannot_attribute_to_key"
    assert (diff["missing_in_destination"], diff["extra_in_destination"]) == (1, 1)
    assert "changed_rows" not in diff and "missing_keys" not in diff
    assert diff["missing_content_examples"][0][1] == "n43"
    assert diff["extra_content_examples"][0][1] == "n42"


def test_keyless_tables_with_duplicate_rows_match_as_multisets(tmp_path) -> None:
    path = make_db(tmp_path, 300, keyless=True)
    execute(path, "INSERT INTO src SELECT * FROM src WHERE id <= 5", "INSERT INTO dst SELECT * FROM dst WHERE id <= 5")
    ev = scan(path, plan_for(keyless=True))
    assert ev["outcome"] == "match"
    assert ev["population"]["rows_source"] == 305
    # Duplicates count: one more copy in the destination is a difference.
    execute(path, "INSERT INTO dst SELECT * FROM dst WHERE id = 1 LIMIT 1")
    ev = scan(path, plan_for(keyless=True))
    assert ev["differences"]["extra_in_destination"] == 1


@pytest.mark.parametrize("mode", MODES)
def test_systematic_difference_stops_the_drill_down(tmp_path, mode) -> None:
    path = make_db(tmp_path, 3000)
    execute(path, "UPDATE dst SET name = upper(name)")
    cfg = ScanConfig(segment_rows=100, leaf_threshold=200, bisection_factor=4, systematic_min_units=10)
    ev = scan(path, plan_for(), mode=mode, config=cfg)
    assert ev["coverage"]["systematic"] is not None
    assert ev["coverage"]["complete"] is False
    assert ev["outcome"] == "differences_found"
    assert ev["differences"]["at_least"] is True
    # The sample names the column without fetching the table.
    assert set(ev["differences"]["changed_cells_by_column"]) == {"name"}
    assert ev["differences"]["changed_rows"] < 3000


@pytest.mark.parametrize("mode", MODES)
def test_a_unit_that_keeps_timing_out_is_not_compared_never_passed(tmp_path, mode) -> None:
    path = make_db(tmp_path)

    def always(op: str, item: dict) -> None:
        if op in {"range_digest", "chunk:destination"}:
            raise UnitTimeout("simulated statement timeout")

    ev = scan(path, plan_for(), mode=mode, fault=always)
    assert ev["outcome"] == "not_fully_compared"
    assert ev["coverage"]["complete"] is False
    assert ev["coverage"]["units_not_compared"] >= 1
    assert "timed out" in ev["coverage"]["not_compared"][0]["reason"]


def test_a_transient_timeout_is_halved_and_retried(tmp_path) -> None:
    path = make_db(tmp_path)
    fired: list[dict] = []

    def once(op: str, item: dict) -> None:
        if op == "range_digest" and item.get("lvl") == 0 and item.get("lo") is None and not fired:
            fired.append(item)
            raise UnitTimeout("simulated")

    execute(path, "UPDATE dst SET name = 'zz' WHERE id = 3")
    ev = scan(path, plan_for(), mode="R", fault=once)
    assert fired
    assert ev["coverage"]["complete"] is True
    assert ev["differences"]["changed_keys"] == [[{"$i": "3"}]]


def test_a_real_sqlite_statement_timeout_marks_units_not_compared(tmp_path) -> None:
    path = make_db(tmp_path, 3000)
    cfg = ScanConfig(segment_rows=3000, statement_timeout_s=1e-9, max_retries=1)
    ev = scan(path, plan_for(), mode="R", config=cfg)
    assert ev["outcome"] == "not_fully_compared"
    assert ev["coverage"]["units_not_compared"] >= 1


@pytest.mark.parametrize("mode", MODES)
def test_bisection_finds_scattered_differences_and_terminates(tmp_path, mode) -> None:
    path = make_db(tmp_path, 4000)
    changed = [3, 777, 778, 2500, 3999]
    execute(path, *[f"UPDATE dst SET amt = amt + 0.01 WHERE id = {i}" for i in changed])
    lengths: list[int] = []

    class Watching(MemoryScanStore):
        def save(self, scan_id, state):
            lengths.append(len(state["queue"]))
            super().save(scan_id, state)

    cfg = ScanConfig(segment_rows=500, leaf_threshold=20, bisection_factor=4, systematic_min_units=100)
    ev = scan(path, plan_for(), mode=mode, config=cfg, store=Watching())
    assert sorted(int(k[0]["$i"]) for k in ev["differences"]["changed_keys"]) == changed
    assert ev["coverage"]["complete"]
    # Depth-first drilling keeps the frontier bounded by f × levels (+ level 0).
    bound = (4000 // 500 + 1) + 4 * cfg.max_levels + 4
    assert max(lengths) <= bound


def test_streamed_columns_use_the_gate8_fingerprint(tmp_path) -> None:
    path = make_db(tmp_path, 800)
    execute(path, "UPDATE dst SET name = upper(name)")  # what an 'upper' transform writes
    maps = [{"source": c, "target": c} for c in TYPES]
    maps[1]["transform"] = "upper"
    plan = plan_for(mappings=maps)
    assert {c.source: c.cls for c in plan.columns}["name"] == "stream"
    ev = scan(path, plan)
    assert ev["outcome"] == "match", ev["differences"]
    assert ev["coverage"]["stream"]["columns"] == ["name"]
    assert ev["coverage"]["columns_streamed"] == 1
    execute(path, "UPDATE dst SET name = 'WRONG' WHERE id = 33")
    ev = scan(path, plan)
    assert ev["differences"]["changed_keys"] == [[{"$i": "33"}]]
    assert ev["differences"]["changed_cells_by_column"] == {"name": 1}


def test_a_key_that_cannot_be_encoded_in_engine_streams_the_table(tmp_path) -> None:
    path = make_db(tmp_path, 300)
    execute(path, "DELETE FROM dst WHERE id = 7", "UPDATE dst SET note = 'q' WHERE id = 8")
    maps = [{"source": c, "target": c} for c in TYPES]
    maps[0]["transform"] = "trim"
    plan = plan_for(mappings=maps)
    assert not plan.key_encodable
    ev = scan(path, plan)
    assert ev["mode"] == "S"
    assert ev["differences"]["missing_in_destination"] == 1
    assert ev["differences"]["changed_keys"] == [[{"$i": "8"}]]


def test_keyless_with_a_streamed_column_is_a_streamed_multiset(tmp_path) -> None:
    path = make_db(tmp_path, 300, keyless=True)
    execute(path, "DELETE FROM dst WHERE id = 5")
    maps = [{"source": c, "target": c} for c in TYPES]
    maps[3]["transform"] = "trim"
    ev = scan(path, plan_for(keyless=True, mappings=maps))
    assert ev["mode"] == "S" and ev["check_kind"] == "multiset_digest"
    assert ev["differences"]["missing_in_destination"] == 1
    assert ev["differences"]["key_attribution"] == "cannot_attribute_to_key"


def test_ce1_stricter_than_fingerprint_is_judged_equal(tmp_path) -> None:
    """SQLite TEXT is its own temporal carrier: two separator spellings are one instant."""
    types = {"id": "INTEGER", "ts": "TEXT"}
    path = tmp_path / "ts.db"
    conn = sqlite3.connect(path)
    for table in ("src", "dst"):
        conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, ts TEXT)")
    conn.executemany("INSERT INTO src VALUES (?, ?)", [(i, f"2024-01-{i:02d} 10:00:00") for i in range(1, 29)])
    conn.executemany("INSERT INTO dst VALUES (?, ?)", [(i, f"2024-01-{i:02d}T10:00:00") for i in range(1, 29)])
    conn.commit()
    conn.close()
    ev = scan(path, plan_for(types=types))
    assert ev["outcome"] == "match"
    assert ev["differences"]["ce_stricter_than_fp"] == 28
    assert ev["differences"]["changed_rows"] == 0


def test_runs_asynchronously_and_reports_progress(tmp_path) -> None:
    path = make_db(tmp_path)
    store = MemoryScanStore()
    run = ParityScan.start(plan_for(), store=store, open_side=opener(path), config=SMALL)
    progress = run.run(max_seconds=0)
    assert progress["status"] == "running" and progress["items_queued"] >= 1
    assert run.result()["outcome"] == "running"
    # A different worker (another process in production) picks it up later.
    later = ParityScan.resume(progress["scan_id"], store=store, open_side=opener(path))
    final = later.run()
    assert final["status"] == "done"
    assert later.result()["outcome"] == "match"


class Crash(RuntimeError):
    pass


class CrashingStore(MemoryScanStore):
    """Fails closed after ``limit`` saves, like a worker killed mid-scan."""

    def __init__(self, limit: int) -> None:
        super().__init__()
        self.limit = limit

    def save(self, scan_id, state):
        if self.saves >= self.limit:
            raise Crash("worker killed")
        super().save(scan_id, state)


_VOLATILE = {"scan_id", "elapsed_s", "started_at", "finished_at", "resumed", "statements"}


def _stable(ev: dict) -> dict:
    return {k: v for k, v in ev.items() if k not in _VOLATILE}


@pytest.mark.parametrize("mode", MODES)
def test_resume_after_a_kill_at_every_checkpoint_gives_the_same_result(tmp_path, mode) -> None:
    path = make_db(tmp_path, 1200)
    execute(path, "UPDATE dst SET name = 'x' WHERE id = 600", "DELETE FROM dst WHERE id = 11",
            "INSERT INTO dst VALUES (9001, 'e', 1, '')")
    plan = plan_for()
    cfg = ScanConfig(segment_rows=150, leaf_threshold=30, bisection_factor=4, mode=mode, systematic_min_units=50)
    reference_store = MemoryScanStore()
    ref = ParityScan.start(plan, store=reference_store, open_side=opener(path), config=cfg, scan_id="ref")
    ref.run()
    expected = _stable(ref.result())
    total_saves = reference_store.saves
    assert total_saves > 5
    for kill_after in range(1, total_saves):
        store = CrashingStore(kill_after)
        try:
            ParityScan.start(plan, store=store, open_side=opener(path), config=cfg, scan_id="ref").run()
        except Crash:
            pass
        store.limit = 10**9
        resumed = ParityScan.resume("ref", store=store, open_side=opener(path), plan=plan)
        resumed.run()
        got = resumed.result()
        assert _stable(got) == expected, f"kill after save {kill_after}"
        assert got["resumed"] >= 1


def test_resume_refuses_a_changed_plan(tmp_path) -> None:
    path = make_db(tmp_path, 50)
    store = FileScanStore(tmp_path / "scans")
    run = ParityScan.start(plan_for(), store=store, open_side=opener(path), config=SMALL, scan_id="s1")
    run.run(max_seconds=0)
    other = plan_for(types={**TYPES, "note": "VARCHAR(10)"})
    with pytest.raises(ScanStateError, match="plan changed"):
        ParityScan.resume("s1", store=store, open_side=opener(path), plan=other)
    resumed = ParityScan.resume("s1", store=store, open_side=opener(path), plan=plan_for())
    resumed.run()
    assert resumed.result()["outcome"] == "match"


def test_dialect_registry_serves_the_first_wave_and_refuses_later_engines() -> None:
    from services.parity_scale.dialects import IMPLEMENTED, PLANNED, open_side
    from services.parity_scale.plan import PairNotCertified

    assert {"sqlite", "postgresql", "mysql", "mariadb", "sqlserver"} <= set(IMPLEMENTED)
    for engine in PLANNED:
        with pytest.raises(PairNotCertified):
            open_side(engine, role="source", plan=plan_for(), table="t")


@pytest.mark.parametrize("mode", MODES)
def test_rows_without_a_key_value_are_their_own_unit(tmp_path, mode) -> None:
    """Review 3, item 1: a NULL key is in no key range; once a table had more than one unit
    its NULL-key rows were compared on neither side. They are a unit of their own now."""
    path = make_db(tmp_path, 1000, keyless=True)  # no PRIMARY KEY: the key comes from the mapping
    execute(path, "INSERT INTO src VALUES (NULL, 'a', 1, '')", "INSERT INTO src VALUES (NULL, 'b', 2, '')",
            "INSERT INTO dst VALUES (NULL, 'a', 1, '')")
    ev = scan(path, plan_for(key=["id"]), mode=mode)
    assert ev["population"]["rows_source"] == 1002 and ev["population"]["rows_destination"] == 1001
    assert ev["population"]["population_check"]["ok"] is True
    assert ev["differences"]["missing_in_destination"] == 1
    assert ev["differences"]["rows_without_key"]["missing_content_examples"][0][1] == "b"
    assert ev["outcome"] == "differences_found"
    execute(path, "INSERT INTO dst VALUES (NULL, 'b', 2, '')")
    assert scan(path, plan_for(key=["id"]), mode=mode)["outcome"] == "match"
