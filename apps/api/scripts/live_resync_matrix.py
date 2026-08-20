"""Live re-sync matrix: what happens when the destination already holds rows.

Every scenario here is the question a customer asks after the first load: "the
table already has data — what does the product do now?" Each one builds real
source and destination tables, seeds the destination *before* the run, drives the
product path (``execute_tracked`` — preflight gates, write, Gate-8), and records
the measured verdict together with the destination state and the per-key state
afterwards.

The scenarios each declare the contract they expect, and the harness reports
whether the measurement met it (``meets_contract``). Nothing is asserted and
nothing is rounded to green: a scenario whose measurement disagrees with its
declared contract is a product gap, and the artifact must say so.
"""

from __future__ import annotations

import json
import sys
from typing import Any, Callable

sys.path.insert(0, "/home/ubuntu/repos/DataFlow/apps/api")
sys.path.insert(0, "/home/ubuntu/repos/DataFlow/apps/api/scripts")

from live_migration_scenario_matrix import (  # noqa: E402
    DESTS,
    DST,
    SRC,
    dest_count,
    dest_exec,
    dest_query,
    dest_table_ref,
    m,
    pg_exec,
    run_transfer,
)

# Engines this file drives. Oracle stays available through DESTS but is opt-in:
# the container is not always up, and a skipped engine is honest while a
# swallowed connection error is not.
ENGINES = ["postgresql", "mysql"]


def _types(engine: str) -> tuple[str, str, str]:
    """(id type, string type, quoted-id helper marker) for this destination."""
    if engine == "oracle":
        return "NUMBER(19)", "VARCHAR2(64)", "oracle"
    return "BIGINT", "VARCHAR(64)", "ansi"


def _col(engine: str, name: str) -> str:
    return f'"{name}"' if engine == "oracle" else name


def _seed_source(rows: list[tuple[int, str]], *, unique_ids: bool = True) -> None:
    # Duplicate source identities are a real customer shape (two CRM exports of
    # the same account), so that scenario needs a source without the constraint.
    key = " PRIMARY KEY" if unique_ids else ""
    pg_exec([f"DROP TABLE IF EXISTS {SRC}",
             f"CREATE TABLE {SRC} (id BIGINT{key}, name VARCHAR(64))"])
    pg_exec([f"INSERT INTO {SRC} VALUES ({i}, '{n}')" for i, n in rows])


def _create_dest(engine: str, *, keyed: bool, seed: list[tuple[int, str]]) -> None:
    ref = dest_table_ref(engine)
    idtype, strtype, _ = _types(engine)
    key = " PRIMARY KEY" if keyed else ""
    dest_exec(engine, [
        f"DROP TABLE {ref}",
        f"CREATE TABLE {ref} ({_col(engine, 'id')} {idtype}{key}, "
        f"{_col(engine, 'name')} {strtype})",
    ])
    if seed:
        dest_exec(engine, [
            f"INSERT INTO {ref} ({_col(engine, 'id')}, {_col(engine, 'name')}) "
            f"VALUES ({i}, '{n}')" for i, n in seed
        ])


def _dest_state(engine: str) -> list[tuple]:
    ref = dest_table_ref(engine)
    try:
        return sorted(
            dest_query(engine, f"SELECT {_col(engine, 'id')}, {_col(engine, 'name')} FROM {ref}"),
            key=lambda r: (str(r[0]), str(r[1])),
        )
    except Exception as exc:
        return [("dest_read_failed", str(exc)[:120])]


def _maps(engine: str, *, keyed: bool) -> list[dict[str, Any]]:
    idtype, strtype, _ = _types(engine)
    return [
        m("id", "id", "BIGINT", idtype, primary_key=keyed),
        m("name", "name", "VARCHAR(64)", strtype),
    ]


def _scenario(
    engine: str,
    *,
    source: list[tuple[int, str]],
    dest_seed: list[tuple[int, str]],
    sync_mode: str,
    keyed: bool,
    contract: str,
    expect_rows: int | None,
    expect_success: bool,
    runs: int = 1,
    unique_source_ids: bool = True,
) -> dict[str, Any]:
    _seed_source(source, unique_ids=unique_source_ids)
    _create_dest(engine, keyed=keyed, seed=dest_seed)
    before = dest_count(engine)
    verdicts: list[dict[str, Any]] = []
    for _ in range(runs):
        verdicts.append(run_transfer(engine, _maps(engine, keyed=keyed), sync_mode=sync_mode))
    after = dest_count(engine)
    success = all(bool(v["success"]) for v in verdicts)
    out: dict[str, Any] = {
        "contract": contract,
        "sync_mode": sync_mode,
        "runs": runs,
        "source_rows": len(source),
        "dest_rows_before": before,
        "dest_rows_after": after,
        "dest_delta": after - before if before >= 0 and after >= 0 else None,
        "success": success,
        "expected_success": expect_success,
        "expected_dest_rows_after": expect_rows,
        "rows_written": sum(int(v["rows_written"]) for v in verdicts),
        "error": next((v["error"] for v in verdicts if v["error"]), ""),
        "dest_state": _dest_state(engine),
    }
    out["meets_contract"] = bool(
        success == expect_success and (expect_rows is None or after == expect_rows)
    )
    return out


# --------------------------------------------------------------------------- cases

A = [(1, "one"), (2, "two"), (3, "three")]
CHANGED = [(1, "one-v2"), (2, "two-v2"), (3, "three-v2")]
NEW_KEYS = [(4, "four"), (5, "five")]
UNRELATED = [(90, "pre-existing-a"), (91, "pre-existing-b")]


def empty_dest_append(engine: str) -> dict[str, Any]:
    return _scenario(
        engine, source=A, dest_seed=[], sync_mode="incremental_append", keyed=False,
        contract="empty destination + append → 3 rows land, dest delta 3",
        expect_rows=3, expect_success=True,
    )


def nonempty_dest_append_unrelated(engine: str) -> dict[str, Any]:
    return _scenario(
        engine, source=A, dest_seed=UNRELATED, sync_mode="incremental_append", keyed=False,
        contract="unrelated rows held + append → 3 added, 2 pre-existing untouched (delta 3)",
        expect_rows=5, expect_success=True,
    )


def nonempty_dest_append_same_keys_keyed(engine: str) -> dict[str, Any]:
    return _scenario(
        engine, source=A, dest_seed=A, sync_mode="full_refresh_append", keyed=True,
        contract="keyed destination already holds these keys + append → refused before the "
                 "write, destination untouched",
        expect_rows=3, expect_success=False,
    )


def upsert_identical_rows(engine: str) -> dict[str, Any]:
    return _scenario(
        engine, source=A, dest_seed=A, sync_mode="incremental_deduped", keyed=True,
        contract="upsert with identical values → idempotent, still 3 rows",
        expect_rows=3, expect_success=True,
    )


def upsert_changed_values(engine: str) -> dict[str, Any]:
    out = _scenario(
        engine, source=CHANGED, dest_seed=A, sync_mode="incremental_deduped", keyed=True,
        contract="upsert with changed values → 3 rows, every value replaced by the source",
        expect_rows=3, expect_success=True,
    )
    names = {str(r[1]) for r in out["dest_state"]}
    out["values_updated"] = names == {"one-v2", "two-v2", "three-v2"}
    out["meets_contract"] = bool(out["meets_contract"] and out["values_updated"])
    return out


def upsert_new_keys(engine: str) -> dict[str, Any]:
    return _scenario(
        engine, source=NEW_KEYS, dest_seed=A, sync_mode="incremental_deduped", keyed=True,
        contract="upsert with new keys → 2 inserted alongside the 3 held (5 rows)",
        expect_rows=5, expect_success=True,
    )


def upsert_twice_is_idempotent(engine: str) -> dict[str, Any]:
    return _scenario(
        engine, source=CHANGED, dest_seed=A, sync_mode="incremental_deduped", keyed=True,
        contract="upsert run twice → converges, still 3 rows (at-least-once safe)",
        expect_rows=3, expect_success=True, runs=2,
    )


def overwrite_replaces_everything(engine: str) -> dict[str, Any]:
    out = _scenario(
        engine, source=A, dest_seed=UNRELATED, sync_mode="full_refresh_overwrite", keyed=False,
        contract="overwrite → destination holds only this run's 3 rows; pre-existing rows gone",
        expect_rows=3, expect_success=True,
    )
    ids = {str(r[0]) for r in out["dest_state"]}
    out["pre_existing_removed"] = not ({"90", "91"} & ids)
    out["meets_contract"] = bool(out["meets_contract"] and out["pre_existing_removed"])
    return out


def append_same_batch_twice_unkeyed(engine: str) -> dict[str, Any]:
    return _scenario(
        engine, source=A, dest_seed=[], sync_mode="incremental_append", keyed=False,
        contract="append the same batch twice into a keyless destination → 6 rows; append "
                 "duplicates by design and the run must not claim otherwise",
        expect_rows=6, expect_success=True, runs=2,
    )


def duplicate_source_keys_into_keyed_dest(engine: str) -> dict[str, Any]:
    return _scenario(
        engine, source=[(1, "one"), (1, "one-dup"), (2, "two")], dest_seed=[],
        sync_mode="incremental_deduped", keyed=True,
        contract="two source rows share a key + upsert → duplicate source identity is "
                 "surfaced, destination never silently keeps one at random",
        expect_rows=None, expect_success=False, unique_source_ids=False,
    )


SCENARIOS: dict[str, Callable[[str], dict[str, Any]]] = {
    "empty_dest_append": empty_dest_append,
    "nonempty_dest_append_unrelated_rows": nonempty_dest_append_unrelated,
    "nonempty_dest_append_same_keys_keyed": nonempty_dest_append_same_keys_keyed,
    "upsert_identical_rows": upsert_identical_rows,
    "upsert_changed_values": upsert_changed_values,
    "upsert_new_keys": upsert_new_keys,
    "upsert_twice_is_idempotent": upsert_twice_is_idempotent,
    "overwrite_replaces_everything": overwrite_replaces_everything,
    "append_same_batch_twice_unkeyed": append_same_batch_twice_unkeyed,
    "duplicate_source_keys_into_keyed_dest": duplicate_source_keys_into_keyed_dest,
}


def main() -> int:
    results: dict[str, dict[str, Any]] = {}
    for engine in ENGINES:
        if engine not in DESTS:
            continue
        per: dict[str, Any] = {}
        for name, fn in SCENARIOS.items():
            try:
                per[name] = fn(engine)
            except Exception as exc:  # harness/engine failure — report, never invent
                per[name] = {"harness_error": f"{type(exc).__name__}: {exc}"[:300]}
            line = per[name]
            flag = (
                "GAP" if line.get("meets_contract") is False
                else "ERR" if "harness_error" in line
                else "ok"
            )
            print(f"{engine:12} {name:40} {flag:3} {json.dumps(line, default=str)[:200]}",
                  flush=True)
        results[engine] = per
    with open("/home/ubuntu/repro/resync_matrix_results.json", "w") as fh:
        json.dump(results, fh, indent=2, default=str)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
