"""Identity edge cases × sync modes through the real engine, read back independently.

Companion to ``test_identity_sync_mode_matrix``: the destination key is enforced
by the table itself (PRIMARY KEY, or a UNIQUE index on a heap), the identity
holds NULLs or repeats in the source, changes type or is dropped from the
contract between runs, mutates, or the destination holds rows the source never
sent. Every refusal must name its reason and leave the destination untouched;
every success must land exactly the mode's documented population.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

import src.transfer.engine as engine_mod
from tests.route_cert_readback import execute_sql, read_back
from tests.test_execute_tracked_universal_matrix import _build_db_endpoint, _endpoint_reachable
from tests.test_identity_sync_mode_matrix import (
    A1,
    B1,
    B2,
    C3,
    COLS,
    ENGINES,
    _as_counter,
    _norm,
    _population,
    _run,
    _seed,
)
from tests.test_property2_golden_path_never_blocked import _FakeMongo

A2 = (1, "eu", "a2", 2)
#: Source text widths equal to the pre-created destination's, so a strict run
#: judges identity rather than refusing a TEXT -> VARCHAR(n) narrowing.
WIDTHS = ("VARCHAR(8)", "VARCHAR(32)")
D99 = (99, "eu", "z", 0)
NULL_KEY = (None, "eu", "n", 1)
FORTY_ONE_NULL = [(i, "eu", "r", 1) for i in range(1, 40)] + [NULL_KEY]
MUTATED = (20, "eu", "b", 2)
TEXT_KEYS = [("2", "eu", "B", 2), ("abc", "eu", "x", 2)]

# id, pre-created destination, mode, key, cursor, policy, run 1, run 2,
# run-2 source id type, run-2 key (None = same), outcome, final rows, why
EDGES = [
    ("append_pk_default", "pk", "full_refresh_append", "id", "", "", [A1, B1], [B2, C3],
     "INTEGER", None, "refused_r2", [A1, B1], "enforces uniqueness"),
    ("append_pk_skip", "pk", "full_refresh_append", "id", "", "dedupe_skip", [A1, B1], [B2, C3],
     "INTEGER", None, "ok", [A1, B1, C3], ""),
    ("append_pk_update", "pk", "full_refresh_append", "id", "", "upsert", [A1, B1], [B2, C3],
     "INTEGER", None, "ok", [A1, B2, C3], ""),
    ("append_unique_index_default", "unique", "full_refresh_append", "id", "", "", [A1, B1],
     [B2, C3], "INTEGER", None, "refused_r2", [A1, B1], "enforces uniqueness"),
    ("append_unique_index_skip", "unique", "full_refresh_append", "id", "", "dedupe_skip",
     [A1, B1], [B2, C3], "INTEGER", None, "ok", [A1, B1, C3], ""),
    ("upsert_unique_index", "unique", "upsert", "id", "", "", [A1, B1], [B2, C3],
     "INTEGER", None, "ok", [A1, B2, C3], ""),
    ("upsert_null_key", "", "upsert", "id", "", "", [A1, NULL_KEY], None,
     "INTEGER", None, "refused_r1", None, "null"),
    ("upsert_one_null_key_in_40", "", "upsert", "id", "", "", FORTY_ONE_NULL, None,
     "INTEGER", None, "refused_r1", None, "null"),
    ("append_declared_key_one_null_in_40", "", "full_refresh_append", "id", "", "",
     FORTY_ONE_NULL, None, "INTEGER", None, "refused_r1", None, "null"),
    ("scd2_composite_null_part", "", "scd2", "region,id", "", "", [A1, (1, None, "q", 1)], None,
     "INTEGER", None, "refused_r1", None, "null"),
    ("upsert_duplicate_source_key", "", "upsert", "id", "", "", [A1, A2], None,
     "INTEGER", None, "refused_r1", None, "repeats"),
    ("incr_deduped_duplicate_source_key", "", "incremental_deduped", "id", "updated_at", "",
     [A1, A2], None, "INTEGER", None, "ok", [A2], ""),
    ("upsert_key_type_drift", "", "upsert", "id", "", "", [A1, B1], TEXT_KEYS,
     "TEXT", None, "refused_r2", [A1, B1], "id"),
    ("upsert_key_removed", "", "upsert", "id", "", "", [A1, B1], [B2, C3],
     "INTEGER", "", "refused_r2", [A1, B1], "primary key"),
    ("mirror_key_mutation", "", "mirror", "id", "", "", [A1, B1], [A1, MUTATED],
     "INTEGER", None, "ok", [A1, B1, MUTATED], ""),
    ("upsert_keeps_destination_only_row", "pk99", "upsert", "id", "", "", [A1, B1], [B2, C3],
     "INTEGER", None, "ok", [A1, B2, C3, D99], ""),
    ("overwrite_drops_destination_only_row", "heap99", "full_refresh_overwrite", "id", "", "",
     [A1, B1], [B2, C3], "INTEGER", None, "ok", [B2, C3], ""),
    ("mirror_soft_deletes_destination_only_row", "pk99", "mirror", "id", "", "", [A1, B1],
     [B2, C3], "INTEGER", None, "ok", [A1, B2, C3, D99], ""),
    ("reverse_etl_upserts_by_key", "", "reverse_etl", "id", "", "", [A1, B1], [B2, C3],
     "INTEGER", None, "ok", [A1, B2, C3], ""),
    ("reverse_etl_composite_key", "", "reverse_etl", "region,id", "", "", [A1, B1], [B2, C3],
     "INTEGER", None, "ok", [A1, B2, C3], ""),
    ("reverse_etl_keyless", "", "reverse_etl", "", "", "", [A1, B1], None,
     "INTEGER", None, "refused_r1", None, "requires primary_key"),
    ("reverse_etl_null_key", "", "reverse_etl", "id", "", "", [A1, NULL_KEY], None,
     "INTEGER", None, "refused_r1", None, "null"),
    # SQLite has no change log: CDC from it is a named refusal, never a full copy.
    ("cdc_from_logless_source", "", "cdc", "id", "updated_at", "", [A1, B1], None,
     "INTEGER", None, "refused_r1", None, "cdc is not supported for source type 'sqlite'"),
    ("cdc_keyless_from_logless_source", "", "cdc", "", "", "", [A1, B1], None,
     "INTEGER", None, "refused_r1", None, "cdc is not supported"),
]
#: Mirror keeps a key the source no longer has as a soft-deleted row.
MIRROR_DELETED = {
    "mirror_key_mutation": {2},
    "mirror_soft_deletes_destination_only_row": {1, 99},
}


def _qualified(dest) -> str:
    table = str(dest.table)
    if dest.format == "mysql":
        return f"`{table}`"
    if dest.format == "oracle":
        return table
    if dest.format == "postgresql" and getattr(dest, "schema", ""):
        return f'"{dest.schema}"."{table}"'
    return f'"{table}"'


def _pre_create(dest, kind: str) -> None:
    if not kind:
        return
    q = _qualified(dest)
    pk = " PRIMARY KEY" if kind in {"pk", "pk99"} else ""
    # SQLite INTEGER is 64-bit: a 32-bit destination column would be a
    # (correctly refused) narrowing, not the identity case under test.
    wide = {"sqlite": "INTEGER", "oracle": "NUMBER(19)"}.get(dest.format, "BIGINT")
    statements = [
        f"CREATE TABLE {q} (id {wide}{pk}, region VARCHAR(8), name VARCHAR(32), "
        f"updated_at {wide})"
    ]
    if kind == "unique":
        statements.append(f"CREATE UNIQUE INDEX ux_{str(dest.table)[-8:]} ON {q} (id)")
    if kind in {"pk99", "heap99"}:
        statements.append(f"INSERT INTO {q} VALUES (99, 'eu', 'z', 0)")
    execute_sql(dest, statements)


def _absent_or_empty(dest) -> bool:
    try:
        rows = read_back(dest, COLS)
    except Exception:  # noqa: BLE001 — the refused run never created the table
        return True
    return not rows


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("edge", EDGES, ids=[e[0] for e in EDGES])
def test_identity_edge_cell(engine: str, edge: tuple, tmp_path: Path, monkeypatch) -> None:
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    dest = _build_db_endpoint(engine, tmp_path, "dst", uuid.uuid4().hex[:8])
    if engine != "sqlite" and not _endpoint_reachable(dest):
        pytest.skip(f"{engine} not reachable on its configured host/port")
    (name, pre, mode, key, cursor, policy, first, second, id_type2, key2,
     outcome, expected, why) = edge
    _pre_create(dest, pre)
    src = tmp_path / "src.db"

    _seed(src, first, text_types=WIDTHS)
    r1 = _run(fake, src, dest, (name, mode, key, cursor, policy))
    if outcome == "refused_r1":
        assert not r1.success
        assert int(r1.records_transferred or 0) == 0
        assert why in (r1.error or "").lower(), r1.error
        assert _absent_or_empty(dest)
        return
    assert r1.success, r1.error
    preexisting = [D99] if pre in {"pk99"} else []
    assert _as_counter(_population(dest, COLS)) == _as_counter(
        first + preexisting if second is not None else expected
    )
    if second is None:
        return

    _seed(src, second, id_type2, text_types=WIDTHS)
    r2 = _run(fake, src, dest, (name, mode, key if key2 is None else key2, cursor, policy))
    if outcome == "refused_r2":
        assert not r2.success
        assert int(r2.records_transferred or 0) == 0
        assert why in (r2.error or "").lower(), r2.error
    else:
        assert r2.success, r2.error
        assert (r2.reconciliation or {}).get("passed") is True
        accounting = (r2.destination_summary or {}).get("row_accounting") or {}
        assert accounting.get("balanced", True) is True
    assert _as_counter(_population(dest, COLS)) == _as_counter(expected)

    if name in MIRROR_DELETED:
        deleted = {
            int(_norm(i))
            for i, flag in _population(dest, ["id", "_deleted"])
            if _norm(flag) in {"1", "True", "true"}
        }
        assert deleted == MIRROR_DELETED[name]


#: Every spelling a caller can send, the canonical mode it means, and the rows
#: two runs (A1,B1 then B2,C3) must leave — the alias may not change semantics.
ALIASES = [
    ("append", "", "full_refresh_append", [A1, B1, B2, C3]),
    ("insert", "", "full_refresh_append", [A1, B1, B2, C3]),
    ("full_append", "", "full_refresh_append", [A1, B1, B2, C3]),
    ("overwrite", "", "full_refresh_overwrite", [B2, C3]),
    ("replace", "", "full_refresh_overwrite", [B2, C3]),
    ("truncate", "", "full_refresh_overwrite", [B2, C3]),
    ("merge", "id", "upsert", [A1, B2, C3]),
    ("incremental_upsert", "id", "incremental_deduped", [A1, B2, C3]),
    ("full_refresh_mirror", "id", "mirror", [A1, B2, C3]),
    ("scd_2", "id", "scd2", [A1, B1, B2, C3]),
]
POLICY_ALIASES = [
    ("skip_duplicates", "dedupe_skip", [A1, B1, C3]),
    ("merge", "upsert", [A1, B2, C3]),
    ("allow_duplicates", "append", [A1, B1, B2, C3]),
    ("insert", "append", [A1, B1, B2, C3]),
]


@pytest.mark.parametrize(("alias", "key", "canonical", "expected"), ALIASES,
                         ids=[a[0] for a in ALIASES])
def test_sync_mode_alias_lands_canonical_population(
    alias: str, key: str, canonical: str, expected: list, tmp_path: Path, monkeypatch
) -> None:
    from services.sync_cursor import normalize_sync_mode

    assert normalize_sync_mode(alias) == canonical
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    dest = _build_db_endpoint("sqlite", tmp_path, "dst", uuid.uuid4().hex[:8])
    cursor = "updated_at" if canonical == "incremental_deduped" else ""
    src = tmp_path / "src.db"
    for rows in ([A1, B1], [B2, C3]):
        _seed(src, rows)
        result = _run(fake, src, dest, (alias, alias, key, cursor, ""))
        assert result.success, result.error
    assert _as_counter(_population(dest, COLS)) == _as_counter(expected)


@pytest.mark.parametrize(("alias", "canonical", "expected"), POLICY_ALIASES,
                         ids=[a[0] for a in POLICY_ALIASES])
def test_duplicate_policy_alias_lands_canonical_population(
    alias: str, canonical: str, expected: list, tmp_path: Path, monkeypatch
) -> None:
    from services.duplicate_policy import normalize_duplicate_policy

    assert normalize_duplicate_policy(alias) == canonical
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    dest = _build_db_endpoint("sqlite", tmp_path, "dst", uuid.uuid4().hex[:8])
    _pre_create(dest, "pk" if canonical != "append" else "")
    key = "id" if canonical != "append" else ""
    src = tmp_path / "src.db"
    for rows in ([A1, B1], [B2, C3]):
        _seed(src, rows, text_types=WIDTHS)
        result = _run(fake, src, dest, ("p", "full_refresh_append", key, "", alias))
        assert result.success, result.error
    assert _as_counter(_population(dest, COLS)) == _as_counter(expected)
