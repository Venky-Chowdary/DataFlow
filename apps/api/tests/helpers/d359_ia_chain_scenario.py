"""D359 live scenario: the QA 3-run incremental_append chain, with transforms.

Shared by the SQLite / PostgreSQL legs (run here) and the SQL Server
(cdc-sqlserver) and Oracle (oracle-live) legs, which only CI can run.

Schedule shape (QA r42_ia_dst): keyless destination created by run 1,
incremental_append on cursor ``updated_at``, ``schema_policy=propagate_columns``,
a recipe step chain on ``name`` (trim, lower) and a Map transform on ``note``
(upper). Run 1 loads 30 rows. Before run 2 the source gains ``region``, two
rows change (cursor bumped), one row is edited without a cursor bump (never
re-read), one row is deleted (it stays: append keeps history), and two rows are
inserted, the later of them all-NULL (it is the watermark row run 3 re-reads).
Before run 3 one more row changes and one is inserted.

Every run must be green and the destination must equal an independent model
row by row: one appended version per change, no boundary row appended twice.
"""

from __future__ import annotations

import uuid
from collections import Counter
from typing import Any, Callable

TRANSFORMS = {"name": ["trim", "lower"], "note": "upper"}
COLUMNS = ["id", "name", "note", "updated_at"]


def ts(i: int, hour: int = 10) -> str:
    return f"2026-11-01 {hour:02d}:{i:02d}:00"


def request(source: Any, dest: Any, stream: str, *, mappings_cols: list[str] = COLUMNS) -> Any:
    """What a saved schedule replays: approved mappings, recipe, no declared key."""
    from services.column_transform_chain import compile_column_chains, split_column_transforms
    from services.shape_models import ShapeRecipe
    from src.transfer.models import TransferRequest

    singles, chains = split_column_transforms(TRANSFORMS)
    recipe, refusal = compile_column_chains(chains, mappings_cols)
    assert not refusal, refusal
    mappings = []
    for col in mappings_cols:
        m = {"source": col, "target": col, "approved": True, "confidence": 0.99}
        if col in singles:
            m.update(transform=singles[col], transform_override=True)
        mappings.append(m)
    return TransferRequest(
        source=source,
        destination=dest,
        mappings=mappings,
        sync_mode="incremental_append",
        validation_mode="strict",
        schema_policy="propagate_columns",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        stream_contracts=[
            {
                "name": stream,
                "cursor_field": "updated_at",
                "cursor_semantics": "modification_timestamp",
                "sync_mode": "incremental_append",
                "selected": True,
            }
        ],
    )


def _shaped(name: Any, note: Any, region: Any) -> tuple:
    return (
        None if name is None else str(name).strip().lower(),
        None if note is None else str(note).upper(),
        region,
    )


def run_chain(
    store: Any,
    *,
    src_exec: Callable[..., Any],
    src_endpoint: Callable[[str], Any],
    dest_endpoint: Callable[[str], Any],
    dest_rows: Callable[[str], list[tuple]],
    drop_dest: Callable[[str], None],
    cursor_type: str = "TIMESTAMP",
    ts_fn: Callable[..., str] = ts,
    on_run: Callable[[int, Any], None] | None = None,
    upper_dest: bool = False,
) -> list[Any]:
    """Run the chain; ``dest_rows(table)`` returns ``(id, name, note, region)`` tuples.

    ``src_exec(sql, params)`` uses ``%s`` placeholders. Returns the three results.
    """
    from src.transfer.engine import UniversalTransferEngine

    src = f"d359s_{uuid.uuid4().hex[:8]}"
    dst = f"d359d_{uuid.uuid4().hex[:8]}"
    if upper_dest:  # Oracle / SQL Server legs, as the D335 scenario names them
        dst = dst.upper()

    def _run(n: int) -> Any:
        job_id = f"d359r{n}" + uuid.uuid4().hex[:12]
        store.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(
            request(src_endpoint(src), dest_endpoint(dst), src), job_id
        )
        if on_run is not None:
            on_run(n, result)
        return result

    model: Counter = Counter()

    def _check(n: int, result: Any) -> None:
        assert result.success, f"run {n} failed: {result.error}"
        got = Counter(tuple(r) for r in dest_rows(dst))
        missing, extra = model - got, got - model
        assert not missing and not extra, (
            f"run {n}: destination differs from the model; missing {sorted(missing.items(), key=str)}, "
            f"extra {sorted(extra.items(), key=str)}"
        )

    results = []
    try:
        src_exec(
            f"CREATE TABLE {src} (id INT PRIMARY KEY, name VARCHAR(40), note VARCHAR(40), "
            f"updated_at {cursor_type} NOT NULL)"
        )
        for i in range(1, 31):
            src_exec(
                f"INSERT INTO {src} (id, name, note, updated_at) VALUES (%s, %s, %s, %s)",
                (i, f"  Name{i} ", f"note {i}", ts_fn(i)),
            )
            model[(i, *_shaped(f"  Name{i} ", f"note {i}", None))] += 1
        results.append(_run(1))
        _check(1, results[-1])

        src_exec(f"ALTER TABLE {src} ADD region VARCHAR(20)")
        src_exec(f"UPDATE {src} SET name = %s, updated_at = %s WHERE id = 3", ("  EDIT3 ", ts_fn(3, 11)))
        src_exec(f"UPDATE {src} SET region = %s, updated_at = %s WHERE id = 5", ("west", ts_fn(5, 11)))
        # Edited without moving the cursor: outside every later window.
        src_exec(f"UPDATE {src} SET note = %s WHERE id = 8", ("silent edit",))
        src_exec(f"DELETE FROM {src} WHERE id = 7")
        src_exec(
            f"INSERT INTO {src} (id, name, note, updated_at, region) VALUES (%s, %s, %s, %s, %s)",
            (31, " New31 ", "n31", ts_fn(31, 11), "east"),
        )
        # All-NULL apart from key and cursor; the latest cursor, so run 3's
        # inclusive seed re-reads it.
        src_exec(
            f"INSERT INTO {src} (id, name, note, updated_at, region) VALUES (%s, %s, %s, %s, %s)",
            (32, None, None, ts_fn(32, 11), None),
        )
        model[(3, *_shaped("  EDIT3 ", "note 3", None))] += 1
        model[(5, *_shaped("  Name5 ", "note 5", "west"))] += 1
        model[(31, *_shaped(" New31 ", "n31", "east"))] += 1
        model[(32, None, None, None)] += 1
        results.append(_run(2))
        _check(2, results[-1])

        src_exec(f"UPDATE {src} SET note = %s, updated_at = %s WHERE id = 9", ("z", ts_fn(9, 12)))
        src_exec(
            f"INSERT INTO {src} (id, name, note, updated_at, region) VALUES (%s, %s, %s, %s, %s)",
            (33, "N33", "n33", ts_fn(33, 12), "north"),
        )
        model[(9, *_shaped("  Name9 ", "z", None))] += 1
        model[(33, *_shaped("N33", "n33", "north"))] += 1
        results.append(_run(3))
        _check(3, results[-1])
        return results
    finally:
        try:
            src_exec(f"DROP TABLE {src}")
        finally:
            drop_dest(dst)


def rows_by_name(description: Any, rows: list[tuple]) -> list[tuple]:
    """``(id, name, note, region)`` from a ``SELECT *`` whatever the column case."""
    names = [str(d[0]).lower() for d in description]
    out = []
    for row in rows:
        rec = dict(zip(names, row))
        out.append(
            (
                int(rec["id"]),
                rec.get("name"),
                rec.get("note"),
                rec.get("region"),
            )
        )
    return out
