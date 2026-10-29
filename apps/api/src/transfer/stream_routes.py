"""Routed-batch splitting and stale route-key purging for streamed runs (re-exported by src.transfer.stream)."""

from __future__ import annotations

import logging
from services.keyset_pagination import header_column_index
from typing import Any

logger = logging.getLogger("src.transfer.stream")


def _split_routed_batch(
    batch: Any, recipe: Any, dest_table: str
) -> list[tuple[str, str, list[str], list[list[Any]]]]:
    """Split a shaped batch into per-route (name, table, headers, rows) writes.

    Every routed row carries the ROUTE_COLUMN tag; each route writes its own
    table (explicit ``targets`` override, else ``{dest_table}__{route}``) with
    exactly the columns that route declares — a mapped route emits only its
    map targets, a pass-through route emits the shared output columns minus
    the tag and other routes' map targets.
    """
    from services.shape_models import ROUTE_COLUMN

    router_step = recipe.router_step
    targets = dict(router_step.options.get("targets") or {}) if router_step else {}
    tag_index = batch.headers.index(ROUTE_COLUMN)
    groups: dict[str, list[int]] = {}
    order: list[str] = []
    for i, row in enumerate(batch.rows):
        name = str(row[tag_index])
        if name not in groups:
            groups[name] = []
            order.append(name)
        groups[name].append(i)
    route_decls = {
        str(r.get("name") or ""): (r.get("map") or {})
        for r in (router_step.options.get("routes") or [])
    }
    splits: list[tuple[str, str, list[str], list[list[Any]]]] = []
    for name in order:
        route_map = route_decls.get(name) or {}
        if route_map:
            # A mapped route renames its columns at write time: extract the
            # map's SOURCE names from the shared batch headers, emit the
            # TARGET names. The row dict is never rewritten at shape time —
            # every routed row keeps the shared shape (RT-3).
            wanted = [
                str(s)
                for s in route_map.keys()
                if header_column_index(batch.headers, str(s)) is not None
            ]
            emit = [str(route_map[s]) for s in wanted]
            ix = [header_column_index(batch.headers, c) for c in wanted]
            headers = emit
            rows = [
                [r[i] for i in ix] for r in (batch.rows[i] for i in groups[name])
            ]
        else:
            wanted = [
                c
                for c in recipe.route_output_columns(name)
                if header_column_index(batch.headers, c) is not None
            ]
            ix = [header_column_index(batch.headers, c) for c in wanted]
            headers = [batch.headers[i] for i in ix]
            rows = [
                [r[i] for i in ix] for r in (batch.rows[i] for i in groups[name])
            ]
        table = str(targets.get(name) or f"{dest_table}__{name}")
        splits.append((name, table, headers, rows))
    return splits


def _purge_route_key_tuples(
    conn: Any,
    *,
    router_step: Any,
    recipe: Any,
    dest_table: str,
    dest_cfg: dict[str, Any],
    batch: Any,
    pk_source_cols: list[str],
    pk_target_cols: list[str],
    pk_audit_cols: list[str],
    diverted_keys: list[tuple],
    max_keys_per_statement: int = 500,
) -> int:
    """Delete this batch's keys from every route table (dedupe-by-partition).

    A keyed routed upsert lands each row in exactly one route; a row that
    changed route must not keep a stale copy in its previous table (P1-B).
    Deleting the batch's keys — plus the keys of rows the recipe diverted —
    from EVERY route table inside the held transaction, before the upserts,
    is what makes the final union equal the source set. Returns the number
    of rows removed. Tables that do not exist yet or lack the key columns
    are skipped — they cannot hold a stale keyed copy.
    """
    from connectors.sql_identifiers import (
        quote_sql_identifier,
        quote_table_ref,
        split_qualified_table,
    )

    key_ix = [
        i if i is not None else -1
        for i in (header_column_index(batch.headers, c) for c in pk_source_cols)
    ]
    if any(i < 0 for i in key_ix):
        # The recipe renamed a key component earlier — audit names are the
        # source-side spelling for the same key.
        key_ix = [
            i if i is not None else -1
            for i in (
                header_column_index(batch.headers, c)
                for c in (pk_audit_cols or [])
            )
        ]
    keys: set[tuple] = set(diverted_keys or [])
    if key_ix and not any(i < 0 for i in key_ix):
        keys.update(tuple(r[i] for i in key_ix) for r in batch.rows)
    if not keys:
        return 0
    # This route table's key columns: a mapped route renames source columns;
    # a pass-through route emits the shared output columns. Either way the
    # key column name lives in the route's own emitted header set.
    targets = dict(router_step.options.get("targets") or {})
    purge_tables: list[tuple[str, str, list[str]]] = []  # (schema, table, pkcols)
    for decl in router_step.options.get("routes") or []:
        rname = str(decl.get("name") or "")
        if not rname:
            continue
        rmap = dict(decl.get("map") or {})
        if rmap:
            rpk = [str(rmap.get(s) or "") for s in pk_source_cols]
        else:
            emitted = set(recipe.route_output_columns(rname))
            rpk = [
                str(pk_target_cols[i] if i < len(pk_target_cols) else s)
                for i, s in enumerate(pk_source_cols)
                if str(pk_target_cols[i] if i < len(pk_target_cols) else s)
                in emitted
            ]
            if len(rpk) != len(pk_source_cols):
                rpk = []
        if not rpk or not all(rpk):
            continue
        table = str(targets.get(rname) or f"{dest_table}__{rname}")
        schema, tbl = split_qualified_table(
            table, str(dest_cfg.get("schema") or "public")
        )
        purge_tables.append((schema, tbl, rpk))
    if not purge_tables:
        return 0
    removed = 0
    key_list = sorted(keys, key=repr)
    with conn.cursor() as cur:
        for schema, tbl, rpk in purge_tables:
            qualified = f"{schema}.{tbl}"
            cur.execute("SELECT to_regclass(%s)", (qualified,))
            if not cur.fetchone()[0]:
                continue
            ref = quote_table_ref(tbl, schema, dialect="postgresql")
            # The batch carries source-side values (often text) while the
            # route table typed the key (bigint etc.) — compare as text so
            # the purge never needs the column's declared type.
            cols = ", ".join(f"{quote_sql_identifier(c)}::text" for c in rpk)
            nkey = len(rpk)
            for off in range(0, len(key_list), max_keys_per_statement):
                chunk = key_list[off : off + max_keys_per_statement]
                ph = ",".join("(" + ",".join(["%s"] * nkey) + ")" for _ in chunk)
                params = [
                    None if v is None else str(v) for kt in chunk for v in kt
                ]
                cur.execute(
                    f"DELETE FROM {ref} WHERE ({cols}) IN (VALUES {ph})",  # nosec B608
                    params,
                )
                removed += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    return removed
