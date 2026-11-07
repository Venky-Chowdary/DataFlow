"""Which destination rows the verification ladder grades for an append (D205).

An append into a table that already holds rows writes a batch; the rows earlier
runs wrote are not this job's to prove. Gate-8 already scopes its own digest
WHERE key IN (written keys) when one key names one destination row
(``checksum_scope = written_batch_keys``). The ladder that runs after Gate-8
must grade that same population. It did not: it read the whole destination
table and, once quarantined keys forced it to recompute both digests, compared
the batch against every pre-existing row. A quarantine append into a populated
table therefore always failed, the batch was kept (an append into an occupied
table is never undone by a partial key list), and replay was refused because
the parent "did not commit".

A quarantined batch whose written key Gate-8 could not use (a composite key,
such as the ``(id, ts)`` key a Timescale hypertable forces) is scoped here by
the key the destination catalog enforces, so its written cells are still
compared instead of being reduced to a row count.
"""

from __future__ import annotations

import logging
from typing import Any

from services.reconcile_coverage import WRITTEN_BATCH_KEYS

_ADVISORY_KEY_TOKENS = ("not enforced", "advisory", "informational")


def destination_enforced_keys(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    columns: list[str],
) -> list[list[str]]:
    """Enforced primary key first, then unique keys, in ``columns``' spelling.

    Only keys whose every column is among ``columns``. Empty when the catalog
    cannot answer or declares its keys advisory.
    """
    from .adapters_introspect import _introspect_table_schema_rich

    if not table_name or not columns:
        return []
    probe_cfg = dict(cfg or {})
    if schema:
        probe_cfg.setdefault("schema", schema)
    try:
        _types, _nulls, keys = _introspect_table_schema_rich(
            db_type, probe_cfg, table_name, [], strict_namespace=True
        )
    except Exception as exc:
        logging.getLogger(__name__).debug("destination key unproven for %s: %s", table_name, exc)
        return []
    warnings = " ".join(str(w).lower() for w in (keys.get("warnings") or []))
    if any(token in warnings for token in _ADVISORY_KEY_TOKENS):
        return []
    by_fold = {str(c).strip().lower(): c for c in columns if str(c or "").strip()}
    candidates = [list(keys.get("primary_key_columns") or [])]
    candidates += [list(uk.get("columns") or []) for uk in keys.get("unique_keys") or []]
    out: list[list[str]] = []
    for key in candidates:
        folded = [str(c).strip().lower() for c in key if str(c or "").strip()]
        if folded and all(c in by_fold for c in folded):
            out.append([by_fold[c] for c in folded])
    return out


def destination_enforced_key(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    columns: list[str],
) -> list[str]:
    """The first key :func:`destination_enforced_keys` finds, else ``[]``."""
    keys = destination_enforced_keys(
        db_type, cfg, schema=schema, table_name=table_name, columns=columns
    )
    return keys[0] if keys else []


def replay_conflict_key(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    columns: list[str],
    resolved: list[str],
) -> list[str]:
    """The key a quarantine replay upserts on, as the destination enforces it.

    Map's identity (``id``) is not a conflict target on a destination whose
    enforced key is wider — a hypertable's unique keys must include its time
    column, so ``(id, ts)`` — and the upsert refused for want of a unique index
    on ``id`` alone (D205). A resolved key the destination enforces is kept;
    with no enforced key at all the resolved one is kept too.
    """
    keys = destination_enforced_keys(
        db_type, cfg, schema=schema, table_name=table_name, columns=columns
    )
    wanted = sorted(str(c).strip().lower() for c in resolved if c)
    if not keys or any(sorted(c.lower() for c in key) == wanted for key in keys):
        return list(resolved)
    return keys[0]


def scope_append_target_to_batch(
    *,
    checksum_scope: str,
    quarantined: bool,
    target_rows: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    pk_cols: list[str],
    dest_type: str,
    dest_cfg: dict[str, Any],
    schema: str,
    table_name: str,
    columns: list[str],
    dest_types: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]] | None, str]:
    """``(target rows on the batch's keys, checksum scope for the ladder)``.

    ``None`` leaves the ladder's population and scope as they were.
    """
    from functools import partial

    from services.verification_ladder import target_rows_on_batch_keys

    on_keys = partial(target_rows_on_batch_keys, dest_db_type=dest_type, dest_types=dest_types)

    if checksum_scope == WRITTEN_BATCH_KEYS:
        return on_keys(target_rows, source_rows, pk_cols), checksum_scope
    if not quarantined:
        return None, checksum_scope
    key = destination_enforced_key(
        dest_type, dest_cfg, schema=schema, table_name=table_name, columns=columns
    )
    scoped = on_keys(target_rows, source_rows, key) if key else None
    if scoped is None:
        return None, checksum_scope
    # The enforced key makes the scoped rows the written batch, one row per
    # key: the whole-table "not comparable" scope no longer applies.
    return scoped, ""
