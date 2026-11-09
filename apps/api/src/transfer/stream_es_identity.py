"""Elasticsearch document identity for the database stream.

The bind lives here so ``stream.py`` stays inside its line budget. Non-ES
destinations are unchanged: the helper only rewrites the key for Elasticsearch,
then applies the same upsert write-mode rule the stream used inline.
"""

from __future__ import annotations

from typing import Any


def bind_stream_elasticsearch_write_mode(run: Any, stream_contracts: Any) -> None:
    """Apply an explicit Elasticsearch document id, then set write mode."""
    from services.primary_key import (
        bind_explicit_elasticsearch_identity,
        identity_source_columns,
    )
    from services.sync_cursor import requires_upsert

    cfg = run.cfg
    if str(cfg.dest_type or "").strip().lower() == "elasticsearch":
        cfg.pk_target_cols = bind_explicit_elasticsearch_identity(
            cfg.dest_type,
            cfg.mappings,
            cfg.pk_target_cols,
            stream_contracts=stream_contracts,
            destination_extra=getattr(cfg.destination, "extra", None),
        )
        if cfg.pk_target_cols and not cfg.pk_source_cols:
            cfg.pk_source_cols = identity_source_columns(
                cfg.pk_target_cols, cfg.mappings
            )
    if requires_upsert(cfg.effective_sync) and not cfg.pk_target_cols:
        raise ValueError(
            f"Sync mode `{cfg.effective_sync}` requires primary_key for upsert; "
            "refuse silent insert fallback (set primary_key on the stream contract)"
        )
    cfg.write_mode = (
        "upsert" if requires_upsert(cfg.effective_sync) and cfg.pk_target_cols else "insert"
    )
