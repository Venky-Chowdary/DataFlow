"""An upsert needs a key — refused at the door when none can be resolved (MySQL D13 review).

``sync_mode=upsert`` is a key-idempotent write. With no declared key and a
source table whose catalog has no primary key, every run is refused by the
sync-contract gate ("Missing primary key"), on a schedule's first beat or a
one-off job alike. The door now says so before anything is stored or queued,
the same way for ``POST /schedules`` (and PATCH) and ``/transfer/execute``.

A key resolves from, in order: the declared ``primary_key``, a stream
contract's ``primary_key`` / ``primary_keys``, the destination's declared
``primary_key`` / ``upsert_key``, then the live source catalog. Only a catalog
that was read and has no key refuses; a source that does not answer in
``PROBE_SECONDS`` (or is a query / procedure extract) is left to the run.
"""

from __future__ import annotations

import threading
from typing import Any, Iterable, Mapping

from services.api_errors import ApiError

UPSERT_KEY_CODE = "UPSERT_KEY_REQUIRED"
PROBE_SECONDS = 8.0


class UpsertKeyRequired(ApiError, ValueError):
    """``sync_mode=upsert`` with no key anywhere (HTTP 422)."""


def _keys(raw: Any) -> list[str]:
    if isinstance(raw, (list, tuple)):
        return [str(k).strip() for k in raw if str(k).strip()]
    return [p.strip() for p in str(raw or "").replace(";", ",").split(",") if p.strip()]


def declared_upsert_keys(
    primary_key: Any = "",
    stream_contracts: Iterable[Mapping[str, Any]] | None = None,
    destination_extra: Mapping[str, Any] | None = None,
) -> list[str]:
    keys = _keys(primary_key)
    for contract in stream_contracts or ():
        if isinstance(contract, Mapping) and contract.get("selected", True):
            keys += _keys(contract.get("primary_key")) or _keys(contract.get("primary_keys"))
    extra = destination_extra or {}
    keys += _keys(extra.get("primary_key")) or _keys(extra.get("upsert_key"))
    return keys


def _catalog_key(connector_id: str, table: str, schema: str, database: str) -> list[str] | None:
    """The source table's catalog key; ``None`` when the catalog was not read."""
    from services.source_schema_authority import _saved_source_endpoint

    box: dict[str, Any] = {}

    def _run() -> None:
        try:
            endpoint = _saved_source_endpoint(
                connector_id, source_table=table, source_collection="", stream=table,
                source_schema=schema, source_database=database, source_config=None,
            )
            if endpoint is None:
                return
            from services.procedure_source import is_callable_source
            from src.transfer.endpoint_intelligence import introspect_endpoint

            if is_callable_source(endpoint):
                return
            extra = dict(getattr(endpoint, "extra", None) or {})
            extra.update(introspect_purpose="source", introspect_strict=True)
            endpoint.extra = extra
            info = introspect_endpoint(endpoint)
            if isinstance(info, dict) and info.get("columns"):
                box["keys"] = [str(c).strip() for c in info.get("primary_key_columns") or []
                               if str(c or "").strip()]
        except Exception:  # noqa: BLE001 - unread is unknown, never "no key"
            return

    worker = threading.Thread(target=_run, name="upsert-key-door", daemon=True)
    worker.start()
    worker.join(PROBE_SECONDS)
    return None if worker.is_alive() else box.get("keys")


def assert_upsert_key_resolvable(
    *,
    sync_mode: Any,
    primary_key: Any = "",
    stream_contracts: Iterable[Mapping[str, Any]] | None = None,
    destination_extra: Mapping[str, Any] | None = None,
    source_connector_id: str = "",
    source_table: str = "",
    source_schema: str = "",
    source_database: str = "",
) -> None:
    from services.transfer_policy_values import canonical_sync_mode

    try:
        if canonical_sync_mode(sync_mode) != "upsert":
            return
    except ValueError:
        return  # the vocabulary check answers an unknown mode
    if declared_upsert_keys(primary_key, stream_contracts, destination_extra):
        return
    if not (source_connector_id or "").strip() or not (source_table or "").strip():
        return
    catalog = _catalog_key(source_connector_id.strip(), source_table.strip(),
                           source_schema or "", source_database or "")
    if catalog is None or catalog:
        return
    raise UpsertKeyRequired(
        422,
        UPSERT_KEY_CODE,
        f"sync_mode upsert needs a key, and none resolves: no primary_key is declared "
        f"and source table '{source_table}' has no primary key in its catalog. Declare "
        "primary_key (or stream_contracts[].primary_key) naming the column(s) that "
        "identify a row, or choose full_refresh_append to allow duplicate rows.",
        extra={"source_table": source_table},
    )
