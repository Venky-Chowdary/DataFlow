"""Stripe source connector — list API read with incremental ``created`` cursor.

Fivetran/Airbyte land Stripe with a ``created`` watermark. Pagination
``starting_after`` is not incremental — it walks one list page. This reader
sends ``created[gte]`` and drops rows at-or-behind the dest-owned watermark so
a replay does not re-extract the whole object.

Catalog tiles are not this SKU. Certification lives in ``connector_capabilities``.
"""

from __future__ import annotations

from typing import Any

from services.value_serializer import load_http_json, present_cell_text

from connectors.saas_common import (
    ReadBatch,
    base_url,
    extract_records,
    humanize_http_error,
    object_name,
    request,
    token,
)

DEFAULT_HOST = "api.stripe.com"
DEFAULT_OBJECT = "customers"
STRIPE_INCREMENTAL_CURSORS = frozenset({"created", "created_at"})


def stripe_created_watermark(
    cursor_column: str = "",
    cursor_after: Any = None,
) -> tuple[int | None, str]:
    """Parse incremental watermark into ``(created_unix, last_id)``.

    Composite bookmarks (unit-separator or legacy ``created|id``) keep same-second
    peers. A bare integer is ``created`` only.
    """
    col = (cursor_column or "").strip().lower()
    if col not in STRIPE_INCREMENTAL_CURSORS:
        return None, ""
    raw = present_cell_text(cursor_after)
    if raw is None or not str(raw).strip():
        return None, ""
    from services.keyset_pagination import KEYSET_SEP, split_cursor_bookmark

    created_raw, last_id = split_cursor_bookmark(str(raw), has_tiebreak=KEYSET_SEP in str(raw) or "|" in str(raw))
    try:
        created = int(float(str(created_raw).strip()))
    except (TypeError, ValueError):
        return None, ""
    return created, str(last_id or "").strip()


def stripe_list_params(
    *,
    limit: int,
    starting_after: str = "",
    created_gte: int | None = None,
) -> dict[str, Any]:
    """Stripe list query. ``created[gte]`` is the incremental cursor, not OFFSET."""
    params: dict[str, Any] = {"limit": min(100, max(1, int(limit)))}
    if starting_after:
        params["starting_after"] = starting_after
    if created_gte is not None:
        params["created[gte]"] = int(created_gte)
    return params


def stripe_row_after_watermark(
    rec: dict[str, Any],
    *,
    created_gte: int | None,
    last_id: str = "",
) -> bool:
    """True when the Stripe object is after the incremental watermark.

    Same unix second is kept (at-least-once). Only the exact last id is
    dropped when the composite bookmark carries it.
    """
    if created_gte is None:
        return True
    if not isinstance(rec, dict):
        return False
    try:
        created = int(rec.get("created"))
    except (TypeError, ValueError):
        return False
    if created > created_gte:
        return True
    if created < created_gte:
        return False
    rid = str(rec.get("id") or "")
    if last_id and rid == last_id:
        return False
    # Same unix second: keep peers. A created-only watermark (empty last_id)
    # must re-extract the boundary second — dropping it is silent loss.
    # Drop only the exact last id when the composite bookmark carries it.
    return True


def test_stripe(
    *,
    host: str = "",
    port: int = 0,
    database: str = "",
    table: str = "",
    connection_string: str = "",
    api_key: str = "",
    username: str = "",
    password: str = "",
    ssl: bool = False,
    **_kwargs: Any,
) -> tuple[bool, str]:
    """Probe Stripe connectivity with a secret key."""
    secret_key = token(api_key, connection_string, username, password)
    if not secret_key:
        return False, "Stripe secret key is required. Paste it in the API key field or connection string."
    url = f"{base_url(host, DEFAULT_HOST)}/v1/account"
    try:
        r = request(method="GET", url=url, token=secret_key, timeout=20)
        r.raise_for_status()
        return True, "Stripe reachable"
    except Exception as exc:
        return False, humanize_http_error(exc, "stripe")


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 100,
    offset: int = 0,
    cursor_column: str = "",
    cursor_after: Any = None,
    **_kwargs: Any,
) -> ReadBatch:
    """Read Stripe object list. Incremental uses ``created`` + optional last id."""
    secret_key = token(
        cfg.get("api_key", ""),
        cfg.get("connection_string", ""),
        cfg.get("username", ""),
        cfg.get("password", ""),
    )
    if not secret_key:
        raise ValueError("Stripe secret key is required")
    obj = (object or object_name(cfg, DEFAULT_OBJECT)).strip()
    if not obj:
        raise ValueError("Stripe object/table name required")

    url = f"{base_url(cfg.get('host', ''), DEFAULT_HOST)}/v1/{obj}"
    # Stripe list APIs are cursor-paged (max 100/page). Never send limit=100000 or
    # treat a numeric OFFSET as starting_after (that silently returns wrong pages).
    requested = max(1, int(limit or 100))
    items: list[dict[str, Any]] = []
    starting_after = ""
    skip_remaining = 0
    last_has_more = False
    created_gte, last_id = stripe_created_watermark(cursor_column, cursor_after)
    if offset is not None and str(offset).strip() and created_gte is None:
        off = str(offset).strip()
        if off.isdigit():
            skip_remaining = int(off)
        else:
            starting_after = off

    while len(items) < requested:
        page_need = skip_remaining + (requested - len(items))
        params = stripe_list_params(
            limit=page_need,
            starting_after=starting_after,
            created_gte=created_gte,
        )
        r = request(method="GET", url=url, token=secret_key, params=params, timeout=60)
        r.raise_for_status()
        data = load_http_json(r)
        page = data.get("data")
        if not isinstance(page, list):
            raise ValueError("Stripe list response missing data array")
        last_has_more = bool(data.get("has_more"))
        if created_gte is not None:
            page = [
                rec
                for rec in page
                if isinstance(rec, dict)
                and stripe_row_after_watermark(
                    rec, created_gte=created_gte, last_id=last_id
                )
            ]
        if skip_remaining:
            if len(page) <= skip_remaining:
                skip_remaining -= len(page)
                if not data.get("has_more"):
                    break
                last = page[-1] if page else None
                if not isinstance(last, dict) or not last.get("id"):
                    if not data.get("has_more"):
                        break
                    raise RuntimeError(
                        "Stripe reports more results without a final object id; refusing partial ingest"
                    )
                starting_after = str(last["id"])
                continue
            page = page[skip_remaining:]
            skip_remaining = 0
        items.extend(page)
        if not last_has_more:
            break
        raw_page = data.get("data") if isinstance(data.get("data"), list) else page
        if not raw_page:
            raise RuntimeError(
                "Stripe reports more results but returned an empty page; "
                "refusing partial ingest"
            )
        last = raw_page[-1] if raw_page else None
        if not isinstance(last, dict) or not last.get("id"):
            raise RuntimeError(
                "Stripe reports more results without a final object id; refusing partial ingest"
            )
        starting_after = str(last["id"])

    from connectors.saas_typed_schema import rows_and_schema_from_saas

    typed_keys, typed_rows, typed_schema = rows_and_schema_from_saas(
        "stripe", items[:requested]
    )
    incremental = created_gte is not None
    # Stripe does not publish COUNT(*). Stopping at ``requested`` while the
    # list still reports ``has_more`` is a silent truncate — stamp it so the
    # execute cap can refuse. Peek/sample callers pass raise_on_truncate=False.
    truncated = bool(last_has_more and len(items) >= requested)
    stripe_meta = {
        "catalog_id": "stripe",
        "incremental_cursor": "created" if incremental else "",
        "created_gte": created_gte,
        "truncated": truncated,
        "has_more": last_has_more,
    }
    if typed_keys and typed_rows:
        return ReadBatch(
            headers=typed_keys,
            rows=typed_rows,
            offset=0,
            total_rows=None,
            meta={
                "native_types": typed_schema,
                "schema": typed_schema,
                "saas_typed": True,
                **stripe_meta,
            },
        )

    batch = extract_records(items[:requested])
    # Stripe list APIs do not publish authoritative totals — never claim the
    # fetched page length is the object cardinality (stream early-stop trap).
    batch.total_rows = None
    batch.meta = dict(batch.meta or {})
    batch.meta.update(stripe_meta)
    return batch
