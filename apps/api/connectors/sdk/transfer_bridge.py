from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from typing import Any

from connectors.base import ReadBatch
from connectors.sdk import get_sdk_connector
from connectors.sdk.declarative.errors import ConnectorError
from services.value_serializer import cell_to_string


def _encode_cursor_state(state: Mapping[str, Any]) -> str:
    payload = json.dumps(dict(state), separators=(",", ":"), ensure_ascii=False).encode()
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_cursor_state(token: str | None) -> dict[str, Any] | None:
    if not token:
        return None
    try:
        payload = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        state = json.loads(payload)
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        raise ConnectorError("SDK checkpoint cursor is invalid; restart the full refresh") from None
    if not isinstance(state, dict):
        raise ConnectorError("SDK checkpoint cursor must decode to an object")
    return state


def _connector_config(cfg: dict[str, Any], driver: str) -> dict[str, Any]:
    config = dict(cfg)
    extra = config.get("extra")
    if isinstance(extra, Mapping):
        config.update(extra)
    if driver == "jira":
        if not config.get("email"):
            config["email"] = config.get("username", "")
        if not config.get("api_token"):
            config["api_token"] = config.get("password") or config.get("api_key") or ""
        if not config.get("username"):
            config["username"] = config.get("email", "")
        if not config.get("password"):
            config["password"] = config.get("api_token", "")
    elif driver in {"github", "intercom"}:
        config.setdefault(
            "access_token",
            config.get("pat") or config.get("api_key") or config.get("token") or "",
        )
    if not config.get("base_url"):
        config["base_url"] = config.get("url") or config.get("host") or ""
    return config


def _native_types(schema: Any) -> dict[str, str]:
    properties = getattr(schema, "properties", None)
    if not isinstance(properties, Mapping):
        return {}
    result: dict[str, str] = {}
    type_map = {
        "array": "JSON",
        "boolean": "BOOLEAN",
        "integer": "INTEGER",
        "number": "NUMERIC",
        "object": "JSON",
        "string": "TEXT",
    }
    for name, definition in properties.items():
        declared = (
            definition.get("type")
            if isinstance(definition, Mapping)
            else definition
        )
        candidates = declared if isinstance(declared, list) else [declared]
        for candidate in candidates:
            native_type = type_map.get(str(candidate or ""))
            if native_type:
                result[str(name)] = native_type
                break
    return result


def read_object(
    *,
    cfg: dict[str, Any],
    object: str,
    limit: int = 1000,
    offset: int = 0,
    sdk_state: str | None = None,
    **_kwargs: Any,
) -> ReadBatch:
    driver = str(cfg.get("type") or cfg.get("driver") or "").lower().strip()
    connector_cls = get_sdk_connector(driver)
    if connector_cls is None:
        raise ConnectorError(f"SDK source {driver!r} is not registered")
    connector = connector_cls(_connector_config(cfg, driver))
    batches = iter(
        connector.read(
            object,
            state=_decode_cursor_state(sdk_state),
            offset=offset,
            limit=limit,
        )
    )
    try:
        page = next(batches, None)
    finally:
        close = getattr(batches, "close", None)
        if callable(close):
            close()
    if page is None:
        return ReadBatch(
            headers=[],
            rows=[],
            offset=offset,
            total_rows=None,
            meta={"sdk_done": True, "sdk_state": ""},
            raw_page_rows=0,
        )
    headers = list(page.schema.properties) if page.schema else []
    if not headers and page.records:
        headers = list(page.records[0])
    for record in page.records:
        for name in record:
            if name not in headers:
                headers.append(name)
    rows = [
        [cell_to_string(record.get(header, "")) for header in headers]
        for record in page.records
    ]
    state = dict(page.state or {})
    has_more = state.get("page_token") not in (None, "")
    return ReadBatch(
        headers=headers,
        rows=rows,
        offset=offset,
        total_rows=None,
        meta={
            "sdk_done": not has_more,
            "sdk_state": _encode_cursor_state(state) if state else "",
            "sdk_stream": object,
            "native_types": _native_types(page.schema),
        },
        raw_page_rows=len(rows),
    )


def test_sdk_connector(cfg: dict[str, Any], driver: str) -> tuple[bool, str]:
    connector_cls = get_sdk_connector(driver)
    if connector_cls is None:
        return False, f"SDK source {driver!r} is not registered"
    return connector_cls(_connector_config(cfg, driver)).check()
