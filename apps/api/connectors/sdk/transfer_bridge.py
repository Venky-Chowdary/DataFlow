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
    elif driver == "gitlab":
        config.setdefault(
            "api_key",
            config.get("pat") or config.get("token") or "",
        )
    elif driver == "mailchimp":
        if not config.get("dc"):
            # Mailchimp DC is the API-key suffix (key-us21); derive it when
            # operators only paste the key.
            key = str(config.get("api_key") or config.get("password") or "")
            config["dc"] = key.rsplit("-", 1)[-1] if "-" in key else ""
        config.setdefault("username", "datawrap")
        config.setdefault("password", config.get("api_key") or "")
    elif driver == "pipedrive":
        config.setdefault(
            "api_key",
            config.get("api_token") or config.get("token") or "",
        )
        config.setdefault("domain", config.get("company_domain") or config.get("host") or "")
    elif driver == "klaviyo":
        config.setdefault(
            "api_key",
            config.get("private_key") or config.get("token") or "",
        )
    elif driver == "servicenow":
        config.setdefault("username", config.get("user") or "")
        config.setdefault("password", config.get("api_key") or config.get("password") or "")
        config.setdefault("instance", config.get("host") or "")
    elif driver == "freshdesk":
        config.setdefault(
            "username",
            config.get("api_key") or config.get("token") or "",
        )
        config.setdefault("password", "X")
        config.setdefault("domain", config.get("host") or "")
    elif driver in {"slack", "asana", "calendly", "brevo"}:
        config.setdefault(
            "api_key",
            config.get("access_token") or config.get("token") or "",
        )
    elif driver == "woocommerce":
        config.setdefault("domain", config.get("host") or "")
        config.setdefault("username", config.get("consumer_key") or "")
        config.setdefault("password", config.get("consumer_secret") or "")
    elif driver == "bigcommerce":
        config.setdefault("api_key", config.get("token") or "")
        config.setdefault(
            "store_hash",
            config.get("store") or config.get("store_id") or "",
        )
    elif driver == "freshservice":
        config.setdefault(
            "username",
            config.get("api_key") or config.get("token") or "",
        )
        config.setdefault("password", "X")
        config.setdefault("domain", config.get("host") or "")
    elif driver == "close":
        config.setdefault(
            "username",
            config.get("api_key") or config.get("token") or "",
        )
        config.setdefault("password", "X")
    elif driver == "airtable":
        config.setdefault(
            "api_key",
            config.get("access_token") or config.get("token") or "",
        )
        config.setdefault("base_id", config.get("base") or config.get("base_id") or "")
        config.setdefault("table_id", config.get("table") or config.get("table_id") or "")
    elif driver == "clickup":
        config.setdefault(
            "api_key",
            config.get("token") or config.get("access_token") or "",
        )
        config.setdefault("list_id", config.get("list") or config.get("list_id") or "")
    elif driver == "mailjet":
        config.setdefault("username", config.get("api_key") or "")
        config.setdefault("password", config.get("secret_key") or "")
    elif driver == "sendgrid":
        config.setdefault(
            "api_key",
            config.get("access_token") or config.get("token") or "",
        )
    elif driver in {"todoist", "typeform", "notion", "posthog"}:
        config.setdefault(
            "api_key",
            config.get("access_token") or config.get("token") or "",
        )
        if driver == "posthog":
            config.setdefault("domain", config.get("host") or "")
            config.setdefault(
                "project_id", config.get("project") or config.get("project_id") or ""
            )
    elif driver == "confluence":
        config.setdefault("domain", config.get("host") or "")
        config.setdefault(
            "username",
            config.get("email") or config.get("user") or config.get("username") or "",
        )
        config.setdefault(
            "password",
            config.get("api_key") or config.get("token") or config.get("password") or "",
        )
    elif driver == "zendesk":
        config.setdefault("domain", config.get("host") or "")
        email = (
            config.get("email") or config.get("user") or config.get("username") or ""
        )
        if email and not str(email).endswith("/token"):
            email = f"{email}/token"
        config.setdefault("username", email)
        config.setdefault(
            "password",
            config.get("api_key") or config.get("token") or config.get("password") or "",
        )
    if not config.get("base_url"):
        # Templated manifests ({instance}/{domain}/{dc}/{store_hash}) take their
        # host piece from the named field above — feeding ``host`` into base_url
        # would bypass the template and drop the scheme.
        if driver not in {"mailchimp", "pipedrive", "servicenow", "freshdesk", "freshservice", "woocommerce", "bigcommerce", "posthog", "confluence", "zendesk"}:
            config["base_url"] = config.get("url") or config.get("host") or ""
        else:
            config["base_url"] = config.get("url") or ""
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
