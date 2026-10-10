from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import urlsplit

from connectors.sdk.declarative.errors import ManifestError
from connectors.sdk.declarative.pagination import PaginatorSpec

AuthType = Literal[
    "none",
    "api_key",
    "bearer",
    "basic",
    "oauth2_refresh",
    "oauth2_client_credentials",
]


@dataclass(frozen=True)
class RetrySpec:
    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 60.0


@dataclass(frozen=True)
class RequestDefaults:
    timeout_s: float = 60.0
    max_pages: int = 10_000
    page_size: int = 100
    max_records: int | None = None
    retry: RetrySpec = field(default_factory=RetrySpec)


@dataclass(frozen=True)
class AuthSpec:
    type: AuthType = "none"
    location: str = "header"
    name: str = "X-API-Key"
    value: str = field(default="", repr=False)
    token: str = field(default="", repr=False)
    username: str = ""
    password: str = field(default="", repr=False)
    token_url: str = ""
    client_id: str = ""
    client_secret: str = field(default="", repr=False)
    refresh_token: str = field(default="", repr=False)
    access_token: str = field(default="", repr=False)
    expires_at: float = 0.0
    scopes: tuple[str, ...] = ()
    client_auth_method: str = "client_secret_post"
    extra_token_params: tuple[tuple[str, str], ...] = field(default=(), repr=False)


@dataclass(frozen=True)
class CursorSpec:
    field: str
    request_param: str
    format: Literal["iso8601", "epoch_s", "epoch_ms"] = "iso8601"
    lookback_s: float = 0.0
    request_template: str = ""
    request_location: Literal["query", "body"] = "query"


@dataclass(frozen=True)
class StreamSpec:
    name: str
    path: str
    method: Literal["GET", "POST"] = "GET"
    records_path: str = "results"
    primary_key: tuple[str, ...] = ()
    cursor: CursorSpec | None = None
    paginator: PaginatorSpec = field(default_factory=PaginatorSpec)
    json_schema: dict[str, Any] | None = None
    request_params: dict[str, Any] = field(default_factory=dict)
    request_headers: dict[str, str] = field(default_factory=dict)
    request_body_template: dict[str, Any] | None = None


@dataclass(frozen=True)
class Manifest:
    name: str
    base_url: str
    auth: AuthSpec
    streams: tuple[StreamSpec, ...]
    defaults: RequestDefaults = field(default_factory=RequestDefaults)
    rate_limit_per_second: float | None = None
    docs: str = ""


def _mapping(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        label = path or "$"
        raise ManifestError(f"{label}: expected an object", path=label)
    return value


def _parse_request_headers(value: Any, path: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ManifestError(f"{path}: expected an object", path=path)
    headers: dict[str, str] = {}
    for raw_name, raw_value in value.items():
        if (
            not isinstance(raw_name, str)
            or not raw_name.strip()
            or any(character in raw_name for character in ":\r\n")
        ):
            raise ManifestError(f"{path}: invalid header name", path=path)
        header_path = f"{path}.{raw_name}"
        if (
            not isinstance(raw_value, str)
            or "\r" in raw_value
            or "\n" in raw_value
        ):
            raise ManifestError(
                f"{header_path}: expected a string without line breaks",
                path=header_path,
            )
        headers[raw_name] = raw_value
    return headers


def _check_keys(value: dict[str, Any], allowed: set[str], path: str) -> None:
    for key in value:
        if key not in allowed:
            bad_path = f"{path}.{key}" if path else str(key)
            raise ManifestError(f"{bad_path}: unknown key", path=bad_path)


def _positive_number(value: Any, path: str, *, allow_zero: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ManifestError(f"{path}: expected a number", path=path)
    try:
        number = float(value)
    except (OverflowError, ValueError):
        raise ManifestError(f"{path}: must be finite", path=path) from None
    if not math.isfinite(number) or number < 0 or (number == 0 and not allow_zero):
        requirement = "non-negative" if allow_zero else "positive"
        raise ManifestError(f"{path}: must be {requirement}", path=path)
    return number


def _positive_integer(value: Any, path: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ManifestError(f"{path}: expected an integer", path=path)
    if value < 0 or (value == 0 and not allow_zero):
        requirement = "non-negative" if allow_zero else "positive"
        raise ManifestError(f"{path}: must be {requirement}", path=path)
    return value


def _parse_auth(raw: Any) -> AuthSpec:
    path = "auth"
    obj = _mapping({} if raw is None else raw, path)
    auth_type = str(obj.get("type", "none"))
    allowed_by_type = {
        "none": {"type"},
        "api_key": {"type", "location", "name", "value"},
        "bearer": {"type", "token"},
        "basic": {"type", "username", "password"},
        "oauth2_refresh": {
            "type", "token_url", "client_id", "client_secret", "refresh_token",
            "access_token", "expires_at", "scopes", "client_auth_method",
            "extra_token_params",
        },
        "oauth2_client_credentials": {
            "type", "token_url", "client_id", "client_secret", "scopes",
            "client_auth_method", "extra_token_params",
        },
    }
    if auth_type not in allowed_by_type:
        raise ManifestError("auth.type: unsupported authentication mode", path="auth.type")
    _check_keys(obj, allowed_by_type[auth_type], path)
    string_fields = allowed_by_type[auth_type] - {
        "type", "expires_at", "scopes", "extra_token_params"
    }
    for key in string_fields:
        if key in obj and not isinstance(obj[key], str):
            raise ManifestError(f"auth.{key}: expected a string", path=f"auth.{key}")
    location = str(obj.get("location", "header"))
    if auth_type == "api_key" and location not in {"header", "query"}:
        raise ManifestError("auth.location: expected 'header' or 'query'", path="auth.location")
    if auth_type == "api_key" and not obj.get("name", "X-API-Key").strip():
        raise ManifestError("auth.name: must be non-empty", path="auth.name")
    if auth_type in {"oauth2_refresh", "oauth2_client_credentials"}:
        token_url = obj.get("token_url", "")
        try:
            parsed_token_url = urlsplit(str(token_url))
        except ValueError:
            raise ManifestError(
                "auth.token_url: expected an absolute HTTP(S) URL",
                path="auth.token_url",
            ) from None
        if parsed_token_url.scheme.lower() not in {"http", "https"} or not parsed_token_url.netloc:
            raise ManifestError(
                "auth.token_url: expected an absolute HTTP(S) URL",
                path="auth.token_url",
            )
        if parsed_token_url.username is not None or parsed_token_url.password is not None:
            raise ManifestError("auth.token_url: URL credentials are not allowed", path="auth.token_url")
        if obj.get("client_auth_method", "client_secret_post") not in {
            "client_secret_post",
            "client_secret_basic",
        }:
            raise ManifestError(
                "auth.client_auth_method: unsupported OAuth2 client authentication method",
                path="auth.client_auth_method",
            )
    scopes = obj.get("scopes", [])
    if not isinstance(scopes, list) or any(not isinstance(item, str) for item in scopes):
        raise ManifestError("auth.scopes: expected an array of strings", path="auth.scopes")
    token_params = obj.get("extra_token_params", {})
    if not isinstance(token_params, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in token_params.items()
    ):
        raise ManifestError(
            "auth.extra_token_params: expected string values", path="auth.extra_token_params"
        )
    expires_at = obj.get("expires_at", 0)
    if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
        raise ManifestError("auth.expires_at: expected a number", path="auth.expires_at")
    try:
        expires_at_number = float(expires_at)
    except (OverflowError, ValueError):
        raise ManifestError("auth.expires_at: expected a finite number", path="auth.expires_at") from None
    if not math.isfinite(expires_at_number):
        raise ManifestError("auth.expires_at: expected a finite number", path="auth.expires_at")
    return AuthSpec(
        type=auth_type,
        location=location,
        name=str(obj.get("name", "X-API-Key")),
        value=str(obj.get("value", "")),
        token=str(obj.get("token", "")),
        username=str(obj.get("username", "")),
        password=str(obj.get("password", "")),
        token_url=str(obj.get("token_url", "")),
        client_id=str(obj.get("client_id", "")),
        client_secret=str(obj.get("client_secret", "")),
        refresh_token=str(obj.get("refresh_token", "")),
        access_token=str(obj.get("access_token", "")),
        expires_at=expires_at_number,
        scopes=tuple(scopes),
        client_auth_method=str(obj.get("client_auth_method", "client_secret_post")),
        extra_token_params=tuple(sorted(token_params.items())),
    )


def _parse_defaults(raw: Any) -> RequestDefaults:
    path = "defaults"
    obj = _mapping({} if raw is None else raw, path)
    _check_keys(obj, {"timeout_s", "max_pages", "page_size", "max_records", "retry"}, path)
    timeout = _positive_number(obj.get("timeout_s", 60), f"{path}.timeout_s")
    max_pages = _positive_integer(obj.get("max_pages", 10_000), f"{path}.max_pages")
    page_size = _positive_integer(obj.get("page_size", 100), f"{path}.page_size")
    max_records = obj.get("max_records")
    if max_records is not None:
        max_records = _positive_integer(max_records, f"{path}.max_records", allow_zero=True)
    retry_raw = _mapping(obj.get("retry", {}), f"{path}.retry")
    _check_keys(retry_raw, {"max_attempts", "base_delay", "max_delay"}, f"{path}.retry")
    retry = RetrySpec(
        max_attempts=_positive_integer(
            retry_raw.get("max_attempts", 5), f"{path}.retry.max_attempts"
        ),
        base_delay=_positive_number(
            retry_raw.get("base_delay", 1), f"{path}.retry.base_delay", allow_zero=True
        ),
        max_delay=_positive_number(
            retry_raw.get("max_delay", 60), f"{path}.retry.max_delay", allow_zero=True
        ),
    )
    if retry.base_delay > retry.max_delay:
        raise ManifestError(
            "defaults.retry.base_delay: cannot exceed max_delay",
            path="defaults.retry.base_delay",
        )
    return RequestDefaults(timeout, max_pages, page_size, max_records, retry)


def _parse_paginator(
    raw: Any,
    path: str,
    *,
    default_page_size: int,
    default_max_pages: int,
) -> PaginatorSpec:
    obj = _mapping({"type": "none"} if raw is None else raw, path)
    fields = {
        "type", "page_size", "page_size_param", "max_pages", "offset_param",
        "start_offset", "page_param", "start_index", "cursor_param",
        "cursor_location", "cursor_path", "cursor_header", "initial_token",
    }
    _check_keys(obj, fields, path)
    obj.setdefault("page_size", default_page_size)
    obj.setdefault("max_pages", default_max_pages)
    for key in ("page_size", "max_pages"):
        if isinstance(obj[key], bool) or not isinstance(obj[key], int) or obj[key] < 1:
            raise ManifestError(f"{path}.{key}: must be a positive integer", path=f"{path}.{key}")
    for key in ("start_offset", "start_index"):
        if key in obj and (
            isinstance(obj[key], bool)
            or not isinstance(obj[key], int)
            or obj[key] < 0
        ):
            raise ManifestError(
                f"{path}.{key}: must be a non-negative integer",
                path=f"{path}.{key}",
            )
    for key in (
        "page_size_param",
        "offset_param",
        "page_param",
        "cursor_param",
        "cursor_location",
        "cursor_path",
        "cursor_header",
    ):
        if key in obj and not isinstance(obj[key], str):
            raise ManifestError(f"{path}.{key}: expected a string", path=f"{path}.{key}")
    cursor_location = obj.get("cursor_location", "query")
    if cursor_location not in {"query", "body"}:
        raise ManifestError(
            f"{path}.cursor_location: expected query or body",
            path=f"{path}.cursor_location",
        )
    if "initial_token" in obj and obj["initial_token"] is not None and not isinstance(
        obj["initial_token"], str
    ):
        raise ManifestError(
            f"{path}.initial_token: expected a string", path=f"{path}.initial_token"
        )
    try:
        return PaginatorSpec(**obj)
    except (TypeError, ValueError) as exc:
        field_path = f"{path}.type" if str(obj.get("type", "none")) not in {
            "none", "cursor", "offset", "page", "link_header"
        } else path
        raise ManifestError(f"{field_path}: {exc}", path=field_path) from None


def parse_manifest(raw: dict[str, Any]) -> Manifest:
    obj = _mapping(raw, "")
    _check_keys(obj, {"name", "base_url", "auth", "streams", "defaults", "rate_limit", "docs"}, "")
    name = obj.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ManifestError("name: must be a non-empty string", path="name")
    base_url = obj.get("base_url")
    if not isinstance(base_url, str):
        raise ManifestError("base_url: expected a URL", path="base_url")
    try:
        parsed_url = urlsplit(base_url)
    except ValueError:
        raise ManifestError("base_url: only absolute HTTP(S) URLs are allowed", path="base_url") from None
    if parsed_url.scheme.lower() not in {"http", "https"} or not parsed_url.netloc:
        raise ManifestError("base_url: only absolute HTTP(S) URLs are allowed", path="base_url")
    if parsed_url.username is not None or parsed_url.password is not None:
        raise ManifestError("base_url: URL credentials are not allowed", path="base_url")
    if parsed_url.query or parsed_url.fragment:
        raise ManifestError("base_url: query and fragment are not allowed", path="base_url")
    docs = obj.get("docs", "")
    if not isinstance(docs, str):
        raise ManifestError("docs: expected an absolute HTTP(S) URL", path="docs")
    if docs:
        try:
            docs_url = urlsplit(docs)
        except (TypeError, ValueError):
            raise ManifestError("docs: expected an absolute HTTP(S) URL", path="docs") from None
        if (
            docs_url.scheme.lower() not in {"http", "https"}
            or not docs_url.netloc
            or docs_url.username is not None
            or docs_url.password is not None
        ):
            raise ManifestError("docs: expected an absolute HTTP(S) URL", path="docs")
    raw_streams = obj.get("streams")
    if not isinstance(raw_streams, list) or not raw_streams:
        raise ManifestError("streams: expected a non-empty array", path="streams")
    defaults = _parse_defaults(obj.get("defaults"))
    streams: list[StreamSpec] = []
    stream_names: set[str] = set()
    for index, raw_stream in enumerate(raw_streams):
        path = f"streams[{index}]"
        stream = _mapping(raw_stream, path)
        _check_keys(
            stream,
            {
                "name",
                "path",
                "method",
                "records_path",
                "primary_key",
                "cursor",
                "paginator",
                "json_schema",
                "request_params",
                "request_headers",
                "request_body_template",
            },
            path,
        )
        stream_name = stream.get("name")
        stream_path = stream.get("path")
        if not isinstance(stream_name, str) or not stream_name.strip():
            raise ManifestError(f"{path}.name: must be non-empty", path=f"{path}.name")
        if stream_name in stream_names:
            raise ManifestError(f"{path}.name: duplicate stream name", path=f"{path}.name")
        stream_names.add(stream_name)
        if not isinstance(stream_path, str) or not stream_path.strip():
            raise ManifestError(f"{path}.path: must be non-empty", path=f"{path}.path")
        try:
            path_url = urlsplit(stream_path)
        except ValueError:
            raise ManifestError(f"{path}.path: expected a relative path", path=f"{path}.path") from None
        if path_url.scheme or path_url.netloc or path_url.query or path_url.fragment:
            raise ManifestError(f"{path}.path: expected a relative path", path=f"{path}.path")
        if stream_path.startswith("//") or ".." in stream_path.split("/"):
            raise ManifestError(f"{path}.path: unsafe relative path", path=f"{path}.path")
        method = str(stream.get("method", "GET")).upper()
        if method not in {"GET", "POST"}:
            raise ManifestError(f"{path}.method: expected GET or POST", path=f"{path}.method")
        records_path = stream.get("records_path", "results")
        if not isinstance(records_path, str):
            raise ManifestError(f"{path}.records_path: expected a dotted path", path=f"{path}.records_path")
        if records_path and any(not item for item in records_path.split(".")):
            raise ManifestError(f"{path}.records_path: invalid dotted path", path=f"{path}.records_path")
        primary_key = stream.get("primary_key", [])
        if not isinstance(primary_key, list) or any(not isinstance(key, str) or not key for key in primary_key):
            raise ManifestError(f"{path}.primary_key: expected an array of field names", path=f"{path}.primary_key")
        if len(set(primary_key)) != len(primary_key):
            raise ManifestError(f"{path}.primary_key: duplicate field", path=f"{path}.primary_key")
        request_params_raw = stream.get("request_params", {})
        request_params_obj = _mapping(request_params_raw, f"{path}.request_params")
        for key, value in request_params_obj.items():
            if not isinstance(key, str) or not key.strip():
                raise ManifestError(
                    f"{path}.request_params: parameter names must be non-empty strings",
                    path=f"{path}.request_params",
                )
            if value is not None and not isinstance(value, (str, int, float, bool)):
                raise ManifestError(
                    f"{path}.request_params.{key}: expected a scalar value",
                    path=f"{path}.request_params.{key}",
                )
            if isinstance(value, float) and not math.isfinite(value):
                raise ManifestError(
                    f"{path}.request_params.{key}: value must be finite",
                    path=f"{path}.request_params.{key}",
                )
        request_headers = _parse_request_headers(
            stream.get("request_headers", {}),
            f"{path}.request_headers",
        )
        request_body_template = stream.get("request_body_template")
        if request_body_template is not None:
            request_body_template = _mapping(
                request_body_template,
                f"{path}.request_body_template",
            )
            if method != "POST":
                raise ManifestError(
                    f"{path}.request_body_template: requires method POST",
                    path=f"{path}.request_body_template",
                )
        cursor = None
        if stream.get("cursor") is not None:
            cursor_path = f"{path}.cursor"
            cursor_raw = _mapping(stream["cursor"], cursor_path)
            _check_keys(
                cursor_raw,
                {
                    "field",
                    "request_param",
                    "format",
                    "lookback_s",
                    "request_template",
                    "request_location",
                },
                cursor_path,
            )
            for key in ("field", "request_param"):
                if not isinstance(cursor_raw.get(key), str) or not cursor_raw[key]:
                    raise ManifestError(f"{cursor_path}.{key}: must be non-empty", path=f"{cursor_path}.{key}")
            cursor_format = str(cursor_raw.get("format", "iso8601"))
            if cursor_format not in {"iso8601", "epoch_s", "epoch_ms"}:
                raise ManifestError(f"{cursor_path}.format: unsupported format", path=f"{cursor_path}.format")
            lookback = _positive_number(cursor_raw.get("lookback_s", 0), f"{cursor_path}.lookback_s", allow_zero=True)
            request_template = cursor_raw.get("request_template", "")
            if not isinstance(request_template, str):
                raise ManifestError(
                    f"{cursor_path}.request_template: expected a string",
                    path=f"{cursor_path}.request_template",
                )
            if request_template:
                rendered_parts = re.sub(r"\{cursor(?::[^{}]*)?\}", "", request_template)
                if (
                    "{cursor" not in request_template
                    or "{" in rendered_parts
                    or "}" in rendered_parts
                ):
                    raise ManifestError(
                        f"{cursor_path}.request_template: expected a {{cursor}} placeholder",
                        path=f"{cursor_path}.request_template",
                    )
            request_location = cursor_raw.get("request_location", "query")
            if not isinstance(request_location, str) or request_location not in {"query", "body"}:
                raise ManifestError(
                    f"{cursor_path}.request_location: expected query or body",
                    path=f"{cursor_path}.request_location",
                )
            if request_location == "body" and (
                method != "POST" or request_body_template is None
            ):
                raise ManifestError(
                    f"{cursor_path}.request_location: body cursors require a POST request_body_template",
                    path=f"{cursor_path}.request_location",
                )
            cursor = CursorSpec(
                field=cursor_raw["field"],
                request_param=cursor_raw["request_param"],
                format=cursor_format,
                lookback_s=lookback,
                request_template=request_template,
                request_location=request_location,
            )
        paginator = _parse_paginator(
            stream.get("paginator", {"type": "none"}),
            f"{path}.paginator",
            default_page_size=defaults.page_size,
            default_max_pages=defaults.max_pages,
        )
        if paginator.cursor_location == "body" and (
            method != "POST" or request_body_template is None
        ):
            raise ManifestError(
                f"{path}.paginator.cursor_location: body cursors require a POST request_body_template",
                path=f"{path}.paginator.cursor_location",
            )
        if cursor is not None and paginator.type == "cursor" and cursor.request_param == paginator.cursor_param:
            raise ManifestError(
                f"{path}.paginator.cursor_param: cannot share the incremental cursor parameter",
                path=f"{path}.paginator.cursor_param",
            )
        json_schema = stream.get("json_schema")
        if json_schema is not None and not isinstance(json_schema, dict):
            raise ManifestError(f"{path}.json_schema: expected an object", path=f"{path}.json_schema")
        if json_schema is not None:
            from connectors.sdk.declarative.schema import validate_stream_schema

            validate_stream_schema(
                json_schema,
                primary_key=primary_key,
                cursor_field=cursor.field if cursor else "",
                path=f"{path}.json_schema",
            )
        streams.append(
            StreamSpec(
                name=stream_name,
                path=stream_path,
                method=method,
                records_path=records_path,
                primary_key=tuple(primary_key),
                cursor=cursor,
                paginator=paginator,
                json_schema=json_schema,
                request_params=dict(request_params_obj),
                request_headers=request_headers,
                request_body_template=request_body_template,
            )
        )
    rate_limit = None
    rate_raw = obj.get("rate_limit")
    if rate_raw is not None:
        rate_obj = _mapping(rate_raw, "rate_limit")
        _check_keys(rate_obj, {"per_second"}, "rate_limit")
        rate_limit = _positive_number(rate_obj.get("per_second"), "rate_limit.per_second")
    return Manifest(
        name=name,
        base_url=base_url.rstrip("/") + "/",
        auth=_parse_auth(obj.get("auth")),
        streams=tuple(streams),
        defaults=defaults,
        rate_limit_per_second=rate_limit,
        docs=docs,
    )
