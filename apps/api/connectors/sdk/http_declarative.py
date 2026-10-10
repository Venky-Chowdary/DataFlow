"""Declarative HTTP source connector (YAML/JSON) — long-tail SaaS without per-brand Python."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator

import requests

from connectors.sdk import RecordBatch, StreamSchema, register_connector
from connectors.sdk.declarative.connector import DeclarativeSource

__all__ = [
    "DeclarativeHttpConnector",
    "DeclarativeHttpSpec",
    "DeclarativeStream",
    "parse_declarative_spec",
    "requests",
]


class _LegacyRequestsSession:
    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        request = getattr(requests, method.lower(), None)
        if request is None:
            return requests.request(method, url, **kwargs)
        return request(url, **kwargs)


@dataclass
class DeclarativeStream:
    name: str
    path: str
    primary_key: list[str] = field(default_factory=list)
    records_path: str = "results"  # dotted path into JSON response
    cursor_field: str = ""
    cursor_param: str = ""
    page_size: int = 100
    page_param: str = "limit"
    offset_param: str = "after"
    properties: dict[str, str] = field(default_factory=dict)


@dataclass
class DeclarativeHttpSpec:
    name: str
    base_url: str
    auth_header: str = "Authorization"
    auth_prefix: str = "Bearer "
    streams: list[DeclarativeStream] = field(default_factory=list)
    extra_headers: dict[str, str] = field(default_factory=dict)


def _dig(data: Any, path: str) -> Any:
    if not path:
        return data
    cur = data
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def parse_declarative_spec(raw: dict[str, Any]) -> DeclarativeHttpSpec:
    streams = []
    for s in raw.get("streams") or []:
        streams.append(
            DeclarativeStream(
                name=str(s["name"]),
                path=str(s.get("path") or f"/{s['name']}"),
                primary_key=list(s.get("primary_key") or []),
                records_path=str(s.get("records_path") or "results"),
                cursor_field=str(s.get("cursor_field") or ""),
                cursor_param=str(s.get("cursor_param") or ""),
                page_size=int(s.get("page_size") or 100),
                page_param=str(s.get("page_param") or "limit"),
                offset_param=str(s.get("offset_param") or "after"),
                properties=dict(s.get("properties") or {}),
            )
        )
    return DeclarativeHttpSpec(
        name=str(raw.get("name") or "declarative_http"),
        base_url=str(raw.get("base_url") or "").rstrip("/") + "/",
        auth_header=str(raw.get("auth_header") or "Authorization"),
        auth_prefix=str(raw.get("auth_prefix") or "Bearer "),
        streams=streams,
        extra_headers=dict(raw.get("extra_headers") or {}),
    )


@register_connector
class DeclarativeHttpConnector(DeclarativeSource):
    """Backward-compatible config adapter for :class:`DeclarativeSource`."""

    name = "declarative_http"
    supports_read = True
    supports_write = False

    def __init__(self, config: dict[str, Any]) -> None:
        raw = config.get("spec") or config.get("declarative_spec") or {}
        if not raw:
            raise ValueError("declarative_http requires config.spec")
        legacy_spec = parse_declarative_spec(raw)
        manifest = {
            "name": legacy_spec.name,
            "base_url": legacy_spec.base_url,
            "auth": {"type": "none"},
            "streams": [
                {
                    "name": stream.name,
                    "path": stream.path,
                    "method": "GET",
                    "records_path": stream.records_path,
                    "primary_key": list(stream.primary_key),
                    "paginator": {"type": "none"},
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            key: {"type": value}
                            for key, value in (
                                stream.properties or {"id": "string"}
                            ).items()
                        },
                    },
                }
                for stream in legacy_spec.streams
            ],
        }
        super().__init__({**config, "manifest": manifest})
        self.requester.session = _LegacyRequestsSession()
        self._legacy_http_spec = legacy_spec
        self.auth.headers.setdefault("Accept", "application/json")
        self.auth.headers.update(legacy_spec.extra_headers)
        token = self._token()
        if token:
            self.auth.headers[legacy_spec.auth_header] = f"{legacy_spec.auth_prefix}{token}"

    def _spec(self) -> DeclarativeHttpSpec:
        return self._legacy_http_spec

    def _token(self) -> str:
        credentials = self.config.get("credentials")
        credentials = credentials if isinstance(credentials, dict) else {}
        return str(
            self.config.get("access_token")
            or self.config.get("api_key")
            or credentials.get("access_token")
            or ""
        )

    def _headers(self) -> dict[str, str]:
        return dict(self.auth.headers)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["api_key", "spec"],
                "properties": {
                    "api_key": {"type": "string", "title": "API token"},
                    "spec": {"type": "object", "title": "Declarative connector spec"},
                },
            }
        }

    def check(self) -> tuple[bool, str]:
        try:
            if not self._legacy_http_spec.streams:
                return False, "No streams defined in declarative spec"
            first = self._legacy_http_spec.streams[0].name
            next(self.read(first, state=None, limit=1), None)
            return True, f"OK — {len(self._legacy_http_spec.streams)} stream(s)"
        except Exception as exc:
            return False, str(exc)

    def test_connection(self) -> bool:
        ok, _ = self.check()
        return ok

    def discover(self) -> list[StreamSchema]:
        discovered = super().discover()
        return [
            StreamSchema(
                name=stream.name,
                properties=dict(stream.properties) or dict(schema.properties),
                primary_key=list(stream.primary_key),
                cursor_field=stream.cursor_field,
                supported_sync_modes=(
                    ["full_refresh", "incremental"]
                    if stream.cursor_field and stream.cursor_param
                    else ["full_refresh"]
                ),
                json_schema=schema.json_schema,
            )
            for stream, schema in zip(self._legacy_http_spec.streams, discovered)
        ]

    def read(
        self,
        stream: str,
        *,
        state: dict[str, Any] | None = None,
        offset: int = 0,
        limit: int = 1000,
    ) -> Iterator[RecordBatch]:
        decl = next((item for item in self._legacy_http_spec.streams if item.name == stream), None)
        if decl is None:
            raise ValueError(f"Unknown stream: {stream}")
        if limit <= 0:
            return
        params: dict[str, Any] = {decl.page_param: min(decl.page_size, limit)}
        current_state = state or {}
        stream_state = current_state.get(stream, current_state)
        cursor_val = (
            stream_state.get(decl.cursor_field)
            if isinstance(stream_state, dict) and decl.cursor_field
            else None
        )
        if cursor_val and decl.cursor_param:
            params[decl.cursor_param] = cursor_val
        elif offset and decl.offset_param:
            params[decl.offset_param] = str(offset)
        for batch in super().read(
            stream,
            state=None,
            limit=limit,
            _request_params=params,
        ):
            next_state = dict(current_state)
            if batch.records and decl.cursor_field:
                last = batch.records[-1].get(decl.cursor_field)
                if last is not None:
                    next_state[stream] = {decl.cursor_field: last}
            schema = StreamSchema(
                name=decl.name,
                properties=dict(decl.properties) or dict(batch.schema.properties),
                primary_key=list(decl.primary_key),
                cursor_field=decl.cursor_field,
                supported_sync_modes=(
                    ["full_refresh", "incremental"]
                    if decl.cursor_field and decl.cursor_param
                    else ["full_refresh"]
                ),
                json_schema=batch.schema.json_schema,
            )
            yield RecordBatch(
                stream=batch.stream,
                records=batch.records,
                schema=schema,
                state=next_state,
            )
