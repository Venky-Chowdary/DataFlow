"""Declarative HTTP source connector (YAML/JSON) — long-tail SaaS without per-brand Python."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator
from urllib.parse import urljoin

import requests

from connectors.sdk import BaseConnector, RecordBatch, StreamSchema, register_connector


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
class DeclarativeHttpConnector(BaseConnector):
    """Config-driven HTTP source. Config keys: ``spec`` (dict) + ``api_key``/``access_token``."""

    name = "declarative_http"
    supports_read = True
    supports_write = False

    def _spec(self) -> DeclarativeHttpSpec:
        raw = self.config.get("spec") or self.config.get("declarative_spec") or {}
        if not raw:
            raise ValueError("declarative_http requires config.spec")
        return parse_declarative_spec(raw)

    def _token(self) -> str:
        return str(
            self.config.get("access_token")
            or self.config.get("api_key")
            or (self.config.get("credentials") or {}).get("access_token")
            or ""
        )

    def _headers(self) -> dict[str, str]:
        spec = self._spec()
        headers = {"Accept": "application/json", **spec.extra_headers}
        token = self._token()
        if token:
            headers[spec.auth_header] = f"{spec.auth_prefix}{token}"
        return headers

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
            streams = self.discover()
            if not streams:
                return False, "No streams defined in declarative spec"
            # Probe first stream with limit=1
            first = streams[0].name
            next(self.read(first, state=None, limit=1), None)
            return True, f"OK — {len(streams)} stream(s)"
        except Exception as exc:
            return False, str(exc)

    def test_connection(self) -> bool:
        ok, _ = self.check()
        return ok

    def discover(self) -> list[StreamSchema]:
        spec = self._spec()
        out: list[StreamSchema] = []
        for s in spec.streams:
            out.append(
                StreamSchema(
                    name=s.name,
                    properties=dict(s.properties) or {"id": "string"},
                    primary_key=list(s.primary_key),
                    cursor_field=s.cursor_field,
                    json_schema={
                        "type": "object",
                        "properties": {
                            k: {"type": v} for k, v in (s.properties or {"id": "string"}).items()
                        },
                    },
                )
            )
        return out

    def read(
        self,
        stream: str,
        *,
        state: dict[str, Any] | None = None,
        offset: int = 0,
        limit: int = 1000,
    ) -> Iterator[RecordBatch]:
        spec = self._spec()
        decl = next((s for s in spec.streams if s.name == stream), None)
        if decl is None:
            raise ValueError(f"Unknown stream: {stream}")
        url = urljoin(spec.base_url, decl.path.lstrip("/"))
        cursor_val = None
        if state and decl.cursor_field:
            cursor_val = (state.get(stream) or state).get(decl.cursor_field)
        params: dict[str, Any] = {decl.page_param: min(decl.page_size, limit)}
        if cursor_val and decl.cursor_param:
            params[decl.cursor_param] = cursor_val
        elif offset and decl.offset_param:
            params[decl.offset_param] = str(offset)

        resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        records_raw = _dig(payload, decl.records_path)
        if records_raw is None and isinstance(payload, list):
            records_raw = payload
        records = [dict(r) for r in (records_raw or []) if isinstance(r, dict)][:limit]
        schema = StreamSchema(
            name=decl.name,
            properties=dict(decl.properties) or {k: "string" for k in (records[0] if records else {"id": ""}).keys()},
            primary_key=list(decl.primary_key),
            cursor_field=decl.cursor_field,
        )
        new_state = dict(state or {})
        if records and decl.cursor_field:
            last = records[-1].get(decl.cursor_field)
            if last is not None:
                new_state[stream] = {decl.cursor_field: last}
        yield RecordBatch(stream=stream, records=records, schema=schema, state=new_state)
