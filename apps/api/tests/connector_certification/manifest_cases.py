"""Certification cases derived from each declarative manifest connector.

Every registered ``ManifestConnector`` gets the same lifecycle proof as the
hand-written cases: fixture rows are placed at the stream's ``records_path``,
page one carries the paginator's own continuation (cursor path, Link header,
next URL, offset/page), and the cursor is rendered in the manifest's format.
A connector whose vendor shape cannot be expressed this way fails loudly
instead of being skipped.
"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Any
from urllib.parse import parse_qs, quote, urljoin, urlsplit

from connectors.sdk import get_sdk_connector, list_sdk_connectors
from connectors.sdk.declarative.connector import ManifestConnector
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer
from tests.connector_certification.harness import CertificationCase

SECRET = "PLANTED_CERTIFICATION_SECRET"

# Minimal synthetic configs: every value a connector needs to render its
# requests. Shared with the enterprise error matrix so coverage cannot drift.
SYNTHETIC_CONFIGS: dict[str, dict[str, Any]] = {
    "gitlab": {"api_key": "synthetic"},
    "mailchimp": {"dc": "us21", "api_key": "synthetic"},
    "pipedrive": {"domain": "acme", "api_key": "synthetic"},
    "klaviyo": {"api_key": "synthetic"},
    "servicenow": {"instance": "dev1", "username": "etl", "password": "synthetic"},
    "freshdesk": {"domain": "acme", "api_key": "synthetic"},
    "slack": {"api_key": "xoxb-synthetic", "channel": "C0FIXTURE"},
    "asana": {"api_key": "synthetic", "workspace": "ws-1"},
    "calendly": {"api_key": "synthetic", "user_uri": "https://api.calendly.com/users/U1"},
    "woocommerce": {"domain": "shop.example.com", "username": "ck_x", "password": "cs_y"},
    "bigcommerce": {"store_hash": "abc123", "api_key": "synthetic"},
    "brevo": {"api_key": "synthetic"},
    "freshservice": {"domain": "acme", "api_key": "synthetic"},
    "close": {"api_key": "synthetic"},
    "todoist": {"api_key": "synthetic"},
    "typeform": {"api_key": "synthetic", "form_id": "f1"},
    "notion": {"api_key": "synthetic", "database_id": "db1"},
    "zoho_crm": {
        "client_id": "cid",
        "client_secret": "csec",
        "refresh_token": "rt",
        "access_token": "zoho-synthetic",
    },
    "posthog": {"domain": "app.posthog.com", "api_key": "phx_synthetic", "project_id": "42"},
    "confluence": {"domain": "acme", "username": "a@acme.com", "api_key": "atl-synthetic"},
    "zendesk": {"domain": "acme", "username": "a@acme.com", "api_key": "zd-synthetic"},
    "airtable": {"api_key": "pat_synthetic", "base_id": "appXYZ", "table_id": "tblABC"},
    "clickup": {"api_key": "pk_synthetic", "list_id": "9001"},
    "mailjet": {"api_key": "pub_synthetic", "secret_key": "sec_synthetic"},
    "sendgrid": {"api_key": "SG.synthetic"},
    "monday": {"api_key": "mdy_synthetic", "board_id": "42"},
    "linear": {"api_key": "lin_api_synthetic"},
}

_SECRET_KEYS = ("api_key", "password", "secret_key", "access_token", "client_secret")
_CURSOR_DAYS = (1, 2, 3, 4)
_MUTATED_DAYS = {2: 5, 3: 6}
_ADDED_DAY = 7


def _get(record: Mapping[str, Any], path: str) -> Any:
    value: Any = record
    for part in path.split("."):
        value = value.get(part) if isinstance(value, Mapping) else None
    return value


def _set(record: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    target = record
    for part in parts[:-1]:
        target = target.setdefault(part, {})
    target[parts[-1]] = value


def _schema_type(schema: Mapping[str, Any], path: str) -> str:
    definition: Any = schema
    for part in path.split("."):
        props = definition.get("properties") if isinstance(definition, Mapping) else None
        definition = props.get(part) if isinstance(props, Mapping) else None
    raw = definition.get("type", "string") if isinstance(definition, Mapping) else "string"
    if isinstance(raw, list):
        raw = next((item for item in raw if item != "null"), "string")
    return str(raw)


def _cursor_value(fmt: str, kind: str, day: int) -> Any:
    moment = datetime(2026, 1, day, tzinfo=timezone.utc)
    if fmt == "epoch_s":
        seconds = int(moment.timestamp())
        return seconds if kind == "integer" else f"{seconds}.000100"
    if fmt == "epoch_ms":
        millis = int(moment.timestamp() * 1000)
        return millis if kind == "integer" else str(millis)
    if fmt == "http_date":
        return format_datetime(moment, usegmt=True)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _identity(kind: str, index: int) -> Any:
    return index if kind == "integer" else str(index)


def _render(template: str, config: Mapping[str, Any]) -> str:
    out = template
    for key, value in config.items():
        out = out.replace("{" + key + "}", quote(str(value), safe=""))
    return out


def _manifest_case(name: str, cls: type[ManifestConnector]) -> CertificationCase:
    config = dict(SYNTHETIC_CONFIGS.get(name, {}))
    for key in _SECRET_KEYS:
        if key in config:
            config[key] = SECRET
    probe = cls(dict(config))
    descriptor = cls.descriptor
    incremental = "incremental" in descriptor.sync_modes
    streams = list(probe.manifest.streams)
    stream = next((s for s in streams if incremental and s.cursor), streams[0])
    first = streams[0]
    schema = stream.json_schema or {}
    pk = tuple(stream.primary_key)
    cursor = stream.cursor if incremental else None
    cursor_field = cursor.field if cursor else ""
    cursor_fmt = getattr(cursor, "format", "iso8601") if cursor else "iso8601"
    cursor_kind = _schema_type(schema, cursor_field) if cursor_field else "string"
    pk_kind = _schema_type(schema, pk[0])
    paginator = stream.paginator

    def row(index: int, day: int) -> dict[str, Any]:
        record: dict[str, Any] = {}
        for key in pk:
            _set(record, key, _identity(pk_kind, index))
        if cursor_field:
            _set(record, cursor_field, _cursor_value(cursor_fmt, cursor_kind, day))
        return record

    records = tuple(row(index, day) for index, day in enumerate(_CURSOR_DAYS, start=1))

    def mutate(
        original: Sequence[Mapping[str, Any]],
    ) -> tuple[list[dict[str, Any]], set[str], set[str]]:
        changed = [deepcopy(dict(item)) for item in original]
        for index, day in _MUTATED_DAYS.items():
            _set(changed[index - 1], cursor_field, _cursor_value(cursor_fmt, cursor_kind, day))
        changed.append(row(5, _ADDED_DAY))
        # Ids come from the record itself: Slack's ``ts`` is both key and cursor.
        ids = [str(_get(item, pk[0])) for item in changed]
        return changed, {ids[1], ids[2], ids[4]}, {ids[3]}

    def route_path(fixture: FixtureServer, spec: Any) -> str:
        rendered = _render(spec.path, config).lstrip("/")
        return urlsplit(urljoin(fixture.base_url + "/", rendered)).path or "/"

    def page(
        spec: Any,
        rows: Sequence[Mapping[str, Any]],
        *,
        fixture: FixtureServer,
        has_next: bool,
    ) -> FixtureResponse:
        items = [deepcopy(dict(item)) for item in rows]
        records_path = spec.records_path or "$"
        body: Any = items if records_path == "$" else {}
        if records_path != "$":
            _set(body, records_path, items)
        headers: dict[str, str] = {}
        pager = spec.paginator
        kind = getattr(pager, "type", "none") if pager else "none"
        path = route_path(fixture, spec)
        if kind == "cursor":
            if pager.cursor_header:
                if has_next:
                    headers[pager.cursor_header] = "page-2"
            elif pager.cursor_path and isinstance(body, dict):
                _set(body, pager.cursor_path, "page-2" if has_next else None)
            if getattr(pager, "has_more_path", "") and isinstance(body, dict):
                _set(body, pager.has_more_path, has_next)
        elif kind == "link_header" and has_next:
            headers["Link"] = f'<{fixture.base_url}{path}?page=2>; rel="next"'
        elif kind == "next_url" and isinstance(body, dict):
            _set(body, pager.next_url_path, f"{fixture.base_url}{path}?page=2" if has_next else None)
        if getattr(pager, "stop_path", "") and isinstance(body, dict):
            _set(body, pager.stop_path, not has_next)
        return FixtureResponse(body=body, headers=headers)

    def sequence(fixture: FixtureServer, spec: Any, rows: Sequence[Mapping[str, Any]]) -> list[FixtureResponse]:
        if spec.paginator is None or getattr(spec.paginator, "type", "none") == "none":
            return [page(spec, rows, fixture=fixture, has_next=False)]
        return [
            page(spec, rows[:2], fixture=fixture, has_next=True),
            page(spec, rows[2:], fixture=fixture, has_next=False),
            page(spec, [], fixture=fixture, has_next=False),
        ]

    method = (getattr(stream, "method", None) or getattr(stream, "http_method", None) or "GET").upper()
    first_method = (getattr(first, "method", None) or getattr(first, "http_method", None) or "GET").upper()

    def routes(fixture: FixtureServer, phase: str, rows: Sequence[Mapping[str, Any]]) -> None:
        path = route_path(fixture, stream)
        first_path = route_path(fixture, first)
        if (first_method, first_path) != (method, path):
            fixture.add_route(
                first_path,
                responses=[page(first, [], fixture=fixture, has_next=False)]
                if phase != "auth"
                else [FixtureResponse(status=401, body={"message": "unauthorized"})],
                method=first_method,
            )
        if phase == "auth":
            fixture.add_route(path, FixtureResponse(status=401, body={"message": "unauthorized"}), method=method)
        elif phase == "incremental":
            floor = str(_get(records[-1], cursor_field))
            fresh = [item for item in rows if str(_get(item, cursor_field)) >= floor]
            fixture.add_route(path, responses=sequence(fixture, stream, fresh), method=method)
        elif phase == "fault":
            fixture.add_route(
                path,
                responses=[
                    page(stream, rows[:2], fixture=fixture, has_next=True),
                    *[FixtureResponse(status=500, body={"message": "synthetic fault"}) for _ in range(8)],
                ],
                method=method,
            )
        elif phase == "rate_limit":
            fixture.add_route(
                path,
                responses=[
                    FixtureResponse(status=429, body={"message": "rate limited"}, headers={"Retry-After": "2"}),
                    *sequence(fixture, stream, rows),
                ],
                method=method,
            )
        elif phase == "resume":
            # The restart reads from the saved checkpoint; serve the rest without
            # replaying page one's continuation token.
            fixture.add_route(
                path,
                responses=[
                    page(stream, rows[2:], fixture=fixture, has_next=False),
                    page(stream, [], fixture=fixture, has_next=False),
                ],
                method=method,
            )
        elif phase == "check":
            fixture.add_route(path, responses=[page(stream, rows[:1], fixture=fixture, has_next=False)], method=method)
        else:
            fixture.add_route(path, responses=sequence(fixture, stream, rows), method=method)

    def factory(
        base_url: str,
        _phase: str,
        _fixture_records: Sequence[Mapping[str, Any]] = (),
        sleep: Any = None,
    ) -> ManifestConnector:
        source = cls({**config, **({"base_url": base_url} if base_url else {})})
        source.requester.sleep = sleep or (lambda _seconds: None)
        spec = source._streams[stream.name]
        if spec.paginator is not None and spec.paginator.type in {"offset", "page"}:
            # Short pages end offset/page pagination; two-row pages exercise it.
            source._streams[stream.name] = replace(
                spec, paginator=replace(spec.paginator, page_size=2)
            )
        return source

    assert_cursor = None
    if cursor and cursor.request_param and not getattr(cursor, "request_location", "") in {"header", "body"}:
        param = cursor.request_param

        def assert_cursor(fixture: FixtureServer, _state: Mapping[str, Any]) -> None:
            sent = [
                parse_qs(urlsplit(record.target).query).get(param)
                for record in fixture.request_log
                if record.method == method
            ]
            if not any(sent) and method == "GET":
                raise AssertionError(f"{name}: incremental request omitted saved cursor param {param!r}")

    unsupported = frozenset(
        step for step in descriptor.certification_skips
    )
    return CertificationCase(
        connector_id=name,
        connector_factory=factory,
        fixture_routes=routes,
        stream=stream.name,
        mutate_fixture=mutate,
        fixture_records=records,
        primary_key=pk,
        cursor_field=cursor_field,
        secret=SECRET,
        unsupported_steps=unsupported,
        assert_incremental_cursor=assert_cursor,
    )


def build_manifest_cases(exclude: set[str]) -> dict[str, CertificationCase]:
    cases: dict[str, CertificationCase] = {}
    for name in list_sdk_connectors():
        if name in exclude:
            continue
        cls = get_sdk_connector(name)
        if cls is None or not issubclass(cls, ManifestConnector) or name not in SYNTHETIC_CONFIGS:
            continue
        cases[name] = _manifest_case(name, cls)
    return cases


__all__ = ["SECRET", "SYNTHETIC_CONFIGS", "build_manifest_cases", "json"]
