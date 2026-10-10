from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any, Iterator
from urllib.parse import urljoin, urlsplit

from connectors.sdk import (
    BaseConnector,
    ConnectorDescriptor,
    RecordBatch,
    StreamSchema,
    register_connector,
)
from connectors.sdk.declarative.auth import build_auth
from connectors.sdk.declarative.errors import ManifestError
from connectors.sdk.declarative.incremental import (
    StreamState,
    advance_stream_state,
    cursor_for_request,
)
from connectors.sdk.declarative.manifest import Manifest, parse_manifest
from connectors.sdk.declarative.pagination import PaginatorSpec, paginate
from connectors.sdk.declarative.requester import HttpRequester
from connectors.sdk.declarative.schema import infer_json_schema, validate_stream_schema


@register_connector
class DeclarativeSource(BaseConnector):
    name = "declarative_source"
    supports_read = True
    supports_write = False
    descriptor = ConnectorDescriptor(
        id="declarative_source",
        display_name="Declarative HTTP source",
        roles=("source",),
        auth_modes=(
            "none",
            "api_key",
            "bearer",
            "basic",
            "oauth2_refresh",
            "oauth2_client_credentials",
        ),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "base_url", "sensitive": False},
            {"name": "auth.value", "sensitive": True},
            {"name": "auth.token", "sensitive": True},
            {"name": "auth.password", "sensitive": True},
            {"name": "auth.client_secret", "sensitive": True},
            {"name": "auth.refresh_token", "sensitive": True},
            {"name": "auth.access_token", "sensitive": True},
        ),
        evidence="synthetic-fixture",
    )

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        raw_manifest = config.get("manifest")
        if not isinstance(raw_manifest, dict):
            raise ManifestError("manifest: expected an object", path="manifest")
        self.manifest: Manifest = parse_manifest(raw_manifest)
        self._streams = {stream.name: stream for stream in self.manifest.streams}
        defaults = self.manifest.defaults
        self.requester = HttpRequester(
            timeout_s=defaults.timeout_s,
            max_attempts=defaults.retry.max_attempts,
            base_delay_seconds=defaults.retry.base_delay,
            max_delay_seconds=defaults.retry.max_delay,
            rate_limit_per_second=self.manifest.rate_limit_per_second,
        )
        self.auth = build_auth(self.manifest.auth, config)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["manifest"],
                "properties": {
                    "manifest": {"type": "object"},
                    "credentials": {"type": "object"},
                },
            }
        }

    def test_connection(self) -> bool:
        stream = self.manifest.streams[0]
        url = urljoin(self.manifest.base_url, stream.path.lstrip("/"))
        params = dict(self.auth.params)
        if stream.cursor:
            params[stream.cursor.request_param] = ""
        self.requester.request_json(
            stream.method,
            url,
            stream=stream.name,
            headers=self.auth.headers,
            params=params,
            refresh_auth=self.auth.refresh_auth,
            json={} if stream.method == "POST" else None,
        )
        return True

    def discover(self) -> list[StreamSchema]:
        result: list[StreamSchema] = []
        for index, stream in enumerate(self.manifest.streams):
            schema = stream.json_schema
            if schema is None:
                url = urljoin(self.manifest.base_url, stream.path.lstrip("/"))
                pages = paginate(
                    self.requester,
                    url,
                    method=stream.method,
                    records_path=stream.records_path,
                    headers=self.auth.headers,
                    refresh_auth=self.auth.refresh_auth,
                    params=self.auth.params,
                    stream=stream.name,
                    paginator=PaginatorSpec(type="none", max_pages=1),
                )
                schema = infer_json_schema(pages[0].records if pages else [])
            validate_stream_schema(
                schema,
                primary_key=stream.primary_key,
                cursor_field=stream.cursor.field if stream.cursor else "",
                path=f"streams[{index}].json_schema",
            )
            properties: dict[str, str] = {}
            for key, value in schema.get("properties", {}).items():
                json_type = value.get("type", "string") if isinstance(value, Mapping) else "string"
                if isinstance(json_type, list):
                    json_type = next((item for item in json_type if item != "null"), "null")
                properties[str(key)] = str(json_type)
            result.append(
                StreamSchema(
                    name=stream.name,
                    properties=properties,
                    primary_key=list(stream.primary_key),
                    cursor_field=stream.cursor.field if stream.cursor else "",
                    json_schema=dict(schema),
                    supported_sync_modes=(
                        ["full_refresh", "incremental"] if stream.cursor else ["full_refresh"]
                    ),
                )
            )
        return result

    def read(
        self,
        stream: str,
        *,
        state: dict[str, Any] | None = None,
        offset: int = 0,
        limit: int = 1000,
        _request_params: Mapping[str, Any] | None = None,
    ) -> Iterator[RecordBatch]:
        stream_spec = self._streams.get(stream)
        if stream_spec is None:
            raise ManifestError(f"streams: unknown stream {stream!r}", path="streams")
        if limit <= 0:
            return
        raw_state: Any = state or {}
        if isinstance(raw_state, Mapping) and stream in raw_state and isinstance(raw_state[stream], Mapping):
            raw_state = raw_state[stream]
        current_state = StreamState.from_dict(raw_state)
        max_records = limit
        if self.manifest.defaults.max_records is not None:
            max_records = min(max_records, self.manifest.defaults.max_records)
        if max_records == 0:
            return

        paginator = replace(
            stream_spec.paginator,
            max_pages=min(stream_spec.paginator.max_pages, self.manifest.defaults.max_pages),
        )
        endpoint = urljoin(self.manifest.base_url, stream_spec.path.lstrip("/"))
        request_url = endpoint
        if paginator.type == "cursor" and current_state.page_token is not None:
            paginator = replace(paginator, initial_token=str(current_state.page_token))
        elif paginator.type == "link_header" and current_state.page_token:
            request_url = str(current_state.page_token)
            base, resumed = urlsplit(endpoint), urlsplit(request_url)
            if (base.scheme.lower(), base.netloc.lower()) != (
                resumed.scheme.lower(),
                resumed.netloc.lower(),
            ):
                raise ManifestError("state.page_token: URL leaves source origin", path="state.page_token")
        elif paginator.type == "offset":
            start_offset = (
                int(current_state.page_token)
                if current_state.page_token is not None
                else paginator.start_offset + current_state.pages_done * paginator.page_size + offset
            )
            paginator = replace(paginator, start_offset=start_offset)
        elif paginator.type == "page":
            start_index = (
                int(current_state.page_token)
                if current_state.page_token is not None
                else paginator.start_index + current_state.pages_done + offset // paginator.page_size
            )
            paginator = replace(paginator, start_index=start_index)

        params = dict(self.auth.params)
        if stream_spec.cursor:
            cursor_value = cursor_for_request(
                current_state,
                cursor_format=stream_spec.cursor.format,
                lookback_s=stream_spec.cursor.lookback_s,
            )
            if cursor_value is not None:
                params[stream_spec.cursor.request_param] = cursor_value
        if _request_params:
            params.update(_request_params)
        pages = paginate(
            self.requester,
            request_url,
            method=stream_spec.method,
            records_path=stream_spec.records_path,
            headers=self.auth.headers,
            refresh_auth=self.auth.refresh_auth,
            params=params,
            stream=stream,
            paginator=paginator,
            max_pages=self.manifest.defaults.max_pages,
            record_limit=max_records,
        )
        schema = stream_spec.json_schema
        if schema is None:
            schema = infer_json_schema(pages[0].records if pages else [])
        validate_stream_schema(
            schema,
            primary_key=stream_spec.primary_key,
            cursor_field=stream_spec.cursor.field if stream_spec.cursor else "",
            path=f"streams[{list(self._streams).index(stream)}].json_schema",
        )
        stream_schema = StreamSchema(
            name=stream,
            properties={
                str(key): str(
                    next(
                        (item for item in field_type if item != "null"),
                        "null",
                    )
                    if isinstance(field_type, list)
                    else field_type
                )
                for key, value in schema.get("properties", {}).items()
                for field_type in [
                    value.get("type", "string")
                    if isinstance(value, Mapping)
                    else "string"
                ]
            },
            primary_key=list(stream_spec.primary_key),
            cursor_field=stream_spec.cursor.field if stream_spec.cursor else "",
            json_schema=dict(schema),
            supported_sync_modes=(
                ["full_refresh", "incremental"] if stream_spec.cursor else ["full_refresh"]
            ),
        )
        for page in pages:
            page_token: Any = (
                page.next_url
                if paginator.type == "link_header"
                else page.next_token
            )
            if stream_spec.cursor:
                current_state = advance_stream_state(
                    current_state,
                    page.records,
                    cursor_field=stream_spec.cursor.field,
                    cursor_format=stream_spec.cursor.format,
                    page_token=page_token,
                    stream=stream,
                )
            else:
                current_state = StreamState(
                    cursor=current_state.cursor,
                    page_token=page_token,
                    pages_done=current_state.pages_done + 1,
                )
            yield RecordBatch(
                stream=stream,
                records=page.records,
                schema=stream_schema,
                state=current_state.to_dict(),
            )
