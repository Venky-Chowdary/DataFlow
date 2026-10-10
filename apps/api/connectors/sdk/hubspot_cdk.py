"""HubSpot golden CDK connector — discover / check / incremental read via CRM v3."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from connectors.saas_common import base_url, humanize_http_error, request, token
from connectors.sdk import BaseConnector, RecordBatch, StreamSchema, register_connector
from services.value_serializer import load_http_json

DEFAULT_HOST = "api.hubapi.com"
_UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_SEARCH_RESULT_CAP = 10_000
logger = logging.getLogger(__name__)


class HubSpotCursorError(RuntimeError):
    """HubSpot incremental cursor cannot be interpreted or advanced safely."""


class HubSpotCursorMissing(HubSpotCursorError):
    """A search result did not contain the configured cursor field."""


class HubSpotCursorStalled(HubSpotCursorError):
    """HubSpot search pagination cannot advance without losing records."""


class HubSpotPaginationError(RuntimeError):
    """HubSpot returned a pagination sequence that cannot be resumed safely."""


def _parse_cursor_datetime(value: Any, *, stream: str, cursor_field: str) -> datetime:
    try:
        if isinstance(value, datetime):
            parsed = value
        else:
            text = str(value).strip()
            if not text:
                raise ValueError("empty cursor")
            if text.lstrip("+-").isdigit():
                number = int(text)
                unit = timedelta(milliseconds=number) if abs(number) >= 100_000_000_000 else timedelta(seconds=number)
                parsed = _UTC_EPOCH + unit
            else:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise HubSpotCursorError(
            f"HubSpot {stream!r} cursor field {cursor_field!r} is not a valid ISO or epoch timestamp"
        ) from exc


def _cursor_epoch_ms(value: datetime) -> int:
    delta = value.astimezone(timezone.utc) - _UTC_EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def _cursor_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _search_after(value: Any, *, stream: str) -> int:
    try:
        after = int(value)
    except (TypeError, ValueError) as exc:
        raise HubSpotPaginationError(
            f"HubSpot search for {stream!r} returned an invalid after cursor"
        ) from exc
    if after < 0:
        raise HubSpotPaginationError(
            f"HubSpot search for {stream!r} returned an invalid after cursor"
        )
    return after

# Core CRM objects with sensible property sets for discover
_HUBSPOT_STREAMS: dict[str, dict[str, Any]] = {
    "contacts": {
        "path": "/crm/v3/objects/contacts",
        "primary_key": ["id"],
        "cursor_field": "lastmodifieddate",
        "properties": {
            "id": "string",
            "email": "string",
            "firstname": "string",
            "lastname": "string",
            "lastmodifieddate": "string",
        },
        "default_props": "email,firstname,lastname,lastmodifieddate",
    },
    "companies": {
        "path": "/crm/v3/objects/companies",
        "primary_key": ["id"],
        "cursor_field": "hs_lastmodifieddate",
        "properties": {
            "id": "string",
            "name": "string",
            "domain": "string",
            "hs_lastmodifieddate": "string",
        },
        "default_props": "name,domain,hs_lastmodifieddate",
    },
    "deals": {
        "path": "/crm/v3/objects/deals",
        "primary_key": ["id"],
        "cursor_field": "hs_lastmodifieddate",
        "properties": {
            "id": "string",
            "dealname": "string",
            "amount": "string",
            "dealstage": "string",
            "hs_lastmodifieddate": "string",
        },
        "default_props": "dealname,amount,dealstage,hs_lastmodifieddate",
    },
}


@register_connector
class HubSpotCDKConnector(BaseConnector):
    """HubSpot source with at-least-once resume; inclusive GTE may reread boundary rows."""

    name = "hubspot_cdk"
    supports_read = True
    supports_write = False

    def _token(self) -> str:
        return token(
            self.config.get("api_key", ""),
            self.config.get("connection_string", ""),
            self.config.get("username", ""),
            self.config.get("password", ""),
        ) or str((self.config.get("credentials") or {}).get("access_token") or "")

    def _base(self) -> str:
        return base_url(self.config.get("host", ""), DEFAULT_HOST)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["api_key"],
                "properties": {
                    "api_key": {  # nosec B105
                        "type": "string",
                        "title": "Private app access token",
                        "airbyte_secret": True,
                    },
                    "host": {"type": "string", "title": "API host", "default": DEFAULT_HOST},
                },
            }
        }

    def test_connection(self) -> bool:
        ok, _ = self.check()
        return ok

    def check(self) -> tuple[bool, str]:
        access = self._token()
        if not access:
            return False, "HubSpot private app token is required"
        url = f"{self._base()}/crm/v3/objects/contacts"
        try:
            r = request(
                method="GET",
                url=url,
                token=access,
                params={"limit": 1, "properties": "email"},
                timeout=20,
            )
            r.raise_for_status()
            return True, "HubSpot reachable"
        except Exception as exc:
            return False, humanize_http_error(exc, "hubspot")

    def discover(self) -> list[StreamSchema]:
        out: list[StreamSchema] = []
        for name, meta in _HUBSPOT_STREAMS.items():
            out.append(
                StreamSchema(
                    name=name,
                    properties=dict(meta["properties"]),
                    primary_key=list(meta["primary_key"]),
                    cursor_field=str(meta.get("cursor_field") or ""),
                    json_schema={
                        "type": "object",
                        "properties": {k: {"type": v} for k, v in meta["properties"].items()},
                    },
                    supported_sync_modes=["full_refresh", "incremental"],
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
        meta = _HUBSPOT_STREAMS.get(stream)
        if meta is None:
            raise ValueError(f"Unknown HubSpot stream: {stream}. Known: {sorted(_HUBSPOT_STREAMS)}")
        access = self._token()
        if not access:
            raise ValueError("HubSpot private app token is required")

        url = f"{self._base()}{meta['path']}"
        stream_state = (state or {}).get(stream) or {}
        cursor_field = str(meta.get("cursor_field") or "")
        raw_cursor = stream_state.get("cursor")
        if raw_cursor is None and cursor_field:
            raw_cursor = stream_state.get(cursor_field)
        cursor = (
            _parse_cursor_datetime(raw_cursor, stream=stream, cursor_field=cursor_field)
            if raw_cursor not in (None, "")
            else None
        )
        raw_after = stream_state.get("after")
        if raw_after == "":
            raw_after = None
        mode = stream_state.get("mode")
        if mode not in {"list", "search"}:
            if raw_after is not None:
                mode = "list"
            elif cursor is not None:
                mode = "search"
            else:
                mode = "list"
        if cursor is None:
            mode = "list"

        raw_query_cursor = stream_state.get("_query_cursor")
        search_filter_cursor = (
            _parse_cursor_datetime(
                raw_query_cursor,
                stream=stream,
                cursor_field=cursor_field,
            )
            if mode == "search" and raw_query_cursor not in (None, "")
            else cursor
        )
        if mode == "search":
            # An offset belongs to the filter that produced it. Older checkpoints
            # lack that filter cursor, so restart from the saved watermark safely.
            search_after_value = raw_after
            if search_after_value is not None and raw_query_cursor in (None, ""):
                search_after_value = None
            search_after = _search_after(
                search_after_value
                if search_after_value is not None
                else (offset if offset else 0),
                stream=stream,
            )
            after: str | int = search_after
        else:
            after = raw_after if raw_after is not None else (str(offset) if offset else None)

        schema = StreamSchema(
            name=stream,
            properties=dict(meta["properties"]),
            primary_key=list(meta["primary_key"]),
            cursor_field=cursor_field,
        )
        properties = [
            value.strip()
            for value in str(meta["default_props"]).split(",")
            if value.strip()
        ]
        emitted = 0
        pages = 0
        current_cursor = cursor
        restart_baseline = cursor
        seen_after: set[str | int] = set()
        starting_mode = str(mode)
        try:
            while emitted < max(0, limit):
                page_limit = min(100, limit - emitted)
                if after is not None:
                    if after in seen_after:
                        raise HubSpotPaginationError(
                            f"HubSpot {stream!r} pagination repeated an after cursor"
                        )
                    seen_after.add(after)

                if mode == "search":
                    if current_cursor is None:
                        raise HubSpotCursorError(
                            f"HubSpot {stream!r} incremental search requires a cursor"
                        )
                    if search_filter_cursor is None:
                        raise HubSpotCursorError(
                            f"HubSpot {stream!r} incremental search requires a cursor"
                        )
                    body: dict[str, Any] = {
                        "filterGroups": [
                            {
                                "filters": [
                                    {
                                        "propertyName": cursor_field,
                                        "operator": "GTE",
                                        "value": str(_cursor_epoch_ms(search_filter_cursor)),
                                    }
                                ]
                            }
                        ],
                        "sorts": [
                            {"propertyName": cursor_field, "direction": "ASCENDING"}
                        ],
                        "properties": properties,
                        "limit": page_limit,
                        "after": int(after),
                    }
                    response = request(
                        method="POST",
                        url=f"{url}/search",
                        token=access,
                        data=body,
                        timeout=60,
                    )
                else:
                    params: dict[str, Any] = {
                        "limit": page_limit,
                        "properties": meta["default_props"],
                    }
                    if after is not None:
                        params["after"] = str(after)
                    response = request(
                        method="GET",
                        url=url,
                        token=access,
                        params=params,
                        timeout=60,
                    )
                response.raise_for_status()
                data = load_http_json(response)
                items = list(data.get("results") or [])[:page_limit]
                records: list[dict[str, Any]] = []
                page_cursor = current_cursor
                for item in items:
                    item_properties = item.get("properties") or {}
                    if mode == "search" and (
                        cursor_field not in item_properties
                        or item_properties.get(cursor_field) in (None, "")
                    ):
                        raise HubSpotCursorMissing(
                            f"HubSpot {stream!r} search record is missing cursor field "
                            f"{cursor_field!r}"
                        )
                    cursor_value = item_properties.get(cursor_field)
                    if cursor_field and cursor_value not in (None, ""):
                        parsed_cursor = _parse_cursor_datetime(
                            cursor_value,
                            stream=stream,
                            cursor_field=cursor_field,
                        )
                        if page_cursor is None or parsed_cursor > page_cursor:
                            page_cursor = parsed_cursor
                    record: dict[str, Any] = {"id": item.get("id", "")}
                    record.update(item_properties)
                    records.append(record)

                pages += 1
                paging = (data.get("paging") or {}).get("next") or {}
                raw_next = paging.get("after")
                if mode == "search":
                    next_after = (
                        _search_after(raw_next, stream=stream)
                        if raw_next is not None
                        else None
                    )
                else:
                    next_after = str(raw_next) if raw_next is not None else None

                if next_after is not None and not records:
                    raise HubSpotPaginationError(
                        f"HubSpot {stream!r} returned an empty page with a next after cursor"
                    )
                if next_after is not None and next_after in seen_after:
                    raise HubSpotPaginationError(
                        f"HubSpot {stream!r} pagination repeated an after cursor"
                    )

                at_search_cap = (
                    mode == "search"
                    and next_after is not None
                    and next_after >= _SEARCH_RESULT_CAP
                )
                if at_search_cap:
                    if page_cursor is None or page_cursor == restart_baseline:
                        raise HubSpotCursorStalled(
                            f"HubSpot search for {stream!r} cannot page beyond "
                            "10,000 results: >10,000 records share the cursor value "
                            f"{_cursor_iso(page_cursor)}; paging further is impossible without loss"
                        )
                    current_cursor = page_cursor
                    search_filter_cursor = page_cursor
                    restart_baseline = page_cursor
                    after = 0
                    seen_after.clear()
                    checkpoint_after = None
                    checkpoint_mode = "search"
                else:
                    current_cursor = page_cursor
                    checkpoint_after = next_after
                    checkpoint_mode = mode if next_after is not None else "search"
                    if next_after is not None:
                        after = next_after

                emitted += len(records)
                new_state = dict(state or {})
                new_state[stream] = {
                    "cursor": _cursor_iso(current_cursor),
                    "after": checkpoint_after,
                    "mode": checkpoint_mode,
                }
                if (
                    checkpoint_mode == "search"
                    and next_after is not None
                    and not at_search_cap
                ):
                    new_state[stream]["_query_cursor"] = _cursor_iso(
                        search_filter_cursor
                    )
                yield RecordBatch(
                    stream=stream,
                    records=records,
                    schema=schema,
                    state=new_state,
                )

                if next_after is None or emitted >= limit:
                    break
                if at_search_cap:
                    continue
        finally:
            logger.info(
                "HubSpot read stream=%s mode=%s pages=%d records=%d",
                stream,
                starting_mode,
                pages,
                emitted,
            )
