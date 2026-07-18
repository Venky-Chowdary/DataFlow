"""HubSpot golden CDK connector — discover / check / incremental read via CRM v3."""

from __future__ import annotations

from typing import Any, Iterator

from connectors.sdk import BaseConnector, RecordBatch, StreamSchema, register_connector
from connectors.saas_common import base_url, humanize_http_error, request, token

DEFAULT_HOST = "api.hubapi.com"

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
    """Certified-path HubSpot source implemented on the DataFlow CDK."""

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
                    "api_key": {
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
        params: dict[str, Any] = {
            "limit": min(100, limit),
            "properties": meta["default_props"],
        }
        # HubSpot list API uses cursor paging via `after`; map offset as after token when stringy
        stream_state = (state or {}).get(stream) or {}
        after = stream_state.get("after") or (str(offset) if offset else None)
        if after:
            params["after"] = str(after)

        r = request(method="GET", url=url, token=access, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        records: list[dict[str, Any]] = []
        for item in data.get("results") or []:
            rec: dict[str, Any] = {"id": item.get("id", "")}
            rec.update(item.get("properties") or {})
            records.append(rec)
        records = records[:limit]

        schema = StreamSchema(
            name=stream,
            properties=dict(meta["properties"]),
            primary_key=list(meta["primary_key"]),
            cursor_field=str(meta.get("cursor_field") or ""),
        )
        paging = (data.get("paging") or {}).get("next") or {}
        new_after = paging.get("after")
        new_state = dict(state or {})
        if new_after:
            new_state[stream] = {"after": new_after}
        elif records and meta.get("cursor_field"):
            cf = meta["cursor_field"]
            last = records[-1].get(cf)
            if last is not None:
                new_state[stream] = {cf: last, "after": stream_state.get("after")}

        yield RecordBatch(stream=stream, records=records, schema=schema, state=new_state)
