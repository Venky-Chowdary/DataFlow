"""AI Connector Factory — generate adapter stubs from OpenAPI specs."""

from __future__ import annotations

import re
from typing import Any


def _slug(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip().lower())
    return re.sub(r"_+", "_", s).strip("_") or "resource"


def _infer_auth(security: list[Any] | None) -> str:
    if not security:
        return "none"
    for item in security:
        if isinstance(item, dict):
            for key in item:
                if "bearer" in key.lower():
                    return "bearer"
                if "basic" in key.lower():
                    return "basic"
                if "oauth" in key.lower() or "apikey" in key.lower():
                    return "api_key"
    return "bearer"


def generate_connector_from_openapi(spec: dict[str, Any]) -> dict[str, Any]:
    info = spec.get("info", {})
    title = info.get("title", "ExternalApi")
    version = info.get("version", "1.0.0")
    connector_id = _slug(title)
    base_url = spec.get("servers", [{}])[0].get("url", "https://api.example.com")
    auth = _infer_auth(spec.get("security"))

    endpoints: list[dict[str, str]] = []
    paths = spec.get("paths", {})
    for path, methods in paths.items():
        if not isinstance(methods, dict):
            continue
        for method, op in methods.items():
            if method.lower() not in {"get", "post", "put", "patch", "delete"}:
                continue
            if not isinstance(op, dict):
                continue
            endpoints.append(
                {
                    "method": method.upper(),
                    "path": path,
                    "operation_id": op.get("operationId", f"{method}_{_slug(path)}"),
                    "summary": op.get("summary", ""),
                }
            )

    class_name = "".join(part.capitalize() for part in connector_id.split("_")) + "Connector"
    methods_code = "\n".join(
        f"    def {e['operation_id']}(self, **params):\n"
        f'        """{e["summary"] or e["path"]}"""\n'
        f"        return self._request({e['method']!r}, {e['path']!r}, params=params)"
        for e in endpoints[:12]
    )

    plugin_code = f'''"""Auto-generated connector for {title} v{version}."""

from connectors.plugin import RestConnectorPlugin


class {class_name}(RestConnectorPlugin):
    connector_id = "{connector_id}"
    base_url = "{base_url}"
    auth_type = "{auth}"

{methods_code or "    pass"}
'''

    return {
        "connector_id": connector_id,
        "name": title,
        "version": version,
        "base_url": base_url,
        "auth_type": auth,
        "endpoint_count": len(endpoints),
        "endpoints": endpoints[:20],
        "plugin_code": plugin_code,
        "certification": {
            "status": "draft",
            "tests_required": ["connection", "schema_probe", "sample_fetch"],
            "next_step": "Run connector cert harness against sandbox credentials",
        },
    }
