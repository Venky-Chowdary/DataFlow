"""DataFlow Connector SDK — extend transfer with community / Singer taps.

Honesty: only connectors registered through this SDK *and* listed in
``connector_capabilities._DRIVER_CAPS`` (or file caps) are advertised as
``transfer_ready``. Catalog stubs without a module stay ``planned``.
"""

from __future__ import annotations

import importlib
import json
import logging
import subprocess
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterator

logger = logging.getLogger(__name__)

# Runtime registry of SDK-loaded connectors (name -> cls)
_SDK_REGISTRY: dict[str, type["BaseConnector"]] = {}


@dataclass
class StreamSchema:
    name: str
    properties: dict[str, str] = field(default_factory=dict)
    primary_key: list[str] = field(default_factory=list)


@dataclass
class RecordBatch:
    stream: str
    records: list[dict[str, Any]]
    schema: StreamSchema | None = None


class BaseConnector(ABC):
    """Minimal source/destination contract for SDK connectors."""

    name: str = "base"
    supports_read: bool = True
    supports_write: bool = False

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    @abstractmethod
    def test_connection(self) -> bool:
        ...

    def discover(self) -> list[StreamSchema]:
        return []

    def read(self, stream: str, *, offset: int = 0, limit: int = 1000) -> Iterator[RecordBatch]:
        raise NotImplementedError(f"{self.name} does not implement read()")

    def write(self, stream: str, records: list[dict[str, Any]]) -> int:
        raise NotImplementedError(f"{self.name} does not implement write()")


def register_connector(cls: type[BaseConnector]) -> type[BaseConnector]:
    key = (cls.name or cls.__name__).lower()
    _SDK_REGISTRY[key] = cls
    return cls


def get_sdk_connector(name: str) -> type[BaseConnector] | None:
    return _SDK_REGISTRY.get((name or "").lower())


def list_sdk_connectors() -> list[str]:
    return sorted(_SDK_REGISTRY)


class SingerTapBridge(BaseConnector):
    """Run a Singer tap as a subprocess and yield RECORD messages.

    Config keys:
      - ``tap_command``: argv list or shell string (e.g. ``["tap-csv", "--config", "..."]``)
      - ``tap_config``: JSON object written to a temp config if needed
      - ``stream``: optional stream name filter
    """

    name = "singer_tap"
    supports_read = True
    supports_write = False

    def test_connection(self) -> bool:
        cmd = self.config.get("tap_command")
        return bool(cmd)

    def read(self, stream: str, *, offset: int = 0, limit: int = 1000) -> Iterator[RecordBatch]:
        cmd = self.config.get("tap_command")
        if not cmd:
            raise ValueError("singer_tap requires tap_command")
        if isinstance(cmd, str):
            argv = cmd.split()
        else:
            argv = list(cmd)

        proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert proc.stdout is not None
        batch: list[dict[str, Any]] = []
        schema: StreamSchema | None = None
        skipped = 0
        emitted = 0
        try:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                mtype = msg.get("type")
                if mtype == "SCHEMA" and (not stream or msg.get("stream") == stream):
                    props = {
                        k: str((v or {}).get("type", "string"))
                        for k, v in (msg.get("schema") or {}).get("properties", {}).items()
                    }
                    schema = StreamSchema(
                        name=msg.get("stream") or stream,
                        properties=props,
                        primary_key=list(msg.get("key_properties") or []),
                    )
                elif mtype == "RECORD" and (not stream or msg.get("stream") == stream):
                    if skipped < offset:
                        skipped += 1
                        continue
                    batch.append(dict(msg.get("record") or {}))
                    if len(batch) >= limit:
                        yield RecordBatch(stream=stream or msg.get("stream") or "stream", records=batch, schema=schema)
                        emitted += len(batch)
                        batch = []
                        if limit and emitted >= limit:
                            break
            if batch:
                yield RecordBatch(stream=stream or "stream", records=batch, schema=schema)
        finally:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except Exception:
                pass


register_connector(SingerTapBridge)


def test_singer_tap(**cfg: Any) -> tuple[bool, str]:
    """Connectivity probe for the Singer tap bridge.

    A Singer tap has no network endpoint of its own — "reachability" means a
    tap command was configured. Returns the (ok, message) tuple the connector
    registry probe contract expects.
    """
    tap_command = cfg.get("tap_command") or cfg.get("connection_string")
    if not tap_command:
        return False, "Singer tap requires a 'tap_command' (argv list or shell string)."
    return True, "Singer tap command configured."


def load_entrypoint(module_path: str, attr: str = "Connector") -> type[BaseConnector]:
    """Import ``module_path:attr`` and register it as an SDK connector."""
    mod = importlib.import_module(module_path)
    cls = getattr(mod, attr)
    if not issubclass(cls, BaseConnector):
        raise TypeError(f"{module_path}.{attr} must subclass BaseConnector")
    register_connector(cls)
    return cls


def sdk_read_as_matrix(
    connector_name: str,
    config: dict[str, Any],
    stream: str,
    *,
    offset: int = 0,
    limit: int = 1000,
) -> tuple[list[str], list[list[str]], dict[str, str]]:
    """Helper for adapters: read one SDK/Singer batch into (headers, rows, schema)."""
    from services.value_serializer import cell_to_string

    cls = get_sdk_connector(connector_name)
    if cls is None:
        raise ValueError(f"Unknown SDK connector: {connector_name}")
    connector = cls(config)
    headers: list[str] = []
    rows: list[list[str]] = []
    schema: dict[str, str] = {}
    for batch in connector.read(stream, offset=offset, limit=limit):
        if batch.schema and batch.schema.properties:
            schema = dict(batch.schema.properties)
            headers = list(schema.keys())
        for rec in batch.records:
            if not headers:
                headers = list(rec.keys())
            rows.append([cell_to_string(rec.get(h, "")) for h in headers])
        break
    return headers, rows, schema


# Ensure package import works when run as scripts
if __name__ == "__main__":  # pragma: no cover
    print("sdk connectors:", list_sdk_connectors(), file=sys.stderr)
