"""Datawrap Connector CDK — Airbyte-shaped contract with Datawrap naming.

Protocol: ``spec`` / ``check`` / ``discover`` / ``read(stream, state)`` / ``write``.
Honesty: only connectors registered here *and* listed in
``connector_capabilities._DRIVER_CAPS`` (or file caps) are advertised as
``transfer_ready``. Catalog stubs without a module stay ``planned``.
"""

from __future__ import annotations

import importlib
import json
import logging
import shlex
import subprocess  # nosec: B404 — used only to run operator-configured Singer tap executables with shell=False
import sys
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterator

# Runtime registry of SDK-loaded connectors (name -> cls)
_SDK_REGISTRY: dict[str, type["BaseConnector"]] = {}
_SDK_DESCRIPTORS: dict[str, "ConnectorDescriptor"] = {}

from services.value_serializer import json_loads_exact

logger = logging.getLogger(__name__)

# Declarative manifest auth modes are separate from engine-level auth modes.
SDK_AUTH_MODES = frozenset(
    {
        "none",
        "bearer",
        "basic",
        "oauth2_refresh",
        "oauth2_client_credentials",
    }
)


@dataclass(frozen=True)
class ConnectorDescriptor:
    id: str
    display_name: str
    roles: tuple[str, ...]
    auth_modes: tuple[str, ...]
    sync_modes: tuple[str, ...]
    form_fields: tuple[Mapping[str, Any], ...]
    evidence: str
    certification_skips: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "roles", tuple(self.roles))
        object.__setattr__(self, "auth_modes", tuple(self.auth_modes))
        object.__setattr__(self, "sync_modes", tuple(self.sync_modes))
        object.__setattr__(
            self,
            "form_fields",
            tuple(MappingProxyType(dict(item)) for item in self.form_fields),
        )
        skips = dict(self.certification_skips)
        if any(not key or not str(reason).strip() for key, reason in skips.items()):
            raise ValueError("certification skip names and reasons must be non-empty")
        object.__setattr__(self, "certification_skips", MappingProxyType(skips))


def load_sdk_protocol_message(line: str) -> dict[str, Any] | None:
    """One Singer / Airbyte protocol line. Numbers match ``json_loads_exact``.

    Invalid JSON or a non-object line is skipped — never invent a record.
    """
    try:
        msg = json_loads_exact(line)
    except (json.JSONDecodeError, ValueError, TypeError):
        return None
    return msg if isinstance(msg, dict) else None


@dataclass
class StreamSchema:
    name: str
    properties: dict[str, str] = field(default_factory=dict)
    primary_key: list[str] = field(default_factory=list)
    cursor_field: str = ""
    json_schema: dict[str, Any] = field(default_factory=dict)
    supported_sync_modes: list[str] = field(default_factory=lambda: ["full_refresh"])

    def __post_init__(self) -> None:
        valid_modes = {"full_refresh", "incremental"}
        unknown_modes = [mode for mode in self.supported_sync_modes if mode not in valid_modes]
        if unknown_modes:
            raise ValueError(
                f"Stream {self.name!r} advertises unsupported sync mode(s): {unknown_modes}"
            )
        if "incremental" in self.supported_sync_modes and not self.cursor_field:
            raise ValueError(
                f"Stream {self.name!r} advertises incremental sync without a cursor_field"
            )


@dataclass
class RecordBatch:
    stream: str
    records: list[dict[str, Any]]
    schema: StreamSchema | None = None
    state: dict[str, Any] = field(default_factory=dict)


class BaseConnector(ABC):
    """Source/destination contract for CDK connectors."""

    name: str = "base"
    supports_read: bool = True
    supports_write: bool = False
    descriptor: ConnectorDescriptor | None = None

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    def spec(self) -> dict[str, Any]:
        """JSON-Schema-like connection specification (Airbyte ``spec``)."""
        return {
            "connectionSpecification": {
                "type": "object",
                "properties": {},
            }
        }

    @abstractmethod
    def test_connection(self) -> bool:
        ...

    def check(self) -> tuple[bool, str]:
        """Live auth/connectivity probe (Airbyte ``check``)."""
        try:
            ok = self.test_connection()
            return (True, "OK") if ok else (False, "Connection check failed")
        except Exception as exc:
            return False, str(exc)

    def discover(self) -> list[StreamSchema]:
        """Return available streams + schemas (Airbyte ``discover``)."""
        return []

    def read(
        self,
        stream: str,
        *,
        state: dict[str, Any] | None = None,
        offset: int = 0,
        limit: int = 1000,
    ) -> Iterator[RecordBatch]:
        """Read records; yield batches with optional incremental ``state``."""
        raise NotImplementedError(f"{self.name} does not implement read()")

    def write(self, stream: str, records: list[dict[str, Any]]) -> int:
        raise NotImplementedError(f"{self.name} does not implement write()")


def register_connector(
    cls: type[BaseConnector],
    *,
    descriptor: ConnectorDescriptor | None = None,
) -> type[BaseConnector]:
    key = (cls.name or cls.__name__).lower()
    registered_descriptor = descriptor or cls.__dict__.get("descriptor")
    if registered_descriptor is not None:
        if registered_descriptor.id.lower() != key:
            raise ValueError(
                f"Connector descriptor id {registered_descriptor.id!r} "
                f"does not match registry id {key!r}"
            )
        cls.descriptor = registered_descriptor
        _SDK_DESCRIPTORS[key] = registered_descriptor
    else:
        _SDK_DESCRIPTORS.pop(key, None)
    _SDK_REGISTRY[key] = cls
    return cls


def get_descriptor(connector_id: str) -> ConnectorDescriptor | None:
    return _SDK_DESCRIPTORS.get((connector_id or "").lower())


def list_descriptors() -> list[ConnectorDescriptor]:
    return [_SDK_DESCRIPTORS[key] for key in sorted(_SDK_DESCRIPTORS)]


def get_sdk_connector(name: str) -> type[BaseConnector] | None:
    return _SDK_REGISTRY.get((name or "").lower())


def list_sdk_connectors() -> list[str]:
    return sorted(_SDK_REGISTRY)


class SingerTapBridge(BaseConnector):
    """Run a Singer tap as a subprocess — supports discover/check/STATE.

    Config keys:
      - ``tap_command``: argv list or shell string
      - ``tap_config``: JSON object written to a temp config file
      - ``stream``: optional stream name filter
    """

    name = "singer_tap"
    supports_read = True
    supports_write = False
    descriptor = ConnectorDescriptor(
        id="singer_tap",
        display_name="Singer tap",
        roles=("source",),
        auth_modes=(),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "tap_command", "sensitive": False},
            {"name": "tap_config", "sensitive": True},
        ),
        evidence="synthetic-fixture",
        certification_skips={
            "rate_limit": "the tap owns HTTP; the bridge has no request layer"
        },
    )

    _SHELL_METACHARS = frozenset({";", "|", "&", ">", "<", "$", "`", "\\"})

    def _argv(self, *extra: str) -> list[str]:
        cmd = self.config.get("tap_command")
        if not cmd:
            raise ValueError("singer_tap requires tap_command")
        if isinstance(cmd, list):
            argv = list(cmd)
        else:
            argv = shlex.split(str(cmd))
        if any(any(c in tok for c in self._SHELL_METACHARS) for tok in argv):
            raise ValueError("tap_command contains shell metacharacters; pass an argv list or a shell-safe command")
        if not argv:
            raise ValueError("tap_command resolved to an empty argv list")
        return argv + list(extra)

    def _config_file(self) -> str | None:
        tap_config = self.config.get("tap_config")
        if not tap_config:
            return None
        tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        try:
            json.dump(tap_config, tmp)
            tmp.flush()
        except BaseException:
            tmp.close()
            Path(tmp.name).unlink(missing_ok=True)
            raise
        else:
            tmp.close()
        return tmp.name

    @staticmethod
    def _parse_streams(output: str | None) -> list[StreamSchema]:
        streams: list[StreamSchema] = []
        for line in (output or "").splitlines():
            line = line.strip()
            if not line:
                continue
            msg = load_sdk_protocol_message(line)
            if msg is None:
                continue
            if msg.get("type") == "SCHEMA":
                schema = msg.get("schema") or {}
                props = {
                    k: str((v or {}).get("type", "string"))
                    for k, v in schema.get("properties", {}).items()
                }
                streams.append(
                    StreamSchema(
                        name=msg.get("stream") or "stream",
                        properties=props,
                        primary_key=list(msg.get("key_properties") or []),
                        json_schema=dict(schema),
                    )
                )
            elif msg.get("streams"):
                for stream in msg["streams"]:
                    schema = stream.get("schema") or {}
                    props = {
                        k: str((v or {}).get("type", "string"))
                        for k, v in (schema.get("properties") or {}).items()
                    }
                    streams.append(
                        StreamSchema(
                            name=stream.get("stream") or stream.get("tap_stream_id") or "stream",
                            properties=props,
                            primary_key=list(stream.get("key_properties") or []),
                            json_schema=schema,
                        )
                    )
        return streams

    def test_connection(self) -> bool:
        ok, _ = self.check()
        return ok

    def check(self) -> tuple[bool, str]:
        cmd = self.config.get("tap_command")
        if not cmd:
            return False, "Singer tap requires tap_command"
        cfg_path = None
        try:
            cfg_path = self._config_file()
            argv = self._argv("--check")
            if cfg_path:
                argv.extend(["--config", cfg_path])
            proc = subprocess.run(argv, capture_output=True, text=True, timeout=60)  # nosec: B603 — argv is shell-safe and shell=False
            if proc.returncode == 0:
                return True, "Singer tap --check OK"
            if "unrecognized" in (proc.stderr or "").lower() or proc.returncode == 2:
                discover_argv = self._argv("--discover")
                if cfg_path:
                    discover_argv.extend(["--config", cfg_path])
                try:
                    discover_proc = subprocess.run(
                        discover_argv,
                        capture_output=True,
                        text=True,
                        timeout=120,
                    )  # nosec: B603 — argv is shell-safe and shell=False
                except subprocess.TimeoutExpired:
                    return False, (
                        "unverified: tap has no --check and --discover produced no streams "
                        "(exit timeout): "
                    )
                except OSError as exc:
                    return False, (
                        "unverified: tap has no --check and --discover produced no streams "
                        f"(exit error): {str(exc)[:300]}"
                    )
                streams = self._parse_streams(discover_proc.stdout)
                if discover_proc.returncode == 0 and streams:
                    return True, (
                        f"Singer tap --discover OK ({len(streams)} streams); "
                        "tap has no --check"
                    )
                stderr = (discover_proc.stderr or "")[:300]
                return False, (
                    "unverified: tap has no --check and --discover produced no streams "
                    f"(exit {discover_proc.returncode}): {stderr}"
                )
            return False, (proc.stderr or proc.stdout or "tap --check failed")[:500]
        except FileNotFoundError:
            return False, "Singer tap binary not found"
        except subprocess.TimeoutExpired:
            return False, "Singer tap --check timed out"
        except OSError as exc:
            return False, f"Singer tap execution failed: {exc}"
        except Exception as exc:
            return False, str(exc)
        finally:
            if cfg_path:
                Path(cfg_path).unlink(missing_ok=True)

    def discover(self) -> list[StreamSchema]:
        cfg_path = None
        try:
            cfg_path = self._config_file()
            argv = self._argv("--discover")
            if cfg_path:
                argv.extend(["--config", cfg_path])
            proc = subprocess.run(argv, capture_output=True, text=True, timeout=120)  # nosec: B603 — argv is shell-safe and shell=False
            return self._parse_streams(proc.stdout)
        except subprocess.TimeoutExpired:
            return []
        except OSError:
            return []
        except Exception:
            return []
        finally:
            if cfg_path:
                Path(cfg_path).unlink(missing_ok=True)

    def read(
        self,
        stream: str,
        *,
        state: dict[str, Any] | None = None,
        offset: int = 0,
        limit: int = 1000,
    ) -> Iterator[RecordBatch]:
        cfg_path = None
        state_path = None
        proc = None
        try:
            cfg_path = self._config_file()
            argv = self._argv()
            if cfg_path:
                argv.extend(["--config", cfg_path])
            if state:
                st = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
                state_path = st.name
                json.dump(state, st)
                st.flush()
                st.close()
                argv.extend(["--state", state_path])

            proc = subprocess.Popen(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )  # nosec: B603 — argv is shell-safe and shell=False
            if proc.stdout is None:
                raise RuntimeError("subprocess stdout is not captured")
            batch: list[dict[str, Any]] = []
            schema: StreamSchema | None = None
            skipped = 0
            emitted = 0
            out_state: dict[str, Any] = dict(state or {})
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                msg = load_sdk_protocol_message(line)
                if msg is None:
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
                        json_schema=dict(msg.get("schema") or {}),
                    )
                elif mtype == "STATE":
                    value = msg.get("value") or msg.get("state") or {}
                    if isinstance(value, dict):
                        out_state.update(value)
                elif mtype == "RECORD" and (not stream or msg.get("stream") == stream):
                    if skipped < offset:
                        skipped += 1
                        continue
                    batch.append(dict(msg.get("record") or {}))
                    if len(batch) >= limit:
                        yield RecordBatch(
                            stream=stream or msg.get("stream") or "stream",
                            records=batch,
                            schema=schema,
                            state=dict(out_state),
                        )
                        emitted += len(batch)
                        batch = []
                        if limit and emitted >= limit:
                            break
            if batch:
                yield RecordBatch(
                    stream=stream or "stream",
                    records=batch,
                    schema=schema,
                    state=dict(out_state),
                )
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError("Singer tap read timed out") from exc
        except OSError as exc:
            raise RuntimeError(f"Singer tap execution failed: {exc}") from exc
        finally:
            if proc is not None:
                try:
                    proc.kill()
                except OSError:
                    pass
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
            if cfg_path:
                Path(cfg_path).unlink(missing_ok=True)
            if state_path:
                Path(state_path).unlink(missing_ok=True)


register_connector(SingerTapBridge)


def test_singer_tap(**cfg: Any) -> tuple[bool, str]:
    """Connectivity probe for the Singer tap bridge."""
    tap_command = cfg.get("tap_command") or cfg.get("connection_string")
    if not tap_command:
        return False, "Singer tap requires a 'tap_command' (argv list or shell string)."
    bridge = SingerTapBridge({"tap_command": tap_command, "tap_config": cfg.get("tap_config")})
    return bridge.check()


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
    state: dict[str, Any] | None = None,
) -> tuple[list[str], list[list[str]], dict[str, str], dict[str, Any]]:
    """Helper for adapters: read one SDK batch into (headers, rows, schema, state)."""
    from services.value_serializer import cell_to_string

    cls = get_sdk_connector(connector_name)
    if cls is None:
        raise ValueError(f"Unknown SDK connector: {connector_name}")
    connector = cls(config)
    headers: list[str] = []
    rows: list[list[str]] = []
    schema: dict[str, str] = {}
    out_state: dict[str, Any] = dict(state or {})
    for batch in connector.read(stream, state=state, offset=offset, limit=limit):
        if batch.schema and batch.schema.properties:
            schema = dict(batch.schema.properties)
            headers = list(schema.keys())
        if batch.state:
            out_state = dict(batch.state)
        for rec in batch.records:
            if not headers:
                headers = list(rec.keys())
            rows.append([cell_to_string(rec.get(h, "")) for h in headers])
        break
    return headers, rows, schema, out_state


def _load_builtin_connectors() -> None:
    """Register declarative HTTP + HubSpot CDK golden connector."""
    try:
        from connectors.sdk import http_declarative  # noqa: F401
    except Exception as exc:
        logger.debug("declarative_http not loaded: %s", exc)
    try:
        from connectors.sdk import hubspot_cdk  # noqa: F401
    except Exception as exc:
        logger.debug("hubspot_cdk not loaded: %s", exc)


_load_builtin_connectors()


if __name__ == "__main__":  # pragma: no cover
    print("sdk connectors:", list_sdk_connectors(), file=sys.stderr)
