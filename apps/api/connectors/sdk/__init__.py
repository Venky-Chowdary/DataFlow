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
import os
import shlex
import shutil
import subprocess  # nosec: B404 — used only to run allowlisted Singer tap executables with shell=False
import sys
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterator

# Runtime registry of SDK-loaded connectors (name -> cls)
_SDK_REGISTRY: dict[str, type["BaseConnector"]] = {}

logger = logging.getLogger(__name__)


@dataclass
class StreamSchema:
    name: str
    properties: dict[str, str] = field(default_factory=dict)
    primary_key: list[str] = field(default_factory=list)
    cursor_field: str = ""
    json_schema: dict[str, Any] = field(default_factory=dict)
    supported_sync_modes: list[str] = field(
        default_factory=lambda: ["full_refresh", "incremental"]
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


def register_connector(cls: type[BaseConnector]) -> type[BaseConnector]:
    key = (cls.name or cls.__name__).lower()
    _SDK_REGISTRY[key] = cls
    return cls


def get_sdk_connector(name: str) -> type[BaseConnector] | None:
    return _SDK_REGISTRY.get((name or "").lower())


def list_sdk_connectors() -> list[str]:
    return sorted(_SDK_REGISTRY)


class SingerTapBridge(BaseConnector):
    """Run a Singer tap as a subprocess — supports discover/check/STATE.

    Config keys:
      - ``tap_command``: argv list or shell string; ``argv[0]`` must name a
        ``tap-*`` executable inside ``DATAWRAP_SINGER_TAPS_DIR``
      - ``tap_config``: JSON object written to a temp config file
      - ``stream``: optional stream name filter
    """

    name = "singer_tap"
    supports_read = True
    supports_write = False

    _SHELL_METACHARS = frozenset({";", "|", "&", ">", "<", "$", "`", "\\"})
    _TAP_EXECUTABLE_PREFIX = "tap-"

    @staticmethod
    def taps_dir() -> str | None:
        """Operator-controlled directory holding the only runnable ``tap-*`` executables."""
        from services.brand_env import getenv_brand

        raw = (getenv_brand("SINGER_TAPS_DIR") or "").strip()
        return os.path.realpath(raw) if raw else None

    @classmethod
    def _resolve_executable(cls, token: str) -> str:
        """Resolve ``token`` to an allowlisted ``tap-*`` executable inside the taps dir."""
        root = cls.taps_dir()
        if not root:
            raise PermissionError(
                "singer_tap is disabled: set DATAWRAP_SINGER_TAPS_DIR to the directory of "
                "operator-installed tap-* executables"
            )
        if not os.path.isdir(root):
            raise PermissionError("singer_tap is disabled: DATAWRAP_SINGER_TAPS_DIR is not a directory")
        token = str(token or "").strip()
        name = os.path.basename(token)
        if not name.lower().startswith(cls._TAP_EXECUTABLE_PREFIX):
            raise PermissionError(
                f"tap_command executable {name!r} is not allowed; only tap-* executables "
                "from DATAWRAP_SINGER_TAPS_DIR may run"
            )
        if os.path.dirname(token):
            candidate = os.path.abspath(os.path.join(root, token))
        else:
            found = shutil.which(name, path=os.pathsep.join([root, os.path.join(root, "bin")]))
            if not found:
                raise FileNotFoundError(f"Singer tap binary not found in DATAWRAP_SINGER_TAPS_DIR: {name}")
            candidate = os.path.abspath(found)
        if os.path.commonpath([candidate, root]) != root:
            raise PermissionError("tap_command executable must live inside DATAWRAP_SINGER_TAPS_DIR")
        if not os.path.basename(candidate).lower().startswith(cls._TAP_EXECUTABLE_PREFIX):
            raise PermissionError("tap_command executable must be a tap-* program")
        if not (os.path.isfile(candidate) and os.access(candidate, os.X_OK)):
            raise FileNotFoundError(f"Singer tap binary not found in DATAWRAP_SINGER_TAPS_DIR: {name}")
        return candidate

    def _argv(self, *extra: str) -> list[str]:
        from services.request_principal import assert_bound_principal_may_use_connector

        assert_bound_principal_may_use_connector(self.name)
        cmd = self.config.get("tap_command")
        if not cmd:
            raise ValueError("singer_tap requires tap_command")
        if isinstance(cmd, (list, tuple)):
            argv = [str(tok) for tok in cmd]
        else:
            argv = shlex.split(str(cmd))
        if not argv:
            raise ValueError("tap_command resolved to an empty argv list")
        if any(any(c in tok for c in self._SHELL_METACHARS) for tok in argv):
            raise ValueError("tap_command contains shell metacharacters; pass an argv list or a shell-safe command")
        argv[0] = self._resolve_executable(argv[0])
        return argv + list(extra)

    def _config_file(self) -> str | None:
        tap_config = self.config.get("tap_config")
        if not tap_config:
            return None
        tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        json.dump(tap_config, tmp)
        tmp.flush()
        tmp.close()
        return tmp.name

    def test_connection(self) -> bool:
        ok, _ = self.check()
        return ok

    def check(self) -> tuple[bool, str]:
        cmd = self.config.get("tap_command")
        if not cmd:
            return False, "Singer tap requires tap_command"
        cfg_path = None
        try:
            argv = self._argv("--check")
            cfg_path = self._config_file()
            if cfg_path:
                argv.extend(["--config", cfg_path])
            proc = subprocess.run(argv, capture_output=True, text=True, timeout=60)  # nosec: B603 — argv[0] is an allowlisted tap-* executable; shell=False
            if proc.returncode == 0:
                return True, "Singer tap --check OK"
            # Many taps lack --check; fall back to command presence
            if "unrecognized" in (proc.stderr or "").lower() or proc.returncode == 2:
                return True, "Singer tap command configured (no --check support)"
            return False, (proc.stderr or proc.stdout or "tap --check failed")[:500]
        except FileNotFoundError as exc:
            return False, str(exc) or "Singer tap binary not found"
        except Exception as exc:
            return False, str(exc)

    def discover(self) -> list[StreamSchema]:
        argv = self._argv("--discover")
        cfg_path = self._config_file()
        if cfg_path:
            argv.extend(["--config", cfg_path])
        try:
            proc = subprocess.run(argv, capture_output=True, text=True, timeout=120)  # nosec: B603 — argv[0] is an allowlisted tap-* executable; shell=False
        except Exception:
            return []
        streams: list[StreamSchema] = []
        for line in (proc.stdout or "").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "SCHEMA":
                props = {
                    k: str((v or {}).get("type", "string"))
                    for k, v in (msg.get("schema") or {}).get("properties", {}).items()
                }
                streams.append(
                    StreamSchema(
                        name=msg.get("stream") or "stream",
                        properties=props,
                        primary_key=list(msg.get("key_properties") or []),
                        json_schema=dict(msg.get("schema") or {}),
                    )
                )
            elif msg.get("streams"):  # catalog document
                for s in msg["streams"]:
                    schema = s.get("schema") or {}
                    props = {
                        k: str((v or {}).get("type", "string"))
                        for k, v in (schema.get("properties") or {}).items()
                    }
                    streams.append(
                        StreamSchema(
                            name=s.get("stream") or s.get("tap_stream_id") or "stream",
                            properties=props,
                            primary_key=list(s.get("key_properties") or []),
                            json_schema=schema,
                        )
                    )
        return streams

    def read(
        self,
        stream: str,
        *,
        state: dict[str, Any] | None = None,
        offset: int = 0,
        limit: int = 1000,
    ) -> Iterator[RecordBatch]:
        argv = self._argv()
        cfg_path = self._config_file()
        if cfg_path:
            argv.extend(["--config", cfg_path])
        state_path = None
        if state:
            st = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
            json.dump(state, st)
            st.flush()
            st.close()
            state_path = st.name
            argv.extend(["--state", state_path])

        proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )  # nosec: B603 — argv[0] is an allowlisted tap-* executable; shell=False
        if proc.stdout is None:
            raise RuntimeError("subprocess stdout is not captured")
        batch: list[dict[str, Any]] = []
        schema: StreamSchema | None = None
        skipped = 0
        emitted = 0
        out_state: dict[str, Any] = dict(state or {})
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
        finally:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except Exception as exc:
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)


register_connector(SingerTapBridge)


def test_singer_tap(**cfg: Any) -> tuple[bool, str]:
    """Connectivity probe for the Singer tap bridge.

    Only an explicit ``tap_command`` is accepted; ``connection_string`` is never
    interpreted as a command.
    """
    tap_command = cfg.get("tap_command")
    if not tap_command:
        return False, "Singer tap requires an explicit 'tap_command' (connection_string is not accepted)."
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
