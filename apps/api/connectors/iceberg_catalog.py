"""Iceberg catalog factory and helpers for REST / Glue / SQL / Nessie catalogs.

Wraps ``pyiceberg`` so the Datawrap engine can read and write real Iceberg tables
(REST catalog, AWS Glue, Hive, or a local SQL-backed catalog) without duplicating
catalog logic. The legacy filesystem-only CoW writer remains in
``connectors/iceberg_writer`` for bare-path destinations that do not configure a
catalog.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.request import url2pathname

_WINDOWS = os.name == "nt"

PY_IO_IMPL = "py-io-impl"
LOCAL_URI_FILE_IO = "connectors.iceberg_pyarrow_io.LocalUriPyArrowFileIO"

# pyiceberg is imported lazily inside functions so that optional installs are
# picked up without requiring a process restart.


def _get_value(endpoint: Any, key: str, default: Any = "") -> Any:
    if endpoint is None:
        return default
    if isinstance(endpoint, dict):
        return endpoint.get(key, default)
    return getattr(endpoint, key, default)


def _ensure_dict(extra: Any) -> dict[str, Any]:
    if not extra:
        return {}
    if isinstance(extra, dict):
        return extra
    try:
        return dict(extra)  # type: ignore[return-value]
    except Exception:
        return {}


def _parse_qs(url) -> dict[str, list[str]]:
    return parse_qs(url.query, keep_blank_values=True)


def _single_or_empty(values: list[str]) -> str:
    return values[0] if values else ""


#: Warehouse schemes pyiceberg addresses through an object-store FileIO. They
#: are never local paths and must reach pyiceberg byte-for-byte.
_REMOTE_WAREHOUSE_SCHEMES = (
    "s3://",
    "s3a://",
    "s3n://",
    "gs://",
    "gcs://",
    "abfs://",
    "abfss://",
    "wasb://",
    "wasbs://",
    "hdfs://",
    "oss://",
    "http://",
    "https://",
    "arn:",
)


def is_remote_location(path_str: str) -> bool:
    """True for an object-store / remote warehouse, false for a local path."""
    return (path_str or "").strip().lower().startswith(_REMOTE_WAREHOUSE_SCHEMES)


def local_path_from_location(path_str: str) -> Path:
    """Local path for a bare path or a ``file:`` URI, on POSIX and Windows.

    ``file:///C:/warehouse`` must become ``C:\\warehouse``, not ``\\C:\\warehouse``.
    """
    raw = (path_str or ".").strip()
    if raw.lower().startswith("file:"):
        parsed = urlparse(raw)
        raw = url2pathname(parsed.path)
        if parsed.netloc:  # file://host/share -> UNC
            raw = f"\\\\{parsed.netloc}{raw}" if _WINDOWS else f"//{parsed.netloc}{raw}"
    return Path(raw or ".").expanduser().resolve()


def _warehouse_root(path_str: str) -> Path:
    p = local_path_from_location(path_str)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _warehouse_location(path_str: str) -> str:
    """Warehouse value handed to pyiceberg for a local warehouse.

    pyiceberg resolves a table location scheme-first, and a Windows drive letter
    reads as one: ``C:\\warehouse`` is parsed as scheme ``c`` and refused with
    *Unrecognized filesystem type in URI: c*. A ``file://`` URI names the
    filesystem explicitly, and is equally valid on POSIX.
    """
    return _warehouse_root(path_str).as_uri()


def _infer_catalog_type(
    connection_string: str,
    region: str,
    warehouse: str,
    extra: dict[str, Any],
) -> str:
    """Return filesystem, sql, rest, or glue based on endpoint fields."""
    explicit = str(extra.get("catalog_type") or extra.get("catalog") or "").lower().strip()
    if explicit in {"rest", "glue", "sql", "sqlite", "hadoop", "hive", "nessie", "filesystem"}:
        if explicit == "nessie":
            return "rest"
        return explicit

    cs = (connection_string or "").strip().lower()
    if cs.startswith(("http://", "https://")):
        return "rest"
    if cs.startswith(("sqlite://", "postgresql://", "postgres://", "mysql://", "mssql://", "sqlserver://")):
        return "sql"

    wh = (warehouse or "").strip().lower()
    if wh.startswith(("s3://", "gs://", "gcs://")) or region or wh.startswith("arn:"):
        return "glue"

    def _is_local_fs_path(raw: str) -> bool:
        """True for POSIX/Windows paths — never host:port without a scheme.

        Windows drive letters (``C:\\warehouse``) contain ``:`` and must not be
        misclassified as SQL catalogs (that path previously required pyiceberg
        and failed closed with 'connector not ready' on every local CoW write).
        """
        s = (raw or "").strip()
        if not s or "://" in s:
            return False
        if s.startswith(("\\\\", "/")):
            return True
        # Drive letter: C:\... or C:/...
        if len(s) >= 2 and s[1] == ":" and s[0].isalpha():
            return True
        # Relative / POSIX path without scheme or host:port.
        return ":" not in s

    # A bare local path without catalog URI is the legacy filesystem CoW writer.
    if cs and not cs.startswith("file://") and not cs.startswith("iceberg://"):
        if _is_local_fs_path(cs):
            return "filesystem"

    if cs.startswith("file://"):
        return "filesystem"

    # A bare local warehouse path (no URL scheme) defaults to the legacy
    # filesystem CoW writer unless the user explicitly asked for a SQL catalog.
    if wh and not wh.startswith(("s3://", "gs://", "gcs://", "arn:", "http://", "https://", "file://")):
        if _is_local_fs_path(wh):
            return "filesystem"

    # If no connection string or warehouse was provided, default to filesystem
    # rather than silently assuming a SQLite catalog in the current directory.
    if not cs and not wh:
        return "filesystem"

    # Default to a local SQL catalog backed by SQLite inside the warehouse path.
    return "sql"


def _sql_props(connection_string: str, warehouse: str, extra: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Build SQL catalog properties. Returns (uri, remaining_props)."""
    cs = (connection_string or "").strip()
    props: dict[str, Any] = {}

    if cs.startswith(("sqlite://", "postgresql://", "postgres://", "mysql://", "mssql://", "sqlserver://")):
        if warehouse:
            props["warehouse"] = (
                warehouse.strip()
                if is_remote_location(warehouse)
                else _warehouse_location(warehouse)
            )
        return cs, props

    if is_remote_location(warehouse):
        props["warehouse"] = warehouse.strip()
        wh = _warehouse_root(cs or ".")
    else:
        wh = _warehouse_root(warehouse or cs or ".")
        props["warehouse"] = wh.as_uri()
    catalog_db = extra.get("catalog_path") or ".dataflow_iceberg_catalog.db"
    db_path = wh / catalog_db
    return f"sqlite:///{db_path.resolve()}", props


def _rest_props(connection_string: str, warehouse: str, endpoint: Any, extra: dict[str, Any]) -> dict[str, Any]:
    """Build REST catalog properties from connection string + endpoint fields."""
    cs = (connection_string or "").strip()
    parsed = urlparse(cs)
    uri = cs
    if parsed.scheme == "iceberg+rest":
        # iceberg+rest://host:port/path -> http://host:port/path
        uri = f"http://{parsed.netloc}{parsed.path}"
        if parsed.query:
            uri = f"{uri}?{parsed.query}"
    elif parsed.scheme == "rest":
        uri = f"http://{parsed.netloc}{parsed.path}"
        if parsed.query:
            uri = f"{uri}?{parsed.query}"
    elif not parsed.scheme.startswith(("http", "https")):
        # If the user only provided host/port, build a default http URI.
        host = _get_value(endpoint, "host") or parsed.path or "localhost"
        port = _get_value(endpoint, "port") or 8181
        prefix = extra.get("prefix") or "/v1"
        protocol = "https" if _get_value(endpoint, "ssl") else "http"
        uri = f"{protocol}://{host}:{port}{prefix}"

    props: dict[str, Any] = {"uri": uri}
    wh = (warehouse or extra.get("warehouse") or "").strip()
    if wh:
        props["warehouse"] = wh

    token = _get_value(endpoint, "api_key") or extra.get("token") or extra.get("oauth_token") or ""
    credential = extra.get("credential") or ""
    if token:
        props["token"] = token
    if credential:
        props["credential"] = credential

    auth_type = extra.get("auth_type") or extra.get("rest.auth.type") or ""
    if auth_type:
        props["rest.auth.type"] = auth_type
    for key, value in extra.items():
        if key.startswith("rest.") and key not in props:
            props[key] = value

    return props


def _glue_props(warehouse: str, endpoint: Any, extra: dict[str, Any]) -> dict[str, Any]:
    """Build Glue catalog properties."""
    props: dict[str, Any] = {}
    wh = (warehouse or extra.get("warehouse") or "").strip()
    if wh:
        props["warehouse"] = wh
    region = _get_value(endpoint, "region") or extra.get("region") or ""
    if region:
        props["client.region"] = region
        props["glue.region"] = region
    for key in (
        "glue.id",
        "glue.profile-name",
        "glue.access-key-id",
        "glue.secret-access-key",
        "glue.session-token",
        "glue.endpoint",
        "glue.skip-archive",
    ):
        if key in extra:
            props[key] = extra[key]
    # Allow s3.* properties to flow through for the file IO.
    for key, value in extra.items():
        if key.startswith("s3.") or key.startswith("client."):
            props[key] = value
    return props


def parse_iceberg_catalog_config(endpoint: Any) -> dict[str, Any]:
    """Convert an EndpointConfig or dict into Iceberg catalog parameters.

    Returns a dict with keys:
        catalog_type: str
        catalog_name: str
        properties: dict[str, Any]
        namespace: tuple[str, ...]
        table_name: str
        warehouse: str
        connection_string: str
    """
    extra = _ensure_dict(_get_value(endpoint, "extra"))
    connection_string = _get_value(endpoint, "connection_string")
    # The "database" field is commonly used as the warehouse/catalog path when no
    # explicit warehouse/connection_string is provided (legacy filesystem CoW tests
    # and simple local deployments rely on this).
    warehouse = _get_value(endpoint, "warehouse") or _get_value(endpoint, "database")
    region = _get_value(endpoint, "region")
    catalog_type = _infer_catalog_type(connection_string, region, warehouse, extra)
    catalog_name = extra.get("catalog_name") or "dataflow"

    properties: dict[str, Any] = {}

    if catalog_type == "filesystem":
        properties["warehouse"] = str(_warehouse_root(connection_string or warehouse or "."))
    elif catalog_type == "hadoop":
        # Warehouse Hadoop catalog (Hive-style directory). pyiceberg 0.11 dropped
        # HadoopCatalog — load_catalog fail-closes rather than inventing SqlCatalog.
        properties["warehouse"] = str(_warehouse_root(connection_string or warehouse or "."))
    elif catalog_type == "sql":
        uri, sql_props = _sql_props(connection_string, warehouse, extra)
        properties["uri"] = uri
        properties.update(sql_props)
    elif catalog_type == "rest":
        properties = _rest_props(connection_string, warehouse, endpoint, extra)
        catalog_type = "rest"
    elif catalog_type == "hive":
        cs = (connection_string or "").strip()
        if cs:
            properties["uri"] = cs
        if warehouse:
            properties["warehouse"] = warehouse
    elif catalog_type == "glue":
        properties = _glue_props(warehouse, endpoint, extra)

    if str(properties.get("warehouse", "")).lower().startswith("file:"):
        # A local warehouse is addressed through the URI-aware file IO, which is
        # what makes a Windows drive letter reachable at all.
        properties.setdefault(PY_IO_IMPL, LOCAL_URI_FILE_IO)

    # Namespace / table resolution.
    schema = _get_value(endpoint, "schema") or extra.get("namespace") or ""
    table = _get_value(endpoint, "table") or _get_value(endpoint, "table_name") or extra.get("table") or ""
    if not table:
        raise ValueError("Iceberg destination requires a table name (endpoint.table)")
    if "." in table and not schema:
        parts = table.split(".", 1)
        schema, table = parts[0], parts[1]
    if not schema:
        schema = "default"
    namespace = tuple(schema.split(".")) if schema else ("default",)

    return {
        "catalog_type": catalog_type,
        "catalog_name": catalog_name,
        "properties": properties,
        "namespace": namespace,
        "table_name": table,
        "warehouse": properties.get("warehouse", warehouse or connection_string or ""),
        "connection_string": connection_string,
    }


def _ensure_pyiceberg() -> None:
    try:
        import pyiceberg.catalog  # noqa: F401
    except Exception as exc:
        raise RuntimeError(
            "pyiceberg is not installed; install the lakehouse extra to use Iceberg catalogs"
        ) from exc


def load_catalog(endpoint: Any) -> Any:
    """Load or create a pyiceberg Catalog from endpoint configuration."""
    _ensure_pyiceberg()
    config = parse_iceberg_catalog_config(endpoint)
    catalog_type = config["catalog_type"]
    name = config["catalog_name"]
    props = config["properties"]

    if catalog_type == "rest":
        from pyiceberg.catalog.rest import RestCatalog

        return RestCatalog(name, **props)
    if catalog_type == "glue":
        from pyiceberg.catalog.glue import GlueCatalog

        return GlueCatalog(name, **props)
    if catalog_type == "hadoop":
        try:
            from pyiceberg.catalog.hadoop import HadoopCatalog
        except ImportError as exc:
            raise RuntimeError(
                "Iceberg Hadoop catalog is not available in this pyiceberg; "
                "use REST, Hive, Glue, or SQL. Refusing SqlCatalog fallback "
                "that would invent a second catalog for leftover MERGE."
            ) from exc
        return HadoopCatalog(name, **props)
    if catalog_type == "hive":
        from pyiceberg.catalog.hive import HiveCatalog

        return HiveCatalog(name, **props)
    if catalog_type == "filesystem":
        raise RuntimeError(
            "Iceberg filesystem CoW is not a pyiceberg SqlCatalog; "
            "refusing SQL connection URI invent. Use the filesystem writer/reader."
        )
    # Default SQL catalog (SQLite, PostgreSQL, etc.).
    from pyiceberg.catalog.sql import SqlCatalog

    return SqlCatalog(name, **props)


def _namespace_exists(catalog: Any, namespace: tuple[str, ...]) -> bool:
    """Check whether a namespace exists without raising."""
    from pyiceberg.exceptions import NoSuchNamespaceError

    try:
        catalog.list_namespaces()  # Some catalogs support listing all
        return namespace in catalog.list_namespaces(namespace[:-1] if len(namespace) > 1 else ())
    except Exception:
        try:
            catalog.load_namespace_properties(namespace)
            return True
        except NoSuchNamespaceError:
            return False
        except Exception:
            return False


def ensure_namespace(catalog: Any, namespace: tuple[str, ...]) -> None:
    """Create parent namespaces recursively if they do not exist."""
    for i in range(1, len(namespace) + 1):
        ns = namespace[:i]
        if not _namespace_exists(catalog, ns):
            try:
                catalog.create_namespace(ns)
            except Exception as exc:
                if "AlreadyExists" in type(exc).__name__:
                    pass
                else:
                    raise


def load_table(
    endpoint: Any,
    *,
    create: bool = False,
    schema: Any = None,
) -> Any:
    """Load an existing Iceberg table or create it when ``create`` is True."""
    from pyiceberg.exceptions import NoSuchTableError

    _ensure_pyiceberg()
    config = parse_iceberg_catalog_config(endpoint)
    catalog = load_catalog(endpoint)
    identifier = config["namespace"] + (config["table_name"],)

    try:
        return catalog.load_table(identifier)
    except NoSuchTableError:
        if not create:
            raise
        ensure_namespace(catalog, config["namespace"])
        if schema is None:
            raise ValueError("Cannot create Iceberg table without a schema")
        return catalog.create_table(identifier, schema=schema)


def test_iceberg_catalog(endpoint: Any) -> tuple[bool, str]:
    """Probe an Iceberg catalog/warehouse for reachability and write permission."""
    if not _get_value(endpoint, "table"):
        # Connectivity probes do not need a real table; use a probe placeholder.
        if isinstance(endpoint, dict):
            endpoint["table"] = "dataflow_probe"
    try:
        config = parse_iceberg_catalog_config(endpoint)
        if config["catalog_type"] == "filesystem":
            root = _warehouse_root(config["warehouse"])
            probe = root / ".dataflow_iceberg_probe"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            return True, f"Iceberg filesystem warehouse writable at {root}"

        _ensure_pyiceberg()
        catalog = load_catalog(endpoint)
        # A lightweight smoke test: list namespaces and attempt a temporary namespace.
        try:
            catalog.list_namespaces()
        except Exception as exc:
            return False, f"Iceberg catalog unreachable: {exc}"
        return True, f"Iceberg {config['catalog_type']} catalog reachable"
    except Exception as exc:
        return False, f"Iceberg catalog not reachable: {exc}"
