"""Where embedded databases and local warehouses may live on the API host.

Always on (every environment, no flag): SQLite/DuckDB paths are resolved
(``..`` and symlinks followed) and may not point into the service's own data
directory (job store, ledgers, secrets) except its ``sqlite/`` and ``duckdb/``
volume sub-directories; Iceberg schema, table and catalog names may not climb
out of the warehouse; an explicit ``DATAFLOW_SQLITE_ROOT`` /
``DATAFLOW_DUCKDB_ROOT`` / ``DATAFLOW_ICEBERG_ROOT`` is honoured.

``DATAFLOW_CONFINE_LOCAL_FILES`` (on by default in every environment, SEC-9;
an explicit ``0``/``false``/``off`` opts the process out, with a startup
warning and a ``local_file_confinement`` entry in the security-settings
snapshot): the *implicit* roots. With it on, SQLite is confined to
``<data_dir>/sqlite`` and DuckDB to ``<data_dir>/duckdb`` (or the SQLite root),
and relative names are anchored there; a production process also confines
Iceberg to ``<data_dir>/iceberg``.
A SQLite/DuckDB connector may also open the platform's upload area (an upload
its own workspace owns), its export area (its own export folder) and any
directory the operator lists in ``DATAFLOW_SOURCE_PATH_ROOTS``. With it off, a
path outside those folders is allowed and, in production, recorded as a
``local_file.would_confine`` audit event so the owner can see what the flag
would refuse (``scripts/audit_sqlite_paths.py`` lists the saved connectors).
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from services.brand_env import getenv_brand
from services.would_apply_audit import audit_would_apply

FLAG = "DATAFLOW_CONFINE_LOCAL_FILES"


_FLAG_ON = frozenset({"1", "true", "yes", "on"})
_FLAG_OFF = frozenset({"0", "false", "no", "off"})


def _flag_value() -> str:
    return str(getenv_brand("CONFINE_LOCAL_FILES", "") or "").strip().lower()


def confinement_opted_out() -> bool:
    """The operator set ``DATAFLOW_CONFINE_LOCAL_FILES`` to an explicit off value."""
    return _flag_value() in _FLAG_OFF


def confine_local_files() -> bool:
    """On unless explicitly opted out, in every environment (SEC-9).

    It used to be on in production only: a staging, QA or on-prem install that
    had not set ``DATAFLOW_ENV=production`` let any editor read and create
    SQLite files anywhere on the host.
    """
    return _flag_value() not in _FLAG_OFF


def local_file_settings() -> dict[str, str]:
    """The confinement entry of the startup security-settings snapshot.

    ``enforced`` (the default, every environment) or ``opted_out`` (an
    explicit off value).
    """
    return {"local_file_confinement": "opted_out" if confinement_opted_out() else "enforced"}


def warn_if_local_file_confinement_off(logger: Any) -> None:
    """Startup: say once that this process runs with the confinement opted out."""
    if local_file_settings()["local_file_confinement"] != "opted_out":
        return
    logger.warning(
        "%s is off: SQLite, DuckDB and Iceberg connectors may open any file this API "
        "process can read, in every workspace. Remove the setting to confine them to "
        "the data, upload and export folders (and DATAFLOW_SOURCE_PATH_ROOTS).",
        FLAG,
    )


def connector_file_areas() -> list[Path]:
    """Besides the data area: the folders a SQLite/DuckDB connector may open under
    the implicit production confinement (not created here).

    The upload area (only an upload the caller's workspace owns, see
    :func:`connector_may_open_upload_path`), the export area (only the caller's
    own folder, ``export_paths.connector_may_open_export_path``) and every
    directory the operator lists in ``DATAFLOW_SOURCE_PATH_ROOTS``.
    """
    from services.brand_env import getenv_brand_str
    from services.export_paths import exports_dir
    from services.platform_config import upload_dir_path

    areas = [upload_dir_path(), exports_dir(create=False)]
    areas += [Path(raw.strip()) for raw in getenv_brand_str("SOURCE_PATH_ROOTS", "").split(os.pathsep) if raw.strip()]
    return areas


def confinement_refusal(label: str, env_name: str) -> ValueError:
    """The refusal for a file outside every confined area; names the fixes, never a host path."""
    return ValueError(
        f"{label} file is outside the folders connectors may open on this server (the "
        f"persistent data volume's {label} area, uploads and this workspace's exports). "
        f"Move the file into the {label} data folder or upload it; or ask the operator to add "
        f"its directory to DATAFLOW_SOURCE_PATH_ROOTS (or set {env_name}); an operator can "
        "also turn this check off with DATAFLOW_CONFINE_LOCAL_FILES=0."
    )


_UPLOAD_NAME = re.compile(r"(?P<file_id>[0-9a-f]{16})_.+")


def connector_may_open_upload_path(resolved: Path) -> bool:
    """Whether an embedded-database path inside the upload area is an upload the caller may open.

    Uploads share one folder; the registry records each one's workspace. The
    binding is the one the data volume uses (the job's workspace, else the
    stored connector's, else the request's verified workspace): a bound
    workspace opens only its own registered uploads. With none bound, an
    unscoped upload opens, and anything else only for the single local
    operator or a platform administrator. A path outside the area is not
    judged here.
    """
    from services.embedded_volume import _bound_workspace, _single_operator
    from services.export_paths import _unbound_may_open_flat_folder
    from services.platform_config import upload_dir_path

    area = upload_dir_path().resolve(strict=False)
    if not is_within(resolved, area) or _single_operator():
        return True
    owner: str | None = None
    match = _UPLOAD_NAME.fullmatch(resolved.name)
    if resolved.parent == area and match:
        from services.file_parser import get_file

        record = get_file(match.group("file_id")) or {}
        recorded = str(record.get("path") or "")
        if recorded and Path(recorded).resolve(strict=False) == resolved:
            owner = str(record.get("workspace_id") or "")
    workspace = _bound_workspace()
    if workspace:
        return owner == workspace
    return owner == "" or _unbound_may_open_flat_folder()


def upload_refusal(label: str) -> ValueError:
    return ValueError(
        f"{label} file is in the upload folder but is not an upload of this workspace, so "
        "this connector cannot open it; upload the file in this workspace and use that upload"
    )


def volume_root(subdir: str) -> Path:
    """``<data_dir>/<subdir>``: the persistent-volume folder the flag would confine to.

    Not created; ``implicit_root`` creates it when the flag is on.
    """
    from services.platform_config import data_dir_path

    return data_dir_path() / subdir


def implicit_root(subdir: str) -> Path | None:
    """The default root while ``DATAFLOW_CONFINE_LOCAL_FILES`` is on (the default).

    SQLite and DuckDB are confined in every environment (SEC-9); Iceberg's
    local warehouse root stays a production default.
    """
    from services.platform_config import is_production

    if not confine_local_files() or (subdir == "iceberg" and not is_production()):
        return None
    root = volume_root(subdir)
    try:
        root.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    return root


def is_within(child: Path, parent: Path) -> bool:
    c = os.path.normcase(str(child))
    p = os.path.normcase(str(parent))
    try:
        return os.path.commonpath([c, p]) == p
    except ValueError:  # different drives on Windows
        return False


def audit_would_confine(subdir: str, resolved: Path) -> None:
    """Flag off in production: record a local file the flag would refuse."""
    from services.platform_config import is_production

    if confine_local_files() or not is_production():
        return
    root = volume_root(subdir).resolve(strict=False)
    if is_within(resolved, root):
        return
    audit_would_apply(
        "local_file.would_confine",
        resource=f"{subdir}:{resolved}",
        dedupe_key=(subdir, str(resolved)),
        flag=FLAG,
        details={"kind": subdir, "path": str(resolved), "would_be_root": str(root)},
    )


# --- D276: SQLite as a destination on the hosted cloud ----------------------
#
# On the hosted cloud the API, the worker and the scheduler are separate
# services, each with its own filesystem; a volume attaches to one service
# only. A job writes the SQLite file on the worker, the API lists the file on
# its own disk, and a redeploy can replace either: "Data kept at <table>" was
# true on one server and false everywhere else. SQLite also has one writer at a
# time. So the hosted cloud does not offer SQLite as a destination; a
# self-hosted single-machine install keeps it, and SQLite as a source is
# unchanged. ``DATAWRAP_SQLITE_DESTINATION=on|off`` (or ``DATAFLOW_…``) decides
# explicitly, so a self-hoster on any cloud can turn it back on.
SQLITE_DESTINATION_FLAG = "DATAWRAP_SQLITE_DESTINATION"
SQLITE_DESTINATION_UNAVAILABLE_LABEL = "Not available on cloud"
SQLITE_DRIVERS = _SQLITE_DRIVERS = frozenset({"sqlite", "sqlite3"})


def sqlite_destination_message() -> str:
    from services.brand import PRODUCT_NAME

    return (
        f"SQLite isn't available as a destination on {PRODUCT_NAME} Cloud: the file "
        "would live on a single server and isn't kept between runs. Use PostgreSQL, "
        f"MySQL or another database destination, or run {PRODUCT_NAME} self-hosted "
        f"(where {SQLITE_DESTINATION_FLAG}=on keeps it)."
    )


def sqlite_destination_allowed() -> bool:
    """May SQLite be a transfer destination in this process? Env reads only.

    1. ``DATAWRAP_SQLITE_DESTINATION`` / ``DATAFLOW_SQLITE_DESTINATION`` on/off wins;
    2. otherwise :func:`local_file_destinations_allowed` (SQLite is one of the
       local-disk destinations): ``DATAWRAP_LOCAL_FILE_DESTINATIONS`` on/off, else
       off on a deployed Railway service (deploy-time markers only — a ``railway
       run`` shell, CI or a test run is not the hosted cloud), else on. Never
       keyed off ``is_production()``: a self-hosted production install keeps SQLite.
    """
    from services.brand_env import getenv_brand_bool

    return getenv_brand_bool("SQLITE_DESTINATION", local_file_destinations_allowed())


def _is_sqlite_destination(dest_type: str, connection_string: str = "") -> bool:
    raw = (dest_type or "").strip().lower()
    if not raw:
        return False
    try:
        from src.transfer.connector_capabilities import resolve_driver_type
    except ImportError:  # loaded with apps/api/src on the path
        from transfer.connector_capabilities import resolve_driver_type  # type: ignore[no-redef]
    driver = resolve_driver_type(raw)
    if raw in _SQLITE_DRIVERS or driver in _SQLITE_DRIVERS:
        return True
    # A generic SQL connector pointed at a ``sqlite:`` URL writes the same local file.
    return driver == "generic_sql" and (connection_string or "").strip().lower().startswith("sqlite:")


def sqlite_destination_refusal(dest_type: str, connection_string: str = "") -> str | None:
    """The operator message when ``dest_type`` is SQLite and SQLite may not be a destination here."""
    if sqlite_destination_allowed() or not _is_sqlite_destination(dest_type, connection_string):
        return None
    return sqlite_destination_message()


# --- Local-disk destinations on the hosted cloud (D276, generalised) ---------
#
# Owner rule: on the hosted cloud a destination is offered only if the data does
# NOT land on the app's own disk. The reason is the D276 one: the API, the
# worker and the scheduler are separate services with separate disks, so a file
# a job writes on the worker is invisible to the API (list, sample, download)
# and is gone after a redeploy. Classified from the code that writes it:
#
# * SQLite: always a local file (``sqlite_common.sqlite_file_path``); D276.
# * DuckDB: ``sqlite_common.duckdb_unconfined_path`` — ``md:``/``motherduck:`` is
#   MotherDuck (remote); any other value is a file the process opens on its own
#   disk (``duckdb:///s3://…`` too: DuckDB opens it as a relative local path),
#   and an empty path or ``:memory:`` lives in the run's own memory.
# * Iceberg: ``iceberg_catalog._infer_catalog_type``/``_sql_props``. filesystem
#   and hadoop write a local warehouse; a ``sql`` catalog without a PostgreSQL /
#   MySQL / SQL Server URI keeps its table pointers in a local SQLite file
#   (``.dataflow_iceberg_catalog.db`` or ``sqlite:///``) even when the warehouse
#   is on S3, so another service or a redeploy loses the table; a remote catalog
#   (REST, Glue, Hive, SQL database) is local only with a local warehouse path.
# * ``file_export``: ``file_export_append.land_export_bytes`` writes under
#   ``export_paths.exports_dir()`` on the writing server. File delivery to
#   S3/MinIO/GCS/ADLS/SFTP is a ``database``-kind destination and stays. With
#   an export bucket configured (``DATAWRAP_EXPORT_BUCKET``) the export goes to
#   that private bucket instead (``services.export_object_store``) and is not
#   refused: it no longer lands on the writing server's disk.
#
# ``DATAWRAP_LOCAL_FILE_DESTINATIONS=on|off`` (or ``DATAFLOW_…``) decides
# explicitly; ``DATAWRAP_SQLITE_DESTINATION`` still decides SQLite on its own.
LOCAL_FILE_DESTINATIONS_FLAG = "DATAWRAP_LOCAL_FILE_DESTINATIONS"
DESTINATION_UNAVAILABLE_LABEL = SQLITE_DESTINATION_UNAVAILABLE_LABEL
#: Engines that are a destination with a local and a remote variant.
LOCAL_VARIANT_ENGINES = ("duckdb", "iceberg")
_FILE_EXPORT_KINDS = frozenset({"file_export", "file"})
_REMOTE_DUCKDB_PREFIXES = ("md:", "motherduck:")
#: The SQL catalog URIs ``iceberg_catalog._sql_props`` hands to pyiceberg as is;
#: any other ``sql`` catalog is the implicit local SQLite file.
_REMOTE_ICEBERG_SQL_CATALOGS = ("postgresql://", "postgres://", "mysql://", "mssql://", "sqlserver://")


def local_file_destinations_allowed() -> bool:
    """May a destination write to this server's own disk? Env reads only.

    ``DATAWRAP_LOCAL_FILE_DESTINATIONS`` on/off wins; otherwise off on a deployed
    Railway service, otherwise on (self-hosted, dev, tests, CI).
    """
    from services.brand_env import getenv_brand_bool
    from services.platform_config import is_railway_deployed

    return getenv_brand_bool("LOCAL_FILE_DESTINATIONS", not is_railway_deployed())


def _cloud_refusal(what: str, instead: str) -> str:
    from services.brand import PRODUCT_NAME

    return (
        f"{what} on {PRODUCT_NAME} Cloud: the data would be written to one server's own "
        "disk, which the other servers can't read and which isn't kept between deploys. "
        f"{instead}, or run {PRODUCT_NAME} self-hosted (where "
        f"{LOCAL_FILE_DESTINATIONS_FLAG}=on keeps it)."
    )


def local_destination_message(kind: str) -> str:
    """The operator message for a refused local-disk destination ``kind``."""
    if kind == "sqlite":
        return sqlite_destination_message()
    if kind == "duckdb":
        return _cloud_refusal(
            "A local DuckDB file isn't available as a destination",
            "Use MotherDuck (an md: database) or another database destination",
        )
    if kind == "iceberg":
        return _cloud_refusal(
            "Iceberg with a local warehouse or a local catalog file isn't available as a destination",
            "Use an S3, GCS or Azure warehouse with a REST, Glue, Hive or SQL-database "
            "(PostgreSQL, MySQL, SQL Server) catalog",
        )
    return _cloud_refusal(
        "File exports aren't available as a destination",
        "Send the rows to an S3, MinIO, Google Cloud Storage, Azure or SFTP destination instead",
    )


def _field(endpoint: Any, key: str) -> Any:
    if isinstance(endpoint, Mapping):
        return endpoint.get(key)
    return getattr(endpoint, key, None)


def _text(endpoint: Any, key: str) -> str:
    return str(_field(endpoint, key) or "").strip()


def _duckdb_is_local(endpoint: Any) -> bool:
    """Judged on what the engine opens (review round 3, H2).

    ``generic_sql._build_url_unconfined`` opens ``connection_string`` when one
    is set and ``database`` only otherwise, so ``database="local.duckdb"`` next
    to ``connection_string="duckdb:///md:db"`` writes to MotherDuck. Any field
    naming ``md:`` / ``motherduck:`` makes the destination remote.
    """
    from connectors.sqlite_common import duckdb_unconfined_path

    fields = (_text(endpoint, "connection_string"), _text(endpoint, "database"), _text(endpoint, "host"))
    for value in fields:
        try:
            named = duckdb_unconfined_path(value, "", "")
        except ValueError:  # an invalid path is refused elsewhere; it is not remote
            continue
        if named.lower().startswith(_REMOTE_DUCKDB_PREFIXES) or "md:" in value.lower():
            return False
    try:
        opened = duckdb_unconfined_path(fields[0], "", "") if fields[0] else duckdb_unconfined_path(
            fields[1], "", fields[2]
        )
    except ValueError:
        return True
    return not opened.lower().startswith(_REMOTE_DUCKDB_PREFIXES)


def _iceberg_is_local(endpoint: Any) -> bool:
    from connectors.iceberg_catalog import _infer_catalog_type, is_remote_location

    raw_extra = _field(endpoint, "extra")
    extra = dict(raw_extra) if isinstance(raw_extra, Mapping) else {}
    # resolve_connector_config flattens ``extra`` into the config it hands writers.
    for key in ("catalog_type", "catalog"):
        if key not in extra and _text(endpoint, key):
            extra[key] = _text(endpoint, key)
    cs = _text(endpoint, "connection_string")
    warehouse = _text(endpoint, "warehouse") or _text(endpoint, "database")
    catalog = _infer_catalog_type(cs, _text(endpoint, "region"), warehouse, extra)
    local_warehouse = bool(warehouse) and not is_remote_location(warehouse)
    if catalog in {"filesystem", "hadoop", "sqlite"}:
        return True
    if catalog == "sql":
        return not cs.lower().startswith(_REMOTE_ICEBERG_SQL_CATALOGS) or local_warehouse
    if catalog in {"glue", "hive"}:
        return local_warehouse
    return False  # REST: the catalog server owns storage ("warehouse" names a catalog)


def local_disk_destination(endpoint_or_type: Any, cfg: Any = None) -> str | None:
    """``"sqlite"``, ``"duckdb"``, ``"iceberg"`` or ``"file_export"`` when this
    destination lands on the writing server's own disk, else ``None``.

    ``endpoint_or_type`` is an endpoint (EndpointConfig, DTO or dict) or a bare
    type; ``cfg`` supplies the endpoint fields for a bare type. A bare type
    without fields is judged on the type alone: only SQLite is local whatever
    its path (DuckDB and Iceberg have remote variants).
    """
    if isinstance(endpoint_or_type, str):
        fields = cfg
        raw = endpoint_or_type.strip()
    else:
        fields = endpoint_or_type if cfg is None else cfg
        raw = ""
    if fields is not None:
        raw = raw or _text(fields, "type") or _text(fields, "format")
    raw = raw.lower()
    connection_string = _text(fields, "connection_string") if fields is not None else ""
    if _is_sqlite_destination(raw, connection_string):
        return "sqlite"
    if fields is None:
        return None
    if _text(fields, "kind").lower() in _FILE_EXPORT_KINDS:
        return "file_export"
    try:
        from src.transfer.connector_capabilities import resolve_driver_type
    except ImportError:  # loaded with apps/api/src on the path
        from transfer.connector_capabilities import resolve_driver_type  # type: ignore[no-redef]
    driver = resolve_driver_type(raw) if raw else ""
    if raw == "duckdb" or driver == "duckdb" or (
        driver == "generic_sql" and connection_string.lower().startswith("duckdb:")
    ):
        return "duckdb" if _duckdb_is_local(fields) else None
    if driver == "iceberg":
        return "iceberg" if _iceberg_is_local(fields) else None
    return None


def local_destination_refusal(endpoint_or_type: Any, cfg: Any = None) -> str | None:
    """Why this destination may not be used on this host, or ``None``.

    The one hosted-cloud question every destination choke point asks (engine,
    ``write_destination_database``, ``validate_transfer``, planning, REST,
    schedules). Costs two env reads when nothing is refused.
    """
    sqlite_ok = sqlite_destination_allowed()
    files_ok = local_file_destinations_allowed()
    if sqlite_ok and files_ok:
        return None
    kind = local_disk_destination(endpoint_or_type, cfg)
    if kind is None or (sqlite_ok if kind == "sqlite" else files_ok):
        return None
    if kind == "file_export":
        from services.export_object_store import export_bucket_configured

        if export_bucket_configured():  # the export goes to the private export bucket
            return None
    return local_destination_message(kind)


def _with_saved_connector(endpoint: Any, saved: Any) -> dict[str, Any]:
    """The destination fields as the engine will resolve them (inline wins, as in
    ``adapters.resolve_connector_config``; the saved type and file path win)."""
    from connectors.sql_dsn import is_masked_secret
    from services.destination_identity import resolve_saved_vs_inline

    saved_d = {k: _field(saved, k) for k in ("type", "database", "connection_string", "warehouse", "host", "region")}
    merged: dict[str, Any] = {"kind": _text(endpoint, "kind") or "database"}
    for key in ("connection_string", "warehouse", "host", "region"):
        inline = _field(endpoint, key)
        merged[key] = inline if inline and not is_masked_secret(inline) else saved_d[key]
    merged["type"] = saved_d["type"] or _text(endpoint, "format") or _text(endpoint, "type")
    merged["database"] = resolve_saved_vs_inline(
        {"database": _text(endpoint, "database")}, {k: v or "" for k, v in saved_d.items()},
        fmt=str(merged["type"] or "").lower(),
    ).database
    saved_extra, inline_extra = _field(saved, "extra"), _field(endpoint, "extra")
    merged["extra"] = {
        **(saved_extra if isinstance(saved_extra, Mapping) else {}),
        **(inline_extra if isinstance(inline_extra, Mapping) else {}),
    }
    return merged


def endpoint_local_destination_refusal(endpoint: Any, workspace_id: str | None = None) -> str | None:
    """:func:`local_destination_refusal` for a destination endpoint (DTO, dict or EndpointConfig).

    A saved ``connector_id`` is looked up (one store read) only while this host
    refuses something, and its fields are merged under the inline ones the way
    the engine resolves them, so a DuckDB/Iceberg connector is judged on its
    saved path and catalog.
    """
    if endpoint is None or (sqlite_destination_allowed() and local_file_destinations_allowed()):
        return None
    connector_id = _text(endpoint, "connector_id")
    if connector_id and _text(endpoint, "kind").lower() not in _FILE_EXPORT_KINDS:
        from services.connector_store import get_connector

        saved = get_connector(connector_id, workspace_id=workspace_id or None)
        if saved is not None:
            return local_destination_refusal(_with_saved_connector(endpoint, saved))
    return local_destination_refusal(endpoint)


def local_variant_notes(dest_ids: Any) -> list[dict[str, str]]:
    """Capabilities entries for offered engines whose local-file variant is refused here."""
    if local_file_destinations_allowed():
        return []
    try:
        from src.transfer.connector_capabilities import resolve_driver_type
    except ImportError:  # loaded with apps/api/src on the path
        from transfer.connector_capabilities import resolve_driver_type  # type: ignore[no-redef]
    notes: list[dict[str, str]] = []
    for cid in dest_ids or ():
        raw = str(cid or "").strip().lower()
        engine = raw if raw in LOCAL_VARIANT_ENGINES else resolve_driver_type(raw)
        if engine in LOCAL_VARIANT_ENGINES:
            notes.append({
                "id": str(cid),
                "engine": engine,
                "variant": "Local file",
                "reason": DESTINATION_UNAVAILABLE_LABEL,
                "message": local_destination_message(engine),
            })
    return notes
