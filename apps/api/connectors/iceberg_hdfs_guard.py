"""HDFS / viewfs namenodes an Iceberg FileIO may dial (follow-up review 4, M).

pyiceberg 0.12's ``PyArrowFileIO._initialize_hdfs_fs`` builds
``pyarrow.fs.HadoopFileSystem.from_uri(f"{scheme}://{netloc}")`` from a
table location's netloc, or, without one, ``HadoopFileSystem(host=hdfs.host,
port=hdfs.port, user=hdfs.user, kerb_ticket=hdfs.kerberos_ticket)``. Those
locations and properties are the catalog's to write (a tenant-run REST, Glue,
Hive or SQL catalog names every metadata, manifest and data file), so they
could make the API dial any namenode, cloud metadata included, or read a
Kerberos ticket cache on the host (probe rev33/p16).

A namenode is dialled only when:

* the connector itself named it: its own warehouse location. The guarded
  catalog records those (:data:`NAMENODES`) in every FileIO's properties
  after any server's or table's properties are merged, so neither can add
  one; a FileIO built from bare properties has none;
* it passes the connector egress check (``egress_guard.new_point_refusal``):
  cloud metadata and link-local always refused, private / loopback refused
  under ``DATAFLOW_EGRESS_ENFORCE_NEW``.

``hdfs.kerberos_ticket`` must name a file in an area a connector may open
(``services.tenant_host_files``). Messages name no host and no path.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

#: FileIO property the guarded catalog writes: the namenodes the connector named, comma-separated.
NAMENODES = "dataflow.hdfs-namenodes"
SCHEMES = frozenset({"hdfs", "viewfs"})
_POINT = "iceberg_hdfs_namenode"


def _netloc(location: Any) -> str:
    try:
        parts = urlsplit(str(location or "").strip())
    except ValueError:
        return ""
    if parts.scheme.lower() not in SCHEMES:
        return ""
    return parts.netloc.lower()


def own_namenodes(own: Mapping[str, Any]) -> str:
    """The namenodes the connector's own settings name (its warehouse location)."""
    found = [_netloc(own.get("warehouse"))]
    return ",".join(sorted({name for name in found if name}))


def _message(what: str) -> str:
    return (
        f"This Iceberg table's HDFS location {what}. Datawrap dials only the HDFS namenode the "
        "connector's own warehouse names, and only one the egress policy allows (no cloud "
        "metadata, link-local, or private address unless the workspace allows it). Set the "
        "connector's warehouse to that namenode, or keep the table in object storage."
    )


def hdfs_refusal(properties: Mapping[str, Any], netloc: str | None, egress_workspace: str = "") -> str:
    """Why a FileIO with ``properties`` may not open HDFS at ``netloc``, else ``""``."""
    target = str(netloc or "").strip().lower()
    if not target:
        host = str(properties.get("hdfs.host") or "").strip()
        port = str(properties.get("hdfs.port") or "").strip()
        target = f"{host}:{port}".lower() if host and port else host.lower()
    ticket = properties.get("hdfs.kerberos_ticket")
    if ticket not in (None, ""):
        from connectors.tls_file_options import _path_refused

        if not isinstance(ticket, str) or _path_refused(ticket):
            return _message("names a Kerberos ticket file on this server")
    if not target:
        return _message("names no namenode, so the server's own Hadoop configuration would choose one")
    allowed = {name.strip().lower() for name in str(properties.get(NAMENODES) or "").split(",") if name.strip()}
    if target not in allowed:
        return _message("names a namenode this connector did not name")
    from services.egress_guard import new_point_refusal

    if new_point_refusal(_POINT, f"http://{target}", workspace_id=egress_workspace):
        return _message("names a namenode the egress policy refuses")
    return ""
