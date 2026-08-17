"""Gate-8 read-back for SFTP destinations.

Kept beside the other object-store verifiers but in its own module so the SSH
trust settings (pin / known_hosts / policy) have one owner: the read-back must
reconnect under the same host key the write trusted.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

__all__ = ["verify_sftp_object"]


def verify_sftp_object(
    *,
    host: str = "",
    port: int = 22,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    database: str = "",
    table_name: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    host_key: str = "",
    known_hosts: str = "",
    host_key_policy: str = "",
) -> tuple[int, str]:
    """Gate-8 cell checksum of an SFTP GET stream. Never JSON-fallback empty.

    Same artifact walk as S3/GCS/ADLS (``checksum_artifact_stream``). Gzip CSV
    as UTF-8 JSON garbage is not dest=0. Missing file is ``(0, "")``.
    Unparseable is ``(-1, "")``. Host-key trust must match the write.
    """
    try:
        from services.dest_precount import checksum_artifact_stream
        from services.object_streaming import open_sftp_binary

        cfg = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "database": database,
            "table": table_name,
            "host_key": host_key,
            "known_hosts": known_hosts,
            "host_key_policy": host_key_policy,
        }
        opened = open_sftp_binary(cfg)
        if opened is False:
            return 0, ""
        if opened is None:
            return -1, ""
        stream, closer = opened
        try:
            return checksum_artifact_stream(
                stream,
                name=str(table_name or ""),
                columns=target_columns,
                limit=limit,
                dest_db_type="sftp",
                dest_types=dest_types,
            )
        finally:
            if closer is not None:
                try:
                    closer()
                except Exception:
                    pass
    except Exception as exc:
        logger.warning("SFTP reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""
