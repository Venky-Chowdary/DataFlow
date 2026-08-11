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
    """Independent SFTP download + parse for Gate-8 (parity with S3/GCS/ADLS).

    The read-back must run under the same host-key trust as the write;
    reconnecting without the pin would verify the destination over a transport
    the operator never trusted.
    """
    try:
        from connectors.sftp_common import connect_sftp, parse_sftp_config
        from services.reconciliation import (
            _rows_from_object_bytes,
            canonical_checksum_from_iter,
        )

        cfg = parse_sftp_config(
            connection_string=connection_string,
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            table=table_name,
            host_key=host_key,
            known_hosts=known_hosts,
            host_key_policy=host_key_policy,
        )
        if not cfg.host or not cfg.path:
            return -1, ""
        transport, sftp = connect_sftp(cfg)
        try:
            with sftp.file(cfg.path, "rb") as fh:
                body = fh.read()
        finally:
            sftp.close()
            transport.close()
        rows, headers = _rows_from_object_bytes(body, cfg.path, target_columns)
        columns = headers or target_columns or []
        return len(rows), canonical_checksum_from_iter(
            rows,
            columns,
            limit=limit,
            dest_db_type="sftp",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("SFTP reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""
