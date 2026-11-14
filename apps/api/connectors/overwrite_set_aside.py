"""Keep the previous generation of a replace-the-object overwrite recoverable.

Snowflake, BigQuery and Elasticsearch overwrites used to DROP the live table
or index before any row was judged, and an S3 overwrite replaced the live
object with whatever this run produced. A run that then quarantined
every row (or failed for any other reason) failed with the previous object
already gone. The owner's rule: no path may destroy a destination when
nothing valid would replace it.

Each engine here sets the previous generation aside before the load instead:

- Snowflake renames the live table to a backup name.
- BigQuery copies the live table to a backup with a copy job (``CREATE TABLE
  AS SELECT`` where copy jobs are unavailable), then deletes the live table.
  ``ALTER TABLE ... RENAME`` is refused while a table has a streaming buffer,
  and the BigQuery writer streams.
- Elasticsearch write-blocks the live index, clones it to a backup index,
  then deletes the live index.
- S3 copies every object of the export (the single object and any part
  objects) under a backup prefix. The writer replaces them as before.

The engine records the backup on ``destination.extra`` (``overwrite_backup`` /
``overwrite_backup_engine``). A failed run calls :func:`restore_set_aside`,
which removes the partial replacement and puts the previous generation back
under the live name. A committed run calls :func:`discard_set_aside`.

ClickHouse is served by the generic SQL driver, so its overwrite keeps a row
copy (``table_manager.retire_generic_sql_overwrite``), not a set-aside.

A backup left by an earlier run that died before settling is handled before a
new one is taken. When the live object is missing, the backup is the only
copy of the pre-run data: it becomes this run's backup, so a failure puts it
back and a commit drops it. When both exist, nothing can tell a failed
restore (the backup is the truth) from a failed discard (the live object is
the truth), so the overwrite is refused with both left in place.
"""

from __future__ import annotations

import logging
from typing import Any

from connectors.sql_identifiers import drop_table_if_exists_sql, rename_table_sql
from connectors.table_manager import TableDropError

logger = logging.getLogger(__name__)

#: Engines whose overwrite clear sets the previous generation aside.
SET_ASIDE_ENGINES = frozenset({"snowflake", "bigquery", "elasticsearch", "s3"})

_SUFFIX = "__df_bak"


def set_aside_engine(db_type: str) -> str:
    """The set-aside engine for ``db_type``, or ``""`` when it has none."""
    dt = (db_type or "").lower().strip()
    return dt if dt in SET_ASIDE_ENGINES else ""


def set_aside_backup_name(engine: str, table_name: str) -> str:
    """The backup name for ``table_name``. Deterministic, so a crash leaves it findable."""
    name = str(table_name or "t")
    if engine == "snowflake" and name.isupper():
        # An unquoted Snowflake table is stored upper case. Keep the backup in
        # the same case so it reads as the same family of object.
        return f"{name}{_SUFFIX.upper()}"
    return f"{name}{_SUFFIX}"


def _live_name_from_backup(backup: str, fallback: str) -> str:
    """The live name a backup belongs to.

    Settling reads it from the backup, not from the endpoint: a multi-stream
    run rebinds one endpoint to each stream's table and resets it afterwards,
    so the endpoint's table may name a different object by then.
    """
    for suffix in (_SUFFIX, _SUFFIX.upper(), f".{_SUFFIX}/"):
        if backup.endswith(suffix) and len(backup) > len(suffix):
            return backup[: -len(suffix)]
    return fallback


def _refuse_ambiguous(table_name: str, backup: str) -> TableDropError:
    return TableDropError(
        table_name,
        f"backup {backup} from an earlier overwrite is still present next to "
        f"{table_name}. One of them holds the pre-run data and the other a "
        "replacement; drop the one you do not need, then run again.",
    )


def retire_set_aside(
    engine: str, cfg: dict[str, Any], table_name: str, schema: str | None = None
) -> str | None:
    """Set the live object aside. Returns the backup name, or ``None`` when absent.

    Raises :class:`TableDropError` when the previous generation could not be
    kept. The overwrite then fails before anything was removed.
    """
    handler = _HANDLERS.get(engine)
    if handler is None:
        raise TableDropError(table_name, f"no set-aside overwrite for {engine!r}")
    try:
        return handler.retire(cfg, table_name, schema)
    except TableDropError:
        raise
    except Exception as exc:  # noqa: BLE001 - any failure must fail the overwrite
        raise TableDropError(table_name, exc) from exc


def restore_set_aside(
    engine: str,
    cfg: dict[str, Any],
    table_name: str,
    backup: str,
    schema: str | None = None,
) -> None:
    """Remove this run's partial replacement and put the backup back."""
    try:
        _HANDLERS[engine].restore(cfg, table_name, backup, schema)
    except TableDropError:
        raise
    except Exception as exc:  # noqa: BLE001 - surface every restore failure
        raise TableDropError(table_name, exc) from exc


def discard_set_aside(
    engine: str,
    cfg: dict[str, Any],
    table_name: str,
    backup: str,
    schema: str | None = None,
) -> None:
    """Drop the backup once the replacement committed."""
    try:
        _HANDLERS[engine].discard(cfg, table_name, backup, schema)
    except TableDropError:
        raise
    except Exception as exc:  # noqa: BLE001 - surface every discard failure
        raise TableDropError(backup, exc) from exc


# ---------------------------------------------------------------- Snowflake


def _snowflake_connection(cfg: dict[str, Any], schema: str | None) -> Any:
    from connectors.snowflake_conn import get_connection, normalize_account
    from connectors.sql_identifiers import use_warehouse_sql

    conn = get_connection(
        account=normalize_account(cfg.get("host", "")),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        database=cfg.get("database", ""),
        schema=schema or cfg.get("schema", "PUBLIC"),
        warehouse=cfg.get("warehouse", ""),
        connection_string=cfg.get("connection_string", ""),
        role=cfg.get("role", ""),
    )
    if cfg.get("warehouse"):
        try:
            with conn.cursor() as cur:
                cur.execute(use_warehouse_sql(cfg["warehouse"]))
        except Exception as exc:  # noqa: BLE001 - same as the drop path
            logger.warning("Exception suppressed: %s", exc, exc_info=exc)
    return conn


class _Snowflake:
    @staticmethod
    def _resolve(cur: Any, schema: str, name: str) -> str | None:
        from connectors.snowflake_conn import resolve_snowflake_table_name

        return resolve_snowflake_table_name(cur, schema, name)

    def retire(self, cfg: dict[str, Any], table_name: str, schema: str | None) -> str | None:
        sch = schema or cfg.get("schema", "PUBLIC")
        conn = _snowflake_connection(cfg, schema)
        try:
            with conn.cursor() as cur:
                live = self._resolve(cur, sch, table_name)
                backup = set_aside_backup_name("snowflake", live or table_name)
                stale = self._resolve(cur, sch, backup)
                if stale and live:
                    raise _refuse_ambiguous(live, stale)
                if stale or not live:
                    # A backup with no live table is the only copy of the
                    # pre-run rows: it is this run's backup.
                    return stale
                cur.execute(rename_table_sql(live, backup))
            return backup
        finally:
            _close_quietly(conn)

    def restore(
        self, cfg: dict[str, Any], table_name: str, backup: str, schema: str | None
    ) -> None:
        sch = schema or cfg.get("schema", "PUBLIC")
        conn = _snowflake_connection(cfg, schema)
        try:
            with conn.cursor() as cur:
                if not self._resolve(cur, sch, backup):
                    raise TableDropError(table_name, f"backup {backup} is missing")
                original = _live_name_from_backup(backup, table_name)
                replacement = self._resolve(cur, sch, original)
                if replacement and replacement != backup:
                    cur.execute(drop_table_if_exists_sql(replacement))
                cur.execute(rename_table_sql(backup, original))
        finally:
            _close_quietly(conn)

    def discard(
        self, cfg: dict[str, Any], table_name: str, backup: str, schema: str | None
    ) -> None:
        conn = _snowflake_connection(cfg, schema)
        try:
            with conn.cursor() as cur:
                cur.execute(drop_table_if_exists_sql(backup))
        finally:
            _close_quietly(conn)


# ----------------------------------------------------------------- BigQuery


class _BigQuery:
    @staticmethod
    def _open(cfg: dict[str, Any], schema: str | None) -> tuple[Any, str, str, dict[str, Any]]:
        from connectors.bigquery_conn import get_client
        from connectors.google_emulator import (
            google_emulator_retry,
            google_emulator_timeout,
            looks_like_google_emulator,
        )

        project = str(cfg.get("database") or cfg.get("project_id") or "").strip()
        dataset = str(schema or cfg.get("schema") or "").strip()
        if not project or not dataset:
            raise ValueError("BigQuery overwrite needs a project and a dataset")
        client = get_client(
            project_id=project,
            service_account=str(cfg.get("service_account") or ""),
            location=str(cfg.get("location") or ""),
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 0),
            connection_string=str(cfg.get("connection_string") or ""),
        )
        call: dict[str, Any] = {}
        if looks_like_google_emulator(
            endpoint=str(cfg.get("connection_string") or ""),
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 0),
        ):
            call["retry"] = google_emulator_retry()
            call["timeout"] = google_emulator_timeout()
        return client, project, dataset, call

    @staticmethod
    def _exists(client: Any, table_id: str, call: dict[str, Any]) -> bool:
        try:
            client.get_table(table_id, **call)
            return True
        except Exception as exc:  # noqa: BLE001 - NotFound, or the emulator's 500
            from services.dest_precount import _is_missing_warehouse_relation

            if type(exc).__name__ == "NotFound" or _is_missing_warehouse_relation(
                exc, "bigquery"
            ):
                return False
            raise

    def _copy(self, client: Any, src_id: str, dst_id: str, call: dict[str, Any], *, by_query: bool = False) -> None:
        """Copy ``src_id`` to ``dst_id``, which does not exist yet.

        A copy job keeps schema, partitioning and clustering; ``CREATE TABLE AS
        SELECT`` is the fallback for a server without copy jobs. A copy job is
        atomic, so a ``dst_id`` present after a failed call is a finished copy.
        """
        try:
            if by_query:
                raise RuntimeError("rows in the streaming buffer: copying with a query")
            from google.cloud.bigquery import CopyJobConfig

            job = client.copy_table(
                src_id,
                dst_id,
                job_config=CopyJobConfig(write_disposition="WRITE_EMPTY"),
                **call,
            )
            job.result()
            return
        except Exception as exc:  # noqa: BLE001 - try the SQL copy before refusing
            if self._exists(client, dst_id, call):
                return
            logger.warning(
                "BigQuery copy job %s -> %s failed (%s); copying with CREATE TABLE AS SELECT",
                src_id,
                dst_id,
                exc,
            )
        src_q = "`" + src_id.replace("`", "") + "`"
        dst_q = "`" + dst_id.replace("`", "") + "`"
        job = client.query(
            f"CREATE TABLE {dst_q} AS SELECT * FROM {src_q}",  # nosec B608 - quoted ids
            job_retry=None,
        )
        job.result()

    @staticmethod
    def _buffered(client: Any, table_id: str, call: dict[str, Any]) -> bool:
        """True when the table has rows in the streaming buffer.

        A copy job leaves those rows out (legacy streaming rows reach copy jobs
        up to 90 minutes late), and the BigQuery writer streams; deleting the
        live table after such a copy lost them (review of 53351cf30). A query
        reads the buffer, so a buffered table is copied with one.
        """
        try:
            table = client.get_table(table_id, **call)
        except Exception:  # noqa: BLE001 - unknown: let the copy decide as before
            return False
        return getattr(table, "streaming_buffer", None) is not None

    def retire(self, cfg: dict[str, Any], table_name: str, schema: str | None) -> str | None:
        client, project, dataset, call = self._open(cfg, schema)
        try:
            live_id = f"{project}.{dataset}.{table_name}"
            backup = set_aside_backup_name("bigquery", table_name)
            bak_id = f"{project}.{dataset}.{backup}"
            live = self._exists(client, live_id, call)
            stale = self._exists(client, bak_id, call)
            if stale and live:
                raise _refuse_ambiguous(table_name, backup)
            if not live:
                return backup if stale else None
            self._copy(client, live_id, bak_id, call, by_query=self._buffered(client, live_id, call))
            client.delete_table(live_id, not_found_ok=True, **call)
            return backup
        finally:
            _close_quietly(client)

    def restore(
        self, cfg: dict[str, Any], table_name: str, backup: str, schema: str | None
    ) -> None:
        client, project, dataset, call = self._open(cfg, schema)
        try:
            live_id = f"{project}.{dataset}.{_live_name_from_backup(backup, table_name)}"
            bak_id = f"{project}.{dataset}.{backup}"
            if not self._exists(client, bak_id, call):
                raise TableDropError(table_name, f"backup {backup} is missing")
            client.delete_table(live_id, not_found_ok=True, **call)
            self._copy(client, bak_id, live_id, call)
            client.delete_table(bak_id, not_found_ok=True, **call)
        finally:
            _close_quietly(client)

    def discard(
        self, cfg: dict[str, Any], table_name: str, backup: str, schema: str | None
    ) -> None:
        client, project, dataset, call = self._open(cfg, schema)
        try:
            client.delete_table(f"{project}.{dataset}.{backup}", not_found_ok=True, **call)
        finally:
            _close_quietly(client)


# ------------------------------------------------------------ Elasticsearch


class _Elasticsearch:
    _BLOCK = "index.blocks.write"

    @staticmethod
    def _client(cfg: dict[str, Any]) -> Any:
        from connectors.elasticsearch_reader import _client

        return _client(cfg)

    def _clone(self, client: Any, source: str, target: str) -> None:
        """Clone a write-blocked index, then wait until the copy can serve reads."""
        client.indices.clone(index=source, target=target)
        client.cluster.health(index=target, wait_for_status="yellow", timeout="60s")

    def _unblock(self, client: Any, index: str) -> None:
        client.indices.put_settings(index=index, settings={self._BLOCK: None})

    def _clone_away(self, client: Any, source: str, target: str) -> None:
        """Clone ``source`` to ``target`` and delete ``source``. ``target`` stays write-blocked."""
        client.indices.put_settings(index=source, settings={self._BLOCK: True})
        try:
            self._clone(client, source, target)
        except Exception:
            try:
                self._unblock(client, source)
            except Exception:  # noqa: BLE001 - report the clone failure
                logger.warning("Could not lift the write block on %s", source, exc_info=True)
            raise
        client.indices.delete(index=source)

    def retire(self, cfg: dict[str, Any], table_name: str, schema: str | None) -> str | None:
        client = self._client(cfg)
        backup = set_aside_backup_name("elasticsearch", table_name)
        live = bool(client.indices.exists(index=table_name))
        stale = bool(client.indices.exists(index=backup))
        if stale and live:
            raise _refuse_ambiguous(table_name, backup)
        if not live:
            return backup if stale else None
        self._clone_away(client, table_name, backup)
        return backup

    def restore(
        self, cfg: dict[str, Any], table_name: str, backup: str, schema: str | None
    ) -> None:
        client = self._client(cfg)
        live = _live_name_from_backup(backup, table_name)
        if not client.indices.exists(index=backup):
            raise TableDropError(live, f"backup {backup} is missing")
        client.indices.delete(index=live, ignore_unavailable=True)
        # The backup is already write-blocked, which clone requires.
        self._clone(client, backup, live)
        self._unblock(client, live)
        client.indices.delete(index=backup)

    def discard(
        self, cfg: dict[str, Any], table_name: str, backup: str, schema: str | None
    ) -> None:
        client = self._client(cfg)
        client.indices.delete(index=backup, ignore_unavailable=True)


# ----------------------------------------------------------------------- S3


class _S3:
    """Copy the export's objects aside. The writer still replaces the live keys."""

    @staticmethod
    def _client(cfg: dict[str, Any]) -> Any:
        from connectors.aws_common import boto3_client

        return boto3_client("s3", cfg)

    @staticmethod
    def _base(cfg: dict[str, Any], table_name: str, schema: str | None) -> str:
        from connectors.object_store_common import normalize_object_base_key
        from services.dialect_profiles import schema_from_cfg

        # The writer's own prefix rule, so the backup covers the key it replaces.
        prefix = schema_from_cfg("s3", cfg, schema=schema)
        return normalize_object_base_key(
            table_name, str(prefix or ""), str(cfg.get("file_format") or "")
        )

    @staticmethod
    def _bucket(cfg: dict[str, Any]) -> str:
        bucket = str(cfg.get("database") or "").strip()
        if not bucket:
            raise ValueError("S3 overwrite needs a bucket")
        return bucket

    @staticmethod
    def _list(client: Any, bucket: str, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(str(item["Key"]) for item in page.get("Contents") or [])
        return keys

    def _export_keys(self, client: Any, bucket: str, base: str) -> list[str]:
        from connectors.object_store_common import object_parts_prefix

        keys = [base] if self._present(client, bucket, base) else []
        for key in self._list(client, bucket, object_parts_prefix(base)):
            if key not in keys:
                keys.append(key)
        return keys

    @staticmethod
    def _missing(exc: BaseException, codes: set[str]) -> bool:
        code = str((getattr(exc, "response", None) or {}).get("Error", {}).get("Code", ""))
        return code in codes

    def _present(self, client: Any, bucket: str, key: str) -> bool:
        try:
            client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as exc:  # noqa: BLE001 - 404 is absent, anything else is real
            if self._missing(exc, {"404", "NoSuchKey", "NotFound"}):
                return False
            raise

    def _bucket_exists(self, client: Any, bucket: str) -> bool:
        try:
            client.head_bucket(Bucket=bucket)
            return True
        except Exception as exc:  # noqa: BLE001 - a missing bucket has nothing to keep
            if self._missing(exc, {"404", "NoSuchBucket", "NotFound"}):
                return False
            raise

    @staticmethod
    def _copy(client: Any, bucket: str, src: str, dst: str) -> None:
        # Managed copy: multipart for objects over 5 GB.
        client.copy({"Bucket": bucket, "Key": src}, bucket, dst)

    @staticmethod
    def _backup_prefix(base: str) -> str:
        return f"{base}.{_SUFFIX}/"

    def _put_back(self, client: Any, bucket: str, prefix: str) -> None:
        for bkey in self._list(client, bucket, prefix):
            self._copy(client, bucket, bkey, bkey[len(prefix):])
        for bkey in self._list(client, bucket, prefix):
            client.delete_object(Bucket=bucket, Key=bkey)

    def retire(self, cfg: dict[str, Any], table_name: str, schema: str | None) -> str | None:
        client = self._client(cfg)
        bucket = self._bucket(cfg)
        base = self._base(cfg, table_name, schema)
        if not self._bucket_exists(client, bucket):
            # The writer creates the bucket; there is no previous export.
            return None
        prefix = self._backup_prefix(base)
        live = self._export_keys(client, bucket, base)
        stale = self._list(client, bucket, prefix)
        if stale and live:
            raise _refuse_ambiguous(base, prefix)
        if not live:
            return prefix if stale else None
        for key in live:
            self._copy(client, bucket, key, prefix + key)
        return prefix

    def restore(
        self, cfg: dict[str, Any], table_name: str, backup: str, schema: str | None
    ) -> None:
        client = self._client(cfg)
        bucket = self._bucket(cfg)
        base = _live_name_from_backup(backup, "") or self._base(cfg, table_name, schema)
        if not self._list(client, bucket, backup):
            raise TableDropError(base, f"backup {backup} is missing")
        for key in self._export_keys(client, bucket, base):
            client.delete_object(Bucket=bucket, Key=key)
        self._put_back(client, bucket, backup)

    def discard(
        self, cfg: dict[str, Any], table_name: str, backup: str, schema: str | None
    ) -> None:
        client = self._client(cfg)
        bucket = self._bucket(cfg)
        for key in self._list(client, bucket, backup):
            client.delete_object(Bucket=bucket, Key=key)


def _close_quietly(handle: Any) -> None:
    try:
        handle.close()
    except Exception as exc:  # noqa: BLE001 - close is best-effort
        logger.debug("close skipped: %s", exc)


_HANDLERS: dict[str, Any] = {
    "snowflake": _Snowflake(),
    "bigquery": _BigQuery(),
    "elasticsearch": _Elasticsearch(),
    "s3": _S3(),
}
