from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any


def _pymssql_connection(reader: Any) -> Any:
    import pymssql

    cfg = dict(reader.cfg)
    extra = cfg.get("extra")
    if isinstance(extra, dict):
        cfg = {**extra, **cfg}

    required = ("host", "database", "username", "password")
    missing = [key for key in required if cfg.get(key) in (None, "")]
    if missing:
        raise ValueError(
            "SQL Server ownership probe requires connection fields: "
            + ", ".join(missing)
        )

    kwargs: dict[str, Any] = {
        "server": cfg["host"],
        "port": int(cfg.get("port") or 1433),
        "database": cfg["database"],
        "user": cfg["username"],
        "password": cfg["password"],
        "autocommit": False,
    }

    raw_timeout = cfg.get("connect_timeout")
    try:
        login_timeout = int(float(str(raw_timeout))) if raw_timeout not in (None, "") else 0
    except (TypeError, ValueError):
        login_timeout = 0
    if login_timeout > 0:
        kwargs["login_timeout"] = login_timeout

    encrypt = str(cfg.get("encrypt") or "").strip().lower()
    if encrypt in {"yes", "true", "1"}:
        kwargs["encryption"] = "require"
    elif encrypt in {"no", "false", "0"}:
        kwargs["encryption"] = "off"
    elif encrypt:
        raise ValueError(
            f"pymssql ownership probe cannot honor SQL Server encrypt={encrypt!r}"
        )

    unsupported_tls = (
        "server_certificate",
        "ssl_ca",
        "hostname_in_certificate",
        "sslrootcert",
        "sslcert",
        "sslkey",
        "ssl_verify_cert",
    )
    configured_tls = [key for key in unsupported_tls if cfg.get(key)]
    if configured_tls or (
        cfg.get("trust_server_certificate") is False
        and kwargs.get("encryption") != "off"
    ):
        options = configured_tls or ["trust_server_certificate=False"]
        raise ValueError(
            "pymssql ownership probe cannot honor SQL Server TLS options: "
            + ", ".join(options)
        )

    return pymssql.connect(**kwargs)


def _fetchone(reader: Any, statement: str, params: tuple[Any, ...] = ()) -> Any:
    conn = _pymssql_connection(reader)
    try:
        with conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchone()
    finally:
        conn.close()


def _fetchall(reader: Any, statement: str, params: tuple[Any, ...] = ()) -> list[Any]:
    conn = _pymssql_connection(reader)
    try:
        with conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall()
    finally:
        conn.close()


def sqlserver_server_now(reader: Any) -> datetime:
    row = _fetchone(reader, "SELECT SYSDATETIME()")
    if not row or not isinstance(row[0], datetime):
        raise AssertionError(f"SQL Server returned an invalid server time: {row!r}")
    return row[0]


def _capture_job_name(reader: Any) -> str:
    database = str(reader.cfg.get("database") or "master")
    return f"cdc.{database}_capture"


def _capture_job_activity(reader: Any, job_name: str) -> Any:
    return _fetchone(
        reader,
        """
        SELECT TOP (1)
            ja.session_id,
            j.name,
            ja.run_requested_date,
            ja.start_execution_date,
            ja.stop_execution_date
        FROM msdb.dbo.sysjobactivity AS ja
        INNER JOIN msdb.dbo.sysjobs AS j ON j.job_id = ja.job_id
        WHERE j.name = %s
          AND ja.session_id = (
              SELECT MAX(session_id) FROM msdb.dbo.syssessions
          )
        ORDER BY ja.session_id DESC
        """,
        (job_name,),
    )


def _exception_number(exc: Exception) -> int | None:
    args = getattr(exc, "args", ())
    if args:
        try:
            return int(args[0])
        except (TypeError, ValueError):
            pass
    match = re.search(r"\b(\d{5})\b", str(exc))
    return int(match.group(1)) if match else None


def _exception_message(exc: Exception) -> str:
    args = getattr(exc, "args", ())
    parts = [
        arg.decode("utf-8", errors="replace") if isinstance(arg, bytes) else str(arg)
        for arg in args
    ]
    return " ".join(parts) or str(exc)


def _probe_sqlserver_log_reader(
    reader: Any, probe_history: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    conn = None
    outcome: dict[str, Any] = {
        "status": "connection_error",
        "probe_spid": None,
    }
    try:
        conn = _pymssql_connection(reader)
        with conn.cursor() as cur:
            cur.execute("SELECT @@SPID")
            spid_row = cur.fetchone()
        if not spid_row:
            outcome = {"status": "probe_spid_error", "probe_spid": None}
        else:
            probe_spid = int(spid_row[0])
            outcome = {"status": "scan_error", "probe_spid": probe_spid}
            try:
                with conn.cursor() as cur:
                    cur.execute("EXEC sys.sp_cdc_scan")
            except Exception as exc:
                message = _exception_message(exc)
                outcome.update(
                    {
                        "error_type": type(exc).__name__,
                        "error": message,
                    }
                )
                if _exception_number(exc) == 22903:
                    match = re.search(
                        r"session ID\s+(\d+)", message, re.IGNORECASE
                    )
                    if not match:
                        outcome["status"] = "owner_session_unparsed"
                    else:
                        owner_session_id = int(match.group(1))
                        outcome["owner_session_id"] = owner_session_id
                        try:
                            with conn.cursor() as cur:
                                cur.execute(
                                    """
                                    SELECT session_id, program_name, status,
                                           open_transaction_count
                                    FROM sys.dm_exec_sessions
                                    WHERE session_id = %s
                                    """,
                                    (owner_session_id,),
                                )
                                owner = cur.fetchone()
                        except Exception as lookup_exc:
                            outcome.update(
                                {
                                    "status": "owner_lookup_error",
                                    "error_type": type(lookup_exc).__name__,
                                    "error": _exception_message(lookup_exc),
                                }
                            )
                        else:
                            owner_details = (
                                {
                                    "session_id": owner[0],
                                    "program_name": owner[1],
                                    "status": owner[2],
                                    "open_transaction_count": owner[3],
                                }
                                if owner
                                else None
                            )
                            is_agent = bool(
                                owner_details
                                and str(
                                    owner_details["program_name"] or ""
                                ).startswith("SQLAgent")
                            )
                            outcome.update(
                                {
                                    "status": (
                                        "agent_owner" if is_agent else "other_owner"
                                    ),
                                    "owner": owner_details,
                                    "owner_program_name": (
                                        owner_details.get("program_name")
                                        if owner_details
                                        else None
                                    ),
                                }
                            )
            else:
                try:
                    with conn.cursor() as cur:
                        cur.execute("EXEC sys.sp_replflush")
                    conn.commit()
                except Exception as exc:
                    outcome.update(
                        {
                            "status": "flush_error",
                            "error_type": type(exc).__name__,
                            "error": _exception_message(exc),
                        }
                    )
                else:
                    outcome.update(
                        {
                            "status": "unowned",
                            "detail": (
                                "sp_cdc_scan succeeded and sp_replflush committed"
                            ),
                        }
                    )
    except Exception as exc:
        outcome.update(
            {
                "status": "connection_error",
                "error_type": type(exc).__name__,
                "error": _exception_message(exc),
            }
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                outcome["close_error"] = _exception_message(exc)
                outcome["status"] = "close_error"

    outcome.setdefault("owner_session_id", None)
    outcome.setdefault("owner_program_name", None)
    if probe_history is not None:
        probe_history.append(
            {
                "probe_spid": outcome.get("probe_spid"),
                "outcome": outcome.get("status"),
                "owner_spid": outcome.get("owner_session_id"),
                "owner_program_name": outcome.get("owner_program_name"),
                "error": outcome.get("error"),
            }
        )
    return outcome


def _latest_cdc_errors(reader: Any, *, since: datetime) -> list[Any]:
    return _fetchall(
        reader,
        """
        SELECT TOP (5)
            session_id,
            entry_time,
            error_number,
            error_message
        FROM sys.dm_cdc_errors
        WHERE entry_time >= %s
        ORDER BY entry_time DESC
        """,
        (since,),
    )


def wait_for_sqlserver_capture_job_healthy(
    reader: Any, *, since: datetime, timeout: float = 75.0
) -> None:
    job_name = _capture_job_name(reader)
    deadline = time.monotonic() + timeout
    job_activity = None
    probe_outcome: dict[str, Any] = {}
    probe_history: list[dict[str, Any]] = []

    while True:
        job_activity = _capture_job_activity(reader, job_name)
        probe_outcome = _probe_sqlserver_log_reader(reader, probe_history)
        if (
            job_activity
            and job_activity[3] is not None
            and job_activity[4] is None
            and probe_outcome["status"] == "agent_owner"
        ):
            return

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.5, remaining))

    cdc_errors = _latest_cdc_errors(reader, since=since)
    raise AssertionError(
        f"SQL Server CDC capture job {job_name!r} was not healthy within "
        f"{timeout}s; job_activity={job_activity!r}; "
        f"last_probe_outcome={probe_outcome!r}; "
        f"owner_details={probe_outcome.get('owner')!r}; "
        f"probe_history={probe_history!r}; "
        f"latest_cdc_errors={cdc_errors!r}"
    )


def wait_for_sqlserver_capture_job_stopped(
    reader: Any, *, timeout: float = 15.0
) -> None:
    job_name = _capture_job_name(reader)
    since = sqlserver_server_now(reader)
    deadline = time.monotonic() + timeout
    job_activity = None
    probe_outcome: dict[str, Any] = {}
    probe_history: list[dict[str, Any]] = []

    while True:
        job_activity = _capture_job_activity(reader, job_name)
        probe_outcome = _probe_sqlserver_log_reader(reader, probe_history)
        if (
            job_activity
            and job_activity[3] is not None
            and job_activity[4] is not None
            and probe_outcome["status"] == "unowned"
        ):
            return

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.5, remaining))

    cdc_errors = _latest_cdc_errors(reader, since=since)
    raise AssertionError(
        f"SQL Server CDC capture job {job_name!r} did not stop within "
        f"{timeout}s; job_activity={job_activity!r}; "
        f"last_probe_outcome={probe_outcome!r}; "
        f"owner_details={probe_outcome.get('owner')!r}; "
        f"probe_history={probe_history!r}; "
        f"latest_cdc_errors={cdc_errors!r}"
    )
