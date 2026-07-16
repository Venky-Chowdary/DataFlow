"""Email destination connector — sends query / transfer results as an attachment."""

from __future__ import annotations

import csv
import io
import json
import smtplib
import ssl
from dataclasses import dataclass, field
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from connectors.writer_common import build_mapped_rows, resolve_target_columns, row_checksum
from services.value_serializer import cell_to_string, json_default


@dataclass
class WriteResult:
    ok: bool
    rows_written: int
    table_name: str
    target_schema: str
    checksum: str
    chunks_completed: int
    error: str | None = None
    driver: str = "smtplib"
    rejected_rows: int = 0
    warnings: list[str] = field(default_factory=list)


class _EmailConfig:
    __slots__ = (
        "scheme", "host", "port", "username", "password", "from_addr",
        "to_addrs", "subject", "body", "use_tls", "format",
    )

    def __init__(self) -> None:
        self.scheme = "smtp"
        self.host = ""
        self.port = 587
        self.username = ""
        self.password = ""
        self.from_addr = ""
        self.to_addrs: list[str] = []
        self.subject = "DataFlow export"
        self.body = "Attached is the data export from DataFlow."
        self.use_tls = True
        self.format = "csv"


def _normalize_addr(value: str) -> list[str]:
    """Split a comma/semicolon separated address string into clean list."""
    if not value:
        return []
    return [a.strip() for a in value.replace(";", ",").split(",") if a.strip()]


def _parse_email_config(
    *,
    connection_string: str = "",
    host: str = "",
    port: int = 0,
    username: str = "",
    password: str = "",
    database: str = "",
    table_name: str = "",
    **_kwargs: Any,
) -> _EmailConfig | None:
    """Merge explicit fields with an smtp:// URI in connection_string."""
    cfg = _EmailConfig()
    raw = (connection_string or "").strip()

    if raw:
        # urllib can have trouble with smtp:// if there is no path; prepend a
        # placeholder netloc-free path when the URI is only a scheme+host.
        url_text = raw
        parsed = urlparse(url_text)
        if parsed.scheme in ("smtp", "smtps"):
            cfg.scheme = parsed.scheme
            cfg.host = (parsed.hostname or "").strip()
            cfg.port = parsed.port or (465 if parsed.scheme == "smtps" else 587)
            cfg.username = unquote(parsed.username or "")
            cfg.password = unquote(parsed.password or "")
            query = parse_qs(parsed.query)
            if "from" in query:
                cfg.from_addr = query["from"][-1].strip()
            if "to" in query:
                cfg.to_addrs = _normalize_addr(query["to"][-1])
            elif "to[]" in query:
                cfg.to_addrs = [a.strip() for a in query["to[]"] if a.strip()]
            if "subject" in query:
                cfg.subject = query["subject"][-1].strip()
            if "body" in query:
                cfg.body = query["body"][-1].strip()
            if "format" in query:
                cfg.format = query["format"][-1].strip().lower()
            if "tls" in query:
                cfg.use_tls = query["tls"][-1].lower() not in ("false", "0", "no", "")

    # Explicit host/port/credentials override URI pieces.
    if host:
        cfg.host = host.strip()
    if port:
        cfg.port = int(port)
    if username:
        cfg.username = username.strip()
    if password:
        cfg.password = password.strip()
    if database:
        # database field can carry comma-separated recipients if connection_string omitted
        if not cfg.to_addrs:
            cfg.to_addrs = _normalize_addr(database)

    # auth_source carries an explicit From address for user-pass connectors.
    from_override = _kwargs.get("auth_source") or ""
    if from_override:
        cfg.from_addr = from_override.strip()

    if not cfg.from_addr:
        cfg.from_addr = cfg.username or "dataflow@example.com"

    # ssl flag forces TLS/STARTTLS when True and disables it when False.
    ssl_flag = _kwargs.get("ssl")
    if ssl_flag is not None:
        cfg.use_tls = bool(ssl_flag)
    if not cfg.subject or table_name:
        cfg.subject = (cfg.subject or "DataFlow export") if not table_name else f"DataFlow export: {table_name}"

    cfg.use_tls = False if cfg.scheme == "smtp" and cfg.port == 25 else cfg.use_tls
    return cfg


def test_email(
    *,
    connection_string: str = "",
    host: str = "",
    port: int = 0,
    username: str = "",
    password: str = "",
    database: str = "",
    **_kwargs: Any,
) -> tuple[bool, str]:
    """Verify SMTP connectivity and optional authentication."""
    cfg = _parse_email_config(
        connection_string=connection_string,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
    )
    if cfg is None or not cfg.host:
        return False, "SMTP host is required. Use an smtp:// URL or the host/port fields."
    if not cfg.to_addrs and not cfg.username:
        return False, "Recipient (to) or SMTP username is required."

    try:
        if cfg.scheme == "smtps" or cfg.port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(cfg.host, cfg.port, context=context, timeout=20) as server:
                server.ehlo()
                if cfg.username and cfg.password:
                    server.login(cfg.username, cfg.password)
        else:
            with smtplib.SMTP(cfg.host, cfg.port, timeout=20) as server:
                server.ehlo()
                if cfg.use_tls:
                    context = ssl.create_default_context()
                    server.starttls(context=context)
                    server.ehlo()
                if cfg.username and cfg.password:
                    server.login(cfg.username, cfg.password)
        return True, f"SMTP server {cfg.host}:{cfg.port} reachable and authenticated."
    except smtplib.SMTPAuthenticationError as exc:
        return False, f"SMTP authentication failed: {exc}"
    except smtplib.SMTPConnectError as exc:
        return False, f"Could not connect to SMTP server: {exc}"
    except Exception as exc:
        return False, f"SMTP test failed: {exc}"


def write_mapped_rows(
    *,
    connection_string: str = "",
    host: str = "",
    port: int = 0,
    username: str = "",
    password: str = "",
    database: str = "",
    table_name: str = "",
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Any | None = None,
    **_kwargs: Any,
) -> WriteResult:
    """Send the mapped rows as an email with a CSV/JSON attachment."""
    cfg = _parse_email_config(
        connection_string=connection_string,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        table_name=table_name,
    )
    if cfg is None:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="Email destination requires an smtp:// URL or host/port credentials.",
        )

    if not cfg.host:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="SMTP host is required.",
        )
    if not cfg.to_addrs:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="At least one recipient (to) is required. Provide it via the 'to' query param or the database/recipients field.",
        )

    target_cols, logical_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    mapped_rows, transform_errors = build_mapped_rows(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types={target_cols[i]: logical_types[i] for i in range(len(target_cols))},
        preserve_case=True,
    )

    rejected_rows = len(data_rows) - len(mapped_rows)
    if transform_errors:
        # surface warnings but continue
        pass

    def _to_json_value(value: Any, col: str) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return value
            try:
                from services.type_system import normalize_logical_type
            except Exception:
                normalize_logical_type = lambda x: str(x or "").lower()
            ctype = normalize_logical_type({target_cols[i]: logical_types[i] for i in range(len(target_cols))}.get(col, ""))
            if ctype in {"json", "array", "object", "struct"}:
                try:
                    return json.loads(text, parse_float=float, parse_constant=lambda v: None)
                except json.JSONDecodeError:
                    return value
            if ctype in {"text", "string", "varchar", "uuid", "binary", "date", "datetime", "time"}:
                return value
            try:
                return json.loads(text, parse_float=float, parse_constant=lambda v: None)
            except json.JSONDecodeError:
                return value
        return value

    records = [{c: _to_json_value(v, c) for c, v in zip(target_cols, row)} for row in mapped_rows]
    fmt = cfg.format.lower()
    if fmt == "jsonl":
        body = "\n".join(json.dumps(r, default=json_default, ensure_ascii=False, allow_nan=False) for r in records).encode("utf-8")
        filename = "export.jsonl"
        mime_subtype = "jsonl"
        mime_main = "application"
    elif fmt == "json":
        body = json.dumps(records, indent=2, default=json_default, ensure_ascii=False, allow_nan=False).encode("utf-8")
        filename = "export.json"
        mime_subtype = "json"
        mime_main = "application"
    else:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=target_cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows([{c: cell_to_string(v) for c, v in r.items()} for r in records])
        body = buf.getvalue().encode("utf-8")
        filename = "export.csv"
        mime_main = "text"
        mime_subtype = "csv"

    try:
        msg = MIMEMultipart()
        msg["From"] = cfg.from_addr
        msg["To"] = ", ".join(cfg.to_addrs)
        msg["Subject"] = cfg.subject
        msg.attach(MIMEText(cfg.body, "plain"))

        part = MIMEBase(mime_main, mime_subtype)
        part.set_payload(body)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={filename}")
        msg.attach(part)

        if cfg.scheme == "smtps" or cfg.port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(cfg.host, cfg.port, context=context, timeout=30) as server:
                server.ehlo()
                if cfg.username and cfg.password:
                    server.login(cfg.username, cfg.password)
                server.sendmail(cfg.from_addr, cfg.to_addrs, msg.as_string())
        else:
            with smtplib.SMTP(cfg.host, cfg.port, timeout=30) as server:
                server.ehlo()
                if cfg.use_tls:
                    context = ssl.create_default_context()
                    server.starttls(context=context)
                    server.ehlo()
                if cfg.username and cfg.password:
                    server.login(cfg.username, cfg.password)
                server.sendmail(cfg.from_addr, cfg.to_addrs, msg.as_string())

        if on_checkpoint:
            on_checkpoint(1, 1, len(records))

        return WriteResult(
            ok=True,
            rows_written=len(records),
            table_name=filename,
            target_schema=cfg.host,
            checksum=row_checksum(mapped_rows, target_cols),
            chunks_completed=1,
            warnings=transform_errors[:10],
            rejected_rows=rejected_rows,
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=filename if "filename" in locals() else table_name,
            target_schema=cfg.host,
            checksum="",
            chunks_completed=0,
            error=f"Email send failed: {exc}",
        )
