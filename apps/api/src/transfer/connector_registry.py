"""Canonical connector module registry — probes, readers, writers.

Import this module from adapters, preflight, routers, and tests instead of
duplicating driver→module maps.
"""

from __future__ import annotations

import inspect
import os
import re
import time
from dataclasses import dataclass
from typing import Any

from connectors.sdk import list_descriptors
from services.connector_telemetry import connector_operation, driver_family
from services.log_redaction import redact_log_text

from .connector_capabilities import _DRIVER_CAPS, default_port, file_source_types, resolve_driver_type


@dataclass(frozen=True)
class ConnectorModules:
    """Import paths for a live database / object-store driver."""

    probe: tuple[str, str] | None
    reader: str | None
    reader_fn: str
    writer: str
    writer_fn: str = "write_mapped_rows"


# MongoDB probe uses adapters.probe_mongodb — no standalone test module tuple.
CONNECTOR_MODULES: dict[str, ConnectorModules] = {
    "postgresql": ConnectorModules(
        probe=("connectors.postgresql", "test_postgresql"),
        reader="connectors.postgresql_reader",
        reader_fn="read_table_batch",
        writer="connectors.postgresql_writer",
    ),
    "mysql": ConnectorModules(
        probe=("connectors.mysql", "test_mysql"),
        reader="connectors.mysql_reader",
        reader_fn="read_table_batch",
        writer="connectors.mysql_writer",
    ),
    "sqlserver": ConnectorModules(
        probe=("connectors.sqlserver", "test_sqlserver"),
        reader="connectors.sqlserver_reader",
        reader_fn="read_table_batch",
        writer="connectors.sqlserver_writer",
    ),
    "oracle": ConnectorModules(
        probe=("connectors.oracle", "test_oracle"),
        reader="connectors.oracle_reader",
        reader_fn="read_table_batch",
        writer="connectors.oracle_writer",
    ),
    "mongodb": ConnectorModules(
        probe=None,
        reader="connectors.mongodb_reader",
        reader_fn="read_collection_batch",
        writer="connectors.mongodb_writer",
    ),
    "snowflake": ConnectorModules(
        probe=("connectors.snowflake", "test_snowflake"),
        reader="connectors.snowflake_reader",
        reader_fn="read_table_batch",
        writer="connectors.snowflake_writer",
    ),
    "bigquery": ConnectorModules(
        probe=("connectors.bigquery", "test_bigquery"),
        reader="connectors.bigquery_reader",
        reader_fn="read_table_batch",
        writer="connectors.bigquery_writer",
    ),
    "redshift": ConnectorModules(
        probe=("connectors.redshift", "test_redshift"),
        reader="connectors.postgresql_reader",
        reader_fn="read_table_batch",
        writer="connectors.postgresql_writer",
    ),
    "dynamodb": ConnectorModules(
        probe=("connectors.dynamodb", "test_dynamodb"),
        reader="connectors.dynamodb_reader",
        reader_fn="read_table_batch",
        writer="connectors.dynamodb_writer",
    ),
    "s3": ConnectorModules(
        probe=("connectors.s3", "test_s3"),
        reader="connectors.s3_reader",
        reader_fn="read_object",
        writer="connectors.s3_writer",
    ),
    "gcs": ConnectorModules(
        probe=("connectors.gcs", "test_gcs"),
        reader="connectors.gcs_reader",
        reader_fn="read_object",
        writer="connectors.gcs_writer",
    ),
    "adls": ConnectorModules(
        probe=("connectors.adls", "test_adls"),
        reader="connectors.adls_reader",
        reader_fn="read_object",
        writer="connectors.adls_writer",
    ),
    "redis": ConnectorModules(
        probe=("connectors.redis_kv", "test_redis"),
        reader="connectors.redis_reader",
        reader_fn="read_keys_batch",
        writer="connectors.redis_writer",
    ),
    "elasticsearch": ConnectorModules(
        probe=("connectors.elasticsearch", "test_elasticsearch"),
        reader="connectors.elasticsearch_reader",
        reader_fn="read_index_batch",
        writer="connectors.elasticsearch_writer",
    ),
    "sqlite": ConnectorModules(
        probe=("connectors.sqlite", "test_sqlite"),
        reader="connectors.sqlite_reader",
        reader_fn="read_table_batch",
        writer="connectors.sqlite_writer",
    ),
    "sftp": ConnectorModules(
        probe=("connectors.sftp", "test_sftp"),
        reader="connectors.sftp_reader",
        reader_fn="read_object",
        writer="connectors.sftp_writer",
    ),
    "email": ConnectorModules(
        probe=("connectors.email", "test_email"),
        reader=None,
        reader_fn="",
        writer="connectors.email",
    ),
    "salesforce": ConnectorModules(
        probe=("connectors.salesforce", "test_salesforce"),
        reader="connectors.salesforce",
        reader_fn="read_object",
        writer="connectors.salesforce_writer",
        writer_fn="write_mapped_rows",
    ),
    "hubspot": ConnectorModules(
        probe=("connectors.hubspot", "test_hubspot"),
        reader="connectors.hubspot",
        reader_fn="read_object",
        writer="connectors.hubspot_writer",
        writer_fn="write_mapped_rows",
    ),
    "iceberg": ConnectorModules(
        probe=("connectors.iceberg_writer", "test_iceberg"),
        reader="connectors.iceberg_reader",
        reader_fn="read_table_batch",
        writer="connectors.iceberg_writer",
    ),
    "kafka": ConnectorModules(
        probe=("connectors.kafka_writer", "test_kafka"),
        reader="connectors.kafka_reader",
        reader_fn="read_topic_batch",
        writer="connectors.kafka_writer",
    ),
    "stripe": ConnectorModules(
        probe=("connectors.stripe", "test_stripe"),
        reader="connectors.stripe",
        reader_fn="read_object",
        writer="connectors.stripe_writer",
    ),
    "rest_api": ConnectorModules(
        probe=("connectors.rest_api", "test_connection"),
        reader="connectors.rest_api",
        reader_fn="read_object",
        writer="connectors.saas_common",
        writer_fn="write_not_supported",
    ),
    "influxdb": ConnectorModules(
        probe=("connectors.influxdb", "test_connection"),
        reader="connectors.influxdb",
        reader_fn="read_object",
        writer="connectors.saas_common",
        writer_fn="write_not_supported",
    ),
    "neo4j": ConnectorModules(
        probe=("connectors.neo4j", "test_connection"),
        reader="connectors.neo4j",
        reader_fn="read_object",
        writer="connectors.saas_common",
        writer_fn="write_not_supported",
    ),
    "couchbase": ConnectorModules(
        probe=("connectors.couchbase", "test_connection"),
        reader="connectors.couchbase",
        reader_fn="read_object",
        writer="connectors.saas_common",
        writer_fn="write_not_supported",
    ),
    "shopify": ConnectorModules(
        probe=("connectors.shopify", "test_shopify"),
        reader="connectors.shopify",
        reader_fn="read_object",
        writer="connectors.shopify_writer",
    ),
    "zendesk": ConnectorModules(
        probe=("connectors.zendesk", "test_zendesk"),
        reader="connectors.zendesk",
        reader_fn="read_object",
        writer="connectors.zendesk_writer",
        writer_fn="write_mapped_rows",
    ),
    "notion": ConnectorModules(
        probe=("connectors.notion", "test_notion"),
        reader="connectors.notion",
        reader_fn="read_object",
        writer="connectors.notion_writer",
        writer_fn="write_mapped_rows",
    ),
    "airtable": ConnectorModules(
        probe=("connectors.airtable", "test_airtable"),
        reader="connectors.airtable",
        reader_fn="read_object",
        writer="connectors.airtable_writer",
    ),
    "pgvector": ConnectorModules(
        probe=("connectors.postgresql", "test_postgresql"),
        reader="connectors.postgresql_reader",
        reader_fn="read_table_batch",
        writer="connectors.pgvector_writer",
    ),
    "qdrant": ConnectorModules(
        probe=("connectors.qdrant_writer", "test_qdrant"),
        reader="connectors.qdrant_reader",
        reader_fn="read_points_batch",
        writer="connectors.qdrant_writer",
    ),
    "weaviate": ConnectorModules(
        probe=("connectors.weaviate_writer", "test_weaviate"),
        reader=None,
        reader_fn="",
        writer="connectors.weaviate_writer",
    ),
    "pinecone": ConnectorModules(
        probe=("connectors.pinecone_writer", "test_pinecone"),
        reader=None,
        reader_fn="",
        writer="connectors.pinecone_writer",
    ),
    "milvus": ConnectorModules(
        probe=("connectors.milvus_writer", "test_milvus"),
        reader=None,
        reader_fn="",
        writer="connectors.milvus_writer",
    ),
    # Singer tap bridge (Connector SDK) — source-only extract over the Singer
    # protocol. Reader is the SDK matrix helper; writes are not supported.
    "singer_tap": ConnectorModules(
        probe=("connectors.sdk.singer_bridge", "test_singer_tap"),
        reader="connectors.sdk",
        reader_fn="sdk_read_as_matrix",
        writer="connectors.saas_common",
        writer_fn="write_not_supported",
    ),
}

for _sdk_descriptor in list_descriptors():
    if "source" not in _sdk_descriptor.roles or not _sdk_descriptor.catalog_ids:
        continue
    CONNECTOR_MODULES[_sdk_descriptor.id] = ConnectorModules(
        probe=("connectors.sdk.transfer_bridge", "test_sdk_connector"),
        reader="connectors.sdk.transfer_bridge",
        reader_fn="read_object",
        writer="connectors.saas_common",
        writer_fn="write_not_supported",
    )


def registered_driver_types() -> list[str]:
    return sorted(CONNECTOR_MODULES.keys())


def assert_registry_matches_capabilities() -> None:
    """Raise if capability manifest and module registry diverge."""
    cap_drivers = {k for k, v in _DRIVER_CAPS.items() if (v.get("read") and v.get("write")) or (v.get("dest_only") and v.get("write")) or v.get("source_only")}
    reg_drivers = set(CONNECTOR_MODULES.keys())
    missing = cap_drivers - reg_drivers
    extra = reg_drivers - cap_drivers
    if missing or extra:
        raise RuntimeError(f"Registry drift — missing={missing}, extra={extra}")


_OPERATOR_COPY_PREFIXES = (
    "authentication failed",
    "kafka sasl:",
    "api authentication failed",
    "snowflake ",
    "that account host is a form placeholder",
    "that is a snowflake account host",
    "that salesforce host is a form placeholder",
    # connector_capabilities.planned_driver_message (D345).
    "not available on this server:",
    "host not found",
    "cannot reach the host",
    "cannot reach this railway",
    "cannot reach the railway",
    "railway private hostname",
    "connection timed out",
    "tls mismatch",
    "dns lookup failed",
    "connection refused (",
    "connection refused:",
    "network unreachable (",
    "network unreachable:",
    "handshake timed out",
    "connection failed:",
    # connectors.mongodb_common.replica_set_members_unreachable (D358).
    "mongodb replica set",
    "connection failed on railway",
    "tls certificate",
    "the server refused the tls",
    "ssl/tls error",
    "the connection string format is invalid",
    "sftp authentication failed",
    "sftp host key",
    "sftp server ",
    "sftp path not found",
    "smtp authentication failed",
    "email recipient is invalid",
    "file or path not found",
    "destination or resource not found",
    "object storage connection failed",
    "dynamodb connection failed",
    "redis authentication failed",
    "elasticsearch connection failed",
    "database is locked",
    "azure authentication failed",
    "gcs authentication failed",
    "gcs bucket not found",
    "container not found",
    "bigquery connection failed",
    "this driver does not accept",
    "needs a file path",
    "path not found:",
    "uri recorded",
    "readable —",
    "catalog support is not",
)


# A server reply dumped after a matching prefix is driver text, not our copy
# (ERR-12: Mongo's "Authentication failed., full error: {...$clusterTime...}").
_DRIVER_DUMP = re.compile(r"full error:|\{'|\[SQL:|\(Background on this error|Traceback \(most recent", re.I)


def _already_operator_copy(raw: Any) -> bool:
    from services.error_taxonomy import OperatorCopy

    if isinstance(raw, OperatorCopy):
        return True
    text = str(raw or "").strip().lower()
    if not text or _DRIVER_DUMP.search(text) or _BARE_DRIVER_SENTENCE.match(text):
        return False
    return any(text.startswith(prefix) for prefix in _OPERATOR_COPY_PREFIXES)


#: A driver's whole reply that happens to start like our copy (Mongo code 18).
_BARE_DRIVER_SENTENCE = re.compile(r"^authentication failed\.?$")


def _driver_snippet(raw: str) -> str:
    """The driver's sentence, sanitised and cut on a word (ERR-13)."""
    from services.error_taxonomy import driver_sentence

    return driver_sentence(raw, limit=160)


def _with_cause(message: str, raw: str) -> str:
    """Keep the driver code visible — never replace 250001/28000 with a slogan."""
    snippet = _driver_snippet(raw)
    if not snippet or snippet.lower() in message.lower():
        return message
    if _already_operator_copy(snippet):
        return message
    return f"{message} Driver: {snippet}"


#: Whole words of a refused login (ERR-14): never "auth" inside a database,
#: file or table name such as ``auth``, ``/srv/auth/app.db`` or ``authors``.
_AUTH_WORDS = (
    r"\bauthentication\b|\bauth(?:entication)? (?:failed|error)|\blogin failed|\bcredentials?\b|"
    r"\bpassword\b|\bincorrect (?:username|user|password|credentials)|access denied|not authorized|"
    r"\bunauthori[sz]ed\b|no such user|permission denied|\binvalid (?:token|api key|key)\b"
)

#: Drivers whose failures keep their own copy (account forms, scopes, paths).
_BESPOKE_COPY_DRIVERS = frozenset({
    "snowflake", "sftp", "email", "salesforce", "hubspot", "stripe", "rest_api", "bigquery",
    "s3", "gcs", "adls", "dynamodb", "redis", "elasticsearch", "kafka",
})
#: TLS copy below names which end requires TLS; the taxonomy's does not.
_BESPOKE_COPY_CODES = frozenset({"network.tls_required", "network.tls_untrusted"})


def humanize_connection_error(
    driver: str, raw: Any, auth_mode: str = "", *, side: str = "connection", phase: str = "connect"
) -> str:
    """Convert low-level driver/connection errors into user-friendly messages.

    The shared taxonomy (``services.error_taxonomy.classify``) decides first:
    the same cause reads the same on Test, Query, introspect, Validate, a job
    and Pilot. Driver-specific copy below covers account forms and services the
    taxonomy does not name.

    ``auth_mode`` is the credential actually attempted (pat/user_pass/key_pair)
    so re-classification of raw driver text can name the right credential —
    a refused PAT is not a key-pair failure."""
    from services.error_taxonomy import OperatorCopy, classify

    from services.error_taxonomy import PRODUCT_COPY_CODES

    if isinstance(raw, OperatorCopy):
        return raw
    raw_s = redact_log_text(str(raw)).strip()
    text = raw_s.lower()
    driver = (driver or "").lower()
    engine = resolve_driver_type(driver) if driver else ""
    if _already_operator_copy(raw_s):
        # Datawrap's own copy (e.g. the replica-set member report, D358).
        return raw_s
    if text.startswith("refused connector host"):
        # The egress policy's own refusal names the host the user typed; only
        # a name that did not resolve is reworded (ERR-18).
        refused = classify(raw_s, side=side, phase=phase, fallback=False)
        if refused is not None and refused.code == "network.dns_not_found":
            return OperatorCopy(refused.text)
        return OperatorCopy(" ".join(raw_s.split()))
    if ".railway.internal" not in text and engine not in _BESPOKE_COPY_DRIVERS and driver not in _BESPOKE_COPY_DRIVERS:
        classified = classify(raw, side=side, phase=phase, engine=engine or driver, fallback=False)
        if classified is not None and classified.code in PRODUCT_COPY_CODES:
            return OperatorCopy(" ".join(raw_s.split()))
        if classified is not None and classified.code not in _BESPOKE_COPY_CODES:
            copy = OperatorCopy(classified.text)
            # Kept off the copy; a Test-only refinement (D353) may cite it.
            copy.sentence = classified.sentence  # type: ignore[attr-defined]
            copy.code = classified.code  # type: ignore[attr-defined]
            return copy

    # Railway private DNS — guidance depends on whether THIS API is on Railway.
    if ".railway.internal" in text:
        try:
            from connectors.sql_dsn import (
                is_running_on_railway,
                private_cloud_host_hint,
            )

            on_railway = is_running_on_railway()
        except Exception:  # noqa: BLE001
            on_railway = False
        if on_railway:
            # Keep the real cause when present; append railway-internal guidance.
            base = raw_s
            # Strip our previously-injected hint if nested
            hint = private_cloud_host_hint("x.railway.internal", "")
            if hint and hint.strip() not in base:
                # Prefer a concise primary message over dumping raw driver noise
                if re.search(
                    r"name or service not known|nodename|getaddrinfo|cannot resolve|not known|"
                    r"could not translate host name|temporary failure in name resolution|unknown host",
                    text,
                ):
                    return (
                        "Railway private hostname did not resolve from this API service. "
                        "Confirm the database service is in the same Railway project and the "
                        "host matches its private domain (e.g. mysql.railway.internal). "
                        "Use private port 3306 (MySQL) or 5432 (Postgres)."
                    )
                if re.search(r"timed out|timeout|connection refused|unreachable", text):
                    return (
                        "Cannot reach the Railway private database host from this API service. "
                        "Check the service is running, use private port 3306/5432, and that both "
                        "services share the same Railway project/network."
                    )
                return (
                    "Connection failed on Railway private network: "
                    f"{_driver_snippet(raw_s.split(hint)[0])}"
                )
            return (
                "Cannot reach the Railway private database host from this API service. "
                "Check project linking, private hostname, and private port (3306/5432)."
            )
        return (
            "Cannot reach this Railway private hostname from here. "
            "In Connectors, use the *public* proxy host (*.proxy.rlwy.net) and the "
            "*public* TCP port from Railway — not mysql.railway.internal / postgres.railway.internal "
            "and not ports 3306/5432."
        )

    # Which side requires TLS (QA r12 bug 8): the generic "SSL/TLS error" below
    # did not say whether to turn SSL on or off.
    from .network_error_copy import is_login_timeout, network_failure_copy, tls_requirement_copy

    tls_required = tls_requirement_copy(text, raw_s)
    if tls_required:
        return _with_cause(tls_required, raw_s)

    # TLS mismatch — must be classified before auth and timeout. A server that
    # does not speak TLS aborts the handshake, and the driver surfaces that as
    # "EOF occurred in violation of protocol" plus a socket timeout. Reporting
    # it as "connection timed out" sent operators to check firewalls when the
    # actual fix is one checkbox on the connector.
    if re.search(
        r"ssl handshake failed|tls handshake|violation of protocol|wrong version number|"
        r"sslv3|record layer failure|unsupported protocol|certificate verify failed|"
        r"ssl: |sslerror",
        text,
    ):
        if re.search(r"certificate verify failed|self.signed|hostname mismatch|CERTIFICATE_VERIFY", text, re.I):
            if driver in {"mssql", "sqlserver", "sql_server", "azure_sql", "sql_server_mssql"}:
                # X-49: the raw pyodbc 08001/SSL Provider text must never reach
                # the user — name the exact connector option that fixes it.
                return (
                    "SQL Server's certificate isn't trusted (self-signed). "
                    "Turn on 'Trust server certificate' for this connector or "
                    "install a trusted certificate on the server."
                )
            return (
                "TLS certificate could not be verified. Check the server certificate, "
                "its hostname, and the CA chain — do not disable SSL to work around a "
                "certificate you cannot verify."
            )
        return (
            "The server refused the TLS handshake — it is most likely not configured "
            "for SSL. Turn off SSL on this connector if the server is plaintext "
            "(common for local/dev databases), or enable TLS on the server."
        )

    # Snowflake: classify role / MFA / network / positional URL before the generic "login" regex.
    # Role 'BOTH' errors contain "attempt to login" and were mislabeled as a bad password.
    if driver == "snowflake":
        from connectors.snowflake_conn import classify_snowflake_connect_error

        classified = classify_snowflake_connect_error(raw_s, auth_mode=auth_mode)
        if classified:
            return classified

    # Keyword-only drivers raise TypeError when a raw URL is passed positionally.
    if re.search(r"takes 0 positional arguments|__init__\(\) takes 0 positional", text):
        return (
            "This driver does not accept a raw URL as a positional argument. "
            "Use Username & password, or a login URL with an encoded password "
            "(use %40 for @)."
        )

    # SFTP host-key and preauth failures contain "key" / "authentication".
    # The generic auth rewrite below would tell the operator to rotate a
    # password that was never sent (DEF-A-001). The probe text already names
    # the fingerprint or the preauth close — return it intact: truncating to a
    # driver snippet cuts the SHA256 pin the operator must save (D-BP SFTP).
    if driver == "sftp" and re.search(
        r"host key|not trusted|fingerprint|known_hosts|preauth|before authentication",
        text,
    ):
        return " ".join(raw_s.split())

    # A wrong SFTP folder used to become "Authentication failed".
    # "SFTP authenticated but path not found" contains the substring "auth",
    # and a missing directory is [Errno 2] / "No such file", not a bad password.
    if driver == "sftp" and re.search(
        r"no such file|errno 2|path not found|file not found|not a directory|"
        r"authenticated but|no such folder|folder not found",
        text,
    ):
        return (
            "SFTP path not found. The login succeeded; the remote folder or "
            "file does not exist. Check the remote directory."
        )

    # SQL Server's "Login timeout expired" (HYT00) is the connect timeout; the
    # auth rule below read its "login" as a refused password (D353).
    if is_login_timeout(text):
        return _with_cause(network_failure_copy(text, raw_s), raw_s)

    # Auth / credentials — first because it is the most common and sensitive.
    if re.search(_AUTH_WORDS, text):
        if driver in ("salesforce", "hubspot", "stripe", "rest_api"):
            return _with_cause(
                "API authentication failed. Check the host/URL, API token/key, required scopes, and that the token is active.",
                raw_s,
            )
        if driver == "mongodb":
            return _with_cause(
                "Authentication failed. Check the username/password and the Auth source field. "
                "Use admin if the user is defined in the admin database, or the database name if the user is defined there.",
                raw_s,
            )
        if driver == "snowflake":
            # Already-classified copy contains "password" / "login" — do not wrap
            # a 250001 as MFA. Keep the raw driver text.
            return _with_cause(
                "Snowflake refused this login. "
                "If this is a 250001, the host was reached — check username and password. "
                "Use Programmatic access token or Key-pair only when Snowflake returns "
                "an MFA / authentication-policy code (390195, 394508, 394504).",
                raw_s,
            )
        if driver == "bigquery":
            return _with_cause(
                "Authentication failed. Check the service account JSON, project ID, and that the account has BigQuery permissions.",
                raw_s,
            )
        if driver in ("s3", "gcs", "adls"):
            return _with_cause(
                "Authentication failed. Check access keys / service account, region, bucket/container, and permissions.",
                raw_s,
            )
        if driver in ("postgresql", "mysql", "redshift", "mariadb", "generic_sql"):
            return _with_cause(
                "Authentication failed. Check username, password, database, and that the user can log in from this host.",
                raw_s,
            )
        return _with_cause("Authentication failed. Check username, password, and permissions.", raw_s)

    # DNS / refused / unreachable / handshake timeout / timeout — each named from
    # the driver's own text, with that text kept as the cause. One slogan
    # ("Cannot reach the host on the specified port") used to cover a refused
    # port, an unroutable network and a down host (QA r12 bug 8).
    def _dns_hint(dns_text: str) -> str:
        if "internal" in dns_text:
            return (
                "Host not found — this looks like a private/internal address. "
                "Use the provider public proxy host and port (for Railway: *.proxy.rlwy.net)."
            )
        if driver == "snowflake":
            return _with_cause(
                "Snowflake account host did not resolve. Check the account locator/host "
                "(e.g. xy12345.us-east-1.snowflakecomputing.com) — this is a DNS failure, "
                "not a credentials failure.",
                raw_s,
            )
        return ""

    network = network_failure_copy(
        text, raw_s, driver=resolve_driver_type(driver) if driver else "", dns_hint=_dns_hint
    )
    if network:
        if network.startswith("Connection timed out") and ("internal" in text or "railway" in text):
            return (
                "Connection timed out. If you used a *.railway.internal host, switch to the "
                "public proxy host and port from Railway. Otherwise check network/firewall."
            )
        return network if network.startswith("Host not found") else _with_cause(network, raw_s)

    # SSL / TLS
    if re.search(r"ssl|tls|certificate|verify|cert|handshake", text):
        return "SSL/TLS error. Try toggling SSL, check certificates, or verify the host supports the selected SSL mode."

    # Driver not installed
    if re.search(r"no module named|module not found|cannot import|driver not installed|not installed", text):
        return "Connector driver is not installed in this environment. Contact support or install the driver package."

    # Invalid URI / connection string
    if re.search(r"invalid uri|invalid connection string|could not parse|malformed|not a valid uri|bad connection string", text):
        return "The connection string format is invalid. Check the URL, credentials, and query parameters."

    # SFTP-specific
    if driver in ("sftp",) and re.search(r"authentication|auth|login|credential|password|key|private key", text):
        return "SFTP authentication failed. Check username, password, private key, host, and port."
    if driver in ("sftp",) and re.search(r"no such file|is a directory|file not found|could not open|not a directory|path not found|no such", text):
        return "SFTP path not found. Check the remote directory and file path."

    # Email-specific
    if driver in ("email",) and re.search(r"authentication|auth|login|credential|password|username", text):
        return "SMTP authentication failed. Check host, port, username, password, and that the account allows SMTP."
    if driver in ("email",) and re.search(r"recipient|to address|to:|invalid address|address", text):
        return "Email recipient is invalid or rejected. Check the 'to' addresses in the SMTP URL or database field."

    # File / path issues
    if re.search(r"no such file|is a directory|file not found|could not open|not a directory|path not found", text):
        return "File or path not found. Check the file path and that the volume is mounted."

    # Snowflake account-host refusals name the account form, not a private-IP hint.
    if driver == "snowflake" and re.search(r"refused connector host|did not resolve|non-routable|private", text):
        return _with_cause(
            "Snowflake account host was refused or did not resolve. Check the account "
            "locator/host (e.g. xy12345.us-east-1.snowflakecomputing.com), not the credentials.",
            raw_s,
        )

    # Snowflake-specific — keep the driver's reason; the discriminator is the fix.
    if driver == "snowflake" and re.search(r"warehouse|role|database|schema", text):
        return _with_cause(
            "Snowflake connection failed. Check account, warehouse, database, schema, and role.",
            raw_s,
        )

    # BigQuery-specific
    if driver == "bigquery" and re.search(r"project|dataset|grant|permission|invalid", text):
        return "BigQuery connection failed. Check project ID, dataset, and service account permissions."

    # Object storage
    if driver in ("s3", "gcs", "adls") and re.search(r"bucket|container|region|not found|access|signature", text):
        return "Object storage connection failed. Check bucket/container, region, credentials, and permissions."

    # DynamoDB
    if driver == "dynamodb" and re.search(r"region|resource|table|access|security|token", text):
        return "DynamoDB connection failed. Check region, access keys, and that the table exists."

    # Redis
    if driver == "redis" and re.search(r"auth|password|noauth|wrongpassword", text):
        return "Redis authentication failed. Check the password and that the host/port are correct."

    # Elasticsearch
    if driver == "elasticsearch" and re.search(r"index|cluster|security|license", text):
        return "Elasticsearch connection failed. Check host/port, credentials, and cluster status."

    # Database locked / overloaded
    if re.search(r"database is locked|deadlock|too many connections|max connections|quota exceeded", text):
        return "Database is locked or overloaded. Try again later or reduce concurrency."

    # Resource / table not found — be precise so unrelated driver text
    # (e.g. fakesnow lock errors that mention an object name) is not mislabeled.
    missing_object = (
        r"(?:table|relation|object|view|sequence|bucket|container|index|resource|database|schema)\s+"
        r"(?:'[^']+'\s+)?(?:does not exist|do not exist|not found|not exist)|"
        r"unknown database|"
        r"no such (?:table|relation|object|view|database|schema|sequence)|"
        r"file or path not found|"
        r"bucket .* not found|container .* not found|"
        r"object '[^']+' does not exist"
    )
    if re.search(missing_object, text, re.IGNORECASE):
        return "Destination or resource not found. Check the database, schema, table, bucket, or index name."

    # Redshift, RDS, cloud Postgres/MySQL share the same auth/host families as their base drivers.
    if driver in ("redshift", "mariadb"):
        if re.search(_AUTH_WORDS, text):
            return "Authentication failed. Check username, password, database, and that the user can log in from this host."

    # Azure Blob / ADLS
    if driver == "adls":
        if re.search(r"authentication|auth|login|credential|account_key|signature|invalid|unauthorized|access denied|permission", text):
            return "Azure authentication failed. Check the storage account name, account key, or service principal JSON and permissions."
        if re.search(r"container|filesystem|not found|does not exist|resource not found", text):
            return "Container not found. Check the container or filesystem name, or Datawrap will create it during the transfer."

    # GCS
    if driver == "gcs":
        if re.search(r"authentication|auth|credential|service_account|invalid_grant|unauthorized|access denied|permission|forbidden", text):
            return "GCS authentication failed. Check the service account JSON, project ID, and Storage permissions."
        if re.search(r"bucket|not found|does not exist|no such bucket", text):
            return "GCS bucket not found. Check the bucket name and permissions."

    # SFTP-specific (path issues)
    if driver == "sftp" and re.search(r"no such file|is a directory|file not found|could not open|not a directory|path not found|no such", text):
        return "SFTP path not found. Check the remote directory and file path."

    # Fallback: the taxonomy's "cause not identified" copy with the sanitised
    # driver sentence (ERR-7) — never the raw text.
    unknown = classify(raw, side=side, phase=phase, engine=engine or driver)
    assert unknown is not None
    return OperatorCopy(unknown.text)


def locate_connector_file(
    *,
    connection_string: str = "",
    database: str = "",
    host: str = "",
    extra: dict[str, Any] | None = None,
) -> str:
    """Path Test should open for a file or embedded-database connector.

    A path that already exists on this server wins. Otherwise an upload
    attached in Configure (``extra.uploaded_file_id``) is the file. A URI is
    left for the probe to refuse — Test does not download it. When nothing
    resolves, the first typed value is returned so the error names what the
    operator entered.
    """
    typed = [
        str(connection_string or "").strip(),
        str(database or "").strip(),
        str(host or "").strip(),
    ]
    # A URI is the operator's remote location. Test refuses to download it;
    # an earlier upload must not silently replace that refusal with a local file.
    for text in typed:
        if text and "://" in text:
            return text
    for text in typed:
        if text and os.path.exists(text):
            return text
    file_id = str((extra or {}).get("uploaded_file_id") or "").strip()
    if file_id:
        from services.file_parser import resolve_attached_upload_path

        stored = resolve_attached_upload_path(file_id)
        if stored:
            return stored
    for text in typed:
        if text:
            return text
    return ""


def probe_file_source(fmt: str, path: str) -> tuple[bool, str]:
    """Honest file Test — catalog support is not a successful connection."""
    kind = (fmt or "").strip().lower()
    location = (path or "").strip()
    label = (kind or "file").upper()
    with connector_operation(
        "probe",
        driver_family=driver_family(kind),
        surface="probe_file_source",
    ) as op:
        if kind not in file_source_types():
            op.failed(category="unsupported")
            return False, f"No file-source probe for {label}."
        if not location:
            op.failed(category="no_input")
            return False, (
                f"{label} needs a file path or object-store URI before Test can prove a read. "
                "Catalog support is not a successful connection."
            )
        if "://" in location:
            op.failed(category="unsupported")
            return False, (
                f"{label} URI recorded. Test does not download remote objects — "
                "that would claim Connected without a read. Use Transfer Validate to prove the file."
            )
        if not os.path.exists(location):
            op.failed(category="not_found")
            return False, f"Path not found: {location}. Create the file or mount the volume before Test."
        try:
            from .file_stream import peek_file_source

            columns, _schema, total, _sample = peek_file_source(location, os.path.basename(location))
            n_cols = len(columns or [])
            op.succeeded(row_count=int(total or 0), column_count=n_cols)
            return True, f"{label} readable — {int(total or 0)} rows, {n_cols} columns."
        except Exception as exc:
            op.errored(exc)
            return False, humanize_connection_error(kind, exc)


def run_probe(db_type: str, cfg: dict[str, Any]) -> tuple[bool, str]:
    """Execute connectivity probe for a driver using resolved config."""
    with connector_operation(
        "probe",
        driver_family=driver_family(db_type),
        surface="run_probe",
    ) as op:
        try:
            from services.egress_guard import EgressRefused, pinned_egress

            with pinned_egress(workspace_id=str(cfg.get("workspace_id") or "")):
                early = _other_wire_on_its_default_port(db_type, cfg)
                if early:
                    result = (False, early)
                else:
                    started = time.monotonic()
                    result = _run_probe_impl(db_type, cfg)
                    if not result[0]:
                        waited = time.monotonic() - started
                        result = _name_protocol_mismatch(db_type, cfg, result)
                        result = _name_silent_endpoint(db_type, cfg, result, waited)
        except EgressRefused as exc:
            op.failed(category="probe_failed")
            return False, humanize_connection_error((db_type or "").lower(), exc)
        except Exception as exc:
            op.errored(exc)
            return False, humanize_connection_error((db_type or "").lower(), exc, auth_mode=str(cfg.get("auth_mode") or "").strip().lower())
        if result[0]:
            op.succeeded()
        else:
            op.failed(category="probe_failed")
        return result


def _other_wire_on_its_default_port(db_type: str, cfg: dict[str, Any]) -> str:
    """A MySQL-family Test aimed at 5432 asks the port what it speaks first.

    QA r12 bug 8: an ``amazon_aurora`` (Aurora MySQL) connector given a
    PostgreSQL endpoint's host and 5432 waited out the 45 s handshake read
    before the mismatch was named. MySQL greets on connect, so pymysql sat
    waiting on a server that never speaks first. Only this suspicious pairing
    is sniffed up front; every other probe keeps sniffing only after a failure.
    """
    if cfg.get("connection_string"):
        return ""
    from src.transfer.engine_family import MYSQL, engine_family

    kind = str(cfg.get("type") or db_type)
    if engine_family(kind) != MYSQL:
        return ""
    try:
        port = int(cfg.get("port") or 0)
    except (TypeError, ValueError):
        return ""
    if port != 5432:
        return ""
    from services.wire_protocol_sniff import protocol_mismatch_message

    return protocol_mismatch_message(kind, str(cfg.get("host") or ""), port)


def _name_protocol_mismatch(
    db_type: str, cfg: dict[str, Any], result: tuple[bool, str]
) -> tuple[bool, str]:
    """A MySQL handshake answered by PostgreSQL (or the reverse) says so."""
    if cfg.get("connection_string"):
        return result
    from services.wire_protocol_sniff import protocol_mismatch_message

    host = str(cfg.get("host") or "")
    port = int(cfg.get("port") or default_port(resolve_driver_type(db_type)) or 0)
    # The catalog tile names the hint (``amazon_aurora``); callers may pass the
    # resolved driver as ``db_type``.
    message = protocol_mismatch_message(str(cfg.get("type") or db_type), host, port)
    return (False, message) if message else result


def _name_silent_endpoint(
    db_type: str, cfg: dict[str, Any], result: tuple[bool, str], waited: float
) -> tuple[bool, str]:
    """A SQL handshake that timed out says whether the port accepted TCP (D353).

    libpq's "timeout expired" and ODBC's "Login timeout expired" read the same
    for a dropped SYN and for a proxy or tunnel that accepted the connection
    and never forwarded it, and a dual-stack host added its unroutable
    address's "Network is unreachable". One bare TCP connection, only after
    such a failure, settles which it was.
    """
    from services.wire_protocol_sniff import tcp_answer

    from .engine_family import SQL_DUPLEX_FAMILIES, engine_family
    from .network_error_copy import observed_endpoint_copy, recheckable

    kind = str(cfg.get("type") or db_type)
    if cfg.get("connection_string") or not recheckable(result[1]):
        return result
    if engine_family(kind) not in SQL_DUPLEX_FAMILIES and resolve_driver_type(kind) != "generic_sql":
        return result
    host = str(cfg.get("host") or "")
    try:
        port = int(cfg.get("port") or default_port(resolve_driver_type(kind)) or 0)
    except (TypeError, ValueError):
        return result
    copy = observed_endpoint_copy(tcp_answer(host, port), host, port, waited)
    if not copy:
        return result
    _, _, cause = result[1].partition(" Driver: ")
    cause = cause or str(getattr(result[1], "sentence", "") or "")
    return False, (f"{copy} Driver: {cause}" if cause else copy)


_MONGO_UNREACHABLE = re.compile(
    r"timed out|timeout|serverselection|no servers|nodename|name or service|"
    r"getaddrinfo|name resolution|connection refused|unreachable|dnspython|srv",
    re.I,
)


def _mongodb_srv_form_hint(cfg: dict[str, Any], raw: Any) -> str:
    """Name the mongodb+srv:// fix when a host/port form cannot reach a cluster.

    QA r12 Mongo D6: an Atlas cluster name in the host field timed out with no
    clue that Atlas is dialled through its SRV record.
    """
    if str(cfg.get("connection_string") or "").strip():
        return ""
    if not _MONGO_UNREACHABLE.search(str(raw or "")):
        return ""
    from connectors.mongodb_common import mongodb_srv_hint

    return mongodb_srv_hint(str(cfg.get("host") or ""), int(cfg.get("port") or 0))


def _run_probe_impl(db_type: str, cfg: dict[str, Any]) -> tuple[bool, str]:
    import importlib

    from services.connector_auth import engine_login_role, resolve_api_key
    from services.dial_target import reconcile_dial_host
    from services.egress_guard import aws_sdk_endpoint, metadata_endpoint_refusal

    # A leftover localhost host must not hide an Atlas URI, and must not be
    # invented when the URI already names the cluster.
    reconcile_dial_host(cfg)
    # The driver this probe runs, not a ``type`` the cfg carries (review L-E3).
    guard_type = str(db_type or cfg.get("type") or "")
    refusal = metadata_endpoint_refusal(
        str(cfg.get("host") or ""),
        str(cfg.get("endpoint_url") or ""),
        str(cfg.get("connection_string") or ""),
        workspace_id=str(cfg.get("workspace_id") or ""),
        db_type=guard_type,
        aws_endpoint=aws_sdk_endpoint(cfg, guard_type),
    )
    if refusal:
        # The policy's own sentence stands; only an unresolvable name is
        # reworded as Host not found (ERR-18).
        if str(refusal).lower().startswith("refused connector host"):
            return False, humanize_connection_error(guard_type, refusal)
        return False, refusal

    db_type = (db_type or "").lower()
    catalog_id = db_type
    resolved = resolve_driver_type(db_type)
    _attempted_auth_mode = str(cfg.get("auth_mode") or "").strip().lower()

    # Resolve catalog aliases (e.g. cockroachdb -> postgresql) to a registered driver
    db_type = resolved
    spec = CONNECTOR_MODULES.get(db_type)

    if db_type == "mongodb" and spec:
        try:
            from .adapters import probe_mongodb

            ok, raw = probe_mongodb(cfg)
        except Exception as exc:
            return False, humanize_connection_error(db_type, exc, auth_mode=_attempted_auth_mode)
        if ok:
            return True, raw
        message = humanize_connection_error(db_type, raw, auth_mode=_attempted_auth_mode)
        hint = _mongodb_srv_form_hint(cfg, raw)
        return False, (f"{message.rstrip()} {hint}" if hint else message)

    from services.dialect_profiles import default_schema_for, normalize_schema

    schema_default = default_schema_for(db_type)
    probe_kwargs = {
        "host": cfg.get("host", ""),
        "port": int(cfg.get("port") or default_port(db_type or catalog_id)),
        "database": cfg.get("database", ""),
        "username": cfg.get("username", ""),
        "password": cfg.get("password", ""),
        "schema": normalize_schema(
            db_type,
            cfg.get("schema") or schema_default,
            username=str(cfg.get("username") or "") or None,
        ),
        "connection_string": cfg.get("connection_string", ""),
        "ssl": cfg.get("ssl", False),
        "warehouse": cfg.get("warehouse", ""),
        "table": cfg.get("table", ""),
        "auth_mode": cfg.get("auth_mode", ""),
        "role": engine_login_role(cfg.get("auth_role"), cfg.get("role")),
        "auth_role": engine_login_role(cfg.get("auth_role"), cfg.get("role")),
        # A key declared by auth_mode and pasted in the password field is the key
        # (one rule for every HTTP tile — D274/D277).
        "api_key": resolve_api_key(cfg, password_is_key=False),
        "service_account": cfg.get("service_account", ""),
        "private_key": cfg.get("private_key", ""),
        "region": cfg.get("region", ""),
        "endpoint_url": cfg.get("endpoint_url", ""),
        "path_style": bool(cfg.get("path_style")),
        # False when the caller is probing a *write* target: the object the run is
        # about to create must not be demanded before the run creates it.
        "require_object": bool(cfg.get("require_object", True)),
    }
    # TLS / SID / driver keywords change which handshake the probe opens.
    # Dropping them made Validate ping a different connection than Execute
    # (SQL Server Driver 18 verify-or-fail vs operator-declared trust).
    try:
        from connectors.generic_sql import connection_options

        probe_kwargs.update(connection_options(cfg))
    except Exception:
        for key in (
            "trust_server_certificate",
            "encrypt",
            "server_certificate",
            "hostname_in_certificate",
            "sslmode",
        ):
            value = cfg.get(key)
            if value not in (None, ""):
                probe_kwargs[key] = value

    if resolved == "generic_sql":
        # The catalog id (e.g. tidb, clickhouse) must reach the generic SQL engine
        # builder so it can pick the right SQLAlchemy drivername and port.
        engine_type = cfg.get("type") or catalog_id
        try:
            from connectors.generic_sql import test_generic_sql

            ok, raw = test_generic_sql(type=engine_type, **probe_kwargs)
        except Exception as exc:
            return False, humanize_connection_error(engine_type, exc, auth_mode=_attempted_auth_mode)
        if ok:
            return True, raw
        return False, humanize_connection_error(engine_type, raw, auth_mode=_attempted_auth_mode)

    if not spec:
        return False, (
            f"Unsupported connector type '{db_type}' — no connectivity probe or "
            "transfer driver is registered for it. The type name itself is "
            "unsupported; host/port/user/password changes will not help."
        )

    if not spec.probe:
        return False, f"No probe configured for {db_type}"

    mod_name, fn_name = spec.probe
    try:
        mod = importlib.import_module(mod_name)
        probe_fn = getattr(mod, fn_name)
    except Exception as exc:
        return False, humanize_connection_error(db_type, exc, auth_mode=_attempted_auth_mode)
    sig = inspect.signature(probe_fn)
    accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    if "cfg" in sig.parameters and "driver" in sig.parameters:
        # SDK manifest probes own the whole resolved cfg (site/email/api_token
        # and other per-driver fields never fit the legacy kwarg surface).
        try:
            result = probe_fn(cfg, resolved)
        except Exception as exc:
            return False, humanize_connection_error(db_type, exc, auth_mode=_attempted_auth_mode)
        return result if isinstance(result, tuple) else (bool(result), str(result))
    if not accepts_var_kw:
        probe_kwargs = {k: v for k, v in probe_kwargs.items() if k in sig.parameters}
    else:
        # Forward additional resolved config fields (warehouse, region, extra catalog
        # properties) to probes that accept arbitrary keyword args.
        for k, v in cfg.items():
            if k not in probe_kwargs and v not in (None, ""):
                probe_kwargs[k] = v
    try:
        result = probe_fn(**probe_kwargs)
    except Exception as exc:
        return False, humanize_connection_error(db_type, exc, auth_mode=_attempted_auth_mode)
    if hasattr(result, "ok"):
        if result.ok:
            return True, str(result.message or "Connection successful")
        return False, humanize_connection_error(db_type, str(result.error or result.message or "unknown error"), auth_mode=_attempted_auth_mode)
    if isinstance(result, tuple) and len(result) >= 2:
        ok = bool(result[0])
        if ok:
            return True, str(result[1])
        return False, humanize_connection_error(db_type, str(result[1]), auth_mode=_attempted_auth_mode)
    return bool(result), "OK" if result else humanize_connection_error(db_type, "Probe failed", auth_mode=_attempted_auth_mode)
