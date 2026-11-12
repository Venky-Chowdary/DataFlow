"""What a destination may say about where it connects, in a fenced workspace (review round 3, H1/H2).

Three review rounds each found another key the residency judge did not read
(``Address=`` / ``Failover_Partner=`` in an ODBC string, ClickHouse
``alt_hosts``, MySQL ``unix_socket``, an Iceberg ``s3.endpoint``, Redshift's
staging endpoint) because the judge enumerated what to read. In a workspace
with a data region this module turns that around:

* every connector family has an **allowlist** of the connection-string query
  keys and ``extra`` keys its driver may be given, each next to the driver
  resolver it mirrors (:data:`FAMILIES`);
* any key that is not on the family's allowlist is **refused**, naming the
  key (a 422 at the routes): in the URL query of ``connection_string`` and of
  a URL in ``host``, in an ODBC / libpq ``key=value`` string, in the request's
  ``extra`` (the per-request override) and in the saved connector's ``extra``;
* every allowlisted key that chooses where the driver connects or writes is
  judged with the driver's own resolution (:func:`address_hosts`): its hosts
  join the destination's other addresses, all of which must prove the region.

Engine options that never choose a place (write modes, procedure parameters,
journal and schema options; :data:`ENGINE_OPTION_EXTRAS`) are allowed for
every family. Nothing here runs for an unfenced workspace.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import parse_qsl, urlparse

#: ``extra`` keys the engine, writers and planners set or read that never
#: choose where data goes. Allowed for every family.
ENGINE_OPTION_EXTRAS = frozenset({
    "activation_batch_size", "allow_append_only", "allow_duplicates", "auto_offset_reset", "batch_size",
    "cdc_drift_repair", "cdc_lease_cursor_key", "cdc_row_filter", "chunk_overlap", "chunk_size",
    "chunk_strategy", "chunk_tokenizer", "chunk_unit", "content_column", "contract_violation",
    "create_empty_source", "cursor_after", "cursor_column", "cursor_primary_key", "dest_procedure",
    "dest_procedure_after", "dest_procedure_before", "dest_procedure_call", "dest_procedure_param_map",
    "dest_procedure_params", "dest_query", "dest_query_sql", "dest_read_mode", "dest_rows_before",
    "dest_table_existed_before", "dest_table_prior_spelling", "dest_write_mode",
    "destination_column_types", "destination_identity", "dropped_fields", "duplicate_policy",
    "dynamo_index_attributes", "dynamo_key_schema", "embedding_column", "embedding_dimensions",
    "embedding_model", "empty_cells_as_null", "exclude_pii_columns", "file_format", "file_wait_seconds",
    "fixed_width_layout", "foreign_keys", "foreign_keys_mode", "generated_columns", "group_id",
    "identity_columns", "in_file_dedupe", "indexes_meta", "job_id", "journal_capture", "journal_id",
    "journal_replay", "journal_stream", "known_total_rows", "materialize_batch", "metadata_columns",
    "mode", "multipart_part_size", "multipart_threshold", "namespace", "object_store_materialize_batch",
    "object_store_multipart_part_size", "object_store_multipart_threshold", "object_store_source_spill_max",
    "object_store_spill_max", "overwrite_backup", "overwrite_backup_engine",
    "overwrite_replaced_column_types", "overwrite_rows_before", "partition_spec",
    "pg_overwrite_in_transaction", "preserve_columns", "primary_key", "primary_key_columns",
    "primary_keys", "procedure_call", "procedure_param_map", "procedure_params",
    "procedure_result_schema", "record_unreadable", "rename_columns", "retry", "route_backups",
    "scan_filter", "scan_state", "schedule_id", "schema", "schema_carrier_authority", "schema_defaults",
    "schema_evolution", "schema_nullability", "schema_types", "sftp_stream_chunk", "source_filter",
    "source_procedure", "source_query", "source_query_params", "source_read_mode", "source_spill_max",
    "spill_max", "sql_materialize_batch", "table", "text_template", "timeout", "unique_keys",
    "upsert_key", "write_options", "privilege_probe", "introspect_purpose", "introspect_strict",
    # Operator acknowledgements a request carries next to the endpoint.
    "destination_override_acknowledged", "ddl_acknowledged", "ddl_changes_acknowledged",
    "duplicate_risk_acknowledged", "empty_source_overwrite_acknowledged", "df_fk_risk_acknowledged",
    "compliance_acknowledged",
    # TLS trust and host-key pins (they make a connection stricter, never move it).
    "ssl_mode", "ssl_ca", "ssl_capath", "ssl_cert", "ssl_key", "ssl_verify_cert", "ssl_verify_identity",
    "trust_server_certificate", "host_key", "known_hosts", "host_key_policy",
})
#: The endpoint's own fields, which ``EndpointConfig.from_dict`` and endpoint
#: resolution also carry in ``extra`` (``resolve_connector_config`` flattens
#: them). Allowed for every family: the address fields among them (``host``,
#: ``endpoint_url``, ``connection_string``, ``account``) are judged by
#: ``data_residency._addresses``, which reads ``extra`` first as the driver does.
ENDPOINT_FIELDS = frozenset({
    "kind", "format", "type", "db_type", "connector_id", "host", "port", "database", "schema", "table",
    "table_name", "collection", "collection_name", "username", "password", "connection_string",
    "warehouse", "ssl", "auth_mode", "auth_role", "auth_source", "api_key", "service_account",
    "private_key", "ssh_private_key", "output_path", "region", "endpoint_url", "path_style", "extra",
    "role", "workspace_id",
})


@dataclass(frozen=True)
class Family:
    """One driver family's allowlist (keys are compared normalised: :func:`_norm_key`)."""

    resolver: str  # the code whose resolution this mirrors
    query: frozenset[str] = frozenset()  # plain URL / DSN keys
    query_prefixes: tuple[str, ...] = ()
    address_query: frozenset[str] = frozenset()  # keys whose value is judged
    extra: frozenset[str] = frozenset()  # plain extra keys beyond ENGINE_OPTION_EXTRAS
    extra_prefixes: tuple[str, ...] = ()
    address_extra: frozenset[str] = frozenset()  # extras whose value is judged
    judge: Callable[[Any, dict[str, list[str]]], tuple[list[str], list[str]]] | None = field(default=None)


def _norm_key(key: Any) -> str:
    """ODBC keywords are case-insensitive and spelled with or without spaces/underscores."""
    return re.sub(r"[\s_\-]", "", str(key or "").strip().lower())


def _keys(*names: str) -> frozenset[str]:
    return frozenset(_norm_key(n) for n in names)


# --------------------------------------------------------------------------- judges
# Each returns ``(hosts, refusals)``: the hosts the driver dials for the
# family's address keys, and why a value cannot be judged.


def _hosts_of(value: str) -> list[str] | None:
    from services.data_residency import _hostname

    names = [_hostname(part.strip()) for part in str(value or "").split(",") if part.strip()]
    if not names or any(n is None for n in names):
        return None
    return [n for n in names if n]


def _judge_list_keys(keys: tuple[str, ...], label: str, *, port_after_comma: bool = False):
    def judge(endpoint: Any, found: dict[str, list[str]]) -> tuple[list[str], list[str]]:
        hosts: list[str] = []
        refusals: list[str] = []
        for key in keys:
            for value in found.get(key, []):
                text = value.strip().removeprefix("tcp:")
                # ODBC ``host,port`` / ``host\instance``; ClickHouse ``h1:9000,h2:9000``.
                text = text.split(",", 1)[0].split("\\", 1)[0] if port_after_comma else text
                names = _hosts_of(text)
                if names is None:
                    refusals.append(f"{label} '{key}' value cannot be read as host names")
                else:
                    hosts.extend(names)
        return hosts, refusals

    return judge


def _judge_sqlserver(endpoint: Any, found: dict[str, list[str]]) -> tuple[list[str], list[str]]:
    """pyodbc / ODBC Driver 17-18: ``Address`` / ``Addr`` / ``Network Address`` win over
    ``Server``; ``Failover_Partner`` is dialled when the primary is unreachable."""
    return _judge_list_keys(
        (_norm_key("address"), _norm_key("addr"), _norm_key("network address"),
         _norm_key("failover_partner"), _norm_key("server"), _norm_key("data source")),
        "SQL Server",
        port_after_comma=True,
    )(endpoint, found)


def _judge_mysql(endpoint: Any, found: dict[str, list[str]]) -> tuple[list[str], list[str]]:
    """pymysql: ``unix_socket`` replaces the TCP host: the server is on this machine."""
    return (["localhost"] if found.get(_norm_key("unix_socket")) else []), []


def _judge_redshift(endpoint: Any, found: dict[str, list[str]]) -> tuple[list[str], list[str]]:
    """``connectors.redshift_copy``: the COPY staging bucket's S3 endpoint (the rows are PUT there)."""
    from connectors.redshift_copy import resolve_redshift_copy_config

    from services.data_residency import _AWS_REGION, _hostname, provider_region

    extra = getattr(endpoint, "extra", None)
    try:
        config = resolve_redshift_copy_config(extra if isinstance(extra, dict) else {})
    except ValueError as exc:
        return [], [str(exc)]
    if config is None:
        return [], []
    if config.endpoint_url:
        host = _hostname(urlparse(config.endpoint_url if "://" in config.endpoint_url
                                  else f"https://{config.endpoint_url}").netloc.rsplit("@", 1)[-1]) or ""
        if not host.endswith(".amazonaws.com") or not provider_region(host):
            return [], ["a Redshift COPY staging endpoint that is not a regional AWS S3 endpoint is not "
                        "accepted in a workspace with a data region"]
        return [host], []
    region = (config.region or "us-east-1").strip().lower()
    if not _AWS_REGION.fullmatch(region):
        return [], [f"the Redshift COPY staging region {region!r} is not an AWS region"]
    return [f"s3.{region}.amazonaws.com"], []


def _judge_iceberg(endpoint: Any, found: dict[str, list[str]]) -> tuple[list[str], list[str]]:
    """``connectors.iceberg_catalog._forward_storage_properties``: the FileIO
    endpoints pyiceberg writes data files through, and the warehouse location."""
    from services.data_residency import _hostname

    hosts: list[str] = []
    refusals: list[str] = []
    for key in (_norm_key("s3.endpoint"), _norm_key("gcs.endpoint"), _norm_key("adls.endpoint")):
        for value in found.get(key, []):
            text = value if "://" in value else f"https://{value}"
            host = _hostname(urlparse(text).netloc.rsplit("@", 1)[-1])
            if host:
                hosts.append(host)
            else:
                refusals.append(f"Iceberg '{key}' value cannot be read as a host")
    warehouse = str(_field(endpoint, "warehouse") or "").strip()
    for value in [warehouse, *found.get(_norm_key("warehouse"), [])]:
        if "://" in value and not value.lower().startswith("file://"):
            # The bucket / container decides where data files land; no URL proves
            # its region. Only a platform-verified region speaks for it.
            hosts.append("")
    for key in (_norm_key("adls.account-name"), _norm_key("adls.account_name")):
        if found.get(key):
            hosts.append("")
    return hosts, refusals


#: A host the judge cannot place: only a platform-verified region speaks for the destination.
NEEDS_VERIFICATION = ""


def _field(endpoint: Any, name: str) -> Any:
    if isinstance(endpoint, dict):
        return endpoint.get(name)
    return getattr(endpoint, name, None)


_PG_QUERY = _keys(
    "sslmode", "sslrootcert", "sslcert", "sslkey", "sslcrl", "sslcrldir", "sslpassword", "sslcompression",
    "sslsni", "sslnegotiation", "ssl_min_protocol_version", "ssl_max_protocol_version", "requiressl",
    "connect_timeout", "application_name", "fallback_application_name", "options", "client_encoding",
    "keepalives", "keepalives_idle", "keepalives_interval", "keepalives_count", "tcp_user_timeout",
    "target_session_attrs", "load_balance_hosts", "gssencmode", "channel_binding", "krbsrvname",
    "require_auth", "dbname", "user", "password", "port",
)
_MYSQL_QUERY = _keys(
    "charset", "collation", "connect_timeout", "read_timeout", "write_timeout", "autocommit",
    "binary_prefix", "program_name", "use_unicode", "max_allowed_packet", "sql_mode", "ssl",
    "tls_version", "server_public_key", "defer_connect", "user", "password", "database", "port",
)

#: Family name (``resolve_driver_type`` / URL scheme) -> its allowlist.
FAMILIES: dict[str, Family] = {
    # connectors.sql_dsn.resolve_sql_endpoint, psycopg2/libpq.
    "postgresql": Family("connectors.sql_dsn.resolve_sql_endpoint (libpq)", _PG_QUERY, (),
                         _keys("host", "hostaddr")),
    # PyMySQL via connectors.sql_dsn / SQLAlchemy; TLS rides as ssl_* (connector_store.dial_connection_string).
    "mysql": Family("connectors.sql_dsn.resolve_sql_endpoint (PyMySQL)", _MYSQL_QUERY, ("ssl",),
                    _keys("unix_socket"), _keys("unix_socket"), (), _keys("unix_socket"), _judge_mysql),
    # SQLAlchemy mssql+pyodbc appends every unknown URL key to the ODBC string; pymssql kwargs.
    "sqlserver": Family(
        "SQLAlchemy mssql+pyodbc / pymssql (ODBC keywords)",
        _keys("driver", "database", "uid", "pwd", "encrypt", "trustservercertificate", "hostnameincertificate",
              "connection timeout", "connect timeout", "login timeout", "applicationintent",
              "multisubnetfailover", "application name", "app", "wsid", "authentication",
              "columnencryption", "mars_connection", "autocommit", "packet size", "servercertificate",
              "transparentnetworkipresolution", "keepalive", "keepaliveinterval", "connectretrycount",
              "connectretryinterval", "language", "trusted_connection", "charset", "tds_version",
              "login_timeout", "timeout", "appname", "port", "odbc_connect"),
        (),
        _keys("server", "data source", "address", "addr", "network address", "failover_partner"),
        _keys("multi_subnet_failover", "driver", "odbc_driver", "encrypt"),
        (),
        _keys("failover_partner", "address", "addr", "network address"),
        _judge_sqlserver,
    ),
    # clickhouse_driver.Client.from_url: alt_hosts is its failover list.
    "clickhouse": Family(
        "clickhouse_driver.Client.from_url",
        _keys("secure", "verify", "ca_certs", "certfile", "keyfile", "ssl_version", "ciphers", "compression",
              "compress_block_size", "connect_timeout", "send_receive_timeout", "sync_request_timeout",
              "client_name", "database", "user", "password", "round_robin", "tcp_keepalive", "quota_key"),
        (),
        _keys("alt_hosts"),
        _keys("secure", "verify", "compression"),
        (),
        _keys("alt_hosts"),
        _judge_list_keys((_norm_key("alt_hosts"),), "ClickHouse"),
    ),
    "oracle": Family("connectors.oracle (python-oracledb)",
                     _keys("encoding", "nencoding", "mode", "events", "threaded", "service_name", "sid",
                           "purity", "cclass", "expire_time", "tcp_connect_timeout", "retry_count",
                           "retry_delay", "ssl_server_dn_match"),
                     (), _keys("dsn"), _keys("service_name", "sid")),
    "mongodb": Family("pymongo MongoClient (URI options)",
                      _keys("authsource", "authmechanism", "authmechanismproperties", "replicaset", "tls", "ssl",
                            "tlscafile", "tlscertificatekeyfile", "tlsallowinvalidcertificates",
                            "tlsallowinvalidhostnames", "tlsinsecure", "retrywrites", "retryreads", "w",
                            "journal", "wtimeoutms", "readpreference", "readpreferencetags",
                            "readconcernlevel", "appname", "connecttimeoutms", "sockettimeoutms",
                            "serverselectiontimeoutms", "directconnection", "maxpoolsize", "minpoolsize",
                            "maxidletimems", "compressors", "zlibcompressionlevel", "uuidrepresentation",
                            "srvmaxhosts")),
    "snowflake": Family("connectors.snowflake_conn (explicit connect kwargs)",
                        _keys("warehouse", "role", "schema", "database", "application", "login_timeout",
                              "network_timeout", "client_session_keep_alive", "paramstyle", "timezone"),
                        (), _keys("account", "host"), _keys("warehouse", "role", "account")),
    # Iceberg: connectors.iceberg_catalog builds the catalog and its FileIO from these.
    "iceberg": Family(
        "connectors.iceberg_catalog (pyiceberg catalog + FileIO properties)",
        frozenset(), (), frozenset(),
        _keys("catalog_type", "catalog", "catalog_name", "rest.auth.type", "credential", "token",
              "oauth_token", "s3.region", "s3.access-key-id", "s3.secret-access-key", "s3.session-token",
              "s3.path-style-access", "client.region", "client.access-key-id", "client.secret-access-key",
              "client.session-token", "gcs.project", "gcs.oauth2.token", "adls.account-key"),
        (),
        _keys("warehouse", "s3.endpoint", "gcs.endpoint", "adls.endpoint", "adls.account-name",
              "adls.account_name"),
        _judge_iceberg,
    ),
    # Redshift: libpq keys on the cluster; connectors.redshift_copy for the COPY staging bucket.
    "redshift": Family(
        "libpq + connectors.redshift_copy.resolve_redshift_copy_config",
        _PG_QUERY, (), _keys("host", "hostaddr"),
        _keys("stage_format", "redshift_stage_format", "staging_prefix", "iam_role", "redshift_iam_role",
              "aws_access_key_id", "aws_secret_access_key"),
        (),
        _keys("staging_bucket", "redshift_staging_bucket", "s3_endpoint", "endpoint_url", "region",
              "aws_region"),
        _judge_redshift,
    ),
    "duckdb": Family("connectors.generic_sql._build_url_unconfined (duckdb)",
                     _keys("threads", "memory_limit", "max_memory", "access_mode", "default_order",
                           "preserve_insertion_order", "motherduck_token")),
    "trino": Family("SQLAlchemy trino", _keys("protocol", "verify", "source", "catalog", "schema",
                                                 "http_scheme", "client_tags", "roles")),
}
for _alias, _name in (("pgvector", "postgresql"), ("cockroachdb", "postgresql"), ("greenplum", "postgresql"),
                      ("yugabytedb", "postgresql"), ("mariadb", "mysql"), ("tidb", "mysql"),
                      ("singlestore", "mysql"), ("presto", "trino"), ("amazon_redshift", "redshift"),
                      ("redshift_serverless", "redshift"), ("motherduck", "duckdb")):
    FAMILIES[_alias] = FAMILIES[_name]
#: Families with no URL keys and no address extras of their own: only the engine options.
_DEFAULT = Family("the endpoint's own fields only")

#: URL schemes -> family (a ``generic_sql`` connector names its driver in the URL).
_SCHEMES = (
    ("postgresql", "postgresql"), ("postgres", "postgresql"), ("redshift", "redshift"),
    ("mysql", "mysql"), ("mariadb", "mysql"), ("mssql", "sqlserver"), ("sqlserver", "sqlserver"),
    ("clickhouse", "clickhouse"), ("oracle", "oracle"), ("mongodb", "mongodb"), ("snowflake", "snowflake"),
    ("duckdb", "duckdb"), ("trino", "trino"), ("presto", "trino"),
)


def family_of(fmt: str, url: str = "") -> tuple[str, Family]:
    """The family whose driver reads ``url`` (its scheme), else the endpoint's format."""
    scheme = url.split("://", 1)[0].lower() if "://" in url else ""
    for prefix, name in _SCHEMES:
        if scheme == prefix or scheme.startswith(prefix + "+"):
            return name, FAMILIES[name]
    from services.data_residency import _driver

    name = _driver(str(fmt or "").strip().lower())
    return name, FAMILIES.get(name, _DEFAULT)


# --------------------------------------------------------------------------- reading keys


def _url_query(text: str) -> list[tuple[str, str]]:
    if "://" not in text:
        return []
    query = urlparse(text).query
    return parse_qsl(query, keep_blank_values=True)


def _dsn_pairs(text: str) -> list[tuple[str, str]] | None:
    """``k=v;k=v`` (ODBC) or ``k=v k=v`` (libpq) pairs; ``None`` when ``text`` is neither."""
    raw = str(text or "").strip()
    if "://" in raw or "=" not in raw or raw.startswith("("):
        return None
    parts = [p for p in re.split(r";", raw) if p.strip()] if ";" in raw else raw.split()
    pairs: list[tuple[str, str]] = []
    for part in parts:
        key, eq, value = part.partition("=")
        if not eq:
            return None
        pairs.append((key.strip(), value.strip().strip("{}")))
    return pairs


def _query_pairs(endpoint: Any) -> list[tuple[str, str, str]]:
    """``(where, key, value)`` for every URL query / DSN key the driver is given."""
    found: list[tuple[str, str, str]] = []
    for name in ("connection_string", "host"):
        text = str(_field(endpoint, name) or "").strip()
        if not text:
            continue
        if text.lower().startswith("jdbc:"):
            text = text[5:]
        for key, value in _url_query(text):
            found.append((name, key, value))
            if _norm_key(key) == "odbcconnect":
                found.extend(("odbc_connect", k, v) for k, v in (_dsn_pairs(value) or []))
        if name == "connection_string":
            found.extend((name, k, v) for k, v in (_dsn_pairs(text) or []))
    return found


#: DSN keys that only restate the endpoint itself (they are judged by ``_field_hosts``).
_DSN_ENDPOINT_KEYS = _keys("host", "hostaddr", "server", "data source", "port", "dbname", "database",
                           "user", "uid", "password", "pwd", "driver", "account")


def _extra_items(endpoint: Any, saved: Any = None) -> list[tuple[str, str, Any]]:
    items: list[tuple[str, str, Any]] = []
    for where, source in (("extra", _field(endpoint, "extra")), ("saved connector extra", _field(saved, "extra"))):
        if isinstance(source, dict):
            items.extend((where, str(k), v) for k, v in source.items())
    return items


def _allowed(key: str, plain: frozenset[str], prefixes: tuple[str, ...], address: frozenset[str]) -> bool:
    norm = _norm_key(key)
    return norm in plain or norm in address or any(norm.startswith(_norm_key(p)) for p in prefixes)


_ENGINE_EXTRAS = _keys(*ENGINE_OPTION_EXTRAS, *ENDPOINT_FIELDS)


def addressing_check(
    endpoint: Any, saved: Any = None, *, extras: bool = True, strict: bool = True
) -> tuple[list[str], str | None]:
    """``(hosts, refusal)``: the hosts the family's address keys make the driver
    dial, and the first key or value that is refused (``None`` when none is).

    ``extras=False`` reads only the URL / DSN keys (a resolved endpoint's
    ``extra`` also holds the engine's own flattened fields). ``strict=False``
    collects the allowlisted address values and skips other keys (the judge
    of an endpoint whose keys :func:`addressing_refusal` has already passed)."""
    fmt = str(_field(endpoint, "format") or _field(endpoint, "type") or "")
    cs = str(_field(endpoint, "connection_string") or "").strip()
    url = cs if "://" in cs else str(_field(endpoint, "host") or "").strip()
    name, family = family_of(fmt, url)
    found: dict[str, list[str]] = {}
    for where, key, value in _query_pairs(endpoint):
        norm = _norm_key(key)
        if where == "connection_string" and "://" not in cs and norm in _DSN_ENDPOINT_KEYS:
            if norm in family.address_query:
                found.setdefault(norm, []).append(value)
            continue
        if not _allowed(key, family.query, family.query_prefixes, family.address_query):
            if not strict:
                continue
            return [], (f"the connection option '{key}' (in {where}) is not accepted for a {name or fmt} "
                        "destination in a workspace with a data region")
        if norm in family.address_query:
            found.setdefault(norm, []).append(value)
    for where, key, value in (_extra_items(endpoint, saved) if extras else []):
        norm = _norm_key(key)
        if norm in _ENGINE_EXTRAS:
            continue
        if not _allowed(key, family.extra, family.extra_prefixes, family.address_extra):
            if not strict:
                continue
            return [], (f"the option '{key}' (in {where}) is not accepted for a {name or fmt} destination "
                        "in a workspace with a data region")
        if norm in family.address_extra and value not in (None, ""):
            found.setdefault(norm, []).append(str(value))
    if family.judge is None:
        return [], None
    hosts, refusals = family.judge(endpoint, found)
    if refusals:
        return [], refusals[0]
    return hosts, None


def address_hosts(endpoint: Any) -> tuple[list[str], bool, bool]:
    """``(hosts, unreadable, needs_verification)``: the extra hosts the family's
    allowlisted address keys make the driver dial; whether a key or value is
    refused; whether a place no address proves (a bucket) receives the data."""
    try:
        hosts, refusal = addressing_check(endpoint, strict=False)
    except Exception:  # noqa: BLE001 - an unreadable configuration proves nothing
        return [], True, False
    return [h for h in hosts if h], refusal is not None, NEEDS_VERIFICATION in hosts


def addressing_refusal(endpoint: Any, saved: Any = None, *, extras: bool = True) -> str | None:
    """Why this destination's configuration may not be used in a fenced workspace, or ``None``."""
    try:
        _hosts, refusal = addressing_check(endpoint, saved, extras=extras)
    except Exception as exc:  # noqa: BLE001 - unreadable is refused, never waved through
        return f"Data residency violation: the destination configuration cannot be read ({type(exc).__name__})"
    if refusal:
        return f"Data residency violation: {refusal}. Remove it, or ask a platform administrator."
    return None


__all__ = ["ENDPOINT_FIELDS", "ENGINE_OPTION_EXTRAS", "FAMILIES", "address_hosts", "addressing_refusal", "family_of"]
