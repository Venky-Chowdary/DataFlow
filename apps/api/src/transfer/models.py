"""Universal transfer request/result models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class EndpointConfig:
    kind: str = "database"  # file, database, file_export
    format: str = ""  # csv, json, postgresql, mongodb, snowflake
    connector_id: Optional[str] = None
    host: str = ""
    port: int = 0
    database: str = ""
    schema: str = ""
    table: str = ""
    collection: str = ""
    username: str = ""
    password: str = ""
    connection_string: str = ""
    warehouse: str = ""
    # False => sslmode "prefer": negotiate TLS when available, fall back for local DBs
    ssl: bool = False
    auth_mode: str = ""
    auth_role: str = ""
    auth_source: str = ""
    api_key: str = ""
    service_account: str = ""
    # SSH/SFTP private key content, PEM text, or base64-encoded key.
    private_key: str = ""
    # For file_export destinations: a server-local path to write the export file.
    # If empty, the file is written to the configured exports directory and a
    # download URL is returned.
    output_path: str = ""
    # Cloud/data region for the endpoint (e.g. us-east-1, eu-west-1).
    region: str = ""
    # S3 / S3-compatible custom endpoint, e.g. http://minio:9000.
    endpoint_url: str = ""
    # Force path-style addressing for S3-compatible stores (MinIO, LocalStack, Wasabi).
    path_style: bool = False
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, kind: str, data: dict | None) -> "EndpointConfig":
        d = data or {}
        known = {
            "kind", "format", "type", "db_type", "connector_id", "host", "port",
            "database", "schema", "table", "table_name", "collection",
            "collection_name", "username", "password", "connection_string",
            "warehouse", "ssl", "auth_mode", "auth_role", "auth_source", "api_key", "service_account",
            "private_key", "ssh_private_key", "output_path", "region", "endpoint_url", "path_style",
            "extra",
        }
        nested = d.get("extra") if isinstance(d.get("extra"), dict) else {}
        flat_extra = {k: v for k, v in d.items() if k not in known}
        return cls(
            kind=kind,
            format=d.get("format", d.get("type", d.get("db_type", ""))),
            connector_id=d.get("connector_id"),
            # Missing host stays empty so a saved connector can fill it.
            # Defaulting to localhost here made every job dial 127.0.0.1
            # while Test still used the stored host.
            host=d.get("host") or "",
            port=int(d.get("port", 0) or 0),
            database=d.get("database", ""),
            schema=d.get("schema", ""),
            table=d.get("table", d.get("table_name", "")),
            collection=d.get("collection", d.get("collection_name", "")),
            username=d.get("username", ""),
            password=d.get("password", ""),
            connection_string=d.get("connection_string", ""),
            warehouse=d.get("warehouse", ""),
            ssl=d.get("ssl", False),
            auth_mode=d.get("auth_mode", ""),
            auth_role=d.get("auth_role", ""),
            auth_source=d.get("auth_source", ""),
            api_key=d.get("api_key", ""),
            service_account=d.get("service_account", ""),
            private_key=d.get("private_key", d.get("ssh_private_key", "")),
            output_path=(d.get("output_path") or "").strip(),
            region=d.get("region", ""),
            endpoint_url=(d.get("endpoint_url") or "").strip(),
            path_style=bool(d.get("path_style", False)),
            extra={**nested, **flat_extra},
        )


@dataclass
class TransferRequest:
    source: EndpointConfig
    destination: EndpointConfig
    mappings: list[dict] = field(default_factory=list)
    column_types: dict[str, str] = field(default_factory=dict)
    skip_preflight: bool = False
    source_filename: str = ""
    source_content: bytes = b""
    # Optional on-disk path for the source file.  Used for billion-row streaming
    # when loading the whole file into memory would exhaust RAM, and for
    # claim-queue workers that reconstruct TransferRequest without in-memory bytes.
    source_path: str = ""
    # Optional s3:// URI from object_store.stage_bytes (multi-replica hydrate).
    source_object_uri: str = ""
    sync_mode: str = "full_refresh_append"
    schema_policy: str = "manual_review"
    validation_mode: str = "strict"
    backfill_new_fields: bool = False
    # Load into {table}_df_staging first; promote only clean rows to primary.
    write_via_staging: bool = False
    # Optional row-level source filter (column predicates, and/or composition).
    source_filter: dict = field(default_factory=dict)
    # Tabular read window (sheet, header row, head/tail skips). See
    # services.read_options.ReadOptions — one declaration reaches the profiler,
    # the source COUNT and the writer, so all three read the same population.
    read_options: dict = field(default_factory=dict)
    # Pre-write shaping recipe (services.shape_models.ShapeRecipe payload). Applied
    # to every read row before mapping, so Map, the narrowing checks, the DDL
    # identity and the writer all see the same shaped truth. Row-local and
    # deterministic by construction — see services.shape_engine.
    shape_recipe: dict = field(default_factory=dict)
    # Recipe identity approved at Validate. Execute refuses when the recipe it was
    # handed hashes to anything else (Validate≡Execute).
    approved_shape_recipe_hash: str = ""
    # Transform Library provenance: which library recipe produced shape_recipe,
    # at what version, and under which parameters. Proof-pack material — the
    # engine executes shape_recipe; it never re-reads the library mid-run.
    transform_recipe_id: str = ""
    transform_recipe_version: int = 0
    transform_recipe_version_mode: str = "pin"
    transform_recipe_params: dict = field(default_factory=dict)
    # Set-graph identity approved at Validate. Empty matches a recipe with no
    # set nodes. A different graph is refused, the same way a different row
    # recipe is refused.
    approved_set_hash: str = ""
    # Priority-first sync: sort source rows by this column before writing.
    priority_column: str = ""
    priority_direction: str = "desc"  # "asc" or "desc"
    limit: int = 0  # 0 means no limit
    # Workspace isolation: transfers and their jobs can be scoped to a workspace.
    workspace_id: str = ""
    # Data residency / region tag for the job.
    data_region: str = ""
    stream_contracts: list[dict] = field(default_factory=list)
    contract_id: str = ""
    enforce_contract: bool = True
    # When True with contract_id, transfer fails unless the contract is SIGNED.
    require_signed_contract: bool = False
    # Operator identity for Jobs audit (email / subject).
    triggered_by: str = ""
    # Locale for ambiguous day/month dates: 'DMY' (European/Indian/Australian),
    # 'MDY' (US), or '' to infer from env DATAFLOW_DATE_ORDER / fail closed.
    date_locale: str = ""
    # Locale for ambiguous grouping: 'US' (1,234.56), 'EU' (1.234,56), or ''
    # to fail closed on a lone 3-digit group. Currency marks still parse.
    number_locale: str = ""
    # True when number_locale is the run's US fallback for a sample that carried
    # no grouping evidence, not an operator declaration. Proof must keep the
    # difference: an assumption is reversible by switching the column to EU.
    number_locale_assumed: bool = False
    # Client-supplied idempotency key. When set it replaces the derived request
    # fingerprint, letting a caller make its own HTTP retries safe. When empty,
    # the fingerprint still guards against accidental double submission.
    idempotency_key: str = ""
    # CDC / stream delivery. ``auto`` selects dest-owned exactly-once when the
    # route can commit apply and the watermark together; otherwise at-least-once.
    # An explicit at_least_once pin is not upgraded. exactly_once on an
    # ineligible route fails closed.
    delivery_guarantee: str = "auto"
    # Operator acks from Validate — Execute must carry the same trail (Validate≡Execute).
    compliance_acknowledged: bool = False
    schema_drift_acknowledged: bool = False
    fk_risk_acknowledged: bool = False
    # Operator approved destination DDL disclosed at Validate — e.g. the
    # unique index an upsert needs on an existing table (D101).
    ddl_changes_acknowledged: bool = False
    # Operator chose to overwrite a populated destination from a source that
    # holds 0 rows — refused without it (D56).
    empty_source_overwrite_acknowledged: bool = False
    # Operator accepted a load far smaller than this route's history (the
    # strict load-volume guard) - D82.
    load_volume_acknowledged: bool = False
    # Sync now / full re-sync. Empty scope runs the full gate set.
    # ``drift_connectivity`` rechecks only drift and connectivity on a signed contract.
    preflight_scope: str = ""
    parent_job_id: str = ""
    # rerun_of is the parent job id when this run was started as a re-run.
    rerun_of: str = ""
    # upsert | dedupe_skip | append. Empty keeps the sync-mode default, and
    # append-with-a-key does not insert duplicates unless this is ``append``.
    duplicate_policy: str = ""
    allow_duplicates: bool = False
    # Seconds a scheduled file pattern waits for a new object. 0 fails at once.
    file_wait_seconds: int = 0
    historical_resync: bool = False
    acknowledgment_actor: str = ""
    acknowledgment_reason: str = ""
    # Map→DDL fingerprint from Validate (or ledger proof). Required when
    # skip_preflight + mappings so Execute still fail-closes on stamp drift.
    approved_ddl_identity_hash: str = ""
    # Phase C11 — Decision Artifact content_hash from Validate / ledger.
    # Programmatic skip_preflight stamps inline when empty.
    approved_decision_artifact_hash: str = ""
    decision_artifact: dict = field(default_factory=dict)
    source_timezone: str = ""

    @property
    def operation(self) -> str:
        sk, dk = self.source.kind, self.destination.kind
        if sk == "file" and dk == "database":
            return "upload"
        if sk == "database" and dk == "database":
            return "migration"
        if sk == "file" and dk == "file_export":
            return "convert"
        if sk == "database" and dk == "file_export":
            return "dump"
        return "transfer"


_SECRET_ENDPOINT_KEYS = (
    "password",
    "connection_string",
    "api_key",
    "service_account",
    "private_key",
    "secret_access_key",
    "access_key_secret",
    "token",
    "refresh_token",
    "client_secret",
)


def _encrypt_endpoint_secrets(ep: dict) -> dict:
    """Encrypt secret fields before persisting a transfer_request on a job."""
    from services.secret_vault import encrypt_secret

    out = dict(ep or {})
    for key in _SECRET_ENDPOINT_KEYS:
        val = out.get(key)
        if isinstance(val, str) and val and val != "****" and not val.startswith("["):
            out[key] = encrypt_secret(val, label=f"job-{key}")
    extra = out.get("extra")
    if isinstance(extra, dict):
        extra_out = dict(extra)
        for key, val in list(extra_out.items()):
            lk = str(key).lower()
            if (
                isinstance(val, str)
                and val
                and val != "****"
                and any(s in lk for s in ("password", "secret", "token", "private_key", "api_key"))
            ):
                extra_out[key] = encrypt_secret(val, label=f"job-extra-{key}")
        out["extra"] = extra_out
    return out


def _decrypt_endpoint_secrets(ep: dict) -> dict:
    """Decrypt secret fields when reconstituting a TransferRequest from storage."""
    from services.secret_vault import decrypt_secret

    out = dict(ep or {})
    for key in _SECRET_ENDPOINT_KEYS:
        val = out.get(key)
        if isinstance(val, str) and val and val != "****":
            try:
                out[key] = decrypt_secret(val)
            except Exception:
                # Leave as-is — resume will fail loudly if the value is unusable.
                pass
    extra = out.get("extra")
    if isinstance(extra, dict):
        extra_out = dict(extra)
        for key, val in list(extra_out.items()):
            lk = str(key).lower()
            if (
                isinstance(val, str)
                and val
                and val != "****"
                and any(s in lk for s in ("password", "secret", "token", "private_key", "api_key"))
            ):
                try:
                    extra_out[key] = decrypt_secret(val)
                except Exception:
                    pass
        out["extra"] = extra_out
    return out


def endpoint_to_dict(ep: EndpointConfig) -> dict:
    return {
        "kind": ep.kind,
        "format": ep.format,
        "connector_id": ep.connector_id,
        "host": ep.host,
        "port": ep.port,
        "database": ep.database,
        "schema": ep.schema,
        "table": ep.table,
        "collection": ep.collection,
        "username": ep.username,
        "password": ep.password,
        "connection_string": ep.connection_string,
        "warehouse": ep.warehouse,
        "ssl": ep.ssl,
        "auth_mode": ep.auth_mode,
        "auth_role": ep.auth_role,
        "auth_source": ep.auth_source,
        "api_key": ep.api_key,
        "service_account": ep.service_account,
        "private_key": ep.private_key,
        "output_path": ep.output_path,
        "region": ep.region,
        "endpoint_url": ep.endpoint_url,
        "path_style": ep.path_style,
        "extra": dict(ep.extra or {}),
    }


def transfer_request_to_dict(request: TransferRequest) -> dict:
    return {
        "source": _encrypt_endpoint_secrets(endpoint_to_dict(request.source)),
        "destination": _encrypt_endpoint_secrets(endpoint_to_dict(request.destination)),
        "mappings": request.mappings,
        "column_types": request.column_types,
        "skip_preflight": request.skip_preflight,
        "source_filename": request.source_filename,
        "sync_mode": request.sync_mode,
        "schema_policy": request.schema_policy,
        "validation_mode": request.validation_mode,
        "backfill_new_fields": request.backfill_new_fields,
        "write_via_staging": request.write_via_staging,
        "source_filter": request.source_filter,
        "read_options": request.read_options,
        "shape_recipe": dict(request.shape_recipe or {}),
        "approved_shape_recipe_hash": request.approved_shape_recipe_hash or "",
        "transform_recipe_id": request.transform_recipe_id or "",
        "transform_recipe_version": int(request.transform_recipe_version or 0),
        "transform_recipe_version_mode": request.transform_recipe_version_mode
        or "pin",
        "transform_recipe_params": dict(request.transform_recipe_params or {}),
        "approved_set_hash": request.approved_set_hash or "",
        "priority_column": request.priority_column,
        "priority_direction": request.priority_direction,
        "limit": request.limit,
        "workspace_id": request.workspace_id,
        "data_region": request.data_region,
        "stream_contracts": request.stream_contracts,
        "contract_id": request.contract_id,
        "enforce_contract": request.enforce_contract,
        "require_signed_contract": request.require_signed_contract,
        "triggered_by": request.triggered_by,
        "date_locale": request.date_locale,
        "number_locale": request.number_locale,
        "number_locale_assumed": bool(request.number_locale_assumed),
        "idempotency_key": request.idempotency_key,
        "delivery_guarantee": request.delivery_guarantee or "at_least_once",
        "compliance_acknowledged": bool(request.compliance_acknowledged),
        "schema_drift_acknowledged": bool(request.schema_drift_acknowledged),
        "fk_risk_acknowledged": bool(request.fk_risk_acknowledged),
        "ddl_changes_acknowledged": bool(request.ddl_changes_acknowledged),
        "empty_source_overwrite_acknowledged": bool(
            request.empty_source_overwrite_acknowledged
        ),
        # Run semantics the persisted job must carry: rerun / retry / resume
        # rebuild the request from this dict, and from_dict already restores
        # them — leaving them out silently ran the rerun with the defaults.
        "source_timezone": request.source_timezone or "",
        "duplicate_policy": request.duplicate_policy or "",
        "allow_duplicates": bool(request.allow_duplicates),
        "file_wait_seconds": int(request.file_wait_seconds or 0),
        "load_volume_acknowledged": bool(request.load_volume_acknowledged),
        "preflight_scope": request.preflight_scope or "",
        "parent_job_id": request.parent_job_id or "",
        "historical_resync": bool(request.historical_resync),
        "acknowledgment_actor": request.acknowledgment_actor or "",
        "acknowledgment_reason": request.acknowledgment_reason or "",
        "approved_ddl_identity_hash": request.approved_ddl_identity_hash or "",
        "approved_decision_artifact_hash": request.approved_decision_artifact_hash or "",
        "decision_artifact": dict(request.decision_artifact or {}),
        # Path/URI are durable; never embed source_content bytes in Mongo.
        "source_path": request.source_path or "",
        "source_object_uri": request.source_object_uri or "",
        "requires_file_reupload": _requires_file_reupload_flag(request),
    }


def _requires_file_reupload_flag(request: TransferRequest) -> bool:
    """True when claim/restart cannot recover file bytes from path or object URI.

    In-memory ``source_content`` alone is not durable across claim-queue workers —
    callers must :func:`persist_file_source` before serializing.
    """
    from services.transfer_file_staging import requires_file_reupload

    return requires_file_reupload(request)


def _mask_endpoint_secrets(ep: dict | None) -> dict:
    """Return a copy of an endpoint dict with secret fields masked for API responses."""
    import copy
    import re

    out = copy.deepcopy(ep or {})
    for key in _SECRET_ENDPOINT_KEYS:
        if out.get(key):
            if key == "connection_string" and isinstance(out[key], str):
                out[key] = re.sub(r":([^:@/]+)@", ":****@", out[key])
            else:
                out[key] = "****"
    extra = out.get("extra")
    if isinstance(extra, dict):
        for key in list(extra.keys()):
            lk = str(key).lower()
            if any(s in lk for s in ("password", "secret", "token", "private_key", "api_key")):
                if extra.get(key):
                    extra[key] = "****"
    return out


def sanitize_job_for_api(job: dict) -> dict:
    """Strip plaintext connector secrets from a job document before returning to clients."""
    import copy

    out = copy.deepcopy(job or {})
    tr = out.get("transfer_request")
    if isinstance(tr, dict):
        if isinstance(tr.get("source"), dict):
            tr["source"] = _mask_endpoint_secrets(tr["source"])
        if isinstance(tr.get("destination"), dict):
            tr["destination"] = _mask_endpoint_secrets(tr["destination"])
        out["transfer_request"] = tr
    _project_operator_quarantine(out)
    from src.ai.copilot.job_reads import job_lineage

    lineage = job_lineage(out)
    if lineage["rerun_of"]:
        out["rerun_of"] = lineage["rerun_of"]
    if lineage["parent_job_id"]:
        out["parent_job_id"] = lineage["parent_job_id"]
    return out


def _project_operator_quarantine(job: dict) -> None:
    """Copy the job already. Show SQL NULL as null, not the transfer token.

    Replay reads the stored job, which still carries the token so NULL and
    an empty string stay distinct on the write path.
    """
    from services.dest_quarantine import project_operator_quarantine_details

    if isinstance(job.get("rejected_details"), list):
        job["rejected_details"] = project_operator_quarantine_details(job["rejected_details"])
    summary = job.get("destination_summary")
    if isinstance(summary, dict):
        for key in ("rejected_details", "rejected_details_sample"):
            if isinstance(summary.get(key), list):
                summary[key] = project_operator_quarantine_details(summary[key])
    checkpoint = job.get("checkpoint")
    if isinstance(checkpoint, dict) and isinstance(checkpoint.get("rejected_details"), list):
        checkpoint["rejected_details"] = project_operator_quarantine_details(
            checkpoint["rejected_details"]
        )


def _confined_source_path(raw: object) -> str:
    """Keep a serialized ``source_path`` only when it lies under the upload roots.

    A path outside them is dropped, so the request falls back to its stored
    file id / object URI or reports that the file must be re-uploaded.
    """
    path = str(raw or "").strip()
    if not path:
        return ""
    from services.dataset_file import confine_source_path

    try:
        return str(confine_source_path(path))
    except ValueError:
        import logging

        logging.getLogger(__name__).warning(
            "transfer request source_path refused: outside upload roots"
        )
        return ""


def transfer_request_from_dict(data: dict) -> TransferRequest:
    src = _decrypt_endpoint_secrets(data.get("source") or {})
    dst = _decrypt_endpoint_secrets(data.get("destination") or {})
    return TransferRequest(
        source=EndpointConfig.from_dict(src.get("kind", "database"), src),
        destination=EndpointConfig.from_dict(dst.get("kind", "database"), dst),
        source_timezone=str(data.get("source_timezone") or "").strip(),
        mappings=data.get("mappings") or [],
        column_types=data.get("column_types") or {},
        skip_preflight=bool(data.get("skip_preflight")),
        source_filename=data.get("source_filename") or "",
        source_content=b"",
        source_path=_confined_source_path(data.get("source_path")),
        source_object_uri=str(data.get("source_object_uri") or "").strip(),
        sync_mode=data.get("sync_mode") or "full_refresh_append",
        schema_policy=data.get("schema_policy") or "manual_review",
        validation_mode=data.get("validation_mode") or "strict",
        backfill_new_fields=bool(data.get("backfill_new_fields")),
        write_via_staging=bool(data.get("write_via_staging")),
        source_filter=data.get("source_filter") or {},
        read_options=data.get("read_options") or {},
        shape_recipe=data.get("shape_recipe") or {},
        approved_shape_recipe_hash=str(
            data.get("approved_shape_recipe_hash") or ""
        ).strip(),
        transform_recipe_id=str(data.get("transform_recipe_id") or "").strip(),
        transform_recipe_version=int(data.get("transform_recipe_version") or 0),
        transform_recipe_version_mode=(
            str(data.get("transform_recipe_version_mode") or "pin").strip().lower()
            or "pin"
        ),
        transform_recipe_params=dict(data.get("transform_recipe_params") or {})
        if isinstance(data.get("transform_recipe_params"), dict)
        else {},
        approved_set_hash=str(data.get("approved_set_hash") or "").strip(),
        priority_column=(data.get("priority_column") or "").strip(),
        priority_direction=(data.get("priority_direction") or "desc").lower(),
        limit=int(data.get("limit") or 0),
        workspace_id=(data.get("workspace_id") or "").strip(),
        data_region=(data.get("data_region") or "").strip(),
        stream_contracts=data.get("stream_contracts") or [],
        contract_id=data.get("contract_id") or "",
        enforce_contract=bool(data.get("enforce_contract", True)),
        require_signed_contract=bool(data.get("require_signed_contract", False)),
        triggered_by=(data.get("triggered_by") or "").strip(),
        date_locale=(data.get("date_locale") or "").strip().upper(),
        number_locale=(
            loc
            if (loc := (data.get("number_locale") or "").strip().upper()) in {"US", "EU"}
            else ""
        ),
        number_locale_assumed=bool(data.get("number_locale_assumed")),
        idempotency_key=(data.get("idempotency_key") or "").strip(),
        delivery_guarantee=(data.get("delivery_guarantee") or "at_least_once").strip().lower(),
        compliance_acknowledged=bool(data.get("compliance_acknowledged")),
        schema_drift_acknowledged=bool(data.get("schema_drift_acknowledged")),
        fk_risk_acknowledged=bool(data.get("fk_risk_acknowledged")),
        ddl_changes_acknowledged=bool(data.get("ddl_changes_acknowledged")),
        empty_source_overwrite_acknowledged=bool(
            data.get("empty_source_overwrite_acknowledged")
        ),
        load_volume_acknowledged=bool(data.get("load_volume_acknowledged")),
        preflight_scope=str(data.get("preflight_scope") or "").strip(),
        parent_job_id=str(data.get("parent_job_id") or "").strip(),
        rerun_of=str(data.get("rerun_of") or "").strip(),
        duplicate_policy=str(data.get("duplicate_policy") or "").strip().lower(),
        allow_duplicates=bool(data.get("allow_duplicates")),
        file_wait_seconds=max(0, int(data.get("file_wait_seconds") or 0)),
        historical_resync=bool(data.get("historical_resync")),
        acknowledgment_actor=str(data.get("acknowledgment_actor") or "").strip(),
        acknowledgment_reason=str(data.get("acknowledgment_reason") or "").strip(),
        approved_ddl_identity_hash=str(
            data.get("approved_ddl_identity_hash") or ""
        ).strip(),
        approved_decision_artifact_hash=str(
            data.get("approved_decision_artifact_hash") or ""
        ).strip(),
        decision_artifact=dict(data.get("decision_artifact") or {}),
    )


@dataclass
class TransferResult:
    success: bool
    job_id: str = ""
    records_transferred: int = 0
    operation: str = ""
    source_summary: dict = field(default_factory=dict)
    destination_summary: dict = field(default_factory=dict)
    ddl_executed: list[str] = field(default_factory=list)
    error: str = ""
    error_details: dict = field(default_factory=dict)
    columns: list[str] = field(default_factory=list)
    reconciliation: dict = field(default_factory=dict)
    validation_plan: dict = field(default_factory=dict)
    payload_shape: dict = field(default_factory=dict)
    contract_id: str = ""
    explanation: str = ""
    mapping_proof: dict = field(default_factory=dict)
    elapsed_seconds: float = 0.0
    records_per_second: float = 0.0
    peak_memory_bytes: int = 0
    # Independent dest COUNT(*) ledger — never closed by writer ack.
    row_accounting: dict = field(default_factory=dict)


@dataclass
class TransferCapabilities:
    live_combinations: list[dict]
    source_formats: list[str]
    destination_types: list[str]
    operations: list[str]
