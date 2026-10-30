"""Preflight validation for DataTransfer transfers."""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterable, Mapping
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

#: Longest a planning tool spends proving an existing destination's upsert
#: keys unique (D187). Unfinished warns; it is never reported clean.
DEST_DUPLICATE_KEY_STEP_SECONDS = 8.0

_ps = sys.modules[__name__]

# Add preflight package to path
# apps/api/services → repo root is parents[3]
_PREFLIGHT_ROOT = Path(__file__).resolve().parents[3] / "packages" / "preflight" / "src"
if str(_PREFLIGHT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PREFLIGHT_ROOT))

from preflight import PreflightEngine
from preflight.models import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    SourceConfig,
    TransferPlan,
)

from services.connector_capability_registry import (
    classify_payload,
    recommended_batch_size,
)
from services.data_profiler import source_types_are_authoritative
from services.db_type_utils import (
    DESTS_WITHOUT_SCHEMA_RECREATE,
    NO_RELATIONAL_DDL_DESTS,
    SCHEMALESS_DESTS,
    dest_declares_column_ddl,
    normalize_dest_kind,
)
from services.destination_key_collision_probe import probe_append_key_collisions
from services.preflight_file_plan import (
    _fit_scan_deadline,
    _lift_in_file_collapse_advisories,
    _plan_duplicate_policy,
    _plan_in_file_dedupe,
)
from services.coercion_gate_reconcile import reconcile_coercion_report
from services.preflight_file_phases import (
    apply_constraint_findings,
    apply_schema_drift_gate,
)
from services.destination_requirements_gate import build_mapping_contract_gates
from services.preflight_cursor_gate import (
    resolve_read_scope as _resolve_read_scope,
)
from services.source_duplicate_probe import probe_source_duplicate_keys_result
from services.transform_engine import (
    ambiguous_date_columns,
    ambiguous_number_columns,
    assumed_number_locale,
    canonical_date_locale,
    canonical_number_locale,
    infer_number_locale,
    reset_active_date_locale,
    reset_active_number_locale,
    set_active_date_locale,
    set_active_number_locale,
)
from services.validation_plan import build_validation_plan
from services.value_serializer import wire_cell_text
from services.brand import PRODUCT_NAME


def _hydrate_risk_contract(
    m: dict[str, Any],
    *,
    table: str = "",
    migration_id: str = "",
) -> dict[str, Any] | None:
    """Verify or sign a Migration Risk Contract draft — SSOT in migration_risk_contract."""
    from services.migration_risk_contract import hydrate_risk_contract_dict

    return hydrate_risk_contract_dict(m, table=table, migration_id=migration_id)


def _with_date_locale(fn):
    """Set date_locale and number_locale for the duration of the call."""

    def wrapper(*args, **kwargs):
        date_token = set_active_date_locale(kwargs.get("date_locale", ""))
        number_token = set_active_number_locale(kwargs.get("number_locale", ""))
        from services.transform_engine import (
            reset_strict_number_reading,
            set_strict_number_reading,
        )

        # Same rule as Execute (engine): strict + no declared locale.
        strict_token = set_strict_number_reading(
            str(kwargs.get("validation_mode") or "strict").strip().lower() == "strict"
            and not str(kwargs.get("number_locale") or "").strip()
        )
        try:
            return fn(*args, **kwargs)
        finally:
            reset_strict_number_reading(strict_token)
            reset_active_number_locale(number_token)
            reset_active_date_locale(date_token)

    return wrapper


def _preflight_source_engine(kwargs: Mapping[str, Any]) -> str:
    fmt = str(kwargs.get("source_format") or "").strip().lower()
    if fmt:
        return fmt
    cfg = kwargs.get("source_config")
    if isinstance(cfg, Mapping):
        fmt = str(cfg.get("type") or cfg.get("format") or "").strip().lower()
        if fmt:
            return fmt
    kind = str(kwargs.get("source_kind") or "file").strip().lower()
    return "" if kind in {"database", "cloud", ""} else kind


def _with_source_engine(fn):
    """Bind the source engine for every gate, as Execute does for the whole job.

    Binding it only around DDL stamping left the gates judging an unknown
    engine, so Validate refused (PostgreSQL ``VARCHAR`` → SQL Server
    ``NVARCHAR``) what Execute's own preflight, run inside the job's binding,
    accepted.
    """

    def wrapper(*args, **kwargs):
        from services.source_engine_scope import active_source_engine, bind_source_engine

        engine = _preflight_source_engine(kwargs)
        if not engine or active_source_engine():
            return fn(*args, **kwargs)
        with bind_source_engine(engine):
            return fn(*args, **kwargs)

    return wrapper


VALIDATION_CONFIDENCE_THRESHOLDS = {
    "balanced": 0.75,
    "lenient": 0.75,
    "migration": 0.75,
    "strict": 0.85,
    "audit": 0.85,
    "maximum": 0.95,
    "discovery": 0.0,
}


def apply_readiness_honesty_caps(out: dict[str, Any]) -> dict[str, Any]:
    """Cap Validate readiness when G9 uniqueness is sample-only.

    Gate pass-ratio can be 100% while population uniqueness is unproven.
    Fivetran/Airbyte do not call that certified-ready. Execute may still
    proceed; the score must not look like a migration certificate.
    """
    payload = out if isinstance(out, dict) else {}
    reasons: list[str] = []
    for gate in payload.get("gates") or []:
        if not isinstance(gate, dict):
            continue
        gid = str(gate.get("id") or "").lower()
        details = gate.get("details") if isinstance(gate.get("details"), dict) else {}
        coverage = str(details.get("coverage") or "").lower()
        msg = str(gate.get("message") or "").lower()
        if "g9" in gid or "data_integrity" in gid:
            if (
                coverage == "sample"
                or "population uniqueness not proven" in msg
                or "validate sample only" in msg
            ):
                reasons.append("g9_sample_uniqueness")
        if "g5" in gid or "dry_run" in gid:
            if (
                coverage == "sample"
                or details.get("sample_cap") is not None
                or "sample" in msg
                or gid == "g5_dry_run"
            ):
                reasons.append("g5_sample_dry_run")
    if reasons:
        try:
            score = float(payload.get("readiness_score") or 0)
        except (TypeError, ValueError):
            score = 0.0
        payload["readiness_score"] = min(score, 92.0)
        if "g9_sample_uniqueness" in reasons:
            payload["population_uniqueness_proven"] = False
        payload["readiness_cap_reason"] = "+".join(dict.fromkeys(reasons))
    return payload


def _block_decision_on_hard_gate(out: dict[str, Any]) -> dict[str, Any]:
    """A block-status gate can never sit beside ``decision=approve``.

    The proof bundle is stamped before the hosted gates (g3f population fit,
    additive stamp) run; without this a g3f block left "No blocking issues
    detected" on the verdict that Execute and Pilot read (QA MX3-22).
    """
    pb = out.get("proof_bundle")
    td = (pb or {}).get("transfer_decision") if isinstance(pb, dict) else None
    if out.get("passed") is not False or not isinstance(td, dict):
        return out
    if str(td.get("decision") or "").lower() != "approve":
        return out
    blocking = [
        g for g in out.get("gates") or []
        if isinstance(g, dict) and g.get("status") == "block"
    ]
    if not blocking:
        return out
    messages = [str(g.get("message") or g.get("id") or "") for g in blocking]
    logger.warning(
        "preflight verdict demoted approve->block: gate(s) %s blocked after the "
        "proof bundle was stamped",
        [g.get("id") for g in blocking],
    )
    out["proof_bundle"] = {
        **pb,
        "passed": False,
        "transfer_decision": {
            **td,
            "decision": "block",
            "blockers": list(dict.fromkeys([*(td.get("blockers") or []), *messages])),
            "reason": "Blocking issues detected: " + "; ".join(messages[:3]),
        },
    }
    return out


def confidence_threshold_for_mode(validation_mode: str | None) -> float:
    try:
        from services.validation_mode_contract import confidence_floor_for_mode

        return confidence_floor_for_mode(validation_mode)
    except Exception:
        return VALIDATION_CONFIDENCE_THRESHOLDS.get(
            (validation_mode or "strict").lower(), 0.85
        )


def _policy_source_config(
    source_config: dict[str, Any] | None,
    source_endpoint: Any,
) -> dict[str, Any] | None:
    """Connection facts for the CDC log probe.

    ``plan_transfer`` and Studio Validate used to pass the connector only as
    ``source_endpoint``. The wal_level probe read ``source_config``, got
    nothing, and a CDC plan on ``wal_level=replica`` stayed safe (R3-H1).
    """
    def _connectable(cfg: Any) -> bool:
        return isinstance(cfg, dict) and bool(
            cfg.get("host") or cfg.get("connection_string")
        )

    if _connectable(source_config):
        return source_config
    if _connectable(source_endpoint):
        return source_endpoint
    if source_endpoint is not None:
        try:
            from src.transfer.adapters import resolve_connector_config

            resolved = resolve_connector_config(source_endpoint)
        except Exception:
            logger.debug("CDC policy could not read the source endpoint", exc_info=True)
            resolved = None
        if isinstance(resolved, dict) and (
            resolved.get("host") or resolved.get("connection_string") or resolved.get("type")
        ):
            return resolved
    if isinstance(source_endpoint, dict):
        return source_endpoint
    return source_config if isinstance(source_config, dict) else None


# F8: policy-gate merge lives in preflight_policy_gates (single authority).


@_with_date_locale
@_with_source_engine
def run_file_preflight(
    *,
    columns: list[str],
    column_types: dict[str, str],
    row_count: int | None,
    mappings: list[dict[str, Any]],
    column_nullability: dict[str, bool] | None = None,
    destination_connected: bool = False,
    destination_error: str | None = None,
    source_connected: bool = True,
    source_error: str | None = None,
    source_kind: str = "file",
    source_format: str = "",
    sync_mode: str = "append",
    sample_rows: list[dict] | None = None,
    estimated_bytes: int = 0,
    confidence_threshold: float = 0.85,
    validation_mode: str = "strict",
    destination_column_types: dict[str, str] | None = None,
    #: Live destination DDL on a run that recreates the table. Callers that
    #: already clear ``destination_column_types`` for an overwrite (Execute
    #: does, so no typing decision reads a doomed carrier) pass the declared
    #: types here for G19, which is the one gate that must see them.
    destination_live_column_types: dict[str, str] | None = None,
    destination_column_nullability: dict[str, bool] | None = None,
    destination_column_defaults: dict[str, str] | None = None,
    destination_identity_columns: list[str] | None = None,
    destination_generated_columns: list[str] | None = None,
    destination_table_exists: bool | None = None,
    destination_can_create: bool | None = None,
    destination_can_write: bool | None = None,
    privilege_probe: dict[str, Any] | None = None,
    redshift_staging_probe: dict[str, Any] | None = None,
    available_staging_bytes: int | None = None,
    destination_db_type: str = "postgresql",
    source_connector_id: str = "",
    source_config: dict[str, Any] | None = None,
    source_table: str = "",
    destination_table: str = "",
    destination_connector_id: str = "",
    source_filename: str = "",
    source_file_id: str = "",
    schema_policy: str = "manual_review",
    preflight_scope: str = "",
    backfill_new_fields: bool = False,
    stored_source_fp: str = "",
    stored_target_fp: str = "",
    declared_source_columns: list[str] | None = None,
    declared_source_schema: dict[str, str] | None = None,
    previous_source_columns: list[str] | None = None,
    previous_source_schema: dict[str, str] | None = None,
    contract_primary_key: str | None = None,
    destination_pk_columns: list[str] | None = None,
    destination_dynamo_key_schema: list[dict[str, Any]] | None = None,
    destination_dynamo_index_attributes: dict[str, str] | None = None,
    destination_unique_keys: list[dict[str, Any]] | None = None,
    destination_foreign_keys: list[dict[str, Any]] | None = None,
    destination_check_constraints: list[Any] | None = None,
    destination_config: Mapping[str, Any] | None = None,
    stream_contracts: list[dict[str, Any]] | None = None,
    date_locale: str = "",
    number_locale: str = "",
    cursor_fields: list[str] | None = None,
    compliance_acknowledged: bool = False,
    schema_drift_acknowledged: bool = False,
    fk_risk_acknowledged: bool = False,
    # Operator approved the DDL DataFlow disclosed for this run (e.g. the
    # unique index an upsert needs on an existing destination table — D101).
    ddl_changes_acknowledged: bool = False,
    # Operator chose to empty a populated destination from a 0-row source (D56).
    empty_source_overwrite_acknowledged: bool = False,
    acknowledgment_actor: str = "",
    acknowledgment_reason: str = "",
    run_population_orphan_scan: bool = False,
    resume: bool = False,
    population_rows: Iterable[Mapping[str, Any]] | None = None,
    population_replay: Callable[[], Iterable[Mapping[str, Any]] | None] | None = None,
    rows_are_population: bool = False,
    shape_recipe: Mapping[str, Any] | None = None,
    source_filter: Mapping[str, Any] | None = None,
    fit_scan_deadline: float | None = None,
    fit_scan_seconds: float | None = None,
    on_fit_progress: Any = None,
    skip_population_fit: bool = False,
    source_population_counted: bool = False,
) -> dict[str, Any]:
    """Run preflight gates for file/DB Studio transfers (G1–G9 + host policy)."""
    from services.schema_drift import (
        PROPAGATE_POLICIES,
        destination_evolution_approved,
        effective_schema_policy,
    )
    from services.timezone_policy import declared_source_column_types

    # ``backfill_new_fields`` is a request for propagate. Resolved once so
    # drift detection, the DDL checks and the evolution plan below all read the
    # policy G10 reports, not the pre-coercion one (D20).
    schema_policy = effective_schema_policy(
        schema_policy, backfill_new_fields=backfill_new_fields
    )

    # A zone declared on Map is a fact about the source, so every gate below
    # reads the declared type rather than the zoneless one it was introspected
    # with. Done once here because this is the single entry every caller
    # (Studio, Pilot, scheduled runs) goes through.
    column_types = declared_source_column_types(column_types, mappings)

    # Canonical sync vocabulary — G9/DDL/policy must match writers (insert→append).
    try:
        from services.sync_cursor import normalize_sync_mode

        sync_mode = normalize_sync_mode(sync_mode)
    except Exception:
        sync_mode = (sync_mode or "").strip().lower() or "full_refresh_append"

    # Relational overwrite empties the existing table and keeps its constraints,
    # so the live column types stay the contract. Only an engine that still
    # DROP+CREATEs (see dest_schema_is_recreated_on_overwrite) replaces the
    # carrier: there a stale VARCHAR(64) must not refuse a TEXT source whose
    # own CREATE would have been LONGTEXT. Append/upsert/CDC/mirror keep the
    # live contract too.
    from services.db_type_utils import dest_schema_is_recreated_on_overwrite
    from services.sync_cursor import is_overwrite_sync

    dest_recreated = is_overwrite_sync(sync_mode) and dest_schema_is_recreated_on_overwrite(
        destination_db_type
    )
    # Relational overwrite keeps the table but replaces every row — G15 must
    # say "replace", not "insert more" (QA T21).
    from services.db_type_utils import overwrite_replaces_rows

    dest_emptied = (
        is_overwrite_sync(sync_mode)
        and not dest_recreated
        and overwrite_replaces_rows(destination_db_type)
    )
    if (
        destination_table_exists is True
        and not dest_recreated
        and destination_live_column_types
    ):
        # The table stays, so a planned recreate DDL is not the carrier the
        # rows hit. Measured types override the plan. A caller that cleared
        # the plan because it still thought overwrite dropped the table still
        # has the live catalog.
        merged = dict(destination_column_types or {})
        merged.update(destination_live_column_types)
        destination_column_types = merged

    # Every sample-judging gate below needs rows. A caller that posted none
    # (failed copilot sampler, a client that never fetched) gets the Execute
    # reader's own sample; a read that fails is named on Gate-8, not hidden.
    sample_unavailable_reason = ""
    source_measured_empty = False
    engine_sample = None
    if not sample_rows:
        from services.coercion_probe import PREFLIGHT_SAMPLE_LIMIT as _SAMPLE_LIMIT
        from services.preflight_sample import engine_sample_rows

        _cfg = dict(source_config or {})
        _extra = _cfg.get("extra") if isinstance(_cfg.get("extra"), dict) else {}
        engine_sample = engine_sample_rows(
            source_kind=source_kind,
            source_format=source_format,
            source_connector_id=source_connector_id,
            source_config=source_config,
            source_table=source_table,
            limit=_SAMPLE_LIMIT,
            source_filter=source_filter
            or (_cfg.get("source_filter") if isinstance(_cfg.get("source_filter"), dict) else None)
            or (_extra.get("source_filter") if isinstance(_extra.get("source_filter"), dict) else None),
        )
        if engine_sample.rows:
            sample_rows = engine_sample.rows
        sample_unavailable_reason = engine_sample.unavailable_reason
        source_measured_empty = engine_sample.measured_empty
        if not source_error and sample_unavailable_reason:
            # Catalog columns without SELECT are not a readable source.
            # Leaving this off source.error made Gate-1 pass and the cast
            # contract the remediation for a grant the role does not have.
            from services.preflight_sample import source_read_is_privilege_denial

            if source_read_is_privilege_denial(sample_unavailable_reason):
                source_error = (
                    "SELECT was denied on this table. Columns visible in the catalog "
                    "are not a readable source. Grant SELECT to the connector role, "
                    f"then re-validate. ({sample_unavailable_reason})"
                )
            elif engine_sample.read_failed:
                # QA MX3-22: G1 said "Source readable — N columns" beside a
                # reader that had just raised. Catalog metadata is not a
                # readable source; the probe failure is the G1 finding.
                source_error = (
                    "The source catalog lists this table but Execute's reader "
                    "could not read rows from it. Fix the source read (connection, "
                    f"table name, timeout), then re-validate. ({sample_unavailable_reason})"
                )
    # A measured COUNT(*) of 0 is the population. Requiring a sample row
    # blocked an empty table whose catalog count was already 0 (R8-005).
    # A failed read still blocks — a count of 0 does not excuse a grant error.
    if (
        source_population_counted
        and isinstance(row_count, int)
        and row_count == 0
        and not sample_rows
        and not source_measured_empty
        and not (engine_sample is not None and engine_sample.read_failed)
    ):
        source_measured_empty = True
    if (
        not sample_rows
        and not source_measured_empty
        and rows_are_population
        and not (isinstance(row_count, int) and row_count > 0)
    ):
        from services.preflight_sample import peek_population_empty

        population_rows, source_measured_empty = peek_population_empty(population_rows)
    if source_measured_empty and not source_types_are_authoritative(
        source_kind, source_format
    ):
        from services.source_schema_authority import (
            empty_source_column_types,
            restamp_mapping_source_types,
        )

        column_types = empty_source_column_types(
            column_types,
            mappings,
            declared=declared_source_schema,
            previous=previous_source_schema,
            destination=destination_live_column_types or destination_column_types,
        )
        mappings = restamp_mapping_source_types(mappings, column_types)

    # Sources with no cheap cardinality — a DynamoDB Scan, a Kafka topic, a
    # search index — report ``None`` rather than inventing a total, which is the
    # honest answer and used to crash this comparison before a single gate ran.
    # Unknown and zero are handled the same way here (size from the sample);
    # the distinction stays visible downstream, where an unmeasured source count
    # is already reported as such rather than as a measured zero.
    row_count_known = isinstance(row_count, int) and row_count > 0
    if not row_count_known:
        row_count = len(sample_rows or [])

    # Preflight is a sample-based safety check, not a full table scan.  Cap the
    # sample size so very large file previews or database samples cannot make the
    # validate step hang. Keep in sync with coercion_probe.DEFAULT_SAMPLE_LIMIT.
    from services.coercion_probe import PREFLIGHT_SAMPLE_LIMIT

    if sample_rows and len(sample_rows) > PREFLIGHT_SAMPLE_LIMIT:
        from services.sample_strategy import position_reservoir_sample

        sample_rows = position_reservoir_sample(sample_rows, PREFLIGHT_SAMPLE_LIMIT)

    # Sign Migration Risk Contract drafts before gates / proof_bundle see them.
    # Stamp destination table onto unsigned drafts (audit field — never invent DDL).
    hydrated_mappings: list[dict[str, Any]] = []
    for m in mappings or []:
        row = dict(m) if isinstance(m, dict) else dict(m or {})
        signed = _hydrate_risk_contract(
            row,
            table=str(destination_table or ""),
            migration_id="",
        )
        if signed is not None:
            row["risk_contract"] = signed
            # Continue-policy contract implies operator ack for G3/G4.
            from services.migration_risk_contract import contract_clears_validate_block

            if contract_clears_validate_block(signed):
                row["risk_acknowledged"] = True
        hydrated_mappings.append(row)
    mappings = hydrated_mappings
    _unstamped_additive: list[str] = []

    # Only the allowlisted tokens are locales. Anything else is Auto, so a
    # typo or a non-locale string cannot skip inference or land on the report.
    date_locale = canonical_date_locale(date_locale)
    number_locale = canonical_number_locale(number_locale)
    # Operator locale wins. Otherwise adopt one inferred order only when
    # every date column agrees.
    if sample_rows and columns and not date_locale:
        from services.preflight_source_kind import agreed_sample_date_locale

        agreed = agreed_sample_date_locale(sample_rows, columns)
        if agreed:
            date_locale = agreed
            set_active_date_locale(date_locale)
    if sample_rows and columns:
        inferred_numbers = infer_number_locale(
            sample_rows, columns, existing_locale=number_locale
        )
        if inferred_numbers and not number_locale:
            number_locale = inferred_numbers
            set_active_number_locale(number_locale)

    if not number_locale:
        # Same contract Execute runs under: values a typed source rendered are
        # canonical, so Validate must not refuse NUMERIC(12,3) 10.129 as
        # US/EU-ambiguous and then let (or block) Run under a different rule.
        from src.transfer.connector_capabilities import typed_wire_number_locale

        wire = typed_wire_number_locale(source_kind, source_format)
        if wire:
            number_locale = wire
            set_active_number_locale(number_locale)

    number_locale_assumed: list[dict[str, Any]] = []
    if not number_locale and sample_rows and columns:
        # Nothing declared it, no typed carrier rendered it, and the sample
        # carries no grouping evidence. Assume US rather than refuse the load,
        # and hand the operator the columns it covers so switching to EU is a
        # decision they can see, not a corruption they find later.
        assumed = assumed_number_locale(
            sample_rows, columns, number_locale=number_locale, column_types=column_types
        )
        if assumed:
            number_locale_assumed = ambiguous_number_columns(
                sample_rows,
                columns,
                number_locale=number_locale,
                column_types=column_types,
            )
            number_locale = assumed
            set_active_number_locale(number_locale)

    # If the caller did not supply rich source types, infer them from the sample
    # rows. This keeps schemaless sources (MongoDB, DynamoDB, Redis, S3 JSON) from
    # being treated as all-VARCHAR against a typed warehouse target.
    # A relational source that genuinely declares TEXT for every column is not
    # a schemaless source: re-inferring it turned a SQLite ``id TEXT`` holding
    # "0".."49" into INTEGER here while Execute kept TEXT, so Validate approved a
    # DDL Execute refused to materialize — and had they agreed, leading zeros
    # would have been dropped on the destination.
    # Profiling is per column through ``merge_profiler_schema`` — the same owner
    # and the same precedence the Map step uses. An all-or-nothing gate here
    # ("only infer when *every* declared type is generic") made the two steps
    # read one source differently: DynamoDB declares ``amount`` as ``N`` but
    # ``id`` as ``S``, so Map profiled ``id`` to INTEGER and created BIGINT while
    # preflight kept VARCHAR and then blocked the run as an ``id VARCHAR →
    # BIGINT`` collapse of a carrier the product itself had just invented.
    if (
        sample_rows
        and columns
        and not source_types_are_authoritative(source_kind, source_format)
    ):
        try:
            from services.data_profiler import merge_profiler_schema, profile_dataset
            from services.type_system import destination_carriers_are_inferred

            profiled = profile_dataset(columns, sample_rows).get("schema") or {}
            if profiled:
                declared_types = dict(column_types or {})
                column_types = merge_profiler_schema(
                    declared_types,
                    {c: profiled[c] for c in columns if c in profiled},
                    authoritative_existing=False,
                    # A keyspace / document store declares no numeric width, so
                    # this page's DECIMAL(p,s) is a description of the sample and
                    # not a bound Execute may enforce on the population.
                    source_carriers_sampled=destination_carriers_are_inferred(
                        source_format or source_kind
                    ),
                )
                # Same declared-string rule Map applies (D190 / D194), for the
                # columns that land in text or in a column this run creates. A
                # typed existing column keeps the profile as cast evidence.
                from services.schema_inference import honor_declared_strings
                from services.type_system import normalize_logical_type as _nlt

                dest_types = {
                    str(k).lower(): str(v or "")
                    for k, v in (destination_column_types or {}).items()
                }
                target_of = {
                    str(m.get("source") or ""): str(m.get("target") or m.get("source") or "")
                    for m in (mappings or [])
                    if isinstance(m, dict)
                }
                column_types = honor_declared_strings(
                    column_types,
                    {
                        c: t
                        for c, t in declared_types.items()
                        if not dest_types.get(target_of.get(c, c).lower())
                        or _nlt(dest_types[target_of.get(c, c).lower()])
                        in {"string", "text", "varchar", "unknown"}
                    },
                    {
                        c: [r.get(c) for r in sample_rows if isinstance(r, dict)]
                        for c in columns
                    },
                    source_format,
                )
        except Exception as exc:
            logger.debug("preflight schema inference failed: %s", exc, exc_info=exc)

    # Additive / create-new under backfill: Decision Kernel stamps target_type
    # after source types are known — same invent path writers honor (never bare
    # VARCHAR invent at Execute after Validate green). Use the same effective
    # backfill authority as Execute (schema policy + create_new maps).
    effective_backfill = bool(backfill_new_fields)
    try:
        from services.batch_progress import effective_backfill_new_fields

        effective_backfill = bool(
            effective_backfill_new_fields(
                backfill_new_fields=bool(backfill_new_fields),
                schema_policy=schema_policy,
                mappings=mappings,
                # A table that exists already has the columns a stale create_new
                # stamp names; only a target it lacks proposes ADD COLUMN.
                dest_columns=(
                    list(destination_column_types or {})
                    if destination_table_exists is True and not dest_recreated
                    else None
                ),
            )
        )
    except Exception:
        effective_backfill = bool(backfill_new_fields)
    try:
        from services.decision_kernel import stamp_additive_mapping_types

        samples_by_src: dict[str, list] = {}
        for row in sample_rows or []:
            if not isinstance(row, dict):
                continue
            for k, v in row.items():
                if v is None:
                    continue
                samples_by_src.setdefault(str(k), []).append(v)
        for k in list(samples_by_src.keys()):
            samples_by_src[k] = samples_by_src[k][:32]
        # Validate must invent under the same source-engine identity Execute
        # binds: the engine id decides code-page-safe carriers and whether a
        # declared numeric domain may be sample-narrowed. Unbound here, Map
        # stamped a carrier Execute would not, and the two DDL identities
        # diverged before a single row moved.
        from services.source_engine_scope import (
            active_source_engine,
            bind_source_engine,
        )

        src_engine = (source_format or "").strip().lower() or (
            (source_kind or "").strip().lower()
            if (source_kind or "").strip().lower() not in {"database", "cloud", ""}
            else ""
        )
        stamp_scope = (
            bind_source_engine(src_engine)
            if src_engine and not active_source_engine()
            else nullcontext()
        )
        with stamp_scope:
            mappings, _unstamped_additive = stamp_additive_mapping_types(
                mappings,
                dest_db=destination_db_type or "",
                live_dest_types=destination_column_types or {},
                source_types=column_types or {},
                samples_by_source=samples_by_src,
                backfill_new_fields=bool(effective_backfill),
                dest_table_exists=destination_table_exists,
            )
    except Exception as stamp_exc:
        # Fail-closed: stamp failure must surface as unstamped additives, not
        # clear the list and let Validate green-wash Map VARCHAR invent.
        logger.warning(
            "additive Map stamp failed (fail-closed): %s", stamp_exc, exc_info=stamp_exc
        )
        _unstamped_additive = [
            str(m.get("target") or m.get("source") or "?")
            for m in (mappings or [])
            if isinstance(m, dict)
            and (
                bool(m.get("create_new"))
                or str(m.get("assignment_strategy") or "")
                in {"create_compatible_new", "identity_passthrough"}
                or effective_backfill
            )
            and not str(m.get("target_type") or m.get("dest_type") or "").strip()
        ]

    # Approved destination evolution (widen, ADD COLUMN for a mapped new
    # column) is applied to the plan once, here, so every gate below grades the
    # source against the destination this run leaves behind. Patching the live
    # types gate by gate left schema contract, mapping confidence, data
    # integrity and population fit each refusing a widen the operator had
    # approved (D107), and a mapped new column blocked under an acknowledgement
    # (D20). Unapproved runs keep the strict live contract.
    destination_evolution: list[dict[str, Any]] = []
    # The Decision Artifact fingerprints the table as it stands: "dest drifted
    # since Validate" compares it with the live table at Execute, which has not
    # been ALTERed yet.
    live_destination_types = dict(destination_column_types or {})
    if (
        destination_table_exists is True
        and not dest_recreated
        and destination_column_types
        and dest_declares_column_ddl(destination_db_type)
        and destination_evolution_approved(
            schema_policy, acknowledged=schema_drift_acknowledged
        )
    ):
        from services.schema_drift import plan_destination_evolution

        mappings, destination_column_types, destination_evolution = (
            plan_destination_evolution(
                mappings,
                source_columns=list(columns),
                source_schema=column_types or {},
                dest_types=destination_column_types,
                dest_db=str(destination_db_type or ""),
                adopt_unmapped=schema_policy in PROPAGATE_POLICIES,
            )
        )

    # Source nullability defaults to True (unknown), which is the safe reading
    # for files. For an introspected database source it is knowable, and
    # assuming otherwise made G3's NOT NULL contract fire on a NOT NULL source
    # column — so copying a table onto an identical table with a PRIMARY KEY
    # blocked with a coercion issue that did not exist.
    src_nulls = column_nullability or {}

    def _src_nullable(name: str) -> bool:
        if name in src_nulls:
            return bool(src_nulls[name])
        return next(
            (bool(v) for k, v in src_nulls.items() if k.lower() == name.lower()),
            True,
        )

    source_cols = [
        ColumnSchema(
            name=c,
            inferred_type=column_types.get(c, "VARCHAR").upper(),
            nullable=_src_nullable(c),
        )
        for c in columns
    ]
    from services.mapping_constraints import is_intentional_omit, write_mappings

    # A sink with no column DDL (document/KV store, object-store prefix) reports
    # types profiled from sampled values. Judging the source against them is
    # judging it against Dataflow's own sample, so the Map stamp is the contract.
    dest_declares_ddl = dest_declares_column_ddl(destination_db_type)
    dest_types = (destination_column_types or {}) if dest_declares_ddl else {}
    dest_nulls = destination_column_nullability or {}
    write_maps = write_mappings(mappings)
    dest_cols = []
    plan_cast_types: dict[str, str] = {}
    for m in write_maps:
        tgt = m.get("target") or ""
        if not tgt:
            continue
        live = dest_types.get(tgt)
        if live is None:
            live = next(
                (
                    dest_types[k]
                    for k in dest_types
                    if str(k).lower() == str(tgt).lower()
                ),
                None,
            )
        if dest_recreated:
            live = None
        # Existing table: only live introspect counts as dest_types for Validate.
        # Map target_type fallback greens empties as VARCHAR while write binds
        # physical DATE/INT — refuse that false-green invent.
        if destination_table_exists is True and dest_declares_ddl and not dest_recreated:
            if not live:
                continue
            inferred = str(live).upper()
        else:
            inferred = str(
                live
                or m.get("target_type")
                or column_types.get(m["source"], "VARCHAR")
            ).upper()
        # Prefer explicit map; else case-insensitive lookup; default nullable=True
        # (create-new / unknown) so we never invent NOT NULL.
        if tgt in dest_nulls:
            nullable = bool(dest_nulls[tgt])
        else:
            nullable = next(
                (bool(dest_nulls[k]) for k in dest_nulls if k.lower() == str(tgt).lower()),
                True,
            )
        if "target_nullable" in m:
            nullable = bool(m.get("target_nullable"))
        dest_cols.append(
            ColumnSchema(name=tgt, inferred_type=inferred, nullable=nullable)
        )
        plan_cast_types[str(tgt)] = inferred

    from services.transform_resolver import write_plan_transform

    def _one_cast_type(mapping: dict) -> str:
        target = str(mapping.get("target") or "")
        chosen = plan_cast_types.get(target)
        if chosen:
            return chosen
        return str(
            mapping.get("target_type")
            or mapping.get("targetType")
            or column_types.get(mapping.get("source") or "", "VARCHAR")
            or "VARCHAR"
        )

    plan_mappings = [
        ColumnMapping(
            source=m["source"],
            target=m.get("target") or "",
            confidence=float(m.get("confidence", 0.0)),
            transform=write_plan_transform(
                {
                    **(m if isinstance(m, dict) else {}),
                    "target_type": _one_cast_type(m if isinstance(m, dict) else {}),
                },
                column_types=column_types,
                dest_types=dest_types,
                destination_table_exists=destination_table_exists,
            ),
            user_override=bool(m.get("user_override", False)),
            reasoning=m.get("reasoning") or m.get("reason", ""),
            requires_review=bool(m.get("requires_review", False)),
            score_gap=float(m.get("score_gap", 1.0)),
            target_type=_one_cast_type(m if isinstance(m, dict) else {}),
            create_new=bool(m.get("create_new") or m.get("createNew", False)),
            struct_policy=m.get("struct_policy") or m.get("structPolicy"),
            struct_derived=bool(
                m.get("struct_derived") or m.get("structDerived", False)
            ),
            struct_parent=m.get("struct_parent") or m.get("structParent"),
            structural_class=m.get("structural_class") or m.get("structuralClass"),
            child_table_spec=(
                m.get("child_table_spec")
                if isinstance(m.get("child_table_spec"), dict)
                else m.get("childTableSpec")
                if isinstance(m.get("childTableSpec"), dict)
                else None
            ),
            fidelity=(m.get("fidelity") or None),
            type_narrowing=bool(m.get("type_narrowing") or m.get("typeNarrowing", False)),
            risk_acknowledged=bool(
                m.get("risk_acknowledged") or m.get("riskAcknowledged", False)
            ),
            intentional_omit=is_intentional_omit(m),
            risk_contract=_hydrate_risk_contract(
                m if isinstance(m, dict) else dict(m or {}),
                table=str(destination_table or ""),
            ),
            assignment_strategy=str(m.get("assignment_strategy") or "") or None,
            review_kind=str(m.get("review_kind") or "") or None,
        )
        for m in mappings
    ]

    has_samples = bool(sample_rows)
    est_bytes = estimated_bytes if estimated_bytes > 0 else max(row_count * 128, 0)
    is_file_source = source_kind == "file"

    if available_staging_bytes is None:
        available_staging_bytes = _available_staging_bytes(est_bytes)

    # Unknown privilege must not invent create-new (Pilot often omits the flag).
    dest_can_create = (
        bool(destination_can_create) if destination_can_create is not None else False
    )
    dest_can_write = (
        bool(destination_can_write)
        if destination_can_write is not None
        else bool(destination_connected)
    )
    # Keep tri-state: None = probe unknown. Never coerce unknown → create-new.
    dest_table_exists = destination_table_exists

    from services.ddl_compatibility import evaluate_ddl_compatibility
    from services.schema_drift import detect_schema_drift

    dest_kind = normalize_dest_kind(destination_db_type, default="postgresql")
    # Document/KV stores and object-store prefixes alike declare no column
    # types: what a probe returns for them was profiled from sampled values, so
    # binding it as the live DDL contract makes the gates judge the source
    # against Dataflow's own sample (``DECIMAL(12,2)`` → ``DECIMAL(2,2)``).
    schemaless = not dest_declares_column_ddl(dest_kind)

    # Split drift vs DDL contracts:
    # - create-new / no dest DDL: no live dest types (stale Studio maps are noise)
    # - unknown existence: keep type hints for G6 lossy/width checks, but drift
    #   still treats the dest as non-live (no fingerprint / orphan locks)
    # - existing table: full live contract
    # - overwrite that really DROP+CREATEs: the carrier standing there now is
    #   not the one the rows land in. Judging against it refused a run for loss
    #   that cannot happen (``TEXT → VARCHAR(64)`` on a route whose own DDL
    #   creates ``LONGTEXT``). Relational overwrite keeps the live table.
    hinted_dest_types = dict(destination_column_types or {})
    if schemaless or dest_table_exists is False or dest_recreated:
        drift_dest_types: dict[str, str] = {}
        ddl_dest_types: dict[str, str] = {}
    elif dest_table_exists is True:
        drift_dest_types = hinted_dest_types
        ddl_dest_types = hinted_dest_types
    else:
        drift_dest_types = {}
        ddl_dest_types = hinted_dest_types

    read_scope = _resolve_read_scope(
        sync_mode=sync_mode,
        stream_contracts=stream_contracts,
        source_format=source_format,
        source_config=source_config,
        source_table=source_table,
        destination_db_type=destination_db_type,
        destination_config=destination_config,
        destination_table=destination_table,
    )
    pre_gate_fit_attempted = not skip_population_fit
    pre_gate_fit_report = None
    pre_gate_fit_error = ""
    pre_gate_fit_filter: dict[str, Any] = {}
    if pre_gate_fit_attempted:
        try:
            from services.preflight_population import scan_existing_table_population_fit

            (
                pre_gate_fit_report,
                pre_gate_fit_error,
                population_rows,
                rows_are_population,
                pre_gate_fit_filter,
            ) = scan_existing_table_population_fit(
                source_kind=source_kind,
                source_format=source_format,
                source_connector_id=source_connector_id,
                source_config=source_config,
                source_table=source_table,
                source_file_id=source_file_id,
                mappings=mappings,
                column_types=column_types or {},
                destination_column_types=ddl_dest_types,
                destination_db_type=destination_db_type,
                sync_mode=sync_mode,
                read_scope=read_scope,
                source_filter=dict(source_filter or {}),
                population_rows=population_rows,
                rows_are_population=rows_are_population,
                sample_rows=sample_rows or [],
                row_count=row_count,
                validation_mode=validation_mode,
                destination_table_exists=dest_table_exists,
                shape_recipe=dict(shape_recipe or {}),
                columns=columns,
                deadline_monotonic=_fit_scan_deadline(
                    fit_scan_deadline, fit_scan_seconds
                ),
                on_progress=on_fit_progress,
                iter_table_population=_ps._iter_table_population_for_preflight,
                population_replay=population_replay,
            )
        except Exception as fit_exc:
            pre_gate_fit_error = str(fit_exc)[:400]
            logger.warning("pre-gate population fit scan failed: %s", fit_exc, exc_info=fit_exc)
    from services.population_fit_narrowing import proven_width_narrowings

    proven_string_narrowings = proven_width_narrowings(
        pre_gate_fit_report,
        mappings,
        column_types or {},
        ddl_dest_types,
        destination_table_exists=(
            dest_table_exists is True and not dest_recreated and not schemaless
        ),
        dest_db=dest_kind,
    )
    # A width narrowing the exact population scan proved (every value fits)
    # is not a lossy cast for G4 either — g3/G6 already accept it. Run 2 into
    # the key column DataFlow sized on run 1 (MySQL TEXT key -> VARCHAR(768))
    # read back as "TEXT -> VARCHAR(768) lossy" and demanded a contract.
    # Only the width is waived: any other loss on the pair keeps its flag.
    if proven_string_narrowings:
        import re

        from services.type_system import is_lossy_coercion as _lossy

        _waived_pairs: set[tuple[str, str]] = set()
        for _pm in plan_mappings:
            _pair = (str(_pm.source or "").casefold(), str(_pm.target or "").casefold())
            if _pair not in proven_string_narrowings:
                continue
            _dest_by_fold = {
                str(k).casefold(): str(v or "") for k, v in (ddl_dest_types or {}).items()
            }
            _tt = _dest_by_fold.get(_pair[1]) or str(_pm.target_type or "")
            # Only the width may be waived: the pair must be lossless once
            # the carrier is unbounded in the engine's own spelling.
            _src_t = str((column_types or {}).get(_pm.source) or "")
            _width_only = False
            for _unb in ("TEXT", "NVARCHAR(MAX)", "VARCHAR(MAX)", "CLOB"):
                _unbounded = re.sub(
                    r"\b(?:N?VARCHAR2?|N?CHAR|CHARACTER VARYING)\s*\(\s*\d+(?:\s+(?:CHAR|BYTE))?\s*\)",
                    _unb,
                    _tt,
                    count=1,
                    flags=re.I,
                )
                if _unbounded != _tt and not _lossy(_src_t, _unbounded, dest_db=dest_kind):
                    _width_only = True
                    break
            if not _width_only:
                continue
            if str(_pm.fidelity or "").lower() == "lossy_cast":
                _pm.fidelity = None
            _pm.type_narrowing = False
            if str(getattr(_pm, "review_kind", "") or "").lower() == "lossy":
                _pm.requires_review = False
            _waived_pairs.add(_pair)
        if _waived_pairs:
            # The proof bundle and Decision Artifact read the mapping dicts:
            # give them the same verdict (copies — the caller's list is theirs).
            _waived_maps: list[dict[str, Any]] = []
            for _m in mappings:
                _key = (
                    str(_m.get("source") or "").casefold(),
                    str(_m.get("target") or "").casefold(),
                ) if isinstance(_m, dict) else None
                if _key in _waived_pairs:
                    _m = dict(_m)
                    if str(_m.get("fidelity") or "").lower() == "lossy_cast":
                        _m["fidelity"] = None
                    _m["type_narrowing"] = False
                    _m.pop("typeNarrowing", None)
                    if str(_m.get("review_kind") or "").lower() == "lossy":
                        _m["requires_review"] = False
                    _m["width_narrowing_proven"] = True
                _waived_maps.append(_m)
            mappings = _waived_maps

    target_cols = list(drift_dest_types.keys())
    ddl_compatible, ddl_issues = evaluate_ddl_compatibility(
        mappings=write_maps,
        source_schema=column_types,
        target_schema=ddl_dest_types,
        sample_rows=sample_rows,
        table_exists=dest_table_exists,
        dest_connected=destination_connected,
        dest_db_type=destination_db_type,
        allow_create=dest_can_create,
        backfill_new_fields=backfill_new_fields,
        schema_policy=schema_policy,
        sync_mode=sync_mode,
        destination_table=destination_table,
        destination_pk_columns=destination_pk_columns,
        contract_primary_key=contract_primary_key,
        proven_width_narrowings=proven_string_narrowings,
        population_fit_report=pre_gate_fit_report,
    )

    # Two questions, two truths, and an approved pre-load transform separates
    # them. "Did the source change under the stored revision?" is answered by the
    # declared source the revision fingerprinted — otherwise the operator's own
    # recipe reads as source drift. "Do these values fit the destination
    # carrier?" is answered by the transformed image the writer will bind, or a
    # column rounded to whole numbers is graded a DECIMAL→INT4 precision
    # collapse for values that are now integers.
    drift = detect_schema_drift(
        source_columns=list(columns),
        source_schema=dict(column_types),
        declared_source_columns=list(declared_source_columns or columns),
        declared_source_schema=dict(declared_source_schema or column_types),
        target_columns=target_cols or [m["target"] for m in write_maps if m.get("target")],
        target_schema=drift_dest_types,
        mappings=mappings,
        destination_db_type=destination_db_type,
        sample_rows=sample_rows,
        stored_source_fp=stored_source_fp or "",
        stored_target_fp=stored_target_fp or "",
        previous_source_columns=previous_source_columns,
        previous_source_schema=previous_source_schema,
        previous_primary_key=None,
        # Source contract PK only — destination DDL PK must not invent PK change.
        live_primary_key=None,
        cursor_fields=cursor_fields,
        schema_policy=schema_policy,
        table_exists=dest_table_exists,
        dest_recreated=dest_recreated,
    )
    # Do NOT fold drift into ddl_issues / ddl_compatible. G6 must mean real DDL
    # (missing columns, width, types). Drift is a separate contract gate below.

    sample_quality: dict[str, Any] = {}
    if sample_rows and columns:
        from services.sample_quality import analyze_dataset_quality

        sample_quality = analyze_dataset_quality(
            columns, sample_rows, schema=column_types, dest_kind=dest_kind
        )
        # Sample-quality findings (high null rates, outliers, etc.) describe the data,
        # not the target schema.  They are surfaced by the data-integrity gate (G9);
        # conflating them with DDL compatibility causes false "Target DDL incompatible"
        # blockers for real-world sparse collections.
        if schemaless:
            sample_quality["blocks_transfer"] = False

    # A keyed mode whose contracts declare no key falls back to the live
    # source catalog — the same fallback Validate (preflight_router) and the
    # Execute policy pass (_catalog_pk_for_policy) already apply. Without it,
    # an API-created CDC schedule (synthesized contract carries
    # ``primary_key: ""``) refused at this gate even though the read side keys
    # on the catalog. Unreadable catalogs stay empty so the gate still
    # refuses and names the missing declaration.
    source_primary_key_columns: list[str] = []
    try:
        from services.preflight_cursor_gate import (
            MODES_REQUIRING_PRIMARY_KEY,
            contract_declares_primary_key,
        )

        if sync_mode in MODES_REQUIRING_PRIMARY_KEY:
            _pk_selected = [
                c
                for c in (stream_contracts or [])
                if isinstance(c, dict) and c.get("selected", True)
            ]
            if not _pk_selected or not all(
                contract_declares_primary_key(c) for c in _pk_selected
            ):
                from services.source_schema_authority import (
                    live_source_primary_key_columns,
                )

                src_cfg = source_config if isinstance(source_config, dict) else {}
                source_primary_key_columns = live_source_primary_key_columns(
                    source_connector_id=source_connector_id,
                    source_table=source_table,
                    source_collection=str((source_config or {}).get("collection") or ""),
                    source_schema=str(src_cfg.get("schema") or ""),
                    source_database=str(src_cfg.get("database") or ""),
                )
    except Exception:  # noqa: BLE001 — unread catalog blocks, it does not approve
        source_primary_key_columns = []

    plan = TransferPlan(
        source=SourceConfig(
            kind=source_kind,
            connected=source_connected and bool(columns),
            parseable=(
                is_file_source and (has_samples or source_measured_empty) and bool(columns)
            )
            or (not is_file_source and bool(columns)),
            columns=source_cols,
            row_count_estimate=row_count,
            error=source_error,
            source_read_mode=str(
                (source_config or {}).get("source_read_mode")
                or ((source_config or {}).get("extra") or {}).get("source_read_mode")
                or ""
            ),
            db_type=str(
                (source_config or {}).get("type")
                or (source_config or {}).get("db_type")
                or source_format
                or ""
            ),
        ),
        destination=DestinationConfig(
            kind="database",
            db_type=dest_kind,
            connected=destination_connected,
            can_create_table=dest_can_create,
            can_write=dest_can_write,
            target_columns=dest_cols,
            table_exists=dest_table_exists,
            error=destination_error,
            privilege_probe=privilege_probe,
            redshift_staging_probe=redshift_staging_probe,
            column_nullability=dict(destination_column_nullability or {}),
            column_defaults=dict(destination_column_defaults or {}),
            identity_columns=list(destination_identity_columns or []),
            generated_columns=list(destination_generated_columns or []),
            column_types=dict(destination_column_types or {}),
        ),
        mappings=plan_mappings,
        dry_run_passed=False,
        ddl_compatible=ddl_compatible,
        ddl_issues=ddl_issues,
        estimated_bytes=est_bytes,
        available_staging_bytes=available_staging_bytes,
        confidence_threshold=confidence_threshold,
        validation_mode=validation_mode,
        sync_mode=sync_mode,
        contract_primary_key=str(contract_primary_key or "").strip(),
        source_primary_key_columns=list(source_primary_key_columns),
        destination_pk_columns=list(destination_pk_columns or []),
        destination_table=str(destination_table or ""),
        destination_dynamo_key_schema=[
            dict(row)
            for row in (destination_dynamo_key_schema or [])
            if isinstance(row, dict)
        ],
        destination_dynamo_index_attributes=dict(
            destination_dynamo_index_attributes or {}
        ),
        destination_unique_keys=list(destination_unique_keys or []),
        destination_foreign_keys=list(destination_foreign_keys or []),
        fk_risk_acknowledged=bool(fk_risk_acknowledged),
        stream_contracts=list(stream_contracts or []),
        in_file_dedupe=_plan_in_file_dedupe(source_config),
        duplicate_policy=_plan_duplicate_policy(source_config, destination_config),
    )

    # Source-side duplicate-key probe: a small sample can miss duplicates in large
    # tables, so query the source directly when we have a resolved identity key.
    # Never stamp full_selected coverage unless status == "ran".
    source_duplicate_findings: list[dict[str, Any]] = []
    source_duplicate_probe_ran = False
    source_duplicate_probe_pk = ""
    source_duplicate_probe_status = ""
    source_duplicate_probe_message = ""
    source_null_key_rows: int | None = None
    source_key_collisions: dict[str, Any] | None = None
    from services.procedure_source import is_callable_source, source_read_mode_of

    callable_src = is_callable_source(source_config)
    source_duplicate_probe_expected = bool(
        (source_connector_id or source_config)
        and source_table
        and source_kind in ("database", "cloud")
        and not callable_src
    )
    if callable_src:
        source_duplicate_probe_status = "skipped_callable"
        source_duplicate_probe_message = (
            "Stored-procedure / SQL extract is a result-set snapshot — "
            "uniqueness GROUP BY is not run against a procedure name."
        )
    if source_duplicate_probe_expected:
        try:
            from services.primary_key import resolve_primary_key_source_columns

            source_pk_cols = resolve_primary_key_source_columns(
                mappings=mappings,
                source_columns=columns,
                dest_kind=dest_kind,
                validation_mode=validation_mode,
                purpose="uniqueness",
                destination_pk_columns=destination_pk_columns,
                contract_primary_key=contract_primary_key,
                stream_contracts=stream_contracts,
                stream_name=str(destination_table or source_table or ""),
            )
            if source_pk_cols:
                source_duplicate_probe_pk = ",".join(source_pk_cols)
                probe_result = probe_source_duplicate_keys_result(
                    source_connector_id=source_connector_id,
                    source_config=source_config,
                    source_table=source_table,
                    primary_key=source_pk_cols[0],
                    primary_key_columns=source_pk_cols,
                )
                source_duplicate_findings = list(probe_result.findings or [])
                source_duplicate_probe_status = probe_result.status
                source_duplicate_probe_message = probe_result.message
                source_duplicate_probe_ran = bool(probe_result.ran)
                source_null_key_rows = probe_result.null_key_rows
                # D39: a transform on the key can merge distinct source keys.
                from services.transformed_key_probe import (
                    key_transforms,
                    probe_transformed_key_collisions,
                )

                _key_xforms = key_transforms(mappings, source_pk_cols)
                if _key_xforms:
                    _key_rows = _ps._iter_table_population_for_preflight(
                        source_columns=list(source_pk_cols),
                        source_kind=source_kind,
                        source_format=source_format,
                        source_connector_id=source_connector_id or "",
                        source_config=source_config,
                        source_table=source_table or "",
                        mappings=list(mappings or []),
                        column_types=dict(column_types or {}),
                        dest_types=dict(destination_column_types or {}),
                        dest_db=destination_db_type,
                        sync_mode=sync_mode,
                    )
                    source_key_collisions = probe_transformed_key_collisions(
                        _key_rows if _key_rows is not None else (sample_rows or []),
                        key_sources=list(source_pk_cols),
                        transforms=_key_xforms,
                    )
            else:
                source_duplicate_probe_status = "skipped_no_pk"
                source_duplicate_probe_message = (
                    "No uniqueness primary key resolved — probe not run"
                )
                # Without a PK the probe was not expected to prove uniqueness.
                source_duplicate_probe_expected = False
        except Exception as exc:
            logger.warning("Source duplicate-key probe skipped: %s", exc, exc_info=exc)
            source_duplicate_probe_status = "error"
            source_duplicate_probe_message = f"Source uniqueness probe skipped: {exc}"[:400]

    # Destination-side collision probe: an append into a table that already
    # enforces this key is a deterministic write abort, so it must be a Validate
    # blocker rather than a duplicate-key error after Execute starts.
    # The rows this run will read. An incremental append past a watermark only
    # writes the delta, so the collision probe must judge the delta — probing
    # the whole table refused every run after the first, on every engine.
    destination_collision = probe_append_key_collisions(
        mappings=mappings,
        source_columns=columns,
        sample_rows=sample_rows or [],
        sync_mode=sync_mode,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
        destination_config=destination_config,
        destination_db_type=destination_db_type,
        destination_table=destination_table,
        destination_table_exists=dest_table_exists,
        destination_pk_columns=destination_pk_columns,
        destination_unique_keys=destination_unique_keys,
        contract_primary_key=contract_primary_key,
        stream_contracts=stream_contracts,
        source_table=source_table,
        resume=resume,
        incremental_cursor_column=read_scope.cursor_column,
        incremental_watermark=read_scope.watermark,
        incremental_tiebreak_column=read_scope.primary_key,
    )

    # D101: an upsert into an existing table needs a unique constraint on the
    # upsert columns. If the destination lacks one, the writer would silently
    # CREATE UNIQUE INDEX at run time — undisclosed DDL that fails raw on
    # duplicates. Inspect, count duplicates, disclose the DDL, require approval.
    upsert_ddl_gate: dict[str, Any] | None = None
    try:
        from services.sync_cursor import (
            normalize_sync_mode as _nsm,
            requires_upsert as _requires_upsert,
        )

        # scd2 is excluded: a type-2 history table holds many rows per business
        # key by design — a unique constraint on the key would be wrong, and the
        # writer keys current-row lookup on (key, is_current), never needing the
        # index this gate exists to disclose.
        _upsert_constraint_mode = _requires_upsert(sync_mode) and _nsm(
            sync_mode, default=""
        ) != "scd2"
        # Non-relational destinations (SaaS REST, object stores, schemaless,
        # Kafka/vector) are natively keyed upserts — DataFlow never runs
        # CREATE UNIQUE INDEX on them, so there is no DDL to disclose. Blocking
        # them here false-blocks every reverse-ETL route with a pre-existing
        # object (e.g. HubSpot 'contacts').
        _dest_no_unique_ddl = (
            dest_kind in DESTS_WITHOUT_SCHEMA_RECREATE
            or dest_kind in NO_RELATIONAL_DDL_DESTS
            or dest_kind in SCHEMALESS_DESTS
        )
        if (
            _upsert_constraint_mode
            and not _dest_no_unique_ddl
            and destination_table_exists is True
        ):
            from services.dest_dialect_facts import dest_upsert_needs_unique_index
            from services.destination_key_collision_probe import _identity_pairs
            from services.upsert_ddl_gate import (
                count_dest_duplicate_keys,
                dest_unique_covering_columns,
            )

            _uk_pairs = _identity_pairs(
                mappings=mappings,
                source_columns=columns,
                dest_kind=dest_kind,
                validation_mode=validation_mode,
                destination_pk_columns=destination_pk_columns,
                contract_primary_key=contract_primary_key,
                stream_contracts=stream_contracts,
                source_table=source_table,
                destination_table=destination_table,
            )
            _uk_cols = [t for _s, t in _uk_pairs]
            if _uk_cols and not dest_unique_covering_columns(
                destination_pk_columns, destination_unique_keys, _uk_cols
            ):
                # One answer for "how does this engine enforce upsert
                # identity?" (capability registry): MERGE engines get no DDL;
                # index engines get the exact statements their writer runs on
                # this table, or a plain refusal when no full-value unique key
                # can exist (MySQL JSON / over-long TEXT key — CR-D126).
                from services.upsert_identity import (
                    UPSERT_IDENTITY_IMPOSSIBLE,
                    plan_from_destination_config,
                    plan_upsert_identity,
                )

                _identity_plan = None
                if dest_upsert_needs_unique_index(dest_kind):
                    try:
                        _identity_plan = plan_from_destination_config(
                            dest_kind,
                            dict(destination_config or {}),
                            table=destination_table,
                            key_columns=_uk_cols,
                        )
                    except Exception as _pexc:  # noqa: BLE001 — writer re-plans
                        logger.warning(
                            "upsert identity plan unmeasured: %s", _pexc, exc_info=_pexc
                        )
                        _identity_plan = plan_upsert_identity(
                            dest_kind,
                            table_name=destination_table,
                            key_columns=_uk_cols,
                            schema=(dict(destination_config or {}).get("schema") or None),
                        )
                _planned_ddl = _identity_plan.disclosure if _identity_plan else ""
                _identity_impossible = bool(
                    _identity_plan
                    and _identity_plan.mode == UPSERT_IDENTITY_IMPOSSIBLE
                )
                _dup = {"ran": False, "duplicate_keys": 0, "duplicate_rows": 0,
                        "samples": [], "message": "destination probe skipped"}
                try:
                    # Optional proof, bounded inside a tool call (D187): on a
                    # Snowflake destination the reflection + GROUP BY ran 25s
                    # and killed a second-run upsert plan. Unfinished is a
                    # labelled ``ran=False`` (the gate warns), never a clean 0.
                    from src.ai.copilot.tool_deadline import bounded_step

                    _dup_run = bounded_step(
                        f"counting duplicate upsert keys on destination {destination_table}",
                        lambda: count_dest_duplicate_keys(
                            dict(destination_config or {}),
                            destination_table,
                            _uk_cols,
                        ),
                        seconds=DEST_DUPLICATE_KEY_STEP_SECONDS,
                        conn=destination_config,
                        role="destination",
                    )
                    if _dup_run.error is not None:
                        raise _dup_run.error
                    if _dup_run.timed_out:
                        _dup = {
                            "ran": False,
                            "duplicate_keys": 0,
                            "duplicate_rows": 0,
                            "samples": [],
                            "message": (
                                "duplicate-key probe did not finish within the plan's "
                                f"step budget ({DEST_DUPLICATE_KEY_STEP_SECONDS:g}s at most); "
                                "Execute's keyed write still enforces the key"
                            ),
                        }
                    else:
                        _dup = _dup_run.value
                except Exception as _dexc:  # noqa: BLE001
                    _dup = {
                        "ran": False,
                        "duplicate_keys": 0,
                        "duplicate_rows": 0,
                        "samples": [],
                        "message": f"duplicate-key probe failed: {_dexc}"[:300],
                    }
                _details = {
                    "rule_id": "g_upsert_dest_key",
                    "remediation_kind": "approve_ddl_or_dedupe",
                    "upsert_key_columns": list(_uk_cols),
                    "planned_ddl": _planned_ddl,
                    "duplicate_key_count": _dup.get("duplicate_keys"),
                    "duplicate_row_count": _dup.get("duplicate_rows"),
                    "duplicate_samples": _dup.get("samples") or [],
                    "probe_ran": bool(_dup.get("ran")),
                    "ddl_acknowledged": bool(ddl_changes_acknowledged),
                    "upsert_identity": (
                        _identity_plan.mode if _identity_plan else "merge"
                    ),
                }
                if _dup.get("duplicate_keys"):
                    upsert_ddl_gate = {
                        "id": "g_upsert_dest_key",
                        "status": "block",
                        "message": (
                            f"Destination '{destination_table}' holds "
                            f"{_dup['duplicate_keys']} duplicate upsert-key "
                            f"value(s) on ({', '.join(_uk_cols)}) and has no "
                            "unique constraint — the merge cannot be keyed "
                            "safely. Deduplicate the destination or choose "
                            "different key columns, then re-validate."
                        ),
                        "duration_ms": 0,
                        "details": _details,
                        "suggested_actions": [
                            "Remove the duplicate key values listed in the probe",
                            "Pick different upsert key columns",
                            "Run full refresh overwrite instead of upsert",
                        ],
                    }
                elif _identity_impossible:
                    upsert_ddl_gate = {
                        "id": "g_upsert_dest_key",
                        "status": "block",
                        "message": _identity_plan.reason[:600],
                        "duration_ms": 0,
                        "details": {
                            **_details,
                            "planned_ddl": "",
                            "requires_approval": False,
                            "remediation_kind": "choose_indexable_key",
                            "impossible_reason": _identity_plan.reason,
                        },
                        "suggested_actions": [
                            "Pick an upsert key column the destination can index on its full value",
                            "Run full refresh overwrite instead of upsert",
                        ],
                    }
                elif not dest_upsert_needs_unique_index(dest_kind):
                    # The writer keys this upsert with MERGE and never creates
                    # an index, so there is no DDL to disclose or approve; the
                    # duplicate probe above is the whole check (D47). A probe
                    # that could not run is unmeasured, not clean.
                    probed = bool(_dup.get("ran"))
                    upsert_ddl_gate = {
                        "id": "g_upsert_dest_key",
                        "status": "pass" if probed else "warn",
                        "message": (
                            f"Upsert merges on ({', '.join(_uk_cols)}); the "
                            "destination holds no duplicate key values and "
                            "needs no new DDL"
                            if probed
                            else (
                                f"Upsert merges on ({', '.join(_uk_cols)}) but "
                                "the destination duplicate-key probe could not "
                                f"run: {_dup.get('message') or 'unknown'}"
                            )[:400]
                        ),
                        "duration_ms": 0,
                        "details": {**_details, "planned_ddl": ""},
                    }
                elif ddl_changes_acknowledged:
                    upsert_ddl_gate = {
                        "id": "g_upsert_dest_key",
                        "status": "pass",
                        "message": (
                            f"Approved destination DDL: {_planned_ddl} "
                            "(operator acknowledged)"
                        ),
                        "duration_ms": 0,
                        "details": {**_details, "acknowledged": True},
                    }
                else:
                    upsert_ddl_gate = {
                        "id": "g_upsert_dest_key",
                        "status": "block",
                        "message": (
                            f"Upsert into existing '{destination_table}' needs "
                            f"a unique constraint on ({', '.join(_uk_cols)}). "
                            f"{PRODUCT_NAME} would run: {_planned_ddl} — disclose "
                            "and approve destination DDL before executing."
                        ),
                        "duration_ms": 0,
                        "details": {**_details, "requires_approval": True},
                        "suggested_actions": [
                            "Approve the disclosed DDL and re-validate",
                            "Create a unique constraint on the upsert key yourself",
                        ],
                    }
            elif _uk_cols:
                upsert_ddl_gate = {
                    "id": "g_upsert_dest_key",
                    "status": "pass",
                    "message": (
                        f"Destination enforces uniqueness on the upsert key "
                        f"({', '.join(_uk_cols)}) — no new DDL required"
                    ),
                    "duration_ms": 0,
                    "details": {"upsert_key_columns": list(_uk_cols)},
                }
    except Exception as _uexc:  # noqa: BLE001 — unmeasured, warn not pass
        logger.warning("upsert dest-key gate skipped: %s", _uexc, exc_info=_uexc)
        upsert_ddl_gate = {
            "id": "g_upsert_dest_key",
            "status": "warn",
            "message": (
                f"Upsert destination key inspection could not run: {_uexc}"
            )[:400],
            "duration_ms": 0,
            "details": {},
        }

    from services.preflight_source_kind import source_is_record_batch

    ctx = _ps.FilePreflightContext(
        plan,
        sample_rows,
        empty_cells_as_null=(
            is_file_source
            or source_is_record_batch(source_kind, source_format)
        ),
        destination_collision=destination_collision,
        source_duplicate_findings=source_duplicate_findings,
        source_duplicate_probe_ran=source_duplicate_probe_ran,
        source_duplicate_probe_pk=source_duplicate_probe_pk,
        source_duplicate_probe_status=source_duplicate_probe_status,
        source_duplicate_probe_message=source_duplicate_probe_message,
        source_duplicate_probe_expected=source_duplicate_probe_expected,
        proven_width_narrowings=proven_string_narrowings,
        population_fit_report=pre_gate_fit_report,
    )
    ctx.sample_unavailable_reason = sample_unavailable_reason
    ctx.source_null_key_rows = source_null_key_rows
    ctx.source_key_collisions = source_key_collisions
    ctx.source_measured_empty = source_measured_empty
    # Always collect every reachable gate on Validate. fail_fast=True hid G6 DDL
    # behind G5 integrity blocks and forced a multi-run fix loop. Transfer still
    # refuses to move rows when any blocker remains (passed=False).
    engine = PreflightEngine(fail_fast=False)
    result = engine.run(ctx)

    # D104: when a destination key collision is caused by another enabled
    # pipeline owning this table, name that pipeline in the blocker so the
    # refusal points at the cause, not just at the keys already at rest.
    if destination_connector_id:
        try:
            from services.schedule_store import (
                conflicting_dest_writers,
                describe_dest_conflict,
            )

            _owners = conflicting_dest_writers(
                destination_connector_id, destination_table
            )
            if _owners:
                _names = [describe_dest_conflict(s) for s in _owners]
                for _b in result.blockers or []:
                    _d = (
                        _b.get("details")
                        if isinstance(_b, dict)
                        else getattr(_b, "details", None)
                    ) or {}
                    if _d.get("sample_collisions") or _d.get(
                        "collision_count"
                    ) is not None:
                        _d["conflicting_pipelines"] = list(_names)
        except Exception:  # noqa: BLE001 — naming must never break preflight
            pass

    from services.preflight_proof_bundle import build_preflight_proof_bundle

    proof_bundle = build_preflight_proof_bundle(
        columns=columns,
        sample_rows=sample_rows or [],
        mappings=mappings,
        source_schemas=[
            {
                "name": c,
                "inferred_type": column_types.get(c, "VARCHAR").upper(),
                "samples": [
                    wire_cell_text(row.get(c, ""))
                    for row in (sample_rows or [])[:20]
                    if row.get(c) is not None
                ],
            }
            for c in columns
        ],
        source_records=sample_rows or [],
        target_records=[],
        validation_mode=validation_mode,
        confidence_threshold=confidence_threshold,
        compliance_acknowledged=compliance_acknowledged,
        acknowledgment_actor=acknowledgment_actor,
        acknowledgment_reason=acknowledgment_reason,
        source_measured_empty=source_measured_empty,
    )
    from services.population_fit_narrowing import proof_details

    proof_bundle["accepted_width_narrowings"] = proof_details(
        proven_string_narrowings
    )
    if pre_gate_fit_report is not None:
        proof_bundle["population_fit"] = pre_gate_fit_report.to_dict()

    # Module 12 + Phase C11 — Conversion Contract, DDL identity, Decision Artifact.
    try:
        from services.decision_kernel import (
            assess_mapping_risk,
            build_artifact_from_mappings,
            ddl_identity_report,
            orchestrate_validation_summary,
        )

        dest_for_ddl = (destination_db_type or "").strip().lower()
        src_engine = (source_format or "").strip().lower()
        from services.schema_fingerprint import (
            live_dest_schema_fingerprint,
            live_source_schema_fingerprint,
        )

        dest_fp = live_dest_schema_fingerprint(
            live_destination_types,
            destination_table_exists=destination_table_exists,
            sync_mode=str(sync_mode or ""),
        )

        src_fp = live_source_schema_fingerprint(
            column_types,
            authoritative=source_types_are_authoritative(source_kind, source_format),
        )
        decision_art = build_artifact_from_mappings(
            list(mappings or []),
            dest_db=dest_for_ddl,
            source_db=src_engine,
            route_id=f"validate:{dest_for_ddl or 'unknown'}",
            source_fingerprint=src_fp,
            dest_fingerprint=dest_fp,
            sync_mode=str(sync_mode or "full_refresh_overwrite"),
        )
        conv_cols = [
            {
                "source": m.get("source"),
                "target": m.get("target"),
                **assess_mapping_risk(m, destination_db_type=dest_for_ddl),
            }
            for m in (mappings or [])[:80]
            if isinstance(m, dict)
        ]
        art_dict = decision_art.to_dict()
        # Gates already ran — classify into Validation Orchestrator buckets.
        def _gate_row(g) -> dict:
            if isinstance(g, dict):
                return {
                    "id": str(g.get("id") or g.get("gate_id") or ""),
                    "status": str(g.get("status") or ""),
                    "message": str(g.get("message") or ""),
                }
            gid = getattr(g, "gate_id", "")
            status = getattr(g, "status", "")
            return {
                "id": str(getattr(gid, "value", gid) or ""),
                "status": str(getattr(status, "value", status) or ""),
                "message": str(getattr(g, "message", "") or ""),
            }

        validation_orch = orchestrate_validation_summary(
            decision_artifact=art_dict,
            gates=[_gate_row(g) for g in list(getattr(result, "gates", None) or [])],
            blockers=[
                {**_gate_row(b), "status": "block"}
                for b in list(getattr(result, "blockers", None) or [])
            ],
        )
        proof_bundle = {
            **proof_bundle,
            "ddl_identity": ddl_identity_report(
                list(mappings or []),
                dest_db=dest_for_ddl,
            ),
            "decision_artifact": art_dict,
            "decision_artifact_hash": decision_art.content_hash,
            "validation_orchestrator": validation_orch,
            "conversion_contract": {
                "version": "conversion_contract.v1",
                "columns": conv_cols,
            },
        }
    except Exception as conv_exc:
        # GA: never soft-skip conversion / DDL identity stamp — surface fail-closed.
        logger.warning(
            "conversion contract stamp failed closed: %s", conv_exc, exc_info=conv_exc
        )
        proof_bundle = {
            **proof_bundle,
            "ddl_identity": {
                "ddl_identity_hash": "",
                "error": str(conv_exc)[:400],
                "stamp_failed": True,
            },
            "conversion_contract": {
                "version": "conversion_contract.v1",
                "stamp_failed": True,
                "error": str(conv_exc)[:400],
            },
        }

    from services.preflight_rules import enrich_blockers

    blockers = [
        {"id": b.gate_id.value, "message": b.message, "details": b.details}
        for b in result.blockers
    ]
    # Missing DDL identity after stamp failure → operator-facing blocker.
    ddl_id = (proof_bundle.get("ddl_identity") or {}) if isinstance(proof_bundle, dict) else {}
    if ddl_id.get("stamp_failed") or (
        isinstance(proof_bundle, dict)
        and proof_bundle.get("ddl_identity") is not None
        and not str(ddl_id.get("ddl_identity_hash") or "").strip()
        and ddl_id.get("error")
    ):
        blockers.append(
            {
                "id": "ddl_identity",
                "message": (
                    "Map→DDL identity stamp failed — re-run Validate after fixing "
                    f"conversion contract: {ddl_id.get('error') or 'unknown'}"
                ),
                "details": {"ddl_identity": ddl_id},
            }
        )
    # Additive ADD without Kernel stamp must not leave Execute as the first refuse.
    create_unstamped = [
        str(m.get("target") or "")
        for m in (mappings or [])
        if isinstance(m, dict)
        and str(m.get("target") or "").strip()
        and str(m.get("assignment_strategy") or "") != "pending_dest_schema"
        and (
            m.get("create_new")
            or str(m.get("assignment_strategy") or "")
            in {"create_compatible_new", "identity_passthrough"}
            or backfill_new_fields
        )
        and not str(m.get("target_type") or m.get("dest_type") or "").strip()
        and str(m.get("target") or "") not in (destination_column_types or {})
        and str(m.get("target") or "").lower()
        not in {str(k).lower() for k in (destination_column_types or {})}
    ]
    create_unstamped = list(dict.fromkeys([c for c in create_unstamped if c]))
    additive_stamp_blocked = False
    if create_unstamped or _unstamped_additive:
        cols = list(dict.fromkeys([*_unstamped_additive, *create_unstamped]))
        sample = ", ".join(repr(c) for c in cols[:5])
        more = f" (+{len(cols) - 5} more)" if len(cols) > 5 else ""
        blockers.append(
            {
                "id": "g6_additive_stamp",
                "message": (
                    f"Additive column(s) {sample}{more} lack Map target_type under "
                    "partial Studio — Decision Kernel refuse VARCHAR ADD invent. "
                    "Re-run Map (create-new stamp) or disable backfill_new_fields."
                ),
                "details": {
                    "columns": cols[:20],
                    "backfill_new_fields": bool(backfill_new_fields),
                    "kind": "additive_map_stamp_required",
                },
            }
        )
        additive_stamp_blocked = True

    # D8: hash_pii fails closed without its HMAC key. Refuse from the plan, not
    # the sample — an all-NULL sample never runs the digest and a shape-recipe
    # step is not in the mapping replay — so the run cannot be the first to say.
    from services.transform_engine import PII_HASH_KEY_SETUP, pii_hash_key

    hashed_columns = [pm.source for pm in plan_mappings if pm.transform == "hash_pii"]
    for step in (shape_recipe or {}).get("steps") or []:
        if (
            isinstance(step, dict)
            and step.get("enabled") is not False
            and step.get("op") == "hash_pii"
        ):
            hashed_columns.append(str(step.get("column") or ""))
    hashed_columns = list(dict.fromkeys(c for c in hashed_columns if c))
    pii_hash_gate: dict[str, Any] | None = None
    if hashed_columns:
        key_missing = not pii_hash_key()
        pii_hash_gate = {
            "id": "g_pii_hash_key",
            "status": "block" if key_missing else "pass",
            "message": (
                PII_HASH_KEY_SETUP if key_missing else "hash_pii HMAC key is configured"
            ),
            "duration_ms": 0,
            "details": {"columns": hashed_columns[:20], "kind": "pii_hash_key"},
        }
        if key_missing:
            blockers.append(
                {
                    "id": pii_hash_gate["id"],
                    "message": PII_HASH_KEY_SETUP,
                    "details": dict(pii_hash_gate["details"]),
                }
            )
    pii_hash_blocked = bool(pii_hash_gate and pii_hash_gate["status"] == "block")

    # Bounded destination carriers (DECIMAL(p,s) / VARCHAR(n) / sized INTEGER)
    # are decidable before the write. Screening a preview and letting the writer
    # discover row 431 is how a 1M-row load ends with zero rows committed and a
    # Run-phase error for a defect Validate could name. Scan whatever rows this
    # caller actually holds — the whole batch at Execute preflight, the preview in
    # Studio — and report the evidence for what it is.
    fit_gate: dict[str, Any] | None = None
    fit_report_payload: dict[str, Any] = {}
    fit_blocked = False
    fit_report = None
    try:
        from connectors.writer_common import transform_error_policy_for_validation_mode
        from services.population_fit_scan import (
            GATE_ID as _FIT_GATE_ID,
            build_population_fit_gate,
        )
        from services.sync_cursor import incremental_read_narrows

        if pre_gate_fit_attempted:
            from services.population_fit_scan import EVIDENCE_UNMEASURED, FitScanReport

            fit_report = pre_gate_fit_report or FitScanReport(
                evidence=EVIDENCE_UNMEASURED,
                rows_total=int(row_count or 0),
                truncated_reason="walk" if pre_gate_fit_error else "",
                probe_error=pre_gate_fit_error,
                note=(
                    "Pre-gate population scan failed; fit is unproven."
                    if pre_gate_fit_error
                    else ""
                ),
            )
            fit_report_payload = fit_report.to_dict()
            if incremental_read_narrows(sync_mode) and read_scope.bounded:
                fit_report_payload["delta_scope"] = {
                    "cursor_column": read_scope.cursor_column,
                    "watermark": str(read_scope.watermark),
                }
            if pre_gate_fit_filter:
                from services.row_filter import filter_columns

                fit_report_payload["filter_scope"] = {
                    "columns": filter_columns(pre_gate_fit_filter),
                }
            fit_gate = build_population_fit_gate(fit_report)
        elif skip_population_fit:
            # Execute after a stamped Studio Validate: dest probe + sample
            # gates still run. The 1M walk does not. Write-time fit binds.
            from services.population_fit_scan import (
                EVIDENCE_PARTIAL,
                FitScanReport,
                bounded_targets,
            )

            targets, undecidable, safe = bounded_targets(
                mappings,
                dest_types=destination_column_types or {},
                source_types=column_types or {},
                dest_db=destination_db_type,
                job_error_policy=transform_error_policy_for_validation_mode(
                    validation_mode
                ),
                source_kind=source_kind,
                source_format=source_format,
                sync_mode=sync_mode,
                dest_table_exists=destination_table_exists,
            )
            fit_report = FitScanReport(
                evidence=EVIDENCE_PARTIAL,
                rows_scanned=0,
                rows_total=int(row_count or 0),
                targets=tuple(targets),
                undecidable=tuple(undecidable),
                safe_by_declaration=tuple(safe),
                truncated_reason="reused_validate",
                note=(
                    "Population fit reused from approved Validate. Execute does "
                    "not re-walk the source; write-time fit still binds every row."
                ),
            )
            fit_report_payload = fit_report.to_dict()
            fit_report_payload["reused_from_validate"] = True
            fit_gate = build_population_fit_gate(fit_report)
        if fit_gate.get("status") == "block":
            fit_blocked = True
            blockers.append(
                {
                    "id": _FIT_GATE_ID,
                    "message": str(fit_gate.get("message") or ""),
                    "details": dict(fit_gate.get("details") or {}),
                    "suggested_actions": list(
                        (fit_gate.get("details") or {}).get("suggested_actions") or []
                    ),
                }
            )
    except Exception as exc:  # noqa: BLE001 - a failed scan is unmeasured, never "fits"
        logger.warning(
            "population fit scan failed: %s", exc, exc_info=exc
        )
        fit_report_payload = {"evidence": "unmeasured", "error": str(exc)[:400]}
        fit_gate = {
            "id": "g3f_population_fit",
            "status": "warn",
            "message": (
                "Population fit could not be measured — bounded destination "
                "carriers stay unproven until the write-time checks run"
            ),
            "duration_ms": 0,
            "details": dict(fit_report_payload),
        }

    enriched_blockers = enrich_blockers(
        blockers,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
    )

    from services.type_system import is_binary_type, is_structural_type

    has_binary = any(is_binary_type(t) for t in column_types.values())
    has_unstructured = any(is_structural_type(t) for t in column_types.values())
    _src_fmt = (source_format or source_kind).lower()
    _tgt_fmt = (destination_db_type or "").lower()
    payload_shape = classify_payload(
        source_format=_src_fmt,
        target_format=_tgt_fmt,
        has_binary=has_binary,
        has_unstructured=has_unstructured,
    )
    validation_plan = build_validation_plan(
        source_format=_src_fmt,
        target_format=_tgt_fmt,
        validation_mode=validation_mode,
        write_semantics=sync_mode,
        confidence_threshold=confidence_threshold,
    )

    # D99: an incremental run bounds the delta with ``cursor > watermark`` —
    # and ``NULL > x`` is never true, so NULL-cursor rows are absent from
    # every delta forever while the job reports verified. Warn (block under
    # strict) with the live count; a probe that cannot measure skips honestly
    # rather than fail-closing on unmeasured state.
    null_cursor_gate: dict[str, Any] | None = None
    try:
        from services.sync_cursor import incremental_read_narrows

        _nc_cursor = str(getattr(read_scope, "cursor_column", "") or "").strip()
        if _nc_cursor and incremental_read_narrows(sync_mode):
            _nc_nullable = (column_nullability or {}).get(_nc_cursor)
            from services.null_cursor_probe import probe_null_cursor_count

            _nc = probe_null_cursor_count(
                source_connector_id=source_connector_id,
                source_config=source_config,
                source_table=source_table,
                source_collection=str(
                    (source_config or {}).get("collection") or ""
                ),
                cursor_column=_nc_cursor,
            )
            _nc_details: dict[str, Any] = {
                "cursor_column": _nc_cursor,
                "probe_status": _nc.status,
                "null_count": _nc.null_count,
                "column_nullable": _nc_nullable,
            }
            if _nc.ran and _nc.null_count:
                _nc_strict = (validation_mode or "strict").lower() == "strict"
                if _nc_strict:
                    enriched_blockers.append(
                        {
                            "id": "g_nullable_cursor",
                            "message": (
                                f"{_nc.null_count} source row(s) carry a NULL "
                                f"value in the incremental cursor column "
                                f"'{_nc_cursor}' — an incremental read can "
                                "never see them: they are absent from every "
                                "delta and only arrive via a full refresh."
                            ),
                            "details": _nc_details,
                            "suggested_actions": [
                                "Fill the cursor column on the NULL rows and re-validate",
                                "Add a COALESCE fallback cursor (e.g. created_at)",
                                "Run as full refresh so every row is read",
                            ],
                        }
                    )
                null_cursor_gate = {
                    "id": "g_nullable_cursor",
                    "status": "block" if _nc_strict else "warn",
                    "message": (
                        f"{_nc.null_count} source row(s) carry a NULL value in "
                        f"the incremental cursor column '{_nc_cursor}' — an "
                        "incremental read can never see them: they are absent "
                        "from every delta and only arrive via a full refresh. "
                        "Fill the cursor column, add a COALESCE fallback "
                        "cursor, or acknowledge the gap."
                    ),
                    "duration_ms": 0,
                    "details": _nc_details,
                }
            elif _nc.ran:
                null_cursor_gate = {
                    "id": "g_nullable_cursor",
                    "status": "warn" if _nc_nullable else "pass",
                    "message": (
                        f"Cursor column '{_nc_cursor}' holds no NULL values"
                        + (
                            " (the column is nullable — a future NULL row "
                            "would be skipped by every incremental delta)"
                            if _nc_nullable
                            else ""
                        )
                    ),
                    "duration_ms": 0,
                    "details": _nc_details,
                }
            elif _nc_nullable:
                null_cursor_gate = {
                    "id": "g_nullable_cursor",
                    "status": "warn",
                    "message": (
                        f"Cursor column '{_nc_cursor}' is nullable — NULL "
                        "rows would be skipped by every incremental delta "
                        f"({_nc.message})"
                    ),
                    "duration_ms": 0,
                    "details": _nc_details,
                }
    except Exception as exc:  # noqa: BLE001 — unmeasured, never a false pass
        logger.warning("NULL-cursor gate probe skipped: %s", exc, exc_info=exc)

    null_cursor_blocked = bool(
        null_cursor_gate and null_cursor_gate.get("status") == "block"
    )
    upsert_ddl_blocked = bool(
        upsert_ddl_gate and upsert_ddl_gate.get("status") == "block"
    )
    if upsert_ddl_blocked:
        enriched_blockers.append(
            {
                "id": "g_upsert_dest_key",
                "message": str(upsert_ddl_gate.get("message") or ""),
                "details": dict(upsert_ddl_gate.get("details") or {}),
                "suggested_actions": list(
                    upsert_ddl_gate.get("suggested_actions") or []
                ),
            }
        )
    extra_gates: list[dict[str, Any]] = (
        ([fit_gate] if fit_gate else [])
        + ([null_cursor_gate] if null_cursor_gate else [])
        + ([upsert_ddl_gate] if upsert_ddl_gate else [])
        + ([pii_hash_gate] if pii_hash_gate else [])
    )
    extra_passed = sum(1 for g in extra_gates if g.get("status") != "block")
    total_gates = result.total_gates + len(extra_gates)
    passed_count = result.passed_count + extra_passed

    out = {
        # Carrier semantics are engine-specific, so the engine the gates ran
        # against travels with the verdict: a remediation resolved without it
        # would describe a different database's carriers.
        "destination_db_type": destination_db_type or "",
        "passed": bool(result.passed) and not additive_stamp_blocked and not fit_blocked
        and not null_cursor_blocked and not upsert_ddl_blocked and not pii_hash_blocked,
        "passed_count": passed_count,
        "total_gates": total_gates,
        "readiness_score": round(passed_count / max(total_gates, 1) * 100, 1),
        "gates": [
            {
                "id": g.gate_id.value,
                "status": g.status.value,
                "message": g.message,
                "duration_ms": round(g.duration_ms, 2),
                "details": g.details,
            }
            for g in result.gates
        ]
        + (
            [
                {
                    "id": "g6_additive_stamp",
                    "status": "block",
                    "message": enriched_blockers[-1]["message"]
                    if additive_stamp_blocked and enriched_blockers
                    else "Additive Map stamp required",
                    "duration_ms": 0,
                    "details": {"kind": "additive_map_stamp_required"},
                }
            ]
            if additive_stamp_blocked
            else []
        )
        + extra_gates,
        "blockers": enriched_blockers,
        "population_fit": fit_report_payload,
        # Echo Kernel-stamped additive types so Map/Execute share Validate authority.
        "stamped_mappings": [
            {
                "source": str(m.get("source") or ""),
                "target": str(m.get("target") or ""),
                "target_type": str(m.get("target_type") or ""),
                "create_new": bool(m.get("create_new")),
                "assignment_strategy": str(m.get("assignment_strategy") or ""),
            }
            for m in (mappings or [])
            if isinstance(m, dict) and str(m.get("target_type") or "").strip()
        ],
        "schema_drift": drift,
        "ddl_issues": ddl_issues,
        "sample_quality": sample_quality,
        "proof_bundle": proof_bundle,
        "payload_shape": payload_shape,
        "validation_plan": validation_plan.to_dict(),
        "coercion_report": ctx.coercion_report(),
        # Canonical Kernel findings — Map / Proof / root-cause must not re-classify.
        "validation_findings": [],
        "date_locale": date_locale,
        "number_locale": number_locale,
        "privilege_probe": privilege_probe or {},
        "redshift_staging_probe": redshift_staging_probe or {},
        "recommended_batch_size": min(
            recommended_batch_size(_src_fmt),
            recommended_batch_size(_tgt_fmt) or recommended_batch_size(_src_fmt),
        ),
        # Echo signed Risk Contracts so Map/Execute share the same stamped rows
        # Validate greened — never leave FE with unsigned drafts after hydrate.
        "signed_mappings": [
            {
                "source": str(m.get("source") or ""),
                "target": str(m.get("target") or ""),
                "risk_contract": m.get("risk_contract"),
                "risk_acknowledged": bool(m.get("risk_acknowledged")),
            }
            for m in (mappings or [])
            if isinstance(m, dict) and isinstance(m.get("risk_contract"), dict)
        ],
    }
    if callable_src:
        out["callable_extract"] = {
            "mode": source_read_mode_of(source_config),
            "catalog_probes": "skipped",
            "note": (
                "Result-set snapshot — FK catalog, uniqueness GROUP BY, and "
                "population orphan scans are not run against a procedure name."
            ),
            "cdc": "refused",
            "history_sync": "refused",
            "incremental": "spool_cursor_filter_at_least_once",
            "honesty": (
                "CDC / SCD2 / mirror are refused on CALL/SELECT. Incremental "
                "filters the spool with cursor > watermark (at-least-once). "
                "Not exactly-once. Not a catalog-breadth claim."
            ),
        }

    # A gate that blocks a declared conversion must show up in the report every
    # other surface reads — otherwise Validate blocks while the panel under it
    # says there are no blocking failures.
    out["coercion_report"] = reconcile_coercion_report(
        out.get("coercion_report"), out.get("gates")
    )

    # Stamp Decision Kernel ValidationFindings onto Validate SSOT.
    # Coercion preview is 25 rows; population-fit overflows (row 293+)
    # must share the same Remap surface or the dest widen stays hidden.
    try:
        from services.decision_kernel import (
            findings_from_coercion_report,
            findings_from_population_fit,
            merge_validation_findings,
        )

        _dest = str(destination_db_type or "")
        _vf = merge_validation_findings(
            findings_from_coercion_report(
                out.get("coercion_report"),
                dest_db=_dest,
            ),
            findings_from_population_fit(
                out.get("population_fit"),
                dest_db=_dest,
            ),
        )
        out["validation_findings"] = _vf
        if isinstance(out.get("proof_bundle"), dict) and _vf:
            out["proof_bundle"] = {
                **out["proof_bundle"],
                "validation_findings": _vf,
                "validation_finding_schema": "validation_finding_v1",
            }
    except Exception as vf_exc:
        logger.warning("validation_findings stamp failed: %s", vf_exc, exc_info=vf_exc)

    # Mapping contract — every source column accounted for (G13) and every
    # required destination column filled (G14).
    src_coverage, contract_gates, contract_blockers = build_mapping_contract_gates(
        source_columns=list(columns or []),
        mappings=list(mappings or []),
        destination_table_exists=destination_table_exists,
        column_nullability=destination_column_nullability,
        column_defaults=destination_column_defaults,
        identity_columns=destination_identity_columns,
        generated_columns=destination_generated_columns,
        # Types keys are the live dest column list. Nullability keys are a
        # tautology for the partial-catalog check — do not substitute them.
        dest_columns=list((destination_column_types or {}).keys()),
        dest_recreated=dest_recreated,
        dest_emptied=dest_emptied,
    )
    out["source_coverage"] = src_coverage

    # Field Reduction Ledger (G16) — a typed, evidenced disposition per source
    # field. G13 proves the drop was declared; this proves it was decided.
    from services.field_reduction_ledger import build_field_reduction_evidence

    field_reduction, reduction_gate = build_field_reduction_evidence(
        source_columns=list(columns or []),
        mappings=list(mappings or []),
        target_columns=list((destination_column_types or {}).keys()),
        source_column_types=dict(column_types or {}),
        target_column_types=dict(destination_column_types or {}),
        sample_rows=list(sample_rows or []),
    )
    out["field_reduction_ledger"] = field_reduction
    contract_gates = [*contract_gates, reduction_gate]
    if reduction_gate.get("status") == "block":
        contract_blockers = [
            *contract_blockers,
            {
                "id": reduction_gate["id"],
                "message": reduction_gate["message"],
                "details": reduction_gate["details"],
            },
        ]

    # G19 — only an overwrite that DROP+CREATEs replaces the declared carrier.
    # Relational overwrite keeps the table, so G3/G6 judge the live column and
    # G19 stays quiet. Where a recreate would discard a narrower type, say so
    # instead of writing through the replacement.
    from services.dest_schema_replacement import build_dest_schema_replacement_gate

    replacement_gate = build_dest_schema_replacement_gate(
        mappings=list(mappings or []),
        source_column_types=dict(column_types or {}),
        destination_column_types=dict(
            destination_live_column_types or destination_column_types or {}
        ),
        destination_table_exists=destination_table_exists,
        dest_recreated=dest_recreated,
        destination_db_type=destination_db_type,
        source_db_type=_src_fmt,
    )
    contract_gates = [*contract_gates, replacement_gate]
    if replacement_gate.get("status") == "block":
        contract_blockers = [
            *contract_blockers,
            {
                "id": replacement_gate["id"],
                "message": replacement_gate["message"],
                "details": replacement_gate["details"],
            },
        ]

    # G20 — a declared code crosswalk must cover every distinct source value
    # in the population, not the Validate sample. Unmapped codes fail closed.
    # Do not consume G3F's population generator: GROUP BY / a replayable file
    # scan is the population proof. A covered sample is not.
    from services.code_crosswalk import (
        build_code_crosswalk_evidence,
        coded_mappings,
        collect_observed_codes,
        probe_population_codes,
    )

    observed_codes: dict[str, dict[str, int]] | None = None
    scan_method = ""
    coded = coded_mappings(list(mappings or []))
    if coded:
        coded_cols = [str(c["source"]) for c in coded]
        probed, _truncated, scan_method = probe_population_codes(
            columns=coded_cols,
            source_connector_id=source_connector_id,
            source_config=source_config,
            source_table=source_table,
            source_file_id=source_file_id,
            shape_recipe=shape_recipe if isinstance(shape_recipe, dict) else None,
            source_columns=list(columns or []),
        )
        if probed is not None:
            observed_codes = probed
        elif isinstance(population_rows, (list, tuple)) and rows_are_population:
            observed_codes, _truncated = collect_observed_codes(
                population_rows, coded_cols
            )
            scan_method = "population_rows"

    population_seq = (
        list(population_rows)
        if isinstance(population_rows, (list, tuple))
        else None
    )
    code_crosswalk, crosswalk_gate = build_code_crosswalk_evidence(
        mappings=list(mappings or []),
        sample_rows=list(sample_rows or []),
        population_rows=population_seq,
        rows_are_population=bool(rows_are_population)
        and observed_codes is None
        and (population_seq is not None or population_rows is None),
        observed_codes=observed_codes,
        row_count=row_count,
    )
    if scan_method:
        code_crosswalk["scan_method"] = scan_method
    out["code_crosswalk"] = code_crosswalk
    contract_gates = [*contract_gates, crosswalk_gate]
    if crosswalk_gate.get("status") == "block":
        contract_blockers = [
            *contract_blockers,
            {
                "id": crosswalk_gate["id"],
                "message": crosswalk_gate["message"],
                "details": crosswalk_gate["details"],
            },
        ]

    from services.control_totals import (
        build_control_totals_gate,
        build_control_totals_report,
        proof_pack_control_totals,
    )

    control_totals_report = build_control_totals_report(
        mappings=list(mappings or []),
        phase="validate",
    )
    control_totals_gate = build_control_totals_gate(
        control_totals_report, phase="validate"
    )
    out["control_totals"] = control_totals_report
    contract_gates = [*contract_gates, control_totals_gate]

    if isinstance(out.get("proof_bundle"), dict):
        out["proof_bundle"] = {
            **out["proof_bundle"],
            "source_coverage": src_coverage,
            # The reduction ledger travels with the pack: an auditor reading the
            # export must see why each field was not carried, not only that it
            # was declared omitted.
            "field_reduction_ledger": field_reduction,
            # Evidence travels with the proof pack, not only with the HTTP
            # response: an exported pack must show whether the bounded carriers
            # were decided on every row or on a preview.
            "population_fit": fit_report_payload,
            "code_crosswalk": code_crosswalk,
            "control_totals": proof_pack_control_totals(control_totals_report),
        }
    # Hosted G13/G14/G15 replace package stubs (full dest nullability / identity).
    contract_ids = {str(g.get("id") or "") for g in contract_gates}
    out["gates"] = [
        g for g in out["gates"] if str(g.get("id") or "") not in contract_ids
    ] + list(contract_gates)
    if contract_blockers:
        out["blockers"] = [
            *out["blockers"],
            *enrich_blockers(
                contract_blockers,
                dest_kind=dest_kind,
                validation_mode=validation_mode,
            ),
        ]
        out["passed"] = False
    out["passed_count"] = sum(1 for g in out["gates"] if g.get("status") == "pass")
    out["total_gates"] = len(out["gates"])
    out["readiness_score"] = round(
        out["passed_count"] / max(out["total_gates"], 1) * 100, 1
    )

    apply_constraint_findings(
        out,
        ctx,
        validation_mode=validation_mode,
        fk_risk_acknowledged=fk_risk_acknowledged,
        destination_foreign_keys=destination_foreign_keys,
        destination_config=destination_config,
        destination_connector_id=destination_connector_id,
        source_config=source_config,
        source_connector_id=source_connector_id,
        source_table=source_table,
        sample_rows=sample_rows,
        mappings=mappings,
        run_population_orphan_scan=run_population_orphan_scan,
        destination_check_constraints=destination_check_constraints,
        source_kind=source_kind,
        source_format=source_format,
        column_types=column_types,
        destination_column_types=destination_column_types,
        destination_db_type=destination_db_type,
        sync_mode=sync_mode,
        acknowledgment_actor=acknowledgment_actor,
        acknowledgment_reason=acknowledgment_reason,
        dest_kind=dest_kind,
    )

    # G22 — dest population RI is a post-write Gate-8 proof. State it every
    # Validate so the question is never unasked; never green-pass a sample.
    from services.destination_ri_probe import build_dest_ri_validate_gate

    has_relationships = bool(
        out.get("source_foreign_keys")
        or destination_foreign_keys
        or (out.get("constraint_findings") or [])
    )
    dest_ri_gate = build_dest_ri_validate_gate(has_relationships=has_relationships)
    out["gates"] = [
        g
        for g in out["gates"]
        if str(g.get("id") or "") != dest_ri_gate["id"]
    ] + [dest_ri_gate]
    out["passed_count"] = sum(1 for g in out["gates"] if g.get("status") == "pass")
    out["total_gates"] = len(out["gates"])
    out["readiness_score"] = round(
        out["passed_count"] / max(out["total_gates"], 1) * 100, 1
    )

    # Soft Snowflake warehouse sizing from G7 volume — never a GateId.
    try:
        dest_fmt = str(destination_db_type or "").strip().lower()
        if "snowflake" in dest_fmt:
            from services.snowflake_warehouse_advice import advise_snowflake_warehouse

            advice = advise_snowflake_warehouse(
                estimated_bytes=int(est_bytes or 0),
                row_count=int(row_count or 0),
            )
            if advice:
                out["snowflake_warehouse_advice"] = advice
    except Exception as sf_exc:
        logger.debug("snowflake warehouse advice skipped: %s", sf_exc, exc_info=sf_exc)

    # Multi-load intelligence: compare sample to last N loads of this route.
    try:
        from services.data_quality_history import compare_route_to_history

        from services.data_quality_history import resolve_history_endpoints

        src_table = (source_table or source_filename or "").strip()
        dst_table = (destination_table or "").strip()
        src_ep, dst_ep = resolve_history_endpoints(
            source_kind=source_kind,
            source_format=source_format or "",
            source_table=src_table,
            source_config=source_config,
            source_connector_id=source_connector_id or "",
            dest_kind="database",
            dest_format=destination_db_type or "",
            dest_table=dst_table,
            dest_config=destination_config,
        )
        out["load_history_report"] = compare_route_to_history(
            sample_rows or [],
            src_ep,
            dst_ep,
            schema=column_types,
        )
        # Module 17 — measured historical success (never invent a rate).
        from services.historical_success_contract import (
            measure_route_historical_success,
            stamp_mappings_historical_success,
        )

        hs = measure_route_historical_success(src_ep, dst_ep)
        out["historical_success"] = hs
        pb = dict(out.get("proof_bundle") or {})
        pb["historical_success"] = hs
        out["proof_bundle"] = pb
        # Stamp route-scoped evidence onto mapping rows carried in conversion contract.
        conv = dict(pb.get("conversion_contract") or {})
        if isinstance(conv.get("columns"), list):
            conv["columns"] = stamp_mappings_historical_success(conv["columns"], hs)
            pb["conversion_contract"] = conv
            out["proof_bundle"] = pb
    except Exception as hist_exc:
        logger.warning("load history compare failed during preflight", exc_info=True)
        out["load_history_report"] = {
            "passed": True,
            "anomalies": [],
            "prior_load_count": 0,
            "warning": f"Load-history compare unavailable: {hist_exc!s}"[:240],
        }
        try:
            from services.historical_success_contract import unmeasured_historical_success

            hs = unmeasured_historical_success(
                reason=f"Load history unavailable: {hist_exc!s}"[:200],
            )
            out["historical_success"] = hs
            pb = dict(out.get("proof_bundle") or {})
            pb["historical_success"] = hs
            out["proof_bundle"] = pb
        except Exception:
            pass

    if destination_evolution:
        out["destination_evolution"] = destination_evolution
        out["effective_mappings"] = mappings

    apply_schema_drift_gate(
        out,
        drift,
        schema_policy=schema_policy,
        schemaless=schemaless,
        schema_drift_acknowledged=schema_drift_acknowledged,
        acknowledgment_actor=acknowledgment_actor,
        acknowledgment_reason=acknowledgment_reason,
        mappings=mappings,
        columns=columns,
        column_types=column_types,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
    )

    _ps._apply_overwrite_emptied_gate(
        out,
        sync_mode=sync_mode,
        destination_table_exists=destination_table_exists,
        live_dest_columns=list(
            (destination_live_column_types or destination_column_types or {}).keys()
        ),
        mappings=mappings,
        regenerated=[
            *(destination_identity_columns or []),
            *(destination_generated_columns or []),
            *(["_id"] if schemaless else []),
        ],
        schema_policy=schema_policy,
        acknowledged=schema_drift_acknowledged,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
    )
    _ps._apply_empty_source_overwrite_gate(
        out,
        sync_mode=sync_mode,
        source_measured_empty=source_measured_empty,
        destination_table_exists=destination_table_exists,
        destination_config=dict(destination_config or {}),
        destination_table=destination_table,
        acknowledged=empty_source_overwrite_acknowledged,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
    )

    from services.root_cause_engine import apply_root_causes_to_preflight
    from services.validation_mode_contract import stamp_validation_mode

    # Schema inference runs after the first locale pass and can retype a
    # declared string column to a number (CSV ``amt`` "1,234" → INTEGER).
    # The assumption has to see that type: otherwise the column is skipped
    # as text, then the report demands an explicit locale for the number
    # the profiler just produced.
    if not number_locale and sample_rows and columns:
        assumed_late = assumed_number_locale(
            sample_rows,
            columns,
            number_locale=number_locale,
            column_types=column_types,
        )
        if assumed_late:
            number_locale_assumed = ambiguous_number_columns(
                sample_rows,
                columns,
                number_locale=number_locale,
                column_types=column_types,
            )
            number_locale = assumed_late
            set_active_number_locale(number_locale)
    number_findings = ambiguous_number_columns(
        sample_rows,
        columns,
        number_locale=number_locale,
        column_types=column_types,
    )
    date_findings = ambiguous_date_columns(
        sample_rows, columns, date_locale=date_locale
    )
    # WIRE is a property of the source, not an operator choice — the locale
    # control stays on Auto so it never reads back a value it cannot offer.
    from services.transform_engine import NUMBER_LOCALE_WIRE

    operator_locale = "" if number_locale == NUMBER_LOCALE_WIRE else number_locale
    out["number_locale"] = operator_locale
    out["number_locale_report"] = {
        "number_locale": operator_locale or "",
        "reads_typed_wire_values": number_locale == NUMBER_LOCALE_WIRE,
        "ambiguous_columns": number_findings or number_locale_assumed,
        "assumed": bool(number_locale_assumed),
        "assumed_columns": number_locale_assumed,
        "decision": (
            "assumed_us"
            if number_locale_assumed
            else ("set_locale" if number_findings else "ok")
        ),
    }
    # D94: PII the content scan found is said out loud even when it does not
    # force a review hold (email / phone / IP are medium-risk).
    from services.preflight_proof_bundle import pii_detection_warning

    _pii_warning = pii_detection_warning(
        (out.get("proof_bundle") or {}).get("compliance")
        if isinstance(out.get("proof_bundle"), dict)
        else None,
        list(mappings or []),
    )
    if _pii_warning:
        out.setdefault("warnings", []).append(_pii_warning)
    if number_locale_assumed:
        cols = ", ".join(f["column"] for f in number_locale_assumed[:6])
        out.setdefault("warnings", []).append(
            {
                "id": "number_locale_assumed",
                "message": (
                    f"{cols}: no locale declared and the sample carries no "
                    "grouping evidence — reading as US (1,234.56). If this "
                    "export is European, set number locale EU in "
                    "Destination → Advanced before you run."
                ),
                "details": {"columns": number_locale_assumed},
            }
        )
    out["date_locale"] = date_locale
    out["date_locale_report"] = {
        "date_locale": date_locale or "",
        "ambiguous_columns": date_findings,
        "decision": "set_locale" if date_findings else "ok",
    }
    if date_findings:
        cols = ", ".join(f["column"] for f in date_findings[:6])
        out.setdefault("warnings", []).append(
            {
                "id": "date_locale",
                "message": (
                    f"{cols}: day/month order is ambiguous (01/02/2024 is "
                    "Jan 2 or Feb 1). Set date locale DMY or MDY in "
                    "Destination → Advanced — Auto will not guess."
                ),
                "details": {"columns": date_findings},
            }
        )
    if number_findings:
        cols = ", ".join(f["column"] for f in number_findings[:6])
        out.setdefault("warnings", []).append(
            {
                "id": "number_locale",
                "message": (
                    f"{cols}: grouping is ambiguous (1,234 vs 1.234). "
                    "Set number locale US or EU in Destination → Advanced — "
                    "Auto will not guess."
                ),
                "details": {"columns": number_findings},
            }
        )

    # D40: a numeric cast written into a text column re-spells the value —
    # '00042' lands as '42' — although the text column could hold the original.
    # Name the sampled values it rewrites; the operator may want it, so warn.
    from services.transform_engine import apply_transform
    from services.type_system import normalize_logical_type
    from services.value_serializer import is_reader_null_cell

    rewritten: list[dict[str, Any]] = []
    for pm in plan_mappings:
        if pm.transform not in {"integer", "decimal"}:
            continue
        if normalize_logical_type(pm.target_type) not in {"string", "text"}:
            continue
        examples: list[str] = []
        for row in sample_rows or []:
            cell = row.get(pm.source)
            if is_reader_null_cell(cell):
                continue
            before = wire_cell_text(cell)
            after, err = apply_transform(before, pm.transform)
            if err is None and after is not None and str(after) != before.strip():
                examples.append(f"'{before}' → '{after}'")
                if len(examples) >= 3:
                    break
        if examples:
            rewritten.append({"column": pm.source, "transform": pm.transform, "examples": examples})
    if rewritten:
        out.setdefault("warnings", []).append(
            {
                "id": "numeric_cast_into_text",
                "message": "; ".join(
                    f"{r['column']}: {r['transform']} cast into a text column rewrites "
                    f"{', '.join(r['examples'])}"
                    for r in rewritten[:6]
                )
                + ". Leading zeros and formatting are lost. Remove the cast to keep "
                "the source text, or keep it if the normalised number is intended.",
                "details": {"columns": rewritten},
            }
        )

    # One cast type for the plan column and the transform that will run.
    # A live INTEGER and a DECIMAL stamp used to both appear (R14).
    out["plan"] = {
        "mappings": [
            {
                "source": m.source,
                "target": m.target,
                "transform": m.transform,
                "target_type": m.target_type,
            }
            for m in plan.mappings
        ],
        "destination_column_types": {
            c.name: c.inferred_type for c in plan.destination.target_columns
        },
    }

    stamp_validation_mode(out, validation_mode)
    # Discovery / Audit: never invent an Execute unlock from Validate readiness.
    try:
        from services.validation_mode_contract import mode_allows_write, mode_contract

        if not mode_allows_write(validation_mode):
            c = mode_contract(validation_mode)
            out["execute_unlocked"] = False
            out["write_refused"] = True
            out.setdefault("warnings", [])
            if isinstance(out["warnings"], list):
                out["warnings"].append(
                    f"Mode `{c['id']}` refuses destination writes — "
                    f"{c['non_guarantees'][0]}"
                )
    except Exception as mode_exc:
        logger.debug("validation mode stamp side-effects skipped: %s", mode_exc)

    from services.file_pickup import attach_direct_dated_file_warning

    attach_direct_dated_file_warning(out, source_table or source_filename)
    _lift_in_file_collapse_advisories(out)
    out = apply_root_causes_to_preflight(
        _block_decision_on_hard_gate(apply_readiness_honesty_caps(out))
    )
    from services.preflight_policy_gates import apply_fast_preflight_scope, stamp_gate_contract

    out = apply_fast_preflight_scope(out, preflight_scope)
    return stamp_gate_contract(out)


# --------------------------------------------------------------------------- #
# Destination probe / shape inspection lives in its own module (Phase F8 size
# freeze) and is re-exported here so every existing import keeps working.
# --------------------------------------------------------------------------- #
from services.preflight_destination import (  # noqa: E402,F401
    _available_staging_bytes,
    inspect_destination_for_preflight,
    probe_destination,
)

from services.preflight_policy_gates import (  # noqa: E402,F401
    apply_policy_gates,
    is_compliance_only_block,
)
from services.preflight_source_kind import resolve_preflight_source_kind  # noqa: E402,F401

_EXTRACTED_PREFLIGHT_EXPORTS = {
    "FilePreflightContext": "preflight_context",
    "_apply_overwrite_emptied_gate": "preflight_population",
    "_apply_empty_source_overwrite_gate": "preflight_population",
    "_cdc_log_capture_gates": "preflight_population",
    "_iter_table_population_for_preflight": "preflight_population",
}


def __getattr__(name: str):
    module_name = _EXTRACTED_PREFLIGHT_EXPORTS.get(name)
    if module_name is not None:
        from importlib import import_module

        module = import_module(f"services.{module_name}")
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


from services.preflight_transfer_policy import (  # noqa: E402,F401 - re-export
    run_transfer_policy_gates,
)
