"""Execute's half of gate G13: no source column leaves a run without a decision.

Validate refuses a run while a source column is neither mapped nor declared
omitted (:mod:`services.source_coverage_gate`). Overwrite into an existing
table relies on that: its auto-map keeps only the columns the destination
already has and leaves the rest unaccounted "so G13 can block"
(``retain_dest_exists_write_mappings``). A run that skips Validate
(``skip_preflight``: quarantine replay, the proof ledger, the SDK, the desktop
lab) never reached G13. The column was read, never written, and Gate-8 then
compared the mapped columns only, so the job read "Row fidelity verified" over
a destination that lacked a source column. A replay of mappings stored before
the column existed did the same in every sync mode.

Rules applied:

* R8 (``defects.txt`` section 3) and ``docs/migration-rules.md`` rule 12:
  "The plan and execute run the same gates on the same inputs." This is the
  G13 predicate itself (:func:`build_source_coverage_gate`), evaluated at
  Execute for every sync mode and for the COPY and row paths alike, because
  all three engine flows call it before the first write.
* ``docs/migration-rules.md`` rule 7: a source column "is added on the
  destination ... or the schedule parks on a schema-drift finding. Events are
  not applied while a new column is dropped on the floor." A propagate policy
  (or backfill) adopts the column as an ADD COLUMN before this check, so only
  a column nobody decided on reaches the refusal.
* R7 and the schema policy (``SCHEMA_POLICY_HONESTY``: under
  ``manual_review`` "Execute does not ADD COLUMN"): a run without an
  approving policy does not add the column by itself here. It stops before
  the first write and names the choices.

What a finished run then reports about the columns it left out by decision is
:mod:`services.column_coverage_report`.
"""

from __future__ import annotations

from typing import Any

from .engine_schema_propagation import SchemaPropagationRefused

_REMEDY = (
    "Nothing was written. Set the schema policy to propagate columns (or turn on "
    "backfill new fields) so the run adds the column to the destination, map the "
    "column to a destination column, or declare it omitted, then run again."
)


_ADD_REMEDY = (
    "Nothing was written. Set the schema policy to propagate columns (or turn on "
    "backfill new fields), or acknowledge the schema drift for this run, so the run "
    "adds the column; or declare it omitted, then run again."
)


class SourceColumnsUnaccounted(SchemaPropagationRefused):
    """A run without Validate would have dropped, or added unapproved, a source column."""

    def __init__(
        self,
        message: str,
        *,
        columns: list[str],
        gate: str = "g13_source_coverage",
        rule_id: str = "g13_source_coverage.unaccounted",
        fix: str = _REMEDY,
    ) -> None:
        super().__init__(message)
        self.columns = list(columns)
        #: Merged into the job's ``error_details`` (``_job_failure_fields``), so
        #: the refusal is classified by the gate id Validate blocks with.
        self.failure_details = {
            "retriable": False,
            "unaccounted_sources": list(columns),
            "rule_id": rule_id,
            "guidance": [{"gate": gate, "message": message, "fix": fix}],
        }


def source_column_coverage(
    columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
    lineage: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Which source columns this plan writes, carries inside a parent, omits, or leaves unanswered."""
    from services.mapping_constraints import classify_source_coverage

    return classify_source_coverage(list(columns or []), list(mappings or []), lineage)


def refuse_unaccounted_without_validate(
    request: Any,
    columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
    lineage: dict[str, str] | None = None,
) -> None:
    """Raise when Validate will not run and a source column has no decision.

    With Validate on, G13 owns the block, its approval request and the recorded
    acknowledgement, so this stays silent and the job reaches that gate.
    """
    if request is None or not bool(getattr(request, "skip_preflight", False)):
        return
    from services.source_coverage_gate import build_source_coverage_gate

    coverage, gate = build_source_coverage_gate(
        source_columns=list(columns or []), mappings=list(mappings or []), lineage=lineage
    )
    if not coverage["unaccounted"]:
        return
    raise SourceColumnsUnaccounted(
        f"{gate['message']} {_REMEDY}", columns=list(coverage["unaccounted"])
    )


#: Keys a quarantined row carries that are not source columns: the marker of a
#: finding that refused the whole row (``"column": "*"``) and, on a CDC route
#: only, the ``_df_lsn`` the CDC writer stamped on the image. A replay of those
#: rows judged them as undecided source columns and refused every CDC/CHECK
#: replay (train 2 CI). Elsewhere ``_df_lsn`` is an ordinary source column (a
#: table a CDC pipeline filled, copied on in batch) and is judged like any other.
_REPLAY_ROW_MARKER = "*"
_CDC_LSN_STAMP = "_df_lsn"


def replay_source_columns(columns: list[str] | None, *, cdc: bool) -> list[str]:
    """The source columns of quarantined rows, without the row marker (and, on CDC, the LSN stamp)."""
    skip = {_REPLAY_ROW_MARKER, _CDC_LSN_STAMP} if cdc else {_REPLAY_ROW_MARKER}
    return [c for c in columns or [] if str(c).strip().lower() not in skip]


def is_cdc_route(request: object) -> bool:
    """True when the run, or any stream it selects, applies CDC."""
    modes = [str(getattr(request, "sync_mode", "") or "")]
    for contract in getattr(request, "stream_contracts", None) or []:
        if isinstance(contract, dict) and contract.get("selected", True):
            modes.append(str(contract.get("sync_mode") or ""))
    return any(m.strip().lower().startswith("cdc") for m in modes)


def _live_has(target: str, live: set[str]) -> bool:
    """Whether the destination already holds ``target``, under any spelling it stores."""
    from connectors.sql_identifiers import sanitized_spellings

    name = str(target or "").strip()
    if not name:
        return True
    if name.lower() in live:
        return True
    return any(str(spelling).lower() in live for spelling in sanitized_spellings(name))


def refuse_unapproved_add_without_validate(
    request: Any,
    columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
    dest_schema_types: dict[str, str] | None,
) -> None:
    """Raise when Validate will not run and a mapping would ADD a column unapproved.

    Under ``manual_review`` Execute does not ADD COLUMN in any sync mode
    (``SCHEMA_POLICY_HONESTY``); Validate blocks the same proposal. Without
    Validate, auto-map (or a stored ``create_new`` stamp) still armed the
    writer's ADD COLUMN lane in every mode but overwrite. The approval is the
    predicate Validate and Execute share (``destination_evolution_approved``):
    a propagate policy, backfill new fields, or an acknowledged drift. The
    refusal is Validate's own sentence (``schema_review_copy``).
    """
    if request is None or not bool(getattr(request, "skip_preflight", False)):
        return
    live = {str(c).strip().lower() for c in (dest_schema_types or {}) if str(c).strip()}
    if not live:
        return  # no existing table: its columns are a CREATE, not an ADD COLUMN
    from services.db_type_utils import dest_declares_column_ddl

    if not dest_declares_column_ddl(str(getattr(request.destination, "format", "") or "")):
        return
    from services.schema_drift import (
        destination_evolution_approved,
        effective_schema_policy,
        schema_review_copy,
    )

    policy = effective_schema_policy(
        getattr(request, "schema_policy", None),
        backfill_new_fields=bool(getattr(request, "backfill_new_fields", False)),
    )
    if destination_evolution_approved(
        policy, acknowledged=bool(getattr(request, "schema_drift_acknowledged", False))
    ):
        return
    from services.shape_contract import write_ready_mappings

    source_names = {str(c).strip().lower() for c in columns or [] if str(c).strip()}
    missing: list[str] = []
    for mapping in write_ready_mappings(mappings):
        source = str(mapping.get("source") or "").strip()
        if source.lower() not in source_names or _live_has(str(mapping.get("target") or ""), live):
            continue
        if source not in missing:
            missing.append(source)
    if not missing:
        return
    copy = schema_review_copy(
        {
            "saved_contract": False,
            "schema_evolution": {
                "additive": [{"kind": "add_column", "column": name} for name in missing]
            },
        },
        policy=policy,
    )
    raise SourceColumnsUnaccounted(
        f"{copy['message']} Schema policy '{policy}' does not add columns. {_ADD_REMEDY}",
        columns=missing,
        gate=str(copy["gate_id"]),
        rule_id=str(copy["rule_id"]),
        fix=_ADD_REMEDY,
    )


#: The engine's pre-write existence probe of the destination table (True /
#: False / None). False means this run creates the table.
DEST_EXISTED_PROBE_KEY = "_df_dest_table_existed_probe"
#: Stamped on ``destination.extra`` by the engine before the first write, so a
#: stream that discovers a field mid-run knows whether the run's policy adds it.
MID_RUN_POLICY_KEY = "_df_mid_run_columns"

#: The same words as docs/use/verification.md ("Resuming with a decision").
_MID_RUN_REMEDY = (
    "Nothing of that batch was written; the batches before it are committed. "
    "Resume the job with schema_policy set to propagate_columns (or "
    "backfill_new_fields true) to add the field, or with the field in "
    "omit_columns to leave it out, and a reason; the resume continues from "
    "that batch."
)


def stamp_mid_run_column_policy(request: Any, dest_schema_types: dict[str, str] | None) -> None:
    """Record, for the stream, whether this run may add a field first seen mid-run.

    Only a propagate policy or backfill new fields approves it. An acknowledged
    drift approves the columns Validate disclosed, and a field no page had shown
    yet was not among them.
    """
    destination = getattr(request, "destination", None)
    if destination is None:
        return
    from services.schema_drift import destination_evolution_approved, effective_schema_policy

    policy = effective_schema_policy(
        getattr(request, "schema_policy", None),
        backfill_new_fields=bool(getattr(request, "backfill_new_fields", False)),
    )
    if not isinstance(getattr(destination, "extra", None), dict):
        destination.extra = {}
    extra = destination.extra
    destination.extra[MID_RUN_POLICY_KEY] = {
        "adds_columns": destination_evolution_approved(policy, acknowledged=False),
        "policy": policy,
        "acknowledged": bool(getattr(request, "schema_drift_acknowledged", False)),
        "dest_columns": sorted({str(c).strip().lower() for c in dest_schema_types or {} if str(c).strip()}),
        # A table this run creates is the run's own: a field it meets mid-run is
        # added and disclosed. Only a table that existed before the run stops,
        # and a table whose existence cannot be told counts as existing.
        "dest_existed": bool(dest_schema_types)
        or bool(extra.get("overwrite_replaced_column_types"))
        or extra.get(DEST_EXISTED_PROBE_KEY) is not False,
    }


def omitted_source_names(mappings: list[dict[str, Any]] | None) -> frozenset[str]:
    """Source fields the operator declared omitted (kept before omits leave the write map)."""
    from services.mapping_constraints import is_intentional_omit

    return frozenset(
        str(m.get("source") or "").strip().lower()
        for m in mappings or []
        if isinstance(m, dict) and is_intentional_omit(m) and str(m.get("source") or "").strip()
    )


def refuse_mid_run_column(
    destination: Any,
    dest_type: str,
    fields: list[str],
    mappings: list[dict[str, Any]] | None,
    lineage: dict[str, str] | None = None,
    *,
    dest_columns: list[str] | None = None,
    dest_existed: bool | None = None,
) -> frozenset[str]:
    """Raise before a batch is written when it brings a field the policy does not add.

    ``fields`` are the batch's new, unmapped, not-omitted fields. A field whose
    name the destination table already has is not an ADD COLUMN. A flattened
    child whose parent object is written whole is carried inside it (G13): it
    is neither refused nor added, and is returned so the caller holds it off
    the write. A stream run without the engine's stamp (a benchmark or a direct
    caller) keeps the writer's previous behaviour.

    ``dest_columns`` / ``dest_existed`` are this stream's own table (a
    multi-table run reads each one); without them the run's stamp answers. A
    table this run created is the run's own: its new fields are added (and
    disclosed as added during the run), never refused.
    """
    stamp = (getattr(destination, "extra", None) or {}).get(MID_RUN_POLICY_KEY)
    if not isinstance(stamp, dict) or stamp.get("adds_columns") is not False:
        return frozenset()
    existed = stamp.get("dest_existed", True) if dest_existed is None else dest_existed
    if existed is False:
        return frozenset()
    from services.db_type_utils import dest_declares_column_ddl

    if not dest_declares_column_ddl(str(dest_type or "")):
        return frozenset()
    from services.mapping_constraints import carried_by_parent

    carried = frozenset(f for f in fields if f and carried_by_parent(f, mappings, lineage))
    live = {
        str(c).lower() for c in (stamp.get("dest_columns") if dest_columns is None else dest_columns) or []
    }
    new = [f for f in fields if f and f not in carried and not _live_has(f, live)]
    if not new:
        return carried
    from services.source_coverage_gate import build_source_coverage_gate

    _coverage, gate = build_source_coverage_gate(source_columns=new, mappings=list(mappings or []))
    raise SourceColumnsUnaccounted(
        f"{gate['message']} First seen in a later batch; schema policy "
        f"'{stamp.get('policy') or 'manual_review'}' does not add columns. {_MID_RUN_REMEDY}",
        columns=new,
        fix=_MID_RUN_REMEDY,
    )


def stream_coverage_verdict(
    *,
    stream: str,
    source_columns: list[str],
    mappings: list[dict[str, Any]] | None,
    lineage: dict[str, str] | None,
    dest_types: dict[str, str] | None,
    dest_type: str,
    policy: str,
    acknowledged: bool,
) -> dict[str, Any]:
    """G13 and the unapproved-ADD rule for ONE table of a run, with its own map.

    The same verdict at Validate (``services.stream_coverage_gates``) and at
    Execute (:func:`enforce_stream_coverage`), so a multi-table job is judged
    per table exactly as a single table is. An acknowledged drift records the
    table's undecided columns as left out under it, the way Validate's
    acknowledgement does for the primary table.
    """
    from services.db_type_utils import dest_declares_column_ddl
    from services.schema_drift import destination_evolution_approved, schema_review_copy
    from services.shape_contract import write_ready_mappings
    from services.source_coverage_gate import build_source_coverage_gate

    coverage, gate = build_source_coverage_gate(
        source_columns=list(source_columns or []), mappings=list(mappings or []), lineage=lineage
    )
    unaccounted = list(coverage["unaccounted"])
    acknowledged_out = unaccounted if (acknowledged and unaccounted) else []
    live = {str(c).strip().lower() for c in dest_types or {} if str(c).strip()}
    names = {str(c).strip().lower() for c in source_columns or []}
    adds: list[str] = []
    if live and dest_declares_column_ddl(dest_type):
        for m in write_ready_mappings(mappings):
            src = str(m.get("source") or "").strip()
            if src.lower() in names and not _live_has(str(m.get("target") or ""), live) and src not in adds:
                adds.append(src)
    approved = destination_evolution_approved(policy, acknowledged=acknowledged)
    present = {n for n in names}
    record = {
        "table": stream,
        "added_columns": adds if approved else [],
        "skipped_columns": [],
        "acknowledged_columns": acknowledged_out,
        "omitted_columns": [c for c in coverage["omitted"] if str(c).lower() in present],
        "carried_columns": list(coverage.get("carried") or []),
        "source_column_count": len(source_columns or []),
    }
    message, gate_id, rule_id, columns = "", "", "", []
    if unaccounted and not acknowledged_out:
        message, gate_id, rule_id, columns = (
            gate["message"], "g13_source_coverage", "g13_source_coverage.unaccounted", unaccounted,
        )
    elif adds and not approved:
        copy = schema_review_copy(
            {"saved_contract": False, "schema_evolution": {"additive": [{"kind": "add_column", "column": c} for c in adds]}},
            policy=policy,
        )
        message = f"{copy['message']} Schema policy '{policy}' does not add columns."
        gate_id, rule_id, columns = str(copy["gate_id"]), str(copy["rule_id"]), adds
    return {
        "stream": stream,
        "status": "block" if message else "pass",
        "message": f"Table {stream}: {message}" if message else "",
        "gate": gate_id,
        "rule_id": rule_id,
        "columns": columns,
        "record": record,
    }


def _stream_live_types(dest_type: str, dest_cfg: dict[str, Any], table: str) -> dict[str, str]:
    from .adapters_introspect import _introspect_table_schema

    try:
        return dict(_introspect_table_schema(dest_type, dest_cfg, table, [], strict_namespace=True) or {})
    except Exception:  # noqa: BLE001 - an unread table is judged as absent, as a CREATE
        return {}


def capture_stream_dest_types(destination: Any, stream_name: str) -> dict[str, str]:
    """The stream's destination columns BEFORE an overwrite sets its table aside.

    Only a destination with column DDL can be asked to ADD COLUMN, so only it
    is read: a topic, a bucket or a key space has no columns to compare, and a
    catalog read there costs a connect timeout per table for nothing.
    """
    from services.db_type_utils import dest_declares_column_ddl

    from .adapters import resolve_connector_config, resolve_dest_table
    from .connector_capabilities import resolve_driver_type

    try:
        dest_type = resolve_driver_type(str(getattr(destination, "format", "") or ""))
        if not dest_declares_column_ddl(dest_type):
            return {}
        table = resolve_dest_table(dest_type, destination, stream_name)
        cfg = resolve_connector_config(destination)
    except Exception:  # noqa: BLE001 - unread: the writer still refuses a missing column
        return {}
    return _stream_live_types(dest_type, cfg, str(table or ""))


def table_existed_before(destination: Any, stream_name: str = "") -> bool | None:
    """Whether this table existed before the run: ``None`` when it cannot be told.

    Asked before anything is written or set aside, so ``False`` means this run
    creates the table. Callers treat ``None`` as existing (fail closed): a table
    whose existence is unknown is never assumed to be the run's own.
    """
    try:
        from services.db_type_utils import dest_declares_column_ddl
        from services.dest_precount import destination_table_exists
        from services.dialect_profiles import schema_from_cfg

        from .adapters import resolve_connector_config, resolve_dest_table
        from .connector_capabilities import resolve_driver_type

        dest_type = resolve_driver_type(str(getattr(destination, "format", "") or ""))
        if not dest_declares_column_ddl(dest_type):
            return None
        cfg = resolve_connector_config(destination)
        table = resolve_dest_table(dest_type, destination, stream_name or "import")
        return destination_table_exists(
            dest_type, cfg, schema=schema_from_cfg(dest_type, cfg), table_name=str(table or "")
        )
    except Exception:  # noqa: BLE001 - unknown existence is never "created by this run"
        return None


def judge_tables_before_any_write(
    *,
    source: Any,
    destination: Any,
    streams: list[tuple[str, list[dict[str, Any]] | None]],
    request_mappings: list[dict[str, Any]] | None,
    original: tuple[Any, Any, Any, Any],
) -> dict[str, dict[str, Any]]:
    """Judge every listed table of a multi-table run before the first is written.

    For the routes that do not reach the stream's map step (SCD2 / mirror
    staging merges, CDC): each table's catalog columns (read the way Validate
    reads them), its own map (or the identity map a table without one gets),
    its destination columns and whether it existed. A blocked table stops the
    run before any table is touched; an unreadable one too (fail closed).
    Returns ``{table: {record, dest_types, existed}}``.
    """
    stamp = (getattr(destination, "extra", None) or {}).get(MID_RUN_POLICY_KEY)
    if not isinstance(stamp, dict):
        return {}
    import copy as _copy

    from services.flatten_protection import source_flattens, stamped_lineage
    from services.multi_stream_plan import adopt_inherited_mappings
    from services.multi_stream_targets import bind_endpoint_to_stream
    from services.source_schema_authority import _endpoint_catalog_info

    src_table, src_collection, dest_table, dest_collection = original
    judged: dict[str, dict[str, Any]] = {}
    for name, own in streams:
        src = _copy.copy(source)
        src.extra = dict(getattr(source, "extra", None) or {})
        bind_endpoint_to_stream(src, name, table=src_table, collection=src_collection)
        dst = _copy.copy(destination)
        dst.extra = dict(getattr(destination, "extra", None) or {})
        if dest_table is not None or dest_collection is not None:
            bind_endpoint_to_stream(dst, name, table=dest_table, collection=dest_collection)
        info = _endpoint_catalog_info(src)
        schema = info.get("schema") if isinstance(info.get("schema"), dict) else {}
        columns = [str(c) for c in schema]
        if not columns:
            raise SourceColumnsUnaccounted(
                f"Table {name}: its source columns could not be read before the run, so "
                "Datawrap cannot show that each one is mapped or declared omitted. Nothing "
                "was written. Check the source connection, then run again.",
                columns=[],
            )
        mappings = list(own) if own else adopt_inherited_mappings(request_mappings, columns)[0]
        lineage = stamped_lineage(mappings)
        if source_flattens(str(getattr(src, "format", "") or ""), src.extra):
            lineage.update(info.get("flatten_lineage") or {})
        dest_types = capture_stream_dest_types(dst, name)
        verdict = stream_coverage_verdict(
            stream=name, source_columns=columns, mappings=mappings, lineage=lineage,
            dest_types=dest_types, dest_type=str(getattr(dst, "format", "") or ""),
            policy=str(stamp.get("policy") or "manual_review"),
            acknowledged=bool(stamp.get("acknowledged")),
        )
        if verdict["status"] == "block":
            fix = _ADD_REMEDY if verdict["gate"] != "g13_source_coverage" else _REMEDY
            raise SourceColumnsUnaccounted(
                f"{verdict['message']} {fix}", columns=verdict["columns"],
                gate=verdict["gate"], rule_id=verdict["rule_id"], fix=fix,
            )
        judged[name] = {
            "record": verdict["record"],
            # The map the table writes with: adopted when it has none, and with
            # the ADD COLUMN the policy approves planned and typed, as the
            # single-table flow plans it (staged merges type their spool from it).
            "mappings": _with_approved_adds(dst, mappings, schema, dest_types, stamp),
            "dest_types": dest_types,
            "existed": table_existed_before(dst, name),
        }
    return judged


def _with_approved_adds(
    destination: Any,
    mappings: list[dict[str, Any]],
    source_types: dict[str, Any],
    dest_types: dict[str, str],
    stamp: dict[str, Any],
) -> list[dict[str, Any]]:
    from services.db_type_utils import dest_declares_column_ddl
    from services.schema_drift import (
        PROPAGATE_POLICIES,
        destination_evolution_approved,
        plan_destination_evolution,
    )

    policy = str(stamp.get("policy") or "manual_review")
    dest_db = str(getattr(destination, "format", "") or "").lower()
    if not (
        dest_types
        and dest_declares_column_ddl(dest_db)
        and destination_evolution_approved(policy, acknowledged=bool(stamp.get("acknowledged")))
    ):
        return mappings
    from types import SimpleNamespace

    from .mapping_write_stamp import stamp_additive_mappings_for_write

    schema = {str(k): str(v) for k, v in (source_types or {}).items()}
    planned, _types, _applied = plan_destination_evolution(
        mappings, source_columns=list(schema), source_schema=schema, dest_types=dest_types,
        dest_db=dest_db, adopt_unmapped=policy in PROPAGATE_POLICIES,
    )
    request = SimpleNamespace(
        destination=destination, sync_mode="", schema_policy=policy, backfill_new_fields=False,
    )
    return stamp_additive_mappings_for_write(
        request, planned, column_types=schema, dest_types=dest_types, dest_table_exists=True
    )


def _run_stamp(cfg: Any) -> dict[str, Any] | None:
    stamp = (getattr(cfg.destination, "extra", None) or {}).get(MID_RUN_POLICY_KEY)
    return stamp if isinstance(stamp, dict) else None


def enforce_stream_coverage(run: Any, probe: Any, write_columns: list[str], per_stream: dict[str, Any]) -> None:
    """Execute: judge this table with the map it writes with; record it on its summary."""
    cfg = run.cfg
    dest_types = dict((per_stream or {}).get("dest_types") or {})
    stamp = _run_stamp(cfg)
    if stamp is None:
        return  # not under the engine (a benchmark or a direct caller)
    from services.flatten_protection import coverage_lineage

    rows = [
        row if isinstance(row, dict) else dict(zip(getattr(probe, "headers", []) or [], row))
        for row in (getattr(probe, "rows", None) or [])[:100]
    ]
    lineage = coverage_lineage(
        str(getattr(cfg.source, "format", "") or ""), getattr(cfg.source, "extra", None), rows, cfg.mappings
    )
    verdict = stream_coverage_verdict(
        stream=str(getattr(cfg.source, "table", "") or getattr(cfg.source, "collection", "") or ""),
        source_columns=list(write_columns or []),
        mappings=list(cfg.mappings or []),
        lineage=lineage,
        dest_types=dest_types,
        dest_type=str(cfg.dest_type or ""),
        policy=str(stamp.get("policy") or "manual_review"),
        acknowledged=bool(stamp.get("acknowledged")),
    )
    cfg.dest_live_columns = sorted({str(c).lower() for c in dest_types or {}})
    # Probed before the run touched the table; unknown counts as existing.
    cfg.dest_existed = (per_stream or {}).get("existed") is not False
    # The stream starts its summary after this point; it copies the record in.
    cfg.coverage_record = verdict["record"]
    if verdict["status"] == "block":
        fix = _ADD_REMEDY if verdict["gate"] != "g13_source_coverage" else _REMEDY
        raise SourceColumnsUnaccounted(
            f"{verdict['message']} {fix}", columns=verdict["columns"],
            gate=verdict["gate"], rule_id=verdict["rule_id"], fix=fix,
        )


def copy_declined_for_stream_map(cfg: Any, dest_types: dict[str, str] | None) -> bool:
    """A table of a multi-table run whose own map is incomplete takes the row path.

    A server-side copy moves the mapped columns only and never reaches the map
    step, so the per-table verdict runs here on the table's catalog columns: a
    block keeps the table on the row path (which judges it again with its
    sampled rows and refuses before any write); a pass lets the copy run and
    leaves the table's record on ``cfg.coverage_record`` for its summary.
    """
    stamp = _run_stamp(cfg)
    if stamp is None or not getattr(cfg, "mappings", None):
        return False
    from services.copy_fast_path import note_copy_decline
    from services.flatten_protection import stamped_lineage

    from .adapters_introspect import _introspect_table_schema

    table = str(getattr(cfg.source, "table", "") or getattr(cfg.source, "collection", "") or "")
    try:
        columns = list(_introspect_table_schema(cfg.src_read_type, cfg.src_cfg, table, []) or {})
    except Exception:  # noqa: BLE001 - an unread catalog proves nothing, so no copy
        columns = []
    verdict = (
        stream_coverage_verdict(
            stream=table, source_columns=columns, mappings=list(cfg.mappings),
            lineage=stamped_lineage(cfg.mappings), dest_types=dest_types,
            dest_type=str(cfg.dest_type or ""), policy=str(stamp.get("policy") or "manual_review"),
            acknowledged=bool(stamp.get("acknowledged")),
        )
        if columns
        else None
    )
    if verdict is not None and verdict["status"] == "pass":
        cfg.coverage_record = verdict["record"]
        return False
    note_copy_decline(
        "this table's own column map leaves a column undecided or adds one; the row "
        "path judges it per table before any write"
    )
    return True


__all__ = [
    "MID_RUN_POLICY_KEY",
    "capture_stream_dest_types",
    "copy_declined_for_stream_map",
    "enforce_stream_coverage",
    "stream_coverage_verdict",
    "omitted_source_names",
    "refuse_mid_run_column",
    "stamp_mid_run_column_policy",
    "SourceColumnsUnaccounted",
    "refuse_unaccounted_without_validate",
    "refuse_unapproved_add_without_validate",
    "source_column_coverage",
]
