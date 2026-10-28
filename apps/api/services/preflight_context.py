from __future__ import annotations

from contextlib import nullcontext
from typing import Any

from preflight.models import PreflightContext, TransferPlan
from services import preflight_service as _ps
from services.preflight_source_kind import source_is_record_batch
from services.value_serializer import cell_to_string, project_row_cells


def _source_is_record_batch(ctx: FilePreflightContext) -> bool:
    source = ctx.plan.source
    return source_is_record_batch(
        getattr(source, "kind", ""),
        getattr(source, "db_type", ""),
    )


def _database_extract(ctx: FilePreflightContext) -> bool:
    kind = str(getattr(ctx.plan.source, "kind", "") or "").lower()
    return kind == "database" and not _source_is_record_batch(ctx)


def _sample_headers(ctx: FilePreflightContext, rows: list[dict[str, Any]]) -> list[str]:
    """Every column the sample can speak for, not just the first row's keys.

    A sparse document omits the keys it does not carry. Taking the headers from
    row 0 turned a field absent from *that* document into "Source column
    missing" for the whole mapping, even after the risk contract was signed
    (D150). A document source's declared fields join too: a field no sampled
    document carries is NULL for those rows, which is what the writer stores.
    """
    from connectors.header_union import SCHEMALESS_SOURCE_TYPES, union_attribute_keys

    source = ctx.plan.source
    declared: list[str] = []
    if str(getattr(source, "db_type", "") or "").strip().lower() in SCHEMALESS_SOURCE_TYPES:
        declared = [c.name for c in getattr(source, "columns", None) or []]
    return union_attribute_keys(
        *(list(row) for row in rows if isinstance(row, dict)), declared
    )


def _sample_file_groups(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Split a peek sample on ``_df_file`` so keys shared only across files do not collide."""
    tagged = any(isinstance(row, dict) and "_df_file" in row for row in rows)
    if not tagged:
        return [list(rows)]
    groups: dict[str, list[dict[str, Any]]] = {}
    order: list[str] = []
    for row in rows:
        tag = str(row.get("_df_file") or "") if isinstance(row, dict) else ""
        if tag not in groups:
            order.append(tag)
            groups[tag] = []
        groups[tag].append(row)
    return [groups[tag] for tag in order]


class FilePreflightContext(PreflightContext):
    """Preflight context for file → database transfers."""

    def __init__(
        self,
        plan: TransferPlan,
        sample_rows: list[dict] | None = None,
        destination_collision: Any = None,
        source_duplicate_findings: list[dict[str, Any]] | None = None,
        source_duplicate_probe_ran: bool = False,
        source_duplicate_probe_pk: str = "",
        source_duplicate_probe_status: str = "",
        source_duplicate_probe_message: str = "",
        source_duplicate_probe_expected: bool = False,
        empty_cells_as_null: bool = False,
        proven_width_narrowings: Any = None,
        population_fit_report: Any = None,
    ):
        super().__init__(plan=plan, sample_rows=sample_rows or [])
        self.proven_width_narrowings = proven_width_narrowings or {}
        self.population_fit_report = population_fit_report
        # Spreadsheet blanks are absence. Only file sources opt in — the same
        # flag the file writer uses. Database extracts keep it off.
        self.empty_cells_as_null = bool(empty_cells_as_null)
        self.destination_collision = destination_collision
        self.source_duplicate_findings = source_duplicate_findings or []
        self.source_duplicate_probe_ran = bool(source_duplicate_probe_ran)
        self.source_duplicate_probe_pk = str(source_duplicate_probe_pk or "")
        self.source_duplicate_probe_status = str(source_duplicate_probe_status or "")
        self.source_duplicate_probe_message = str(source_duplicate_probe_message or "")
        self.source_duplicate_probe_expected = bool(source_duplicate_probe_expected)

    def _mapping_dict_for_probe(self, m: Any, dest_types: dict[str, str]) -> dict[str, Any]:
        """Serialize plan mappings for coercion / integrity — keep Map Accept risk.

        Stripping risk_acknowledged / fidelity left Validate blocked after Map
        Accept risk (G3 probe severity + G9 coercion_safety).
        """
        live = dest_types.get(m.target)
        if live is None:
            from services.column_case import lookup_column

            live = lookup_column(dest_types, m.target)
        stamped = live or getattr(m, "target_type", None)
        src_type = None
        from services.column_case import column_type_or_none

        src_type = column_type_or_none(
            {c.name: c.inferred_type for c in self.plan.source.columns},
            m.source,
        )
        if (
            not live
            and stamped
            and src_type
            and not bool(getattr(m, "user_override", False))
        ):
            try:
                from services.decision_kernel import refuse_create_new_numeric_collapse

                dest_db = str(getattr(self.plan.destination, "db_type", "") or "")
                stamped = refuse_create_new_numeric_collapse(
                    str(src_type), str(stamped), dest_db
                )
            except Exception:
                pass
        return {
            "source": m.source,
            "target": m.target,
            "confidence": getattr(m, "confidence", 0.0),
            "transform": getattr(m, "transform", None),
            "requires_review": bool(getattr(m, "requires_review", False)),
            "user_override": bool(getattr(m, "user_override", False)),
            # Prefer live dest DDL; refuse sample-invented create-new collapse.
            "target_type": stamped,
            "source_type": src_type,
            "create_new": bool(getattr(m, "create_new", False)),
            "struct_policy": getattr(m, "struct_policy", None) or None,
            "struct_derived": bool(getattr(m, "struct_derived", False)),
            "struct_parent": getattr(m, "struct_parent", None) or None,
            "structural_class": getattr(m, "structural_class", None) or None,
            "child_table_spec": getattr(m, "child_table_spec", None) or None,
            "fidelity": getattr(m, "fidelity", None) or None,
            "type_narrowing": bool(getattr(m, "type_narrowing", False)),
            "risk_acknowledged": bool(getattr(m, "risk_acknowledged", False)),
            "intentional_omit": bool(getattr(m, "intentional_omit", False)),
            "risk_contract": getattr(m, "risk_contract", None),
            "code_crosswalk": getattr(m, "code_crosswalk", None),
            "code_crosswalk_system": getattr(m, "code_crosswalk_system", None),
            "rule_contracts": getattr(m, "rule_contracts", None),
            "business_rule": getattr(m, "business_rule", None),
        }

    def _dest_nullability(self) -> dict[str, bool]:
        from services.preflight_source_kind import destination_nullability
        return destination_nullability(getattr(self.plan.destination, "target_columns", None))

    def run_dry_run(self, sample_size: int = 1000) -> tuple[bool, list[str]]:
        if not self.sample_rows and getattr(self, "source_measured_empty", False):
            self._last_dry_run_meta = {
                "sample_rows_scanned": 0,
                "sample_rows_available": 0,
                "sample_cap": sample_size,
                "source_measured_empty": True,
            }
            return True, []
        if not self.sample_rows:
            reason = str(getattr(self, "sample_unavailable_reason", "") or "").strip()
            if reason:
                return False, [
                    f"No sample rows available for dry-run validation — {reason}. "
                    "Fix the source read, then re-run Validate."
                ]
            return False, [
                (
                    "No sample rows available for dry-run validation. "
                    "Re-run Source introspect so Datawrap can load a preview sample "
                    "from the source table (column metadata alone is not enough)."
                )
            ]

        scanned = self.sample_rows[:sample_size]
        headers = _sample_headers(self, scanned)
        # Nested lists/dicts from schemaless sources become valid JSON strings
        # instead of Python repr() artifacts; a key the document does not carry
        # stays the missing sentinel rather than an empty string the typed
        # transforms would reject as a cast failure.
        # A database NULL is None or the reader sentinel. Flattening it to ""
        # made G5 report EMPTY_VALUE_NOT_NULLABLE on nullable amount/ts.
        database_extract = _database_extract(self)
        rows = [
            project_row_cells(row, headers, preserve_sql_null=database_extract)
            for row in scanned
        ]
        self._last_dry_run_meta = {
            "sample_rows_scanned": len(scanned),
            "sample_rows_available": len(self.sample_rows),
            "sample_cap": sample_size,
        }
        column_types = {c.name: c.inferred_type for c in self.plan.source.columns}
        dest_types_by_name = {
            c.name: c.inferred_type for c in self.plan.destination.target_columns
        }
        mapping_dicts = [
            self._mapping_dict_for_probe(m, dest_types_by_name)
            for m in self.plan.mappings
        ]

        try:
            from services.transform_engine import dry_run_sample

            return dry_run_sample(
                headers=headers,
                sample_rows=rows,
                mappings=mapping_dicts,
                column_types=column_types,
                empty_cells_as_null=self.empty_cells_as_null,
                dest_nullability=self._dest_nullability(),
                database_extract=database_extract,
                file_row_quarantine=_source_is_record_batch(self),
            )
        except Exception as exc:
            _ps.logger.debug("dry-run sample failed: %s", exc, exc_info=exc)
            errors: list[str] = []
            from services.column_case import lookup_row_value

            known = {c.name for c in self.plan.source.columns}
            for i, row in enumerate(self.sample_rows[:sample_size]):
                for m in self.plan.mappings:
                    present = lookup_row_value(row, m.source, None) is not None or (
                        isinstance(row, dict) and m.source in row
                    )
                    known_hit = m.source in known or any(
                        str(n).casefold() == str(m.source).casefold() for n in known
                    )
                    if not present and known_hit:
                        errors.append(f"Row {i}: missing source column '{m.source}'")
                        if len(errors) >= 10:
                            return False, errors
            return len(errors) == 0, errors

    def coercion_report(self) -> dict[str, Any]:
        """Predict per-value write coercion for the plan against sample rows.

        Reuses the exact transform-resolution and coercion the write path uses so
        the preflight verdict matches the real write outcome. Cached so G3 and the
        API response layer share one computation.
        """
        cached = getattr(self, "_coercion_report_cache", None)
        if cached is not None:
            return cached
        try:
            from services.coercion_probe import analyze_coercion

            source_types = {c.name: c.inferred_type for c in self.plan.source.columns}
            dest_types = {
                c.name: c.inferred_type for c in self.plan.destination.target_columns
            }
            mapping_dicts = [
                self._mapping_dict_for_probe(m, dest_types)
                for m in self.plan.mappings
            ]
            from services.source_engine_scope import (
                active_source_engine,
                bind_source_engine,
            )

            src_engine = str(getattr(self.plan.source, "db_type", "") or "")
            # The lossy gate reads the source engine from this scope. Binding
            # it only around DDL stamping left PostgreSQL VARCHAR → SQL Server
            # NVARCHAR flagged as a fidelity collapse after the stamp window
            # had already closed (DEF-B-003).
            engine_scope = (
                bind_source_engine(src_engine)
                if src_engine and not active_source_engine()
                else nullcontext()
            )
            with engine_scope:
                report = analyze_coercion(
                    sample_rows=self.sample_rows,
                    mappings=mapping_dicts,
                    source_types=source_types,
                    dest_types=dest_types,
                    dest_db_type=self.plan.destination.db_type,
                    table_exists=getattr(self.plan.destination, "table_exists", None),
                    validation_mode=getattr(self.plan, "validation_mode", None) or "strict",
                    empty_cells_as_null=self.empty_cells_as_null,
                    dest_nullability=self._dest_nullability(),
                    database_extract=_database_extract(self),
                    file_row_quarantine=_source_is_record_batch(self),
                    proven_width_narrowings=self.proven_width_narrowings,
                )
            if isinstance(report, dict):
                from services.validation_coverage import stamp_validation_coverage

                report = {
                    **report,
                    "validation_coverage": stamp_validation_coverage(
                        layer="datatype",
                        rows_examined=len(self.sample_rows or []),
                        note=(
                            "Datatype / coercion classification only — "
                            "not sample population proof."
                        ),
                    ),
                }
        except Exception as exc:
            _ps.logger.warning(
                "coercion probe failed during preflight: %s", exc, exc_info=exc
            )
            report = {}
        self._coercion_report_cache = report
        return report

    def probe_unique_constraint(self, columns: list[str]) -> list[dict[str, Any]]:
        if not columns or not self.sample_rows:
            return []
        # Case-insensitive dest collations equate A/a — uniqueness must too
        # (MySQL utf8mb4_*_ci / SQL Server *_CI_AS / CITEXT), else Validate false-greens.
        # Every named column is part of the key. The first column alone false-fails
        # a composite (same id, different tenant) and false-greens the reverse.
        from services.type_system import (
            composite_unique_equality_key,
            unique_key_forces_casefold,
            unique_key_nulls_collide,
            unique_key_row_in_scope,
        )

        unique_keys = getattr(self.plan, "destination_unique_keys", None) or []
        specs: list[dict[str, Any]] = []
        for col in columns:
            source_col = col
            for m in self.plan.mappings:
                if m.target == col:
                    source_col = m.source
                    break
            dest_type = ""
            for c in getattr(self.plan.destination, "target_columns", None) or []:
                if getattr(c, "name", None) == col:
                    dest_type = str(getattr(c, "inferred_type", "") or "")
                    break
            specs.append(
                {
                    "col": col,
                    "source": source_col,
                    "dest_type": dest_type,
                    "casefold": unique_key_forces_casefold(
                        col, ddl_type=dest_type, unique_keys=unique_keys
                    ),
                    "nulls_collide": unique_key_nulls_collide(col, unique_keys=unique_keys),
                }
            )
        casefold = any(spec["casefold"] for spec in specs)
        nulls_collide = any(spec["nulls_collide"] for spec in specs)
        label = specs[0]["col"] if len(specs) == 1 else ", ".join(spec["col"] for spec in specs)
        dest_kind = ""
        try:
            dest_kind = str(getattr(self.plan.destination, "db_type", "") or "")
        except Exception:  # noqa: BLE001 — a missing plan field is an empty dest kind
            dest_kind = ""
        seen: dict[str, int] = {}
        examples: dict[str, str] = {}
        dupes: list[dict[str, Any]] = []
        keep_last = (
            str(getattr(self.plan, "in_file_dedupe", "") or "").strip().lower().replace("-", "_")
            == "keep_last"
        )
        source_keys = [str(spec["source"]) for spec in specs]
        sample_groups = _sample_file_groups(list(self.sample_rows))
        if keep_last and source_keys:
            from services.duplicate_policy import collapse_keep_last

            sample_groups = [collapse_keep_last(group, source_keys)[0] for group in sample_groups]
        for group in sample_groups:
            group_seen: dict[str, int] = {}
            for row in group:
                scope = dict(row)
                for spec in specs:
                    if spec["col"] not in scope and spec["source"] in scope:
                        scope[spec["col"]] = scope.get(spec["source"])
                if any(
                    not unique_key_row_in_scope(scope, spec["col"], unique_keys=unique_keys)
                    for spec in specs
                ):
                    continue
                parts: list[tuple[Any, str | None, bool, str | None]] = []
                displays: list[str] = []
                for spec in specs:
                    raw_cell = row.get(spec["source"], "")
                    raw = cell_to_string(raw_cell) if raw_cell is not None else ""
                    sentinel = "\x00NULL\x00" if spec["nulls_collide"] else None
                    parts.append(
                        (
                            None if raw_cell is None else raw,
                            spec["dest_type"],
                            bool(spec["casefold"]),
                            sentinel,
                        )
                    )
                    displays.append(raw if raw else "<NULL>")
                key = composite_unique_equality_key(parts, dest_kind=dest_kind)
                if not key:
                    continue
                group_seen[key] = group_seen.get(key, 0) + 1
                shown = displays[0] if len(displays) == 1 else ", ".join(displays)
                examples.setdefault(key, shown)
                seen[key] = group_seen[key]
            for key, count in group_seen.items():
                if count > 1 and key:
                    dupes.append(
                        {
                            "column": label,
                            "value": examples.get(key, key),
                            "count": count,
                            "collation_casefold": casefold,
                            "nulls_not_distinct": nulls_collide,
                        }
                    )
        return dupes[:5]

    def run_integrity_audit(self) -> dict[str, Any]:
        from services.data_integrity import run_integrity_audit as audit

        source_columns = [c.name for c in self.plan.source.columns]
        dest_types = {
            c.name: c.inferred_type for c in self.plan.destination.target_columns
        }
        mapping_dicts = [
            self._mapping_dict_for_probe(m, dest_types)
            for m in self.plan.mappings
        ]
        source_schemas = [
            {"name": c.name, "inferred_type": c.inferred_type, "samples": c.samples}
            for c in self.plan.source.columns
        ]
        target_schemas = [
            {"name": c.name, "inferred_type": c.inferred_type}
            for c in self.plan.destination.target_columns
        ]
        mode = getattr(self.plan, "validation_mode", "strict") or "strict"
        sync_mode = getattr(self.plan, "sync_mode", "") or ""
        report = audit(
            source_columns=source_columns,
            mappings=mapping_dicts,
            source_schemas=source_schemas,
            target_schemas=target_schemas,
            sample_rows=self.sample_rows,
            validation_mode=mode,
            destination_db_type=self.plan.destination.db_type,
            source_db_type=str(getattr(self.plan.source, "db_type", "") or ""),
            sync_mode=sync_mode,
            contract_primary_key=getattr(self.plan, "contract_primary_key", None)
            or None,
            destination_pk_columns=getattr(self.plan, "destination_pk_columns", None)
            or None,
            destination_unique_keys=getattr(self.plan, "destination_unique_keys", None)
            or None,
            source_duplicate_findings=self.source_duplicate_findings,
            source_duplicate_probe_ran=self.source_duplicate_probe_ran,
            source_duplicate_probe_pk=self.source_duplicate_probe_pk,
            source_duplicate_probe_status=self.source_duplicate_probe_status,
            source_duplicate_probe_message=self.source_duplicate_probe_message,
            source_duplicate_probe_expected=self.source_duplicate_probe_expected,
            source_null_key_rows=getattr(self, "source_null_key_rows", None),
            source_key_collisions=getattr(self, "source_key_collisions", None),
            dest_table_exists=getattr(self.plan.destination, "table_exists", None),
            empty_cells_as_null=self.empty_cells_as_null,
            dest_nullability=self._dest_nullability(),
            database_extract=_database_extract(self),
            file_row_quarantine=_source_is_record_batch(self),
            source_measured_empty=bool(getattr(self, "source_measured_empty", False)),
            stream_contracts=list(
                getattr(self.plan, "stream_contracts", None) or []
            ),
            in_file_dedupe=str(getattr(self.plan, "in_file_dedupe", "") or ""),
            duplicate_policy=str(getattr(self.plan, "duplicate_policy", "") or ""),
            proven_width_narrowings=self.proven_width_narrowings,
        )
        # Normalize/hybrid without a valid child_table_spec — fail closed in G9.
        try:
            from services.structural_array import array_strategy_gate_issues

            array_issues = array_strategy_gate_issues(
                mapping_dicts,
                known_source_columns=set(source_columns),
            )
        except Exception:
            array_issues = []
        if array_issues:
            issues = list(report.get("issues") or [])
            issues.extend(array_issues)
            report = {
                **report,
                "issues": issues,
                "blocks_transfer": True,
            }
        return report
