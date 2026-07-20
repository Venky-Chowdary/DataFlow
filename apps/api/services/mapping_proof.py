"""Mapping proof payload — honest operator evidence for any source×dest pair.

Single source of truth for Map/Validate "how this map works" surfaces.
Never invents 0.99 confidence or silent-no-loss claims.
"""

from __future__ import annotations

from typing import Any

from services.type_system import ddl_type, is_lossy_coercion, normalize_logical_type

# Transforms that mutate string content (fidelity risk even when intentional).
_MUTATING_TRANSFORMS = frozenset({
    "trim",
    "trim_id",
    "upper",
    "lower",
    "hash_pii",
    "mask_pii",
    "strip_controls",
    "normalize_unicode",
    "phone",
    "email",
    "url",
    "iban",
    "postal",
    "currency",
    "percentage",
    "base64",
})

_LOSSY_CAST_TRANSFORMS = frozenset({
    "decimal",
    "integer",
    "boolean",
    "date",
    "datetime",
    "time",
    "uuid",
    "json",
    "binary",
})

_PRESERVE_TRANSFORMS = frozenset({"none", "identity", ""})

IDENTITY_PASSTHROUGH_CONF_CAP = 0.93

QUARANTINE_POSTURE = (
    "Bad or unparseable rows are quarantined and surfaced for review — "
    "DataFlow does not silently drop them."
)

DELIVERY_SEMANTICS = (
    "Default delivery is at-least-once with upsert/idempotent write where supported; "
    "exactly-once is not claimed unless a route proves it."
)


def transform_fidelity(transform: str | None) -> str:
    """Classify transform risk: preserve | mutate | lossy_cast."""
    t = (transform or "none").strip().lower()
    if t in _PRESERVE_TRANSFORMS:
        return "preserve"
    if t in _LOSSY_CAST_TRANSFORMS:
        return "lossy_cast"
    if t in _MUTATING_TRANSFORMS:
        return "mutate"
    return "mutate"


def _quality_notes_from_reasoning(reasoning: str) -> list[str]:
    if not reasoning:
        return []
    notes: list[str] = []
    if "quality:" in reasoning.lower():
        # Take the quality segment(s) after the marker.
        for part in reasoning.split("·"):
            p = part.strip()
            if p.lower().startswith("quality:"):
                body = p[len("quality:") :].strip()
                notes.extend(n.strip() for n in body.split(",") if n.strip())
    return notes[:6]


def _pii_tags(mapping: dict, profile: dict | None = None) -> list[str]:
    tags: list[str] = []
    prof = profile or mapping.get("column_profile") or {}
    if mapping.get("is_pii") or prof.get("likely_email"):
        tags.append("email")
    if prof.get("likely_phone"):
        tags.append("phone")
    if prof.get("likely_uuid"):
        tags.append("uuid")
    name = f"{mapping.get('source', '')} {mapping.get('target', '')}".lower()
    if "ssn" in name or "social" in name:
        tags.append("ssn")
    # Dedupe preserve order
    seen: set[str] = set()
    out: list[str] = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


# CDC / Debezium metadata columns — not business payload; need explicit posture.
_CDC_META_COLUMNS = frozenset({
    "__op",
    "__deleted",
    "__source_ts_ms",
    "__source_ls",
    "__lsn",
    "__ts_ms",
    "_ab_cdc_cursor",
    "_ab_cdc_updated_at",
    "_ab_cdc_deleted_at",
})


def _is_bigint_unsigned(src_raw: str) -> bool:
    return "unsigned" in src_raw and ("bigint" in src_raw or "int8" in src_raw)


def _mapping_risks(
    mapping: dict,
    *,
    dest_mode: str,
    destination_db_type: str,
    sync_mode: str = "",
) -> list[dict[str, str]]:
    risks: list[dict[str, str]] = []
    transform = (mapping.get("transform") or "none").lower()
    fidelity = transform_fidelity(transform)
    src_type = str(mapping.get("source_type") or "VARCHAR")
    tgt_type = str(mapping.get("target_type") or mapping.get("dest_type") or src_type)
    src_name = str(mapping.get("source") or "").strip().lower()
    tgt_name = str(mapping.get("target") or "").strip().lower()

    if fidelity == "mutate":
        if transform in {"trim", "trim_id"}:
            risks.append({
                "code": "trim_mutates",
                "severity": "info",
                "message": "Trim strips leading/trailing whitespace — values change vs source.",
            })
        elif transform in {"hash_pii", "mask_pii"}:
            risks.append({
                "code": "pii_transform",
                "severity": "info",
                "message": f"Transform '{transform}' irreversibly alters values for governance.",
            })
        else:
            risks.append({
                "code": "value_mutate",
                "severity": "info",
                "message": f"Transform '{transform}' may change values before write.",
            })
    elif fidelity == "lossy_cast":
        risks.append({
            "code": "coerce_cast",
            "severity": "warn",
            "message": (
                f"Cast via '{transform}' may fail or coerce-to-null on bad samples; "
                "failures quarantine rather than silent drop."
            ),
        })

    if is_lossy_coercion(src_type, tgt_type):
        risks.append({
            "code": "type_narrowing",
            "severity": "warn",
            "message": (
                f"Type path {src_type} → {tgt_type} may lose precision or fail at write; "
                "review before production."
            ),
        })

    src_logical = normalize_logical_type(src_type)
    tgt_logical = normalize_logical_type(tgt_type)
    dest = (destination_db_type or "").lower()
    if dest in {"spark", "delta", "delta_lake", "databricks_sql", "unity_catalog"}:
        dest = "databricks"
    if dest in {"apache_iceberg", "iceberg_rest", "nessie"}:
        dest = "iceberg"
    src_raw = src_type.lower()
    lakehouse = dest in {"databricks", "iceberg", "snowflake", "bigquery", "redshift"}

    if src_logical in {"datetime", "timestamp"} and tgt_logical in {"datetime", "timestamp", "date"}:
        if dest in {
            "snowflake", "bigquery", "redshift", "postgresql", "postgres", "mysql",
            "databricks", "iceberg",
        } or "timestamp" in tgt_type.lower():
            risks.append({
                "code": "timezone_policy",
                "severity": "info",
                "message": (
                    "Temporal write follows destination timezone/bind policy "
                    f"({destination_db_type or 'dest'}); MySQL TIMESTAMP vs DATETIME "
                    "and Iceberg timestamptz vs timestamp without TZ differ — confirm expectations."
                ),
            })

    # Per-SKU fidelity: unsigned MySQL integers into warehouse/PG/lakehouse.
    if "unsigned" in src_raw and src_logical in {"integer", "decimal"}:
        native = ddl_type(dest, src_type) if dest else tgt_type
        native_l = (native or "").lower()
        tgt_l = (tgt_type or "").lower()
        widened = (
            src_logical == "decimal"
            or "decimal" in native_l
            or "numeric" in native_l
            or "number" in native_l
            or "bignumeric" in native_l
            or "decimal" in tgt_l
            or "numeric" in tgt_l
        )
        if _is_bigint_unsigned(src_raw):
            if widened:
                risks.append({
                    "code": "unsigned_bigint_widened",
                    "severity": "info",
                    "message": (
                        f"BIGINT UNSIGNED auto-widened to {native} on "
                        f"{destination_db_type or 'dest'} so values above signed 2^63-1 are preserved."
                    ),
                })
            else:
                risks.append({
                    "code": "unsigned_bigint_range",
                    "severity": "warn",
                    "message": (
                        f"MySQL BIGINT UNSIGNED can exceed signed 64-bit max. Destination "
                        f"{native} on {destination_db_type or 'dest'} may overflow — "
                        "prefer DECIMAL/NUMBER or quarantine out-of-range values."
                    ),
                })
        else:
            risks.append({
                "code": "unsigned_range",
                "severity": "warn" if not widened else "info",
                "message": (
                    f"Source appears UNSIGNED ({src_type}). Destination {native} must cover "
                    "the full unsigned range or values can overflow / quarantine."
                ),
            })

    # Semi-structured → document/warehouse/lakehouse variants.
    if src_logical in {"json", "array"} or "json" in src_raw or "variant" in tgt_type.lower() or "struct" in src_raw:
        if dest in {
            "snowflake", "bigquery", "mongodb", "postgresql", "postgres",
            "databricks", "iceberg", "redshift",
        }:
            risks.append({
                "code": "semi_structured",
                "severity": "info",
                "message": (
                    f"Semi-structured path {src_type} → {tgt_type} on {destination_db_type or 'dest'}: "
                    "nested shape is preserved as document/VARIANT/JSON/STRING — not flattened unless configured."
                ),
            })

    # Float → fixed decimal precision loss.
    if ("float" in src_raw or "double" in src_raw or "real" in src_raw) and tgt_logical in {"decimal", "integer"}:
        risks.append({
            "code": "float_to_decimal",
            "severity": "warn",
            "message": "Floating source into fixed decimal/integer can round or fail edge values.",
        })

    # Text / CLOB into VARCHAR warehouses — length risk.
    if src_logical in {"text", "string"} and dest in {
        "snowflake", "redshift", "bigquery", "mysql", "databricks", "iceberg",
    }:
        if "text" in src_raw or "clob" in src_raw or "long" in src_raw:
            risks.append({
                "code": "text_length",
                "severity": "info",
                "message": (
                    f"Long text source mapped to {tgt_type} on {destination_db_type or 'dest'}; "
                    "oversized values quarantine rather than silent truncate when policy is fail-fast."
                ),
            })

    # CDC metadata / tombstone columns — operator must understand upsert semantics.
    if src_name in _CDC_META_COLUMNS or tgt_name in _CDC_META_COLUMNS:
        risks.append({
            "code": "cdc_metadata_column",
            "severity": "info",
            "message": (
                f"Column `{mapping.get('source')}` looks like CDC metadata "
                "(__op / __deleted / LSN). It is change-stream machinery, not business payload — "
                "confirm destination consumers expect these fields."
            ),
        })
    if src_name in {"__deleted", "_ab_cdc_deleted_at"} or tgt_name in {"__deleted", "_ab_cdc_deleted_at"}:
        risks.append({
            "code": "cdc_tombstone",
            "severity": "warn",
            "message": (
                "Delete/tombstone signal present. Default delivery is at-least-once upsert; "
                "hard deletes require destination delete-by-PK support — not silent drop of history."
            ),
        })

    sync = (sync_mode or str(mapping.get("sync_mode") or "")).strip().lower()
    if sync in {"cdc", "incremental", "change_stream", "log"} and lakehouse:
        if not any(r["code"] == "cdc_at_least_once" for r in risks):
            risks.append({
                "code": "cdc_at_least_once",
                "severity": "info",
                "message": (
                    "CDC/incremental into lakehouse defaults to at-least-once upsert/MERGE; "
                    "exactly-once is not claimed unless the route proves idempotent keys + watermark handoff."
                ),
            })

    reasoning = str(mapping.get("reasoning") or "")
    if "unsigned" in reasoning.lower() or "out of range" in reasoning.lower():
        if not any(r["code"] in {"unsigned_range", "unsigned_bigint_range"} for r in risks):
            risks.append({
                "code": "unsigned_range",
                "severity": "warn",
                "message": "Numeric range / unsigned overflow risk flagged in mapping reason.",
            })

    notes = _quality_notes_from_reasoning(reasoning)
    for n in notes:
        low = n.lower()
        if "pii" in low or "email-like" in low or "mask" in low:
            risks.append({
                "code": "pii_governance",
                "severity": "info",
                "message": n,
            })
        elif "non-temporal" in low:
            risks.append({
                "code": "temporal_mismatch",
                "severity": "warn",
                "message": n,
            })
        elif "boolean" in low or "enum" in low:
            risks.append({
                "code": "enum_boolean",
                "severity": "warn",
                "message": n,
            })

    if dest_mode == "create_new":
        conf = float(mapping.get("confidence") or 0)
        if conf > IDENTITY_PASSTHROUGH_CONF_CAP + 0.001:
            # Honesty guard — should not happen after quality caps.
            risks.append({
                "code": "confidence_overclaim",
                "severity": "warn",
                "message": (
                    f"Create-new confidence {conf:.0%} exceeds expected ≤{IDENTITY_PASSTHROUGH_CONF_CAP:.0%}; "
                    "treat as review signal."
                ),
            })

    # Dedupe by code
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for r in risks:
        if r["code"] in seen:
            continue
        seen.add(r["code"])
        out.append(r)
    return out


def _schema_decision(mapping: dict, *, dest_mode: str, destination_db_type: str) -> str:
    tgt = mapping.get("target") or ""
    src_type = mapping.get("source_type") or "VARCHAR"
    tgt_type = mapping.get("target_type") or mapping.get("dest_type") or src_type
    dest = (destination_db_type or "").strip().lower()
    if dest_mode == "create_new":
        native = ddl_type(dest, src_type) if dest else tgt_type
        return f"CREATE column `{tgt}` as {native}"
    exists = mapping.get("exists_in_destination")
    if exists is False:
        native = ddl_type(dest, src_type) if dest else tgt_type
        return f"ADD new column `{tgt}` as {native} (not in introspected schema)"
    return f"MATCH existing `{tgt}` ({tgt_type})"


def _sample_preview(mapping: dict) -> list[str]:
    """Up to 4 distinct sample values for overlap / fidelity UI — never invents data."""
    raw: list[Any] = []
    for key in ("samples", "sample_values", "preview_values"):
        val = mapping.get(key)
        if isinstance(val, list):
            raw.extend(val)
    profile = mapping.get("column_profile") or {}
    for key in ("samples", "sample_values", "examples", "top_values"):
        val = profile.get(key)
        if isinstance(val, list):
            raw.extend(val)
        elif isinstance(val, dict):
            raw.extend(list(val.keys())[:6])
    out: list[str] = []
    seen: set[str] = set()
    for v in raw:
        if v is None:
            continue
        s = str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s[:80])
        if len(out) >= 4:
            break
    return out


def _evidence(mapping: dict) -> dict[str, Any]:
    profile = mapping.get("column_profile") or {}
    sample_n = mapping.get("sample_count")
    if sample_n is None:
        sample_n = profile.get("sample_count") or profile.get("non_empty_count")
    rate = mapping.get("sample_parse_rate")
    strategy = mapping.get("assignment_strategy") or (
        "identity_passthrough" if mapping.get("create_new") else "unknown"
    )
    src = str(mapping.get("source") or "").lower()
    tgt = str(mapping.get("target") or "").lower()
    name_match = src == tgt or src.replace("_", "") == tgt.replace("_", "")
    src_type = normalize_logical_type(mapping.get("source_type"))
    tgt_type = normalize_logical_type(mapping.get("target_type") or mapping.get("source_type"))
    type_aligned = src_type == tgt_type or not is_lossy_coercion(
        str(mapping.get("source_type") or ""),
        str(mapping.get("target_type") or mapping.get("source_type") or ""),
    )
    preview = _sample_preview(mapping)
    return {
        "strategy": strategy,
        "name_match": name_match,
        "type_aligned": type_aligned,
        "sample_n": int(sample_n) if sample_n is not None else (len(preview) or None),
        "sample_parse_rate": rate,
        "score_gap": mapping.get("score_gap"),
        "quality_notes": _quality_notes_from_reasoning(str(mapping.get("reasoning") or "")),
        "create_new": bool(mapping.get("create_new") or strategy == "identity_passthrough"),
        "sample_preview": preview,
    }


def confidence_breakdown(
    mapping: dict,
    evidence: dict[str, Any],
    display_conf: float,
) -> dict[str, float]:
    """Numeric evidence weights that sum to display_conf (honest decomposition)."""
    create_new = bool(evidence.get("create_new"))
    name_w = 0.22 if evidence.get("name_match") else 0.10
    type_w = 0.18 if evidence.get("type_aligned") else 0.08
    sample_n = evidence.get("sample_n")
    rate = evidence.get("sample_parse_rate")
    if sample_n and rate is not None:
        # Cap sample contribution; never invent high sample weight from n=1.
        n_factor = min(1.0, float(sample_n) / 12.0)
        sample_w = 0.18 * float(rate) * n_factor
    elif sample_n:
        sample_w = 0.06 * min(1.0, float(sample_n) / 12.0)
    else:
        sample_w = 0.0

    gap = evidence.get("score_gap")
    if create_new:
        strategy_w = 0.55  # "will CREATE" — not proven against live dest
    elif gap is not None:
        try:
            strategy_w = 0.35 + min(0.20, max(0.0, float(gap)) * 0.4)
        except (TypeError, ValueError):
            strategy_w = 0.40
    else:
        strategy_w = 0.40

    raw = {
        "strategy": strategy_w,
        "name": name_w,
        "type": type_w,
        "sample": sample_w,
    }
    total = sum(raw.values()) or 1.0
    # Scale to display_conf so UI bars match the shown percentage.
    scaled = {k: round(display_conf * (v / total), 3) for k, v in raw.items()}
    # Fix rounding drift on last key
    drift = round(display_conf - sum(scaled.values()), 3)
    if drift:
        scaled["strategy"] = round(scaled["strategy"] + drift, 3)
    return scaled


def build_mapping_proof(
    mappings: list[dict],
    *,
    target_columns: list[str] | None = None,
    destination_db_type: str = "",
    source_kind: str = "",
    dest_kind: str = "",
    sync_mode: str = "",
) -> dict[str, Any]:
    """Build universal mapping proof for Map/Validate UI — any connector pair."""
    has_targets = bool(target_columns)
    identity = any(
        m.get("assignment_strategy") == "identity_passthrough" or m.get("create_new")
        for m in mappings
    )
    dest_mode = "create_new" if (not has_targets or identity) else "match_existing"
    # If some targets exist but mappings invent new cols, still match_existing with adds.
    if has_targets and not identity:
        dest_mode = "match_existing"

    rows: list[dict[str, Any]] = []
    all_risks: list[dict[str, str]] = []
    confidences: list[float] = []

    # Detect CDC even when sync_mode unset (Debezium/Airbyte meta columns in map).
    inferred_cdc = sync_mode.strip().lower() in {"cdc", "incremental", "change_stream", "log"}
    if not inferred_cdc:
        for m in mappings:
            name = str(m.get("source") or m.get("target") or "").strip().lower()
            if name in _CDC_META_COLUMNS:
                inferred_cdc = True
                break
    effective_sync = sync_mode or ("cdc" if inferred_cdc else "")

    for m in mappings:
        conf = float(m.get("confidence") or 0)
        confidences.append(conf)
        transform = m.get("transform") or "none"
        fidelity = transform_fidelity(str(transform))
        risks = _mapping_risks(
            m,
            dest_mode=dest_mode,
            destination_db_type=destination_db_type,
            sync_mode=effective_sync,
        )
        all_risks.extend(risks)
        evidence = _evidence(m)
        # Cap display confidence honesty for create-new identity
        display_conf = conf
        if evidence.get("create_new"):
            display_conf = min(conf, IDENTITY_PASSTHROUGH_CONF_CAP)
        breakdown = confidence_breakdown(m, evidence, display_conf)
        evidence = {**evidence, "confidence_breakdown": breakdown}

        rows.append({
            "source": m.get("source"),
            "target": m.get("target"),
            "source_type": m.get("source_type") or "VARCHAR",
            "target_type": m.get("target_type") or m.get("dest_type") or m.get("source_type") or "VARCHAR",
            "dest_native_type": (
                ddl_type(destination_db_type, str(m.get("source_type") or "VARCHAR"))
                if destination_db_type else None
            ),
            "transform": transform,
            "transform_fidelity": fidelity,
            "confidence": round(display_conf, 3),
            "reasoning": m.get("reasoning") or "",
            "requires_review": bool(m.get("requires_review")),
            "evidence": evidence,
            "risks": risks,
            "pii": _pii_tags(m),
            "schema_decision": _schema_decision(
                m, dest_mode=dest_mode, destination_db_type=destination_db_type,
            ),
            "assignment_strategy": m.get("assignment_strategy"),
            "match_quality": (
                "exact_name" if evidence.get("name_match") and evidence.get("type_aligned")
                else "name_only" if evidence.get("name_match")
                else "type_only" if evidence.get("type_aligned")
                else "semantic"
            ),
            "sample_preview": evidence.get("sample_preview") or [],
        })

    # Unique global risks by code+message
    seen_g: set[str] = set()
    global_risks: list[dict[str, str]] = []
    for r in all_risks:
        key = f"{r['code']}|{r['message']}"
        if key in seen_g:
            continue
        seen_g.add(key)
        global_risks.append(r)

    # One global CDC posture line when change-stream columns or sync mode present.
    if inferred_cdc and not any(r["code"] == "cdc_delivery_posture" for r in global_risks):
        global_risks.insert(0, {
            "code": "cdc_delivery_posture",
            "severity": "info",
            "message": (
                "CDC/change-stream route: default is at-least-once upsert with watermark/LSN resume; "
                "exactly-once is not claimed. Deletes surface as tombstones when supported."
            ),
        })

    if inferred_cdc and destination_db_type:
        try:
            from services.cdc_effectively_once import (
                SINK_APPEND_ONLY,
                classify_sink_delivery,
            )

            sink = classify_sink_delivery(
                dest_type=destination_db_type,
                has_primary_key=True,
                write_mode="upsert",
            )
            if sink.get("class") == SINK_APPEND_ONLY and not any(
                r["code"] == "cdc_append_only_sink" for r in global_risks
            ):
                global_risks.insert(0, {
                    "code": "cdc_append_only_sink",
                    "severity": "error",
                    "message": (
                        f"Destination '{destination_db_type}' does not support upsert — "
                        "CDC redelivery will duplicate rows (not effectively-once). "
                        "Use a PK upsert sink or set allow_append_only=true."
                    ),
                })
        except Exception:
            pass

    avg_conf = round(sum(confidences) / len(confidences), 3) if confidences else 0.0
    max_conf = round(max(confidences), 3) if confidences else 0.0
    if dest_mode == "create_new":
        max_conf = min(max_conf, IDENTITY_PASSTHROUGH_CONF_CAP)

    create_ddl = sum(1 for r in rows if str(r.get("schema_decision", "")).startswith("CREATE") or str(r.get("schema_decision", "")).startswith("ADD"))
    match_n = len(rows) - create_ddl if dest_mode == "match_existing" else 0

    return {
        "dest_mode": dest_mode,
        "destination_db_type": (destination_db_type or "").lower(),
        "source_kind": source_kind or "",
        "dest_kind": dest_kind or "",
        "sync_mode": effective_sync or "",
        "quarantine_posture": QUARANTINE_POSTURE,
        "delivery_semantics": DELIVERY_SEMANTICS,
        "summary": {
            "mapped_count": len(rows),
            "create_ddl_count": create_ddl if dest_mode == "create_new" else create_ddl,
            "match_existing_count": match_n if dest_mode == "match_existing" else 0,
            "risk_count": len(global_risks),
            "review_count": sum(1 for r in rows if r.get("requires_review")),
            "avg_confidence": avg_conf if dest_mode != "create_new" else min(avg_conf, IDENTITY_PASSTHROUGH_CONF_CAP),
            "max_confidence": max_conf,
            "confidence_cap_create_new": IDENTITY_PASSTHROUGH_CONF_CAP,
            "cdc_detected": inferred_cdc,
        },
        "mappings": rows,
        "global_risks": global_risks[:40],
    }
